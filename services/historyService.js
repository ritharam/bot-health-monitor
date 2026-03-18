const MONITORED_BOTS = [
    { id: 'x1749095342235', apiKey: 'oC73e4WTensl0_l4O4L4cgXHCQ4y0dGaoxyEXVjr', name: 'Kent RO' },
    { id: 'x1674052117168', apiKey: '_-8bXdPQjVIxzhvRh1ihw1WEzItbzAnL_2o65QMz', name: 'Decathalon' },
    { id: 'x1752564834557', apiKey: '5BJIvSMO1WQr8MuaLXdvadBndCOnywO3dmjD5NqF', name: 'Swiggy' },
    { id: 'x1751972733090', apiKey: 'LkXSo4PeUuk8o0fXrsOwK8C9UWcxecO80MDWukxJ', name: 'JFL Dominos' }
];

async function yellowFetch(botId, apiKey, payload) {
    const url = `https://cloud.yellow.ai/api/insights/data-explorer?bot=${botId}`;
    let allRecords = [];
    let currentOffset = 0;
    const batchLimit = 100;
    const maxTotalRecords = 500000;

    while (allRecords.length < maxTotalRecords) {
        const batchPayload = { ...payload, offset: currentOffset, limit: batchLimit };
        let response;
        try {
            response = await fetch(url, {
                method: 'POST',
                headers: {
                    'accept': 'application/json',
                    'content-type': 'application/json',
                    'x-api-key': apiKey,
                    'origin': 'https://cloud.yellow.ai',
                    'platform': 'cloud',
                    'user-agent': 'Mozilla/5.0'
                },
                body: JSON.stringify(batchPayload)
            });
        } catch (e) {
            if (allRecords.length > 0) break;
            throw new Error(`Connection failed: ${e.message}`);
        }

        if (!response.ok) {
            if (allRecords.length > 0) break;
            const body = await response.text().catch(() => 'No body');
            throw new Error(`API ${response.status}: ${body.substring(0, 100)}`);
        }

        const data = await response.json();
        if (!data.success) {
            if (allRecords.length > 0) break;
            throw new Error(data.message || 'API request failed');
        }

        const records = data.data?.records || data.data?.rows || [];
        allRecords = allRecords.concat(records);
        console.log(`[Batch] [${botId}] Source: ${payload.dataSource}, Offset: ${currentOffset}, Fetched: ${records.length}, Total: ${allRecords.length}`);

        if (records.length < batchLimit) break;
        currentOffset += batchLimit;

        // Minor delay to be kind to the API if fetching many batches
        if (currentOffset % 20000 === 0) await new Promise(r => setTimeout(r, 200));
    }

    return allRecords;
}

function getIntervalFilter(startDate, endDate, days) {
    if (startDate || endDate) {
        let start = startDate ? new Date(startDate) : new Date(0);
        let end = endDate ? new Date(endDate) : new Date();

        if (endDate) {
            end.setHours(23, 59, 59, 999);
        }

        return {
            type: "interval",
            comparator: "between",
            operands: {
                _1: "__time",
                _2: [start.toISOString(), end.toISOString()]
            }
        };
    }

    return {
        type: "interval",
        comparator: "previous",
        operands: {
            _1: "__time",
            _2: { count: parseInt(days || 1), type: "day", includeCurrent: true }
        }
    };
}

export async function getConsolidatedHistory(db, filters) {
    const { botId, category, days = 1, startDate, endDate } = filters;
    const botsToFetch = botId ? MONITORED_BOTS.filter(b => b.id === botId) : MONITORED_BOTS;

    let results = [];
    const fetchers = [];

    // 1. API Failures (includes LLM and actual APIs now)
    if (!category || category === 'All' || category === 'API Failure') {
        botsToFetch.forEach(bot => {
            fetchers.push((async () => {
                const payload = {
                    type: "json",
                    timeZone: "Asia/Kolkata",
                    json: { filters: [getIntervalFilter(startDate, endDate, days)] },
                    limit: 10000,
                    sourceType: "druid",
                    dataSource: "apiEvents",
                    datasetType: "default"
                };
                const records = await yellowFetch(bot.id, bot.apiKey, payload);
                const filtered = records
                    .filter(r => {
                        const status = parseInt(r.statusCode || r.status_code || r.status || '0');
                        return status < 200 || status >= 300;
                    });
                console.log(`[History] [${bot.id}] API Failure: Raw ${records.length}, Filtered ${filtered.length} (Days: ${days})`);
                return filtered
                    .map(r => {
                        const apiName = r.name || r.apiName || r.api || r.api_name || r.path || 'Unknown API';
                        const status = r.statusCode || r.status_code || r.status || '0';
                        return {
                            botId: bot.id,
                            timestamp: r.__time || r.timestamp || new Date().toISOString(),
                            sessionId: r.sessionId || r.uid,
                            chatURL: r.chatURL || `https://cloud.yellow.ai/bot/${bot.id}/analytics/chat-history?sid=${r.sessionId || r.uid}`,
                            type: 'API Failure',
                            details: `Status: ${status} (${apiName})`,
                            apiName: apiName,
                            statusCode: status
                        };
                    });
            })());
        });
    }

    // 2. Messages-based (Downtime & Unresponsive)
    if (!category || category === 'All' || category === 'Bot Downtime' || category === 'Bot Unresponsive') {
        botsToFetch.forEach(bot => {
            fetchers.push((async () => {
                const payload = {
                    type: "json",
                    timeZone: "Asia/Kolkata",
                    json: { filters: [getIntervalFilter(startDate, endDate, days)] },
                    limit: 10000,
                    sourceType: "druid",
                    dataSource: "messages",
                    datasetType: "default"
                };
                const rawRecords = await yellowFetch(bot.id, bot.apiKey, payload);
                console.log(`[History] [${bot.id}] Messages: fetched ${rawRecords.length} records (Days: ${days})`);
                const sessions = {};
                rawRecords.forEach(r => {
                    const sid = r.sessionId || r.sid || r.uid;
                    if (sid) {
                        if (!sessions[sid]) sessions[sid] = [];
                        sessions[sid].push(r);
                    }
                });

                const alerts = [];
                for (const sid in sessions) {
                    const records = sessions[sid]
                        .filter(r => (r.messageType || r.senderType || '').toLowerCase() !== 'notification')
                        .sort((a, b) => new Date(a.timestamp || a.__time) - new Date(b.timestamp || b.__time));

                    if (records.length === 0) continue;

                    // 30-minute buffer: Skip if latest activity is within the last 30 minutes
                    const lastActivity = records[records.length - 1];
                    const lastActivityTime = new Date(lastActivity.timestamp || lastActivity.__time).getTime();
                    const bufferMs = 30 * 60 * 1000;
                    if (Date.now() - lastActivityTime < bufferMs) continue;

                    // A. Bot Downtime (Scenario 1: Delay)
                    if (!category || category === 'All' || category === 'Bot Downtime') {
                        for (let i = 0; i < records.length; i++) {
                            const curr = records[i];
                            const isCurrBot = (curr.messageType || curr.senderType)?.toLowerCase() === 'bot';
                            const isPrevUser = i > 0 && (records[i - 1].messageType || records[i - 1].senderType)?.toLowerCase() === 'user';

                            if (isCurrBot && isPrevUser) {
                                const next = records[i + 1];
                                const isNextBot = next && (next.messageType || next.senderType)?.toLowerCase() === 'bot';

                                if (!isNextBot) {
                                    const delaySecs = parseFloat(curr.session_sum || 0);
                                    if (delaySecs > 180) {
                                        alerts.push({
                                            botId: bot.id,
                                            timestamp: curr.__time || curr.timestamp || new Date().toISOString(),
                                            sessionId: sid,
                                            chatURL: curr.chatURL || `https://cloud.yellow.ai/bot/${bot.id}/analytics/chat-history?sid=${sid}`,
                                            type: 'Bot Downtime',
                                            details: `Delay: ${Math.floor(delaySecs)}s`,
                                            delaySeconds: Math.floor(delaySecs)
                                        });
                                    }
                                }
                            }
                        }
                    }

                    // B. Bot Unresponsive (Last message is User)
                    if (!category || category === 'All' || category === 'Bot Unresponsive') {
                        const last = records[records.length - 1];
                        if (last && (last.messageType || last.senderType)?.toLowerCase() === 'user') {
                            alerts.push({
                                botId: bot.id,
                                timestamp: last.__time || last.timestamp || new Date().toISOString(),
                                sessionId: sid,
                                chatURL: last.chatURL || `https://cloud.yellow.ai/bot/${bot.id}/analytics/chat-history?sid=${sid}`,
                                type: 'Bot Unresponsive',
                                details: last.message || last.text || last.msg || 'User message',
                                lastMessage: last.message || last.text || last.msg || 'User message'
                            });
                        }
                    }
                }
                return alerts;
            })());
        });
    }

    // 4. Knowledge Base
    if (!category || category === 'All' || category === 'Knowledge Base') {
        botsToFetch.forEach(bot => {
            fetchers.push((async () => {
                const kbFilter = getIntervalFilter(startDate, endDate, days);
                kbFilter.operands._1 = "timestamp";
                const payload = {
                    type: "json",
                    timeZone: "Asia/Kolkata",
                    json: { filters: [kbFilter] },
                    limit: 10000,
                    sourceType: "elasticsearch",
                    dataSource: "doccog-kb-analytics",
                    datasetType: "default"
                };
                const records = await yellowFetch(bot.id, bot.apiKey, payload);
                return records
                    .filter(r =>
                        r.was_answered === false || r.was_answered === "false" ||
                        r.status === "unanswered" || r.status === "failed" ||
                        (r.status && r.status.toLowerCase().includes('fail'))
                    )
                    .map(r => ({
                        botId: bot.id,
                        timestamp: r.timestamp || new Date().toISOString(),
                        sessionId: r.sessionId || r.uid,
                        chatURL: r.chatURL || r.chaturl || '',
                        type: 'Knowledge Base',
                        details: `Status: ${r.status || 'Unanswered'}`,
                        kbStatus: r.status || (r.was_answered === false || r.was_answered === "false" ? 'Unanswered' : 'Failed')
                    }));
            })());
        });
    }

    // 5. LLM Performance
    if (!category || category === 'All' || category === 'LLM Performance') {
        botsToFetch.forEach(bot => {
            fetchers.push((async () => {
                const llmFilter = getIntervalFilter(startDate, endDate, days);
                llmFilter.operands._1 = "timestamp";
                const payload = {
                    type: "json",
                    timeZone: "Asia/Kolkata",
                    json: { filters: [llmFilter] },
                    limit: 10000,
                    sourceType: "elasticsearch",
                    dataSource: "llm-api-usage-metrics",
                    datasetType: "default"
                };
                const records = await yellowFetch(bot.id, bot.apiKey, payload);
                return records
                    .filter(r => r.success === false || r.success === "false")
                    .map(r => ({
                        botId: bot.id,
                        timestamp: r.timestamp || new Date().toISOString(),
                        sessionId: r.sessionId || r.uid,
                        chatURL: r.chatURL || r.chaturl || `https://cloud.yellow.ai/bot/${bot.id}/analytics/chat-history?sid=${r.sessionId || r.uid}`,
                        type: 'LLM Performance',
                        details: `LLM Failure: ${r.error || 'Unknown error'}`,
                        llmError: r.error || 'Unknown error'
                    }));
            })());
        });
    }

    // Execute live fetches with Settled to capture errors without killing the whole result set
    const fetchResults = await Promise.allSettled(fetchers);
    fetchResults.forEach((res) => {
        if (res.status === 'fulfilled' && Array.isArray(res.value)) {
            results = results.concat(res.value);
        } else if (res.status === 'rejected') {
            console.error('History Fetcher Error:', res.reason);
            // Push a virtual record to show the error in the UI table
            results.push({
                botId: 'System',
                timestamp: new Date().toISOString(),
                sessionId: 'ERROR',
                chatURL: '#',
                type: 'API Failure',
                details: `Fetch Error: ${res.reason?.message || 'Unknown fetching error'}`
            });
        }
    });

    // 6. Custom Alerts (from Local SQLite)
    if (!category || category === 'All' || category === 'Custom Alerts') {
        let dateLimit;
        if (startDate) {
            dateLimit = new Date(startDate);
        } else {
            dateLimit = new Date();
            dateLimit.setDate(dateLimit.getDate() - parseInt(days));
        }
        const dateStr = dateLimit.toISOString();

        let sql = `
            SELECT botId, timestamp, 'Snapshot' as sessionId, '' as chatURL, 'Custom Alerts' as type, tableName || ' (' || recordsCount || ' records)' as details 
            FROM custom_archives 
            WHERE timestamp >= ?
            ${botId ? 'AND botId = ?' : ''}
            ${endDate ? 'AND timestamp <= ?' : ''}
        `;
        const snapshotParams = [dateStr];
        if (botId) snapshotParams.push(botId);
        if (endDate) snapshotParams.push(endDate);
        const manualSnapshots = db.prepare(sql).all(...snapshotParams);
        results = results.concat(manualSnapshots);
    }

    return results.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
}
