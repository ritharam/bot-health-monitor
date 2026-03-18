export async function fetchDowntimeMetrics(botId, apiKey, db) {
    console.log(`Fetching Bot Downtime metrics for bot: ${botId}...`);
    const url = `https://cloud.yellow.ai/api/insights/data-explorer?bot=${botId}`;

    const payload = {
        type: "json",
        timeZone: "Asia/Kolkata",
        json: {
            filters: [
                {
                    type: "interval",
                    comparator: "previous",
                    operands: {
                        _1: "__time",
                        _2: { count: 24, type: "hour", includeCurrent: true }
                    }
                }
            ]
        },
        limit: 10000,
        sourceType: "druid",
        dataSource: "messages",
        datasetType: "default"
    };

    try {
        let allRecords = [];
        let currentOffset = 0;
        const batchLimit = 10000;
        const maxRecords = 300000;

        while (allRecords.length < maxRecords) {
            const batchPayload = { ...payload, offset: currentOffset, limit: batchLimit };
            const response = await fetch(url, {
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

            if (!response.ok) {
                if (allRecords.length > 0) break;
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            const data = await response.json();
            const records = data.data?.records || data.data?.rows || [];
            allRecords = allRecords.concat(records);

            if (records.length < batchLimit) break;
            currentOffset += batchLimit;
        }

        if (allRecords.length === 0) {
            console.log(`[${botId}] No message records found.`);
            return 0;
        }

        const sessions = {};
        allRecords.forEach(r => {
            const sid = r.sessionId || r.sid || r.uid;
            if (sid) {
                if (!sessions[sid]) sessions[sid] = [];
                sessions[sid].push(r);
            }
        });

        const delayedRecords = [];
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
            for (let i = 0; i < records.length; i++) {
                const curr = records[i];
                const isCurrBot = (curr.messageType || curr.senderType)?.toLowerCase() === 'bot';
                const isCurrUser = (curr.messageType || curr.senderType)?.toLowerCase() === 'user';
                const isPrevUser = i > 0 && (records[i - 1].messageType || records[i - 1].senderType)?.toLowerCase() === 'user';

                // Scenario 1: Delay
                if (isCurrBot && isPrevUser) {
                    const next = records[i + 1];
                    const isNextBot = next && (next.messageType || next.senderType)?.toLowerCase() === 'bot';

                    if (!isNextBot) {
                        const delaySecs = parseFloat(curr.session_sum || 0);
                        if (delaySecs > 180) {
                            delayedRecords.push({
                                botId: botId,
                                timestamp: curr.__time || curr.timestamp,
                                sessionId: sid,
                                chatURL: curr.chatURL || `https://cloud.yellow.ai/bot/${botId}/analytics/chat-history?sid=${sid}`,
                                has_delay: 1,
                                delay_seconds: Math.floor(delaySecs)
                            });
                        }
                    }
                }

                // Scenario 2: Unresponsive
                if (i === records.length - 1 && isCurrUser) {
                    const ageMins = (Date.now() - new Date(curr.timestamp || curr.__time).getTime()) / (1000 * 60);
                    if (ageMins > 5) {
                        delayedRecords.push({
                            botId: botId,
                            timestamp: curr.__time || curr.timestamp,
                            sessionId: sid,
                            chatURL: curr.chatURL || `https://cloud.yellow.ai/bot/${botId}/analytics/chat-history?sid=${sid}`,
                            has_delay: 1,
                            delay_seconds: 0
                        });
                    }
                }
            }
        }

        db.prepare(`DELETE FROM bot_downtime WHERE botId = ?`).run(botId);
        const insert = db.prepare(`
            INSERT INTO bot_downtime (botId, timestamp, sessionId, chatURL, has_delay, delay_seconds)
            VALUES (?, ?, ?, ?, ?, ?)
        `);

        for (const record of delayedRecords) {
            insert.run(record.botId, record.timestamp, record.sessionId, record.chatURL, record.has_delay, record.delay_seconds);
        }

        console.log(`[${botId}] Processed ${allRecords.length} records, found ${delayedRecords.length} delays.`);
        return delayedRecords.length;
    } catch (error) {
        console.error(`[${botId}] Error fetching Downtime metrics: ${error.message}`);
        throw error;
    }
}
