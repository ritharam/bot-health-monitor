export async function fetchUnresponsiveMetrics(botId, apiKey, db) {
    console.log(`Fetching Bot Unresponsive metrics for bot: ${botId}...`);
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

        const unresponsiveSessions = [];
        for (const sid in sessions) {
            const records = sessions[sid]
                .filter(r => (r.messageType || r.senderType || '').toLowerCase() !== 'notification')
                .sort((a, b) => new Date(a.timestamp || a.__time) - new Date(b.timestamp || b.__time));

            if (records.length === 0) continue;

            const last = records[records.length - 1];
            if (last && (last.messageType || last.senderType)?.toLowerCase() === 'user') {
                const ageMins = (Date.now() - new Date(last.timestamp || last.__time).getTime()) / (1000 * 60);
                // 30-minute buffer: Skip if latest activity is within the last 30 minutes
                if (ageMins > 30) {
                    unresponsiveSessions.push(last);
                }
            }
        }

        db.prepare(`DELETE FROM bot_unresponsive WHERE botId = ?`).run(botId);
        const insert = db.prepare(`
            INSERT INTO bot_unresponsive (botId, timestamp, sessionId, lastMessage, chatURL)
            VALUES (?, ?, ?, ?, ?)
        `);

        for (const record of unresponsiveSessions) {
            const tsVal = record.timestamp || record.__time || new Date().toISOString();
            const sessionVal = record.sessionId || record.sid || record.uid;
            const msgContent = record.message || record.text || record.msg || 'User message';
            const chatUrlVal = record.chatURL || `https://cloud.yellow.ai/bot/${botId}/analytics/chat-history?sid=${sessionVal}`;

            insert.run(botId, tsVal, sessionVal, msgContent, chatUrlVal);
        }

        console.log(`[${botId}] Processed ${allRecords.length} records, found ${unresponsiveSessions.length} unresponsive.`);
        return unresponsiveSessions.length;
    } catch (error) {
        console.error(`[${botId}] Error fetching Unresponsive metrics: ${error.message}`);
        throw error;
    }
}
