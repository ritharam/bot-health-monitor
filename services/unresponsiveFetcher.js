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
                        _2: { count: 1, type: "day", includeCurrent: true }
                    }
                }
            ]
        },
        limit: 1000,
        offset: 0,
        sourceType: "druid",
        dataSource: "messages",
        datasetType: "default"
    };

    try {
        const response = await fetch(url, {
            method: 'POST',
            headers: {
                'accept': 'application/json',
                'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
                'content-type': 'application/json',
                'x-api-key': apiKey,
                'origin': 'https://cloud.yellow.ai',
                'platform': 'cloud',
                'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                'priority': 'u=1, i'
            },
            body: JSON.stringify(payload)
        });

        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);

        const data = await response.json();
        const rawRecords = data.data?.records || data.data?.rows || [];

        if (rawRecords.length === 0) {
            console.log("No message records found for unresponsiveness check.");
            return;
        }

        // Group by sessionId
        const sessions = {};
        for (const r of rawRecords) {
            const sid = r.sessionId || r.sid || r.uid;
            if (!sid) continue;
            if (!sessions[sid]) sessions[sid] = [];
            sessions[sid].push(r);
        }

        const unresponsiveRecords = [];

        for (const sid in sessions) {
            // Sort by timestamp descending to find the LAST message
            sessions[sid].sort((a, b) => {
                const tsA = new Date(a.timestamp || a.__time).getTime();
                const tsB = new Date(b.timestamp || b.__time).getTime();
                return tsB - tsA;
            });

            const lastMessage = sessions[sid][0];
            // Check if lastMessageType is "user" (or similar case-insensitive)
            if (lastMessage.lastMessageType?.toLowerCase() === 'user' || lastMessage.senderType?.toLowerCase() === 'user') {
                unresponsiveRecords.push(lastMessage);
            }
        }

        const insert = db.prepare(`
            INSERT OR REPLACE INTO bot_unresponsive (botId, timestamp, sessionId, lastMessage, chatURL)
            VALUES (?, ?, ?, ?, ?)
        `);

        for (const record of unresponsiveRecords) {
            const tsVal = record.timestamp || record.__time || new Date().toISOString();
            const sessionVal = record.sessionId || record.sid || record.uid;
            const lastMsgText = record.message || record.lastMessageText || 'User Message';
            const chatUrlVal = record.chatURL || `https://cloud.yellow.ai/bot/${botId}/analytics/chat-history?sid=${sessionVal}`;

            insert.run(botId, tsVal, sessionVal, lastMsgText, chatUrlVal);
        }

        console.log(`Processed ${rawRecords.length} messages, found ${unresponsiveRecords.length} unresponsive sessions for ${botId}`);
    } catch (error) {
        console.error(`[${botId}] Error fetching Unresponsive metrics: ${error.message}`);
    }
}
