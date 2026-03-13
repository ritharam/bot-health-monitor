export async function fetchDowntimeMetrics(botId, apiKey, db) {
    console.log(`Fetching Bot Downtime metrics for bot: ${botId}...`);
    const url = `https://cloud.yellow.ai/api/insights/data-explorer?bot=${botId}&x-api-key=${apiKey}`;

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
                        _2: { count: 31, type: "day", includeCurrent: true }
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
            console.log("No message records found for downtime calculation.");
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

        const delayedRecords = [];
        const DELAY_THRESHOLD_MS = 5 * 60 * 1000; // 5 minutes

        for (const sid in sessions) {
            // Sort by timestamp
            sessions[sid].sort((a, b) => {
                const tsA = new Date(a.timestamp || a.__time).getTime();
                const tsB = new Date(b.timestamp || b.__time).getTime();
                return tsA - tsB;
            });

            const records = sessions[sid];
            for (let i = 1; i < records.length; i++) {
                const prevTs = new Date(records[i - 1].timestamp || records[i - 1].__time).getTime();
                const currTs = new Date(records[i].timestamp || records[i].__time).getTime();
                const diff = currTs - prevTs;

                if (diff > DELAY_THRESHOLD_MS) {
                    // This record is delayed
                    delayedRecords.push({
                        ...records[i],
                        has_delay: 1,
                        delay_seconds: Math.floor(diff / 1000)
                    });
                }
            }
        }

        const insert = db.prepare(`
            INSERT OR IGNORE INTO bot_downtime (botId, timestamp, sessionId, chaturl, has_delay, delay_seconds)
            VALUES (?, ?, ?, ?, ?, ?)
        `);

        for (const record of delayedRecords) {
            const tsVal = record.timestamp || record.__time || new Date().toISOString();
            const sessionVal = record.sessionId || record.sid || record.uid;
            const chatUrlVal = record.chaturl || `https://cloud.yellow.ai/bot/${botId}/chat-history?sid=${sessionVal}`;
            const hasDelay = 1;
            const delaySecs = record.delay_seconds || 0;

            insert.run(botId, tsVal, sessionVal, chatUrlVal, hasDelay, delaySecs);
        }

        console.log(`Processed ${rawRecords.length} messages, found ${delayedRecords.length} delays for ${botId}`);
    } catch (error) {
        console.error(`[${botId}] Error fetching Downtime metrics: ${error.message}`);
    }
}
