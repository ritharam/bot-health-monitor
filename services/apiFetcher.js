export async function fetchAlerts(botId, apiKey, db) {
    console.log(`Fetching alerts for bot: ${botId}...`);
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
        dataSource: "apiEvents",
        datasetType: "default"
    };

    try {
        let allRecords = [];
        let currentOffset = 0;
        const batchLimit = 10000;
        const maxRecords = 300000; // Cap at 300k for regular sync to prevent timeouts

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

        // Filtering logic identical to historyService.js
        const failures = allRecords.filter(r => {
            const status = parseInt(r.statusCode || r.status_code || r.status || '0');
            return status < 200 || status >= 300;
        });

        console.log(`[${botId}] Fetched ${allRecords.length} API records, found ${failures.length} failures.`);

        const insert = db.prepare(`
            INSERT OR REPLACE INTO api_alerts (botId, apiName, chatURL, timestamp, sessionId, statusCode)
            VALUES (?, ?, ?, ?, ?, ?)
        `);

        for (const record of failures) {
            const apiNameVal = record.name || record.apiName || record.api || record.api_name || record.path || 'Unknown API';
            const chatURLVal = record.chatURL || `https://cloud.yellow.ai/bot/${botId}/analytics/chat-history?sid=${record.sessionId || record.uid}`;
            const tsVal = record.__time || record.timestamp || new Date().toISOString();
            const sessionVal = record.sessionId || record.uid;
            const statusVal = parseInt(record.statusCode || record.status_code || record.status || '0');

            insert.run(botId, apiNameVal, chatURLVal, tsVal, sessionVal, statusVal);
        }

        db.prepare('DELETE FROM sync_errors WHERE botId = ?').run(botId);
        return failures.length;
    } catch (error) {
        console.error(`[${botId}] Error fetching alerts: ${error.message}`);
        db.prepare('INSERT OR REPLACE INTO sync_errors (botId, error) VALUES (?, ?)').run(botId, error.message);
        throw error;
    }
}
