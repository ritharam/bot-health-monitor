export async function fetchAlerts(botId, apiKey, db) {
    console.log(`Fetching alerts for bot: ${botId}...`);
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
        dataSource: "apiEvents",
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

        if (!response.ok) {
            const errorText = await response.text();
            throw new Error(`HTTP error! status: ${response.status}, body: ${errorText}`);
        }

        const data = await response.json();
        const rawRecords = data.data?.records || data.data?.rows || [];
        const records = rawRecords.filter(r => {
            const status = r.statusCode || r.status_code || r.status;
            return status && status.toString() !== '200';
        });

        console.log(`API response - Success: ${data.success}, Message: ${data.message}, RawRecords: ${rawRecords.length}, Failures: ${records.length}`);

        const insert = db.prepare(`
            INSERT OR IGNORE INTO api_alerts (botId, chatURL, timestamp, sessionId, statusCode)
            VALUES (?, ?, ?, ?, ?)
        `);

        for (const record of records) {
            const chatURLVal = record.chatURL;
            const tsVal = record.__time || record.timestamp || new Date().toISOString();
            const sessionVal = record.sessionId || record.uid;
            const statusVal = parseInt(record.statusCode || record.status_code || record.status || '0');

            insert.run(botId, chatURLVal, tsVal, sessionVal, statusVal);
        }

        // Cleanup any existing 200 codes to ensure dashboard only shows failures
        db.prepare('DELETE FROM api_alerts WHERE statusCode = 200').run();

        console.log(`Saved ${records.length} failures for ${botId}`);
        db.prepare('DELETE FROM sync_errors WHERE botId = ?').run(botId);
    } catch (error) {
        console.error(`[${botId}] Error fetching alerts: ${error.message}`);
        db.prepare('INSERT OR REPLACE INTO sync_errors (botId, error) VALUES (?, ?)').run(botId, error.message);
    }
}
