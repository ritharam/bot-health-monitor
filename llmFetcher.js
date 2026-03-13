export async function fetchLlmMetrics(botId, apiKey, db) {
    console.log(`Fetching LLM metrics for bot: ${botId}...`);
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
                        _1: "timestamp",
                        _2: { count: 31, type: "day", includeCurrent: true }
                    }
                }
            ]
        },
        limit: 1000,
        offset: 0,
        sourceType: "elasticsearch",
        dataSource: "llm-api-usage-metrics",
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
        const records = rawRecords.filter(r => r.success === false || r.success === "false");

        const insert = db.prepare(`
            INSERT OR IGNORE INTO llm_metrics (botId, timestamp, sessionId, success)
            VALUES (?, ?, ?, ?)
        `);

        for (const record of records) {
            const tsVal = record.timestamp || new Date().toISOString();
            const sessionVal = record.sessionId || record.uid;
            const successVal = 0; // These are failures, map to 0 for red status on dashboard
            insert.run(botId, tsVal, sessionVal, successVal);
        }

        console.log(`Saved ${records.length} LLM failures for ${botId}`);
    } catch (error) {
        console.error(`[${botId}] Error fetching LLM metrics: ${error.message}`);
    }
}
