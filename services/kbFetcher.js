export async function fetchKbMetrics(botId, apiKey, db) {
    console.log(`Fetching KB metrics for bot: ${botId}...`);
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
                        _1: "timestamp",
                        _2: { count: 24, type: "hour", includeCurrent: true }
                    }
                }
            ]
        },
        limit: 10000,
        sourceType: "elasticsearch",
        dataSource: "doccog-kb-analytics",
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

        // Logic matched from historyService.js
        const failures = allRecords.filter(r => 
            r.was_answered === false || r.was_answered === "false" || 
            r.status === "unanswered" || r.status === "failed" ||
            (r.status && r.status.toLowerCase().includes('fail'))
        );

        const insert = db.prepare(`
            INSERT OR IGNORE INTO kb_metrics (botId, timestamp, sessionId, chatURL, was_answered)
            VALUES (?, ?, ?, ?, ?)
        `);

        for (const record of failures) {
            const tsVal = record.timestamp || new Date().toISOString();
            const sessionVal = record.sessionId || record.uid;
            const chatUrlVal = record.chatURL || record.chaturl || '';
            const answeredVal = 0; // These are failures/unanswered

            insert.run(botId, tsVal, sessionVal, chatUrlVal, answeredVal);
        }

        console.log(`[${botId}] Fetched ${allRecords.length} KB records, found ${failures.length} failures.`);
        return failures.length;
    } catch (error) {
        console.error(`[${botId}] Error fetching KB metrics: ${error.message}`);
        throw error;
    }
}
