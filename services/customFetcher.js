// Standardizing to native fetch for better production compatibility (Node 18+)

export async function fetchCustomData(botId, apiKey, tableName) {
    console.log(`--- CUSTOM DATA REQUEST (24h) ---`);
    console.log(`Bot ID: ${botId}`);
    console.log(`Table: ${tableName}`);

    const url = `https://cloud.yellow.ai/api/insights/data-explorer?bot=${botId}`;
    let allRecords = [];
    let currentOffset = 0;
    const batchLimit = 10000;
    const maxTotalRecords = process.env.VERCEL ? 5000 : 300000;

    while (allRecords.length < maxTotalRecords) {
        const payload = {
            type: "json",
            timeZone: "Asia/Kolkata",
            json: {
                filters: [
                    {
                        type: "interval",
                        comparator: "previous",
                        operands: {
                            _1: "created_at",
                            _2: { count: 1, type: "day", includeCurrent: true }
                        }
                    }
                ]
            },
            limit: batchLimit,
            offset: currentOffset,
            sourceType: "sql",
            dataSource: tableName,
            datasetType: "custom"
        };

        try {
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
                body: JSON.stringify(payload)
            });

            if (!response.ok) {
                const errorText = await response.text();
                console.error(`--- CUSTOM FETCH ERROR (${response.status}) ---`);
                console.error(`Table: ${tableName}, Offset: ${currentOffset}, Body: ${errorText.substring(0, 200)}`);
                if (allRecords.length > 0) break;
                throw new Error(`Yellow.ai API returned ${response.status}: ${errorText.substring(0, 100)}`);
            }

            const data = await response.json();
            const records = data.data?.records || data.data?.rows || [];
            allRecords = allRecords.concat(records);
            
            console.log(`[Custom Batch] ${tableName}: Offset ${currentOffset}, Fetched ${records.length}, Total ${allRecords.length}`);

            if (records.length < batchLimit) break;
            currentOffset += batchLimit;

        } catch (error) {
            console.error(`Error in fetchCustomData for ${tableName}:`, error.message);
            if (allRecords.length > 0) break;
            throw error;
        }
    }

    return allRecords;
}
