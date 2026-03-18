import fetch from 'node-fetch';

export async function fetchCustomData(botId, apiKey, tableName) {
    console.log(`--- CUSTOM DATA REQUEST ---`);
    console.log(`Bot ID: ${botId}`);
    console.log(`Table: ${tableName}`);

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
                        _1: "created_at",
                        _2: { count: 31, type: "day", includeCurrent: true }
                    }
                }
            ]
        },
        limit: 100,
        offset: 0,
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
                'platform': 'cloud'
            },
            body: JSON.stringify(payload)
        });

        if (!response.ok) {
            const errorText = await response.text();
            console.error(`--- CUSTOM FETCH ERROR (${response.status}) ---`);
            console.error(`Table: ${tableName}`);
            console.error(`Payload: ${JSON.stringify(payload)}`);
            console.error(`Error Body: ${errorText}`);
            console.error('---------------------------------');
            throw new Error(`Yellow.ai API returned ${response.status}`);
        }

        const data = await response.json();

        // Return data.data.records or data.data.rows safely
        const records = data.data?.records || data.data?.rows || [];
        console.log(`Successfully fetched ${records.length} records for ${tableName}`);
        return records;

    } catch (error) {
        console.error(`Error in fetchCustomData for ${tableName}:`, error.message);
        throw error;
    }
}
