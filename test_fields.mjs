import fetch from 'node-fetch';

async function checkFields() {
    const botId = 'x1749095342235';
    const apiKey = 'oC73e4WTensl0_l4O4L4cgXHCQ4y0dGaoxyEXVjr';
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
        limit: 1,
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
                'content-type': 'application/json',
                'x-api-key': apiKey
            },
            body: JSON.stringify(payload)
        });
        const data = await response.json();
        const records = data.data?.records || data.data?.rows || [];
        if (records.length > 0) {
            console.log('Record Keys:', Object.keys(records[0]).join(', '));
            console.log('Sample Record:', JSON.stringify(records[0], null, 2));
        } else {
            console.log('No records found');
        }
    } catch (e) {
        console.error(e);
    }
}

checkFields();
