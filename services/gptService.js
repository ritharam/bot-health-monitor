import fetch from 'node-fetch';
import dotenv from 'dotenv';
dotenv.config();

const OPENAI_API_TOKEN = process.env.OPENAI_API_TOKEN;

/**
 * Categorizes a list of items (tables or columns) into logical groups using GPT.
 * @param {string[]} items List of item names to categorize.
 * @param {string} context Description of what these items are (e.g., "Bot Tables" or "Columns in User Table").
 * @returns {Promise<Object>} Map of category names to lists of item names.
 */
export async function categorizeItems(items, context) {
    if (!items || items.length === 0) return {};
    if (!OPENAI_API_TOKEN) {
        console.warn('OPENAI_API_TOKEN not found in .env. Returning items uncategorized.');
        return { "Other": items };
    }

    const prompt = `
Categorize the following ${context} into three priority tiers:
1. "P0: Critical" - Essential system/user data or high-failure-risk items.
2. "P1: Important" - Secondary data or common operational items.
3. "P2: General" - Informational data or low-priority metrics.

Format your response as a JSON object where keys are "P0: Critical", "P1: Important", or "P2: General".
Values should be arrays of item names belonging to that tier.

Items:
${items.join(', ')}

Return ONLY valid JSON.
`;

    try {
        const response = await fetch('https://api.openai.com/v1/chat/completions', {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${OPENAI_API_TOKEN}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                model: 'gpt-4o',
                messages: [{ role: 'user', content: prompt }],
                response_format: { type: "json_object" }
            })
        });

        if (!response.ok) {
            const err = await response.text();
            throw new Error(`OpenAI API Error: ${response.status} - ${err}`);
        }

        const data = await response.json();
        const content = JSON.parse(data.choices[0].message.content);
        return content;
    } catch (error) {
        console.error('Error in gptService.categorizeItems:', error);
        return { "Other": items };
    }
}
