import express from 'express';
import Database from 'better-sqlite3';
import dotenv from 'dotenv';
import cors from 'cors';
import { fetchCustomData } from './services/customFetcher.js';
import { categorizeItems } from './services/gptService.js';
import { getConsolidatedHistory } from './services/historyService.js';

dotenv.config();

const app = express();
const port = 3001;

app.use(cors());
app.use(express.json());
app.use(express.static('public'));

// Database setup
// Use /tmp for writable database in Vercel environment
const dbPath = process.env.VERCEL ? '/tmp/alerts.db' : 'alerts.db';
const db = new Database(dbPath);
db.exec(`
  CREATE TABLE IF NOT EXISTS api_alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    botId TEXT,
    apiName TEXT,
    chatURL TEXT,
    timestamp TEXT,
    sessionId TEXT,
    statusCode INTEGER,
    UNIQUE(sessionId, timestamp)
  )
`);
try {
  db.prepare('ALTER TABLE api_alerts ADD COLUMN apiName TEXT').run();
} catch (e) {
  // Column might already exist
}

db.exec(`
  CREATE TABLE IF NOT EXISTS bot_unresponsive (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    botId TEXT,
    timestamp TEXT,
    sessionId TEXT,
    lastMessage TEXT,
    chatURL TEXT,
    UNIQUE(sessionId, timestamp)
  )
`);

db.exec(`
  CREATE TABLE IF NOT EXISTS sync_errors (
    botId TEXT PRIMARY KEY,
    error TEXT,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
  )
`);

db.exec(`
  CREATE TABLE IF NOT EXISTS kb_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    botId TEXT,
    timestamp TEXT,
    sessionId TEXT,
    chatURL TEXT,
    was_answered BOOLEAN,
    UNIQUE(sessionId, timestamp)
  )
`);

db.exec(`
  CREATE TABLE IF NOT EXISTS llm_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    botId TEXT,
    timestamp TEXT,
    sessionId TEXT,
    chatURL TEXT,
    success BOOLEAN,
    UNIQUE(sessionId, timestamp)
  )
`);

db.exec(`
  CREATE TABLE IF NOT EXISTS bot_downtime (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    botId TEXT,
    timestamp TEXT,
    sessionId TEXT,
    chatURL TEXT,
    has_delay BOOLEAN,
    delay_seconds INTEGER,
    UNIQUE(sessionId, timestamp)
  )
`);

db.exec(`
  CREATE TABLE IF NOT EXISTS sync_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    botId TEXT,
    fetcherType TEXT,
    status TEXT,
    itemsSynced INTEGER,
    error TEXT
  )
`);

db.exec(`
  CREATE TABLE IF NOT EXISTS custom_archives (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    botId TEXT,
    tableName TEXT,
    recordsCount INTEGER,
    recordsJson TEXT
  )
`);

import { fetchAlerts } from './services/apiFetcher.js';
import { fetchLlmMetrics } from './services/llmFetcher.js';
import { fetchKbMetrics } from './services/kbFetcher.js';
import { fetchDowntimeMetrics } from './services/downtimeFetcher.js';
import { fetchUnresponsiveMetrics } from './services/unresponsiveFetcher.js';

const MONITORED_BOTS = [
  { id: 'x1749095342235', apiKey: 'oC73e4WTensl0_l4O4L4cgXHCQ4y0dGaoxyEXVjr', name: 'Kent RO' },
  { id: 'x1674052117168', apiKey: '_-8bXdPQjVIxzhvRh1ihw1WEzItbzAnL_2o65QMz', name: 'Decathalon' },
  { id: 'x1752564834557', apiKey: '5BJIvSMO1WQr8MuaLXdvadBndCOnywO3dmjD5NqF', name: 'Swiggy' },
  { id: 'x1751972733090', apiKey: 'LkXSo4PeUuk8o0fXrsOwK8C9UWcxecO80MDWukxJ', name: 'JFL Dominos' }
];

console.log(`Bot monitoring initialized for: ${MONITORED_BOTS.map(b => b.name).join(', ')}`);

let lastSyncTimestamp = Date.now();
const SYNC_INTERVAL = 15 * 60 * 1000;

async function pollAllBots() {
  lastSyncTimestamp = Date.now();
  for (const bot of MONITORED_BOTS) {
    const results = [
      { type: 'APIs', promise: fetchAlerts(bot.id, bot.apiKey, db) },
      { type: 'LLM', promise: fetchLlmMetrics(bot.id, bot.apiKey, db) },
      { type: 'KB', promise: fetchKbMetrics(bot.id, bot.apiKey, db) },
      { type: 'Downtime', promise: fetchDowntimeMetrics(bot.id, bot.apiKey, db) },
      { type: 'Unresponsive', promise: fetchUnresponsiveMetrics(bot.id, bot.apiKey, db) }
    ];

    for (const res of results) {
      try {
        const count = await res.promise;
        db.prepare(`
          INSERT INTO sync_logs (botId, fetcherType, status, itemsSynced)
          VALUES (?, ?, ?, ?)
        `).run(bot.id, res.type, 'Success', count || 0);
      } catch (error) {
        db.prepare(`
          INSERT INTO sync_logs (botId, fetcherType, status, error)
          VALUES (?, ?, ?, ?)
        `).run(bot.id, res.type, 'Error', error.message);
      }
    }
  }
}

// Initial fetch (skip in Vercel to avoid timeout on startup)
if (!process.env.VERCEL) {
  pollAllBots();
  // Polling for all every 15 minutes
  setInterval(pollAllBots, SYNC_INTERVAL);
}

const apiRouter = express.Router();

// Manual sync endpoint for Vercel Cron
apiRouter.get('/sync', async (req, res) => {
  try {
    await pollAllBots();
    res.json({ success: true, message: 'Sync completed' });
  } catch (error) {
    console.error('Sync failed:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

apiRouter.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    environment: process.env.VERCEL ? 'vercel' : 'local',
    lastSyncTimestamp,
    syncInterval: SYNC_INTERVAL
  });
});

// API endpoint
apiRouter.get('/alerts', (req, res) => {
  const { botId } = req.query;
  let query = 'SELECT * FROM api_alerts';
  let params = [];

  if (botId) {
    query += ' WHERE botId = ?';
    params.push(botId);
  }

  query += ' ORDER BY timestamp DESC LIMIT 1000';

  const alerts = db.prepare(query).all(...params);
  const error = botId ? db.prepare('SELECT error FROM sync_errors WHERE botId = ?').get(botId) : null;

  res.json({ alerts, error: error?.error });
});

apiRouter.get('/llm-metrics', (req, res) => {
  const { botId } = req.query;

  // Hard cleanup: Remove any accidentally stored success records
  db.prepare('DELETE FROM llm_metrics WHERE success = 1').run();

  let query = 'SELECT * FROM llm_metrics WHERE success = 0';
  let params = [];

  if (botId) {
    query += ' AND botId = ?';
    params.push(botId);
  }

  query += ' ORDER BY timestamp DESC LIMIT 1000';

  const metrics = db.prepare(query).all(...params);
  const error = botId ? db.prepare('SELECT error FROM sync_errors WHERE botId = ?').get(botId) : null;
  res.json({ metrics, error: error?.error });
});

apiRouter.get('/kb-metrics', (req, res) => {
  const { botId } = req.query;

  // Hard cleanup: Remove any accidentally stored answered records
  db.prepare('DELETE FROM kb_metrics WHERE was_answered = 1').run();

  let query = 'SELECT * FROM kb_metrics WHERE was_answered = 0';
  let params = [];

  if (botId) {
    query += ' AND botId = ?';
    params.push(botId);
  }

  query += ' ORDER BY timestamp DESC LIMIT 1000';

  const metrics = db.prepare(query).all(...params);
  const error = botId ? db.prepare('SELECT error FROM sync_errors WHERE botId = ?').get(botId) : null;
  res.json({ metrics, error: error?.error });
});

apiRouter.get('/downtime-metrics', (req, res) => {
  const { botId } = req.query;
  let query = 'SELECT * FROM bot_downtime';
  let params = [];

  if (botId) {
    query += ' WHERE botId = ?';
    params.push(botId);
  }

  query += ' ORDER BY timestamp DESC LIMIT 1000';

  const metrics = db.prepare(query).all(...params);
  const error = botId ? db.prepare('SELECT error FROM sync_errors WHERE botId = ?').get(botId) : null;
  res.json({ metrics, error: error?.error });
});

apiRouter.get('/unresponsive-metrics', (req, res) => {
  const { botId } = req.query;
  // Use bot_downtime table (where delay_seconds=0) as the source for unresponsive sessions
  let query = 'SELECT * FROM bot_downtime WHERE delay_seconds = 0';
  let params = [];

  if (botId) {
    query += ' AND botId = ?';
    params.push(botId);
  }

  query += ' ORDER BY timestamp DESC LIMIT 1000';

  const records = db.prepare(query).all(...params);
  const error = botId ? db.prepare('SELECT error FROM sync_errors WHERE botId = ?').get(botId) : null;
  res.json({ records, error: error?.error });
});

apiRouter.get('/summary', (req, res) => {
  const { botId } = req.query;
  const summary = {};

  try {
    // API Failures
    let apiQuery = 'SELECT COUNT(*) as count FROM api_alerts WHERE statusCode != 200';
    let params = [];
    if (botId) {
      apiQuery += ' AND botId = ?';
      params.push(botId);
    }
    summary.apis = db.prepare(apiQuery).get(...params).count;

    // LLM Failures
    let llmQuery = 'SELECT COUNT(*) as count FROM llm_metrics WHERE success = 0';
    if (botId) {
      llmQuery += ' AND botId = ?';
    }
    summary.llm = db.prepare(llmQuery).get(...params).count;

    // KB Unanswered
    let kbQuery = 'SELECT COUNT(*) as count FROM kb_metrics WHERE was_answered = 0';
    if (botId) {
      kbQuery += ' AND botId = ?';
    }
    summary.kb = db.prepare(kbQuery).get(...params).count;

    // Bot Downtime (Delays > 3m) - Exclude unresponsive (0s) from this specific card
    let downtimeQuery = 'SELECT COUNT(*) as count FROM bot_downtime WHERE delay_seconds >= 180';
    if (botId) {
      downtimeQuery += ' AND botId = ?';
    }
    summary.uptime = db.prepare(downtimeQuery).get(...params).count;

    // Bot Unresponsive (Sessions with 0s delay in downtime table)
    let unresponsiveQuery = 'SELECT COUNT(*) as count FROM bot_downtime WHERE delay_seconds = 0';
    if (botId) {
      unresponsiveQuery += ' AND botId = ?';
    }
    summary.unresponsive = db.prepare(unresponsiveQuery).get(...params).count;

    res.json(summary);
  } catch (error) {
    console.error('Error fetching summary:', error);
    res.status(500).json({ error: 'Failed to fetch summary' });
  }
});

apiRouter.get('/schema', async (req, res) => {
    const { botId } = req.query;
    const bot = MONITORED_BOTS.find(b => b.id === botId);

    if (!bot) {
      return res.status(404).json({ error: 'Bot not found' });
    }

    try {
      const url = `https://cloud.yellow.ai/api/insights/data-explorer/schema?bot=${botId}&timeZone=Asia/Kolkata`;
      const response = await fetch(url, {
        headers: {
          'x-api-key': bot.apiKey,
          'accept': 'application/json',
          'platform': 'cloud',
          'origin': 'https://cloud.yellow.ai'
        }
      });

      if (!response.ok) {
        const errorText = await response.text();
        console.error(`--- SCHEMA API ERROR (${response.status}) ---`);
        console.error(errorText);
        console.error('-----------------------------');
        throw new Error(`Yellow.ai API error: ${response.status}`);
      }

      const data = await response.json();

      // Extract tables from data.data (as array), data.data.tables, or root.tables
      let allTables = [];
      if (Array.isArray(data.data)) {
        allTables = data.data;
      } else if (data.data?.tables) {
        allTables = data.data.tables;
      } else if (data.tables) {
        allTables = data.tables;
      }

      // Filter for only custom tables
      const customTables = allTables.filter(t => t.datasetType === 'custom');
      
      // Categorize tables using GPT
      const tableNames = customTables.map(t => t.tableName);
      const categorized = await categorizeItems(tableNames, `Custom database tables for bot ${bot.name} (${botId})`);
      
      // Map categories back to table objects
      customTables.forEach(table => {
        for (const [priority, tables] of Object.entries(categorized)) {
          if (tables.includes(table.tableName)) {
            table.priority = priority;
            break;
          }
        }
      });
      
      res.json({ tables: customTables, categorized });
    } catch (error) {
      console.error('Error fetching schema:', error);
      res.status(500).json({ error: 'Failed to fetch schema', message: error.message });
    }
});

apiRouter.get('/custom-data', async (req, res) => {
    const { botId, tableName } = req.query;
    const bot = MONITORED_BOTS.find(b => b.id === botId);

    if (!bot) {
      return res.status(404).json({ error: 'Bot not found' });
    }

    if (!tableName) {
      return res.status(400).json({ error: 'Table name is required' });
    }

    try {
      const records = await fetchCustomData(botId, bot.apiKey, tableName);
      res.json({ records });
    } catch (error) {
      console.error(`Error fetching custom data for ${tableName}:`, error);
      res.status(500).json({ error: 'Failed to fetch custom data', message: error.message });
    }
});

apiRouter.get('/categorize-attributes', async (req, res) => {
    const { botId, tableName, attributes } = req.query;
    if (!attributes) return res.status(400).json({ error: 'Attributes are required' });
    
    const attributeList = attributes.split(',');
    try {
      const categorized = await categorizeItems(attributeList, `Columns in table "${tableName}" for bot ${botId}`);
      res.json({ categorized });
    } catch (error) {
      console.error('Error in /api/categorize-attributes:', error);
      res.status(500).json({ error: 'Failed to categorize attributes' });
    }
});

apiRouter.get('/history', async (req, res) => {
    try {
      const results = await getConsolidatedHistory(db, req.query);
      res.json(results);
    } catch (error) {
      console.error('Error fetching unified history:', error);
      res.status(500).json({ error: 'Failed to fetch history' });
    }
});

apiRouter.post('/archive-custom', (req, res) => {
    const { botId, tableName, records } = req.body;
    if (!botId || !tableName || !records) {
      return res.status(400).json({ error: 'Incomplete data for archiving' });
    }
    try {
      db.prepare(`
        INSERT INTO custom_archives (botId, tableName, recordsCount, recordsJson)
        VALUES (?, ?, ?, ?)
      `).run(botId, tableName, records.length, JSON.stringify(records));
      res.json({ success: true });
    } catch (error) {
      console.error('Error archiving custom data:', error);
      res.status(500).json({ error: 'Failed to archive data' });
    }
});

apiRouter.get('/archive-detail/:id', (req, res) => {
    const { id } = req.params;
    const archive = db.prepare('SELECT * FROM custom_archives WHERE id = ?').get(id);
    if (!archive) return res.status(404).json({ error: 'Archive not found' });
    res.json({ ...archive, recordsJson: JSON.parse(archive.recordsJson) });
});

app.use('/api', apiRouter);
app.use('/', apiRouter);

if (!process.env.VERCEL) {
  app.listen(port, () => {
    console.log(`Server running at http://localhost:${port}`);
  });
}

// 404 Handler
app.use((req, res) => {
  console.warn(`[404] ${req.method} ${req.url}`);
  res.status(404).json({ error: `Path ${req.url} not found` });
});

// Global Error Handler
app.use((err, req, res, next) => {
  console.error('[SERVER ERROR]', err);
  res.status(500).json({
    error: 'Internal Server Error',
    message: err.message,
    path: req.url
  });
});

export default app;
