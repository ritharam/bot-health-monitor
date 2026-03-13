import express from 'express';
import Database from 'better-sqlite3';
import dotenv from 'dotenv';
import cors from 'cors';

dotenv.config();

const app = express();
const port = 3001;

app.use(cors());
app.use(express.json());
app.use(express.static('public'));

// Database setup
const db = new Database('alerts.db');
db.exec(`
  CREATE TABLE IF NOT EXISTS api_alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    botId TEXT,
    chatURL TEXT,
    timestamp TEXT,
    sessionId TEXT,
    statusCode INTEGER,
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
    chaturl TEXT,
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
    chaturl TEXT,
    has_delay BOOLEAN,
    delay_seconds INTEGER,
    UNIQUE(sessionId, timestamp)
  )
`);

import { fetchAlerts } from './apiFetcher.js';
import { fetchLlmMetrics } from './llmFetcher.js';
import { fetchKbMetrics } from './kbFetcher.js';
import { fetchDowntimeMetrics } from './downtimeFetcher.js';

const MONITORED_BOTS = [
  { id: 'x1749095342235', apiKey: 'oC73e4WTensl0_l4O4L4cgXHCQ4y0dGaoxyEXVjr', name: 'Kent RO' },
  { id: 'x1674052117168', apiKey: '_-8bXdPQjVIxzhvRh1ihw1WEzItbzAnL_2o65QMz', name: 'Decathalon' },
  { id: 'x1752564834557', apiKey: 'AILuVtwLDn17gXlvjhcU1oW02GqEwYN2xy0T1CPg', name: 'Swiggy' }
];

console.log(`Bot monitoring initialized for: ${MONITORED_BOTS.map(b => b.name).join(', ')}`);

async function pollAllBots() {
  for (const bot of MONITORED_BOTS) {
    fetchAlerts(bot.id, bot.apiKey, db);
    fetchLlmMetrics(bot.id, bot.apiKey, db);
    fetchKbMetrics(bot.id, bot.apiKey, db);
    fetchDowntimeMetrics(bot.id, bot.apiKey, db);
  }
}

// Initial fetch
pollAllBots();

// Polling for all every 15 minutes
setInterval(pollAllBots, 15 * 60 * 1000);

// API endpoint
app.get('/api/alerts', (req, res) => {
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

app.get('/api/llm-metrics', (req, res) => {
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
  res.json(metrics);
});

app.get('/api/kb-metrics', (req, res) => {
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
  res.json(metrics);
});

app.get('/api/downtime-metrics', (req, res) => {
  const { botId } = req.query;
  let query = 'SELECT * FROM bot_downtime';
  let params = [];

  if (botId) {
    query += ' WHERE botId = ?';
    params.push(botId);
  }

  query += ' ORDER BY timestamp DESC LIMIT 1000';

  const metrics = db.prepare(query).all(...params);
  res.json(metrics);
});

app.get('/api/summary', (req, res) => {
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

    // Bot Downtime (Delays > 5m)
    let downtimeQuery = 'SELECT COUNT(*) as count FROM bot_downtime WHERE delay_seconds >= 300';
    if (botId) {
      downtimeQuery += ' AND botId = ?';
    }
    summary.uptime = db.prepare(downtimeQuery).get(...params).count;

    res.json(summary);
  } catch (error) {
    console.error('Error fetching summary:', error);
    res.status(500).json({ error: 'Failed to fetch summary' });
  }
});

app.listen(port, () => {
  console.log(`Server running at http://localhost:${port}`);
});
