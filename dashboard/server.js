#!/usr/bin/env node
'use strict';

const express  = require('express');
const fetch    = require('node-fetch');
const cors     = require('cors');
const path     = require('path');

// ─── Configuration ────────────────────────────────────────────────────────────
const PORT           = 3000;
const HBASE_BASE_URL = 'http://localhost:8080';

const app = express();
app.use(cors());
app.use(express.json());

// ─── Serve static frontend files ──────────────────────────────────────────────
app.use(express.static(path.join(__dirname, 'public')));

// ─── HBase decode helper ──────────────────────────────────────────────────────
function decode(b64str) {
  return Buffer.from(b64str, 'base64').toString('utf-8');
}

// ─── Parse raw HBase REST response into plain JS objects ─────────────────────
function parseHBaseRows(rawRows) {
  if (!rawRows || !Array.isArray(rawRows)) return [];

  return rawRows.map((row) => {
    const obj = { rowKey: decode(row.key) };
    if (Array.isArray(row.Cell)) {
      row.Cell.forEach((cell) => {
        const qualifier = decode(cell.column).split(':')[1];
        obj[qualifier]  = decode(cell.$);
      });
    }
    return obj;
  });
}

// ─── GET /api/namespace-counts ────────────────────────────────────────────────
// Returns the latest change_count per namespace from wikimedia_counts table
app.get('/api/namespace-counts', async (req, res) => {
  try {
    const response = await fetch(`${HBASE_BASE_URL}/wikimedia_counts/*`, {
      headers: { Accept: 'application/json' },
    });

    if (!response.ok) {
      throw new Error(`HBase responded with HTTP ${response.status}`);
    }

    const json = await response.json();
    const rows = parseHBaseRows(json.Row);

    // Keep only the latest record per namespace (highest window_start)
    const latestByNamespace = {};
    for (const row of rows) {
      const ns = row.namespace;
      if (
        !latestByNamespace[ns] ||
        row.window_start > latestByNamespace[ns].window_start
      ) {
        latestByNamespace[ns] = row;
      }
    }

    const counts = Object.values(latestByNamespace).map((r) => ({
      namespace:    r.namespace,
      change_count: Number(r.change_count ?? 0),
      window_start: r.window_start,
      window_end:   r.window_end,
    }));

    res.json({ counts });
  } catch (err) {
    console.error(`❌ /api/namespace-counts error: ${err.message}`);
    res.status(500).json({ error: err.message });
  }
});

// ─── GET /api/recent ──────────────────────────────────────────────────────────
// Returns the 50 most recent raw events from wikimedia_raw table
app.get('/api/recent', async (req, res) => {
  try {
    const response = await fetch(`${HBASE_BASE_URL}/wikimedia_raw/*?limit=50`, {
      headers: { Accept: 'application/json' },
    });

    if (!response.ok) {
      throw new Error(`HBase responded with HTTP ${response.status}`);
    }

    const json   = await response.json();
    const events = parseHBaseRows(json.Row)
      .sort((a, b) => Number(b.timestamp) - Number(a.timestamp))
      .slice(0, 50);

    res.json({ events });
  } catch (err) {
    console.error(`❌ /api/recent error: ${err.message}`);
    res.status(500).json({ error: err.message });
  }
});

// ─── GET /api/stats ───────────────────────────────────────────────────────────
// Aggregates totals, bot vs human breakdown from both HBase tables in parallel
app.get('/api/stats', async (req, res) => {
  try {
    const [countsResp, rawResp] = await Promise.all([
      fetch(`${HBASE_BASE_URL}/wikimedia_counts/*`, {
        headers: { Accept: 'application/json' },
      }),
      fetch(`${HBASE_BASE_URL}/wikimedia_raw/*?limit=100`, {
        headers: { Accept: 'application/json' },
      }),
    ]);

    if (!countsResp.ok) {
      throw new Error(`wikimedia_counts fetch failed: HTTP ${countsResp.status}`);
    }
    if (!rawResp.ok) {
      throw new Error(`wikimedia_raw fetch failed: HTTP ${rawResp.status}`);
    }

    const [countsJson, rawJson] = await Promise.all([
      countsResp.json(),
      rawResp.json(),
    ]);

    const countRows = parseHBaseRows(countsJson.Row);
    const rawRows   = parseHBaseRows(rawJson.Row);

    const total_edits  = countRows.reduce((sum, r) => sum + Number(r.change_count ?? 0), 0);
    const total_events = rawRows.length;
    const bot_edits    = rawRows.filter((r) => r.bot === 'true').length;
    const human_edits  = total_events - bot_edits;

    res.json({ total_edits, total_events, bot_edits, human_edits });
  } catch (err) {
    console.error(`❌ /api/stats error: ${err.message}`);
    res.status(500).json({ error: err.message });
  }
});

// ─── Graceful shutdown ────────────────────────────────────────────────────────
process.on('SIGINT', () => {
  console.log('\n⚠️  SIGINT received — shutting down dashboard server...');
  process.exit(0);
});

// ─── Start server ─────────────────────────────────────────────────────────────
app.listen(PORT, () => {
  console.log(`🚀 Dashboard running at http://localhost:${PORT}`);
});
