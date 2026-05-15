#!/usr/bin/env node
'use strict';

const { Kafka } = require('kafkajs');
const fetch = require('node-fetch');

// ─── Configuration ────────────────────────────────────────────────────────────
const KAFKA_BROKER    = 'kafka-server:9092';
const KAFKA_TOPIC     = 'wikimedia-events';
const KAFKA_GROUP_ID  = 'hbase-storage-group';
const HBASE_BASE_URL  = 'http://localhost:8080';
const HBASE_TABLE     = 'wikimedia_raw';

// ─── Kafka setup ──────────────────────────────────────────────────────────────
const kafka = new Kafka({
  clientId: 'hbase-writer',
  brokers: [KAFKA_BROKER],
});

const consumer = kafka.consumer({ groupId: KAFKA_GROUP_ID });

// ─── Base64 helpers ───────────────────────────────────────────────────────────
const b64 = (str) => Buffer.from(String(str)).toString('base64');

// ─── Build HBase REST payload ─────────────────────────────────────────────────
function buildHBasePayload(rowKey, fields) {
  const cells = Object.entries(fields).map(([qualifier, value]) => ({
    column: b64(`event:${qualifier}`),
    $: b64(value),
  }));

  return {
    Row: [
      {
        key: b64(rowKey),
        Cell: cells,
      },
    ],
  };
}

// ─── Write a single row to HBase via REST ─────────────────────────────────────
async function writeToHBase(rowKey, fields) {
  const url     = `${HBASE_BASE_URL}/${HBASE_TABLE}/${encodeURIComponent(rowKey)}`;
  const payload = buildHBasePayload(rowKey, fields);

  const response = await fetch(url, {
    method: 'PUT',
    headers: {
      'Content-Type': 'application/json',
      'Accept':       'application/json',
    },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`HTTP ${response.status}: ${body}`);
  }
}

// ─── Process a single Kafka message ──────────────────────────────────────────
async function processMessage(messageValue) {
  const event = JSON.parse(messageValue.toString());

  const id        = event.id        ?? '';
  const type      = event.type      ?? '';
  const title     = event.title     ?? '';
  const user      = event.user      ?? '';
  const bot       = String(event.bot ?? false);
  const namespace = event.namespace ?? 0;
  const timestamp = event.timestamp ?? Date.now();

  // Row key format: NS{namespace}_{timestamp}_{id}
  const rowKey = `NS${namespace}_${timestamp}_${id}`;

  const fields = { id, type, title, user, bot, namespace, timestamp };

  await writeToHBase(rowKey, fields);
  console.log(`[HBASE] ✅ Wrote: ${rowKey}`);
}

// ─── Main consumer loop ───────────────────────────────────────────────────────
async function run() {
  console.log('🔌 Connecting to Kafka broker...');
  await consumer.connect();
  console.log(`[KAFKA] ✅ Connected. Subscribing to topic: ${KAFKA_TOPIC}`);

  await consumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        await processMessage(message.value);
      } catch (err) {
        console.error(`❌ HBase write error: ${err.message}`);
      }
    },
  });
}

// ─── Graceful shutdown ────────────────────────────────────────────────────────
process.on('SIGINT', async () => {
  console.log('\n⚠️  SIGINT received — shutting down gracefully...');
  try {
    await consumer.disconnect();
    console.log('✅ Kafka consumer disconnected.');
  } catch (err) {
    console.error(`❌ Error during shutdown: ${err.message}`);
  }
  process.exit(0);
});

// ─── Entry point ──────────────────────────────────────────────────────────────
run().catch((err) => {
  console.error(`❌ Fatal error: ${err.message}`);
  process.exit(1);
});
