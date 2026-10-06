#!/usr/bin/env node

import { once } from "node:events";
import net from "node:net";
import { setTimeout as sleep } from "node:timers/promises";
import { parseArgs } from "node:util";

const { values } = parseArgs({
  options: {
    host: { type: "string", default: "127.0.0.1" },
    port: { type: "string", default: "9601" },
    idle: { type: "string", default: "0,256,512,900,2000,5000" },
    active: { type: "string", default: "8" },
    size: { type: "string", default: "512" },
    seconds: { type: "string", default: "5" },
  },
});

// Target host.
const HOST = values.host;
// Target port.
const PORT = Number(values.port);
// Number of idle connections for each iteration.
const IDLE_COUNTS = values.idle.split(",").map(Number);
// Number of active connections on top of the idle connections.
const ACTIVE = Number(values.active);
// Request size.
const SIZE = Number(values.size);
// Duration of each iteration.
const SECONDS = Number(values.seconds);

const COLUMNS = [
  { label: "idle", width: 7 },
  { label: "req/s", width: 10 },
  { label: "p50 ms", width: 10 },
  { label: "p99 ms", width: 10 },
  { label: "max ms", width: 10 },
];

async function connect() {
  const socket = net.connect({ host: HOST, port: PORT, noDelay: true });

  await once(socket, "connect");

  socket.on("error", (err) => {
    console.log(err)
  });

  return socket;
}

/**
 * Open `target` connections that are not sending requests, but nevertheless need to
 * be checked for socket activity.
 */
async function idleUpTo(pool, target) {
  while (pool.length < target) {
    const batchSize = Math.min(64, target - pool.length);
    const pending = [];

    for (let i = 0; i < batchSize; i++) {
      pending.push(connect());
    }

    pool.push(...(await Promise.all(pending)));

    await sleep(10);
  }
}

function percentile(sortedValues, share) {
  const index = Math.ceil((share / 100) * sortedValues.length);

  return sortedValues[Math.min(index, sortedValues.length - 1)];
}

async function measure() {
  const payload = Buffer.alloc(SIZE, 0x61);
  const sockets = [];
  const latencies = [];
  let recording = false;
  let running = true;

  for (let i = 0; i < ACTIVE; i++) {
    const socket = await connect();
    let sentAt = 0;
    let pending = 0;

    socket.on("data", (chunk) => {
      pending += chunk.length;

      while (pending >= SIZE) {
        pending -= SIZE;

        if (recording) {
          latencies.push(performance.now() - sentAt);
        }

        if (running) {
          sentAt = performance.now();
          socket.write(payload);
        }
      }
    });

    sentAt = performance.now();
    socket.write(payload);
    sockets.push(socket);
  }

  await sleep(1000);

  recording = true;
  const startedAt = performance.now();
  await sleep(SECONDS * 1000);
  const elapsed = (performance.now() - startedAt) / 1000;
  recording = false;
  running = false;

  for (const socket of sockets) {
    socket.destroy();
  }

  if (latencies.length === 0) {
    return null;
  }

  latencies.sort((left, right) => left - right);

  return {
    rps: latencies.length / elapsed,
    p50: percentile(latencies, 50),
    p99: percentile(latencies, 99),
    max: latencies[latencies.length - 1],
  };
}

function formatRow(cells) {
  let row = "";

  for (let i = 0; i < COLUMNS.length; i++) {
    row += cells[i].padStart(COLUMNS[i].width);
  }

  return row;
}

console.log(
  `${HOST}:${PORT}  active=${ACTIVE} size=${SIZE}B window=${SECONDS}s`,
);
console.log(formatRow(COLUMNS.map((column) => column.label)));

const pool = [];

try {
  for (const idleCount of IDLE_COUNTS) {
    try {
      await idleUpTo(pool, idleCount);
    } catch (error) {
      console.log("");
      console.log(
        `Only ${pool.length} idle connections could be opened: ${error.code}.`,
      );

      if (error.code === "EMFILE") {
        console.log("Raise ulimit -n for this process.");
      } else {
        console.log("Check the output of the server.");
      }

      break;
    }

    await sleep(500);

    const result = await measure();

    if (result === null) {
      console.log("");
      console.log(
        `The server stopped answering at ${pool.length} idle connections.`,
      );

      break;
    }

    console.log(
      formatRow([
        String(pool.length),
        result.rps.toFixed(0),
        result.p50.toFixed(3),
        result.p99.toFixed(3),
        result.max.toFixed(3),
      ]),
    );
  }
} finally {
  for (const socket of pool) {
    socket.destroy();
  }
}
