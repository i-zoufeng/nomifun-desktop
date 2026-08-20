/**
 * @license
 * Copyright 2025-2026 NomiFun (nomifun.com)
 * SPDX-License-Identifier: Apache-2.0
 */

import { describe, expect, test } from 'bun:test';
import { InvalidEntityIdError, parseCrawlJobId } from '@/common/types/ids';
import { crawl, crawlEvents } from './ipcBridge';

const JOB_ID = '0190f5fe-7c00-7a00-8000-000000000030';
const TASK_ID = '0190f5fe-7c00-7a00-8000-000000000031';
const KNOWLEDGE_BASE_ID = '0190f5fe-7c00-7a00-8000-000000000032';
const realFetch = globalThis.fetch;

const rawJob = (job_id: unknown = JOB_ID, knowledge_base_id: unknown = KNOWLEDGE_BASE_ID) => ({
  job_id,
  name: 'Boundary test',
  seeds: ['https://example.com'],
  scope: { same_site: true, path_prefixes: [], allow: [], deny: [] },
  max_depth: 1,
  max_urls: 10,
  render_mode: 'http',
  concurrency: 1,
  per_host_concurrency: 1,
  delay_ms: 0,
  respect_robots: true,
  sink: { knowledge_base_id },
  status: 'draft',
  created_at: 1,
  progress: { pending: 0, in_progress: 0, done: 0, failed: 0, skipped: 0 },
});

const rawTask = (task_id: unknown = TASK_ID) => ({
  task_id,
  url: 'https://example.com',
  host: 'example.com',
  depth: 0,
  status: 'pending',
  attempt_count: 0,
});

function respondWith(data: unknown): void {
  globalThis.fetch = (() =>
    Promise.resolve(
      new Response(JSON.stringify({ success: true, data }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      })
    )) as unknown as typeof fetch;
}

async function expectInvalidEntityId(action: () => Promise<unknown>): Promise<void> {
  let error: unknown;
  try {
    await action();
  } catch (caught) {
    error = caught;
  }
  expect(error instanceof InvalidEntityIdError).toBe(true);
}

describe('crawl REST response ID contract', () => {
  test('maps canonical job, task, and sink IDs through the strict UUIDv7 boundary', async () => {
    try {
      respondWith([rawJob()]);
      const jobs = await crawl.listJobs.invoke();
      expect(jobs[0]?.job_id).toBe(JOB_ID);
      expect(jobs[0]?.sink.knowledge_base_id).toBe(KNOWLEDGE_BASE_ID);

      respondWith([rawTask()]);
      const tasks = await crawl.listTasks.invoke({ job_id: parseCrawlJobId(JOB_ID) });
      expect(tasks[0]?.task_id).toBe(TASK_ID);
    } finally {
      globalThis.fetch = realFetch;
    }
  });

  test('rejects malformed IDs instead of exposing them as branded business IDs', async () => {
    try {
      for (const invalidId of [30, `crawl_${JOB_ID}`, '550e8400-e29b-41d4-a716-446655440000']) {
        respondWith([rawJob(invalidId)]);
        await expectInvalidEntityId(() => crawl.listJobs.invoke());
      }

      respondWith([rawJob(JOB_ID, `kb_${KNOWLEDGE_BASE_ID}`)]);
      await expectInvalidEntityId(() => crawl.listJobs.invoke());

      for (const invalidId of [31, `task_${TASK_ID}`, '550e8400-e29b-41d4-a716-446655440000']) {
        respondWith([rawTask(invalidId)]);
        await expectInvalidEntityId(() =>
          crawl.listTasks.invoke({ job_id: parseCrawlJobId(JOB_ID) })
        );
      }
    } finally {
      globalThis.fetch = realFetch;
    }
  });
});

describe('crawl WebSocket response ID contract', () => {
  test('maps canonical IDs and drops events with malformed job or task IDs', () => {
    const originalWindow = (globalThis as { window?: unknown }).window;
    const originalWebSocket = globalThis.WebSocket;
    const originalWarn = console.warn;

    class FakeWebSocket {
      static readonly CONNECTING = 0;
      static readonly OPEN = 1;
      static readonly CLOSING = 2;
      static readonly CLOSED = 3;
      static instance: FakeWebSocket | undefined;

      readyState = FakeWebSocket.OPEN;
      private readonly listeners = new Map<string, Array<(event: unknown) => void>>();

      constructor(..._args: unknown[]) {
        FakeWebSocket.instance = this;
      }

      addEventListener(type: string, listener: (event: unknown) => void): void {
        const listeners = this.listeners.get(type) ?? [];
        listeners.push(listener);
        this.listeners.set(type, listeners);
      }

      send(_data: string): void {}

      close(): void {
        this.readyState = FakeWebSocket.CLOSED;
      }

      dispatch(name: string, data: unknown): void {
        const event = { data: JSON.stringify({ name, data }) };
        for (const listener of this.listeners.get('message') ?? []) listener(event);
      }
    }

    (globalThis as { window?: unknown }).window = {
      location: { protocol: 'http:', host: 'localhost:13400' },
    };
    globalThis.WebSocket = FakeWebSocket as unknown as typeof WebSocket;
    console.warn = () => {};

    const progressIds: string[] = [];
    const taskIds: Array<[string, string]> = [];
    const finishedIds: string[] = [];
    const unsubscribes: Array<() => void> = [];

    try {
      unsubscribes.push(
        crawlEvents.progress.on((event) => progressIds.push(event.job_id)),
        crawlEvents.task.on((event) => taskIds.push([event.job_id, event.task_id])),
        crawlEvents.finished.on((event) => finishedIds.push(event.job_id))
      );

      const socket = FakeWebSocket.instance;
      if (!socket) throw new Error('crawl event subscription did not create a WebSocket');

      const progress = { pending: 0, in_progress: 1, done: 0, failed: 0, skipped: 0 };
      socket.dispatch('crawl.progress', { kind: 'progress', job_id: JOB_ID, progress });
      socket.dispatch('crawl.task', {
        kind: 'task',
        job_id: JOB_ID,
        task_id: TASK_ID,
        url: 'https://example.com',
        status: 'done',
      });
      socket.dispatch('crawl.finished', { kind: 'finished', job_id: JOB_ID, status: 'done', progress });

      expect(progressIds).toEqual([JOB_ID]);
      expect(taskIds).toEqual([[JOB_ID, TASK_ID]]);
      expect(finishedIds).toEqual([JOB_ID]);

      socket.dispatch('crawl.progress', { kind: 'progress', job_id: `crawl_${JOB_ID}`, progress });
      socket.dispatch('crawl.task', {
        kind: 'task',
        job_id: JOB_ID,
        task_id: 31,
        url: 'https://example.com',
        status: 'done',
      });
      socket.dispatch('crawl.finished', { kind: 'finished', job_id: 30, status: 'done', progress });

      expect(progressIds).toEqual([JOB_ID]);
      expect(taskIds).toEqual([[JOB_ID, TASK_ID]]);
      expect(finishedIds).toEqual([JOB_ID]);
    } finally {
      for (const unsubscribe of unsubscribes) unsubscribe();
      FakeWebSocket.instance?.close();
      (globalThis as { window?: unknown }).window = originalWindow;
      globalThis.WebSocket = originalWebSocket;
      console.warn = originalWarn;
    }
  });
});
