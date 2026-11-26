// Orden de escrituras: lo idempotente (user_activity) va antes del único $inc de cada handler.
import { describe, expect, it, vi } from 'vitest';
import { handlers } from '../../src/messaging/handlers.js';

function fakeDb() {
  const calls = [];
  const collection = (name) => ({
    // La cuenta no está borrada (sin lápida): no cambia el orden de las escrituras.
    findOne: vi.fn(async () => null),
    updateOne: vi.fn(async () => {
      calls.push(name);
      return {};
    }),
  });
  return { db: { collection }, calls };
}

describe('handlers: touchUser antes del $inc', () => {
  it('engagement.viewed escribe user_activity antes que event_stats', async () => {
    const { db, calls } = fakeDb();
    await handlers['engagement.viewed']({ eventId: 'e1', viewerId: 'u1', source: 'map', viewedAt: '2026-11-15T12:00:00Z' }, {}, db);
    expect(calls).toEqual(['user_activity', 'event_stats']);
  });

  it('pass.checked_in escribe user_activity antes que event_stats', async () => {
    const { db, calls } = fakeDb();
    await handlers['pass.checked_in']({ eventId: 'e1', userId: 'u1', checkedInAt: '2026-11-15T12:00:00Z' }, {}, db);
    expect(calls).toEqual(['user_activity', 'event_stats']);
  });
});
