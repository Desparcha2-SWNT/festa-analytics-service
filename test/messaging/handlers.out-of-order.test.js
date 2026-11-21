// Entregas fuera de orden: un mensaje atrasado no debe reponer datos ya borrados ni una resolución ya registrada.
import { describe, expect, it } from 'vitest';
import { handlers } from '../../src/messaging/handlers.js';

// Base en memoria con lo mínimo que usan los handlers: updateOne (upsert, $set, $setOnInsert, $inc),
// updateMany, findOne, findOneAndUpdate (devuelve el documento anterior) y deleteOne.
function memDb() {
  const store = {};
  const matches = (doc, filter) => {
    if (filter._id !== undefined && doc._id !== filter._id) return false;
    if (filter.organizerId !== undefined && doc.organizerId !== filter.organizerId) return false;
    if (filter.$or) {
      return filter.$or.some((clause) => {
        const v = clause.version;
        if (v.$exists === false) return doc.version === undefined;
        if (v.$lt !== undefined) return doc.version !== undefined && doc.version < v.$lt;
        return false;
      });
    }
    return true;
  };
  const apply = (doc, update, inserting) => {
    if (update.$set) Object.assign(doc, update.$set);
    if (inserting && update.$setOnInsert) Object.assign(doc, update.$setOnInsert);
    for (const [k, v] of Object.entries(update.$inc ?? {})) doc[k] = (doc[k] ?? 0) + v;
  };
  const collection = (name) => {
    store[name] ??= {};
    const docs = store[name];
    const find = (filter) => Object.values(docs).filter((d) => matches(d, filter));
    return {
      updateOne: async (filter, update, opts = {}) => {
        const hit = find(filter)[0];
        if (hit) {
          apply(hit, update, false);
          return { matchedCount: 1, upsertedCount: 0 };
        }
        if (!opts.upsert) return { matchedCount: 0, upsertedCount: 0 };
        const doc = { _id: filter._id };
        apply(doc, update, true);
        docs[doc._id] = doc;
        return { matchedCount: 0, upsertedCount: 1 };
      },
      updateMany: async (filter, update) => {
        const hits = find(filter);
        for (const d of hits) apply(d, update, false);
        return { matchedCount: hits.length };
      },
      findOne: async (filter) => find(filter)[0] ?? null,
      findOneAndUpdate: async (filter, update, opts = {}) => {
        const hit = find(filter)[0];
        if (hit) {
          const before = { ...hit };
          apply(hit, update, false);
          return before;
        }
        if (opts.upsert) {
          const doc = { _id: filter._id };
          apply(doc, update, true);
          docs[doc._id] = doc;
        }
        return null;
      },
      deleteOne: async (filter) => {
        const hit = find(filter)[0];
        if (hit) delete docs[hit._id];
        return { deletedCount: hit ? 1 : 0 };
      },
    };
  };
  return { db: { collection }, store };
}

const eventData = (version, title) => ({
  eventId: 'e1',
  organizerId: 'o1',
  merchantId: null,
  title,
  category: 'musical',
  startsAt: '2026-12-01T20:00:00Z',
  endsAt: '2026-12-01T22:00:00Z',
  capacity: 10,
  ticketed: false,
  priceCents: 0,
  isLive: false,
  status: 'published',
  version,
  updatedAt: '2026-11-15T10:00:00Z',
});

describe('account.deleted y snapshots atrasados', () => {
  it('un evento que llega después del borrado de su organizador no vuelve a guardar el organizerId', async () => {
    const { db, store } = memDb();
    await handlers['event.published'](eventData(1, 'v1'), { eventType: 'event.published' }, db);
    await handlers['account.deleted']({ userId: 'o1', deletedAt: '2026-11-16T10:00:00Z' }, { eventType: 'account.deleted' }, db);
    expect(store.event_stats.e1.organizerId).toBeNull();

    // Mensaje atrasado: sigue con el organizador borrado.
    await handlers['event.updated'](eventData(2, 'v2'), { eventType: 'event.updated' }, db);
    expect(store.event_stats.e1.organizerId).toBeNull();
    expect(store.event_stats.e1.title).toBe('v2');
  });

  it('si el snapshot llega antes del borrado, el borrado deja el organizerId en null', async () => {
    const { db, store } = memDb();
    await handlers['event.published'](eventData(1, 'v1'), { eventType: 'event.published' }, db);
    await handlers['account.deleted']({ userId: 'o1', deletedAt: '2026-11-16T10:00:00Z' }, { eventType: 'account.deleted' }, db);
    expect(store.event_stats.e1.organizerId).toBeNull();
    expect(store.deleted_accounts.o1).toBeDefined();
  });
});

describe('report.resolved y report.created fuera de orden', () => {
  it('report.created que llega después de report.resolved no borra la resolución registrada', async () => {
    const { db, store } = memDb();
    await handlers['report.resolved'](
      { reportId: 'r1', resolvedAt: '2026-11-15T12:00:00Z', outcome: 'dismissed' },
      { eventType: 'report.resolved' },
      db,
    );
    await handlers['report.created'](
      { reportId: 'r1', createdAt: '2026-11-15T11:00:00Z', targetType: 'event', reason: 'spam' },
      { eventType: 'report.created' },
      db,
    );
    expect(store.reports.r1.resolvedAt).toEqual(new Date('2026-11-15T12:00:00Z'));
    expect(store.reports.r1.outcome).toBe('dismissed');
    expect(store.reports.r1.reason).toBe('spam');
  });

  it('en orden normal (created y luego resolved) el reporte queda resuelto', async () => {
    const { db, store } = memDb();
    await handlers['report.created'](
      { reportId: 'r2', createdAt: '2026-11-15T11:00:00Z', targetType: 'event', reason: 'spam' },
      { eventType: 'report.created' },
      db,
    );
    await handlers['report.resolved'](
      { reportId: 'r2', resolvedAt: '2026-11-15T12:00:00Z', outcome: 'removed' },
      { eventType: 'report.resolved' },
      db,
    );
    expect(store.reports.r2.resolvedAt).toEqual(new Date('2026-11-15T12:00:00Z'));
    expect(store.reports.r2.outcome).toBe('removed');
  });
});
