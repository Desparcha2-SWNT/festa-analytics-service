// Un mensaje entregado dos veces (reintento tras caída a mitad del handler) no debe sumar dos veces,
// aunque entre la caída y el reintento lleguen muchos mensajes al mismo documento.
import { describe, expect, it } from 'vitest';
import { handlers } from '../../src/messaging/handlers.js';

const getPath = (obj, path) => path.split('.').reduce((o, k) => (o == null ? undefined : o[k]), obj);
function setPath(obj, path, value) {
  const keys = path.split('.');
  let o = obj;
  for (const k of keys.slice(0, -1)) o = o[k] ??= {};
  o[keys.at(-1)] = value;
}

// Base en memoria con los operadores que usan los handlers de analytics.
function memoryDb() {
  const cols = new Map();
  const collection = (name) => {
    if (!cols.has(name)) cols.set(name, new Map());
    const docs = cols.get(name);
    return {
      docs,
      async findOne(filter) {
        return docs.get(filter._id) ?? null;
      },
      async updateOne(filter, update, opts = {}) {
        if (db.failNext?.(name, update)) throw new Error(`caída simulada en ${name}`);
        let doc = docs.get(filter._id);
        const matches = (d) =>
          d &&
          Object.entries(filter).every(([k, v]) => {
            if (k === '_id') return d._id === v;
            if (v && typeof v === 'object' && '$ne' in v) return !(d[k] ?? []).includes(v.$ne);
            return d[k] === v;
          });
        if (!doc) {
          if (!opts.upsert) return { matchedCount: 0, upsertedCount: 0 };
          doc = { _id: filter._id };
          if (update.$setOnInsert) for (const [k, v] of Object.entries(update.$setOnInsert)) setPath(doc, k, Array.isArray(v) ? [...v] : v);
          docs.set(filter._id, doc);
          if (!matches(doc)) return { matchedCount: 0, upsertedCount: 0 };
        } else if (!matches(doc)) {
          return { matchedCount: 0, upsertedCount: 0 };
        }
        for (const [k, v] of Object.entries(update.$inc ?? {})) setPath(doc, k, (getPath(doc, k) ?? 0) + v);
        for (const [k, v] of Object.entries(update.$set ?? {})) setPath(doc, k, v);
        for (const [k, v] of Object.entries(update.$push ?? {})) {
          const arr = [...(doc[k] ?? []), ...(v?.$each ?? [v])];
          doc[k] = v?.$slice < 0 ? arr.slice(v.$slice) : arr;
        }
        for (const [k, v] of Object.entries(update.$pull ?? {})) doc[k] = (doc[k] ?? []).filter((x) => x !== v);
        for (const [k, v] of Object.entries(update.$addToSet ?? {})) {
          const arr = doc[k] ?? [];
          if (!arr.includes(v)) arr.push(v);
          doc[k] = arr;
        }
        for (const [k, v] of Object.entries(update.$min ?? {})) if (!doc[k] || v < doc[k]) doc[k] = v;
        for (const [k, v] of Object.entries(update.$max ?? {})) if (!doc[k] || v > doc[k]) doc[k] = v;
        return { matchedCount: 1, upsertedCount: 0 };
      },
    };
  };
  const db = { collection, failNext: null };
  return db;
}

/** Hace fallar UNA vez la escritura que cumpla `predicate(colección, update)`: simula una caída ahí. */
function crashOnce(db, predicate) {
  let armed = true;
  db.failNext = (name, update) => {
    if (armed && predicate(name, update)) {
      armed = false;
      return true;
    }
    return false;
  };
}

const scoped = (db, effectId) => ({ collection: db.collection, effectId });
const VIEW = { eventId: 'e1', viewerId: 'u1', source: 'map', viewedAt: '2026-11-15T12:00:00Z' };
const views = (db) => db.collection('event_stats').docs.get('e1').views.total;

describe('efectos idempotentes por mensaje', () => {
  it('engagement.viewed entregado dos veces suma views.total una sola vez', async () => {
    const db = memoryDb();
    const data = { eventId: 'e1', viewerId: 'u1', source: 'map', viewedAt: '2026-11-15T12:00:00Z' };
    await handlers['engagement.viewed'](data, {}, scoped(db, 'msg-1'));
    await handlers['engagement.viewed'](data, {}, scoped(db, 'msg-1')); // reintento
    expect(db.collection('event_stats').docs.get('e1').views.total).toBe(1);
  });

  it('dos mensajes distintos sí suman', async () => {
    const db = memoryDb();
    const data = { eventId: 'e1', viewerId: 'u1', source: 'map', viewedAt: '2026-11-15T12:00:00Z' };
    await handlers['engagement.viewed'](data, {}, scoped(db, 'msg-1'));
    await handlers['engagement.viewed'](data, {}, scoped(db, 'msg-2'));
    expect(db.collection('event_stats').docs.get('e1').views.total).toBe(2);
  });

  it('payout.recorded reintentado no duplica el pago', async () => {
    const db = memoryDb();
    const data = { organizerId: 'o1', merchantId: 'm1', amountCents: 5000, paidAt: '2026-11-15T12:00:00Z' };
    await handlers['payout.recorded'](data, {}, scoped(db, 'msg-9'));
    await handlers['payout.recorded'](data, {}, scoped(db, 'msg-9'));
    const stats = db.collection('merchant_stats').docs.get('o1');
    expect(stats.payoutsCents).toBe(5000);
    expect(stats.payoutCount).toBe(1);
  });

  it('un reintento corrige solo el documento que faltó (dos documentos en un handler)', async () => {
    const db = memoryDb();
    const data = { eventId: 'e1', orderId: 'o1', buyerId: 'u1', quantity: 2, subtotalCents: 100, commissionCents: 8, paymentMethod: 'CARD', approvedAt: '2026-11-15T12:00:00Z' };
    // La primera entrega aplicó event_stats y murió al escribir platform_daily.
    crashOnce(db, (name, update) => name === 'platform_daily' && update.$inc);
    await expect(handlers['order.approved'](data, {}, scoped(db, 'msg-7'))).rejects.toThrow('caída simulada');
    expect(db.collection('platform_daily').docs.get('2026-11-15')?.ticketsSold).toBeUndefined();
    await handlers['order.approved'](data, {}, scoped(db, 'msg-7'));
    expect(db.collection('event_stats').docs.get('e1').ticketsSold).toBe(2);
    expect(db.collection('platform_daily').docs.size).toBe(1);
  });

  it('un reintento después de más de 500 mensajes al mismo documento no vuelve a sumar', async () => {
    const db = memoryDb();
    // Caída después del incremento y antes de registrarlo en el ledger.
    crashOnce(db, (name) => name === 'applied_effects');
    await expect(handlers['engagement.viewed'](VIEW, {}, scoped(db, 'msg-late'))).rejects.toThrow('caída simulada');
    for (let i = 0; i < 600; i += 1) await handlers['engagement.viewed'](VIEW, {}, scoped(db, `msg-${i}`));
    await handlers['engagement.viewed'](VIEW, {}, scoped(db, 'msg-late')); // el reintento llega tarde
    expect(views(db)).toBe(601);
  });

  it('un mensaje ya aplicado no vuelve a sumar aunque pasen más de 500 mensajes', async () => {
    const db = memoryDb();
    await handlers['engagement.viewed'](VIEW, {}, scoped(db, 'msg-old'));
    for (let i = 0; i < 600; i += 1) await handlers['engagement.viewed'](VIEW, {}, scoped(db, `msg-${i}`));
    await handlers['engagement.viewed'](VIEW, {}, scoped(db, 'msg-old'));
    expect(views(db)).toBe(601);
  });

  it('una caída entre el ledger y la limpieza no deja ids en el documento', async () => {
    const db = memoryDb();
    crashOnce(db, (name, update) => name === 'event_stats' && update.$pull);
    await expect(handlers['engagement.viewed'](VIEW, {}, scoped(db, 'msg-1'))).rejects.toThrow('caída simulada');
    await handlers['engagement.viewed'](VIEW, {}, scoped(db, 'msg-1'));
    const doc = db.collection('event_stats').docs.get('e1');
    expect(doc.views.total).toBe(1);
    expect(doc.pendingEffects).toEqual([]);
  });
});
