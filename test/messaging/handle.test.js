// Despacho del inbox (sobre -> productor/exchange -> data -> handler -> idempotencia) sin RabbitMQ.
import { describe, expect, it, vi } from 'vitest';

const h = vi.hoisted(() => ({ handler: vi.fn() }));
vi.mock('../../src/messaging/handlers.js', () => ({ handlers: { 'account.registered': h.handler } }));

const { _internal } = await import('../../src/messaging/inboxConsumer.js');

const fakeChannel = () => ({ ack: vi.fn(), nack: vi.fn(), sendToQueue: vi.fn() });
function fakeRegistry({ validEnvelope = true, dataValid = true, producer = 'auth-service', exchange = 'festa.auth' } = {}) {
  const validateEnvelope = vi.fn().mockReturnValue(validEnvelope);
  return {
    validateEnvelope,
    getExpectedProducer: () => producer,
    getExpectedExchange: () => exchange,
    getDataValidator: () => vi.fn().mockReturnValue(dataValid),
  };
}
const msgWith = (envelope, exchange = 'festa.auth', headers = {}) => ({
  content: Buffer.from(JSON.stringify(envelope)),
  properties: { headers },
  fields: { exchange },
});
// Fake con estado de la colección processed_events (claim con lease, done, toma de lease vencido).
function fakeDb({ existing = null } = {}) {
  const store = { doc: existing ? { ...existing } : null };
  const duplicate = () => Promise.reject(Object.assign(new Error('E11000 duplicate key'), { code: 11000 }));
  const processed = {
    insertOne: vi.fn((doc) => {
      if (store.doc) return duplicate();
      store.doc = { ...doc };
      return Promise.resolve({ acknowledged: true });
    }),
    findOne: vi.fn(async () => store.doc),
    findOneAndUpdate: vi.fn(async (filter, update) => {
      const d = store.doc;
      const ok = d && d._id === filter._id && d.status === filter.status && d.leaseUntil <= filter.leaseUntil.$lte;
      if (!ok) return null;
      Object.assign(d, update.$set);
      return { ...d };
    }),
    updateOne: vi.fn(async (filter, update) => {
      const d = store.doc;
      const matched = Boolean(d && d._id === filter._id && (filter.claimToken === undefined || d.claimToken === filter.claimToken));
      if (matched) Object.assign(d, update.$set);
      return { acknowledged: true, matchedCount: matched ? 1 : 0 };
    }),
    deleteOne: vi.fn(async (filter) => {
      const d = store.doc;
      const matched = Boolean(d && d._id === filter._id && (filter.claimToken === undefined || d.claimToken === filter.claimToken));
      if (matched) store.doc = null;
      return { deletedCount: matched ? 1 : 0 };
    }),
  };
  return { collection: vi.fn(() => processed), processed, store };
}
const envelope = { eventId: 'e1', eventType: 'account.registered', producer: 'auth-service', data: { userId: 'u' } };

describe('handle', () => {
  it('JSON inválido va a la DLQ', async () => {
    const channel = fakeChannel();
    const msg = { content: Buffer.from('no-json'), properties: {}, fields: {} };
    await _internal.handle(channel, msg, fakeRegistry());
    expect(channel.nack).toHaveBeenCalledWith(msg, false, false);
  });

  it('productor falso va a la DLQ', async () => {
    const channel = fakeChannel();
    const msg = msgWith({ ...envelope, producer: 'event-service' });
    await _internal.handle(channel, msg, fakeRegistry());
    expect(channel.nack).toHaveBeenCalledWith(msg, false, false);
  });

  it('exchange equivocado va a la DLQ', async () => {
    const channel = fakeChannel();
    const msg = msgWith(envelope, 'festa.user');
    await _internal.handle(channel, msg, fakeRegistry());
    expect(channel.nack).toHaveBeenCalledWith(msg, false, false);
  });

  it('data inválida va a la DLQ sin reintentos', async () => {
    const channel = fakeChannel();
    const msg = msgWith(envelope);
    await _internal.handle(channel, msg, fakeRegistry({ dataValid: false }));
    expect(channel.nack).toHaveBeenCalledWith(msg, false, false);
    expect(h.handler).not.toHaveBeenCalled();
  });

  it('reclama el evento antes de ejecutar el handler y confirma', async () => {
    h.handler.mockReset().mockResolvedValue();
    const channel = fakeChannel();
    const db = fakeDb();
    const msg = msgWith(envelope);
    await _internal.handle(channel, msg, fakeRegistry(), db);
    expect(db.processed.insertOne).toHaveBeenCalledWith(expect.objectContaining({ _id: 'e1' }));
    expect(db.processed.insertOne.mock.invocationCallOrder[0]).toBeLessThan(h.handler.mock.invocationCallOrder[0]);
    expect(h.handler).toHaveBeenCalledWith(envelope.data, envelope, expect.objectContaining({ effectId: 'e1' }));
    expect(db.processed.deleteOne).not.toHaveBeenCalled();
    expect(channel.ack).toHaveBeenCalledWith(msg);
  });

  it('si el handler falla, libera la reclamación para que el reintento sí se procese', async () => {
    h.handler.mockReset().mockRejectedValue(new Error('mongo caído'));
    const channel = fakeChannel();
    channel.sendToQueue.mockImplementation((q, c, o, cb) => cb(null));
    const db = fakeDb();
    await _internal.handle(channel, msgWith(envelope), fakeRegistry(), db);
    expect(db.processed.deleteOne).toHaveBeenCalledWith(expect.objectContaining({ _id: 'e1', claimToken: expect.any(String) }));
    expect(db.store.doc).toBeNull();
  });

  it('si otra entrega retomó el lease durante el handler, no borra su reclamación al fallar', async () => {
    h.handler.mockReset().mockImplementation(async () => {
      // Simula: el lease venció y otra entrega lo tomó mientras esta seguía trabajando.
      db.store.doc.claimToken = 'otra-entrega';
      throw new Error('falla tardía');
    });
    const channel = fakeChannel();
    channel.sendToQueue.mockImplementation((q, c, o, cb) => cb(null));
    const db = fakeDb();
    await _internal.handle(channel, msgWith(envelope), fakeRegistry(), db);
    expect(db.store.doc).not.toBeNull();
    expect(db.store.doc.claimToken).toBe('otra-entrega');
  });

  it('si el lease se perdió antes de terminar, no marca como done la reclamación ajena', async () => {
    h.handler.mockReset().mockImplementation(async () => {
      db.store.doc.claimToken = 'otra-entrega';
    });
    const channel = fakeChannel();
    const msg = msgWith(envelope);
    const db = fakeDb();
    await _internal.handle(channel, msg, fakeRegistry(), db);
    expect(db.store.doc.status).toBe('processing');
    expect(channel.ack).toHaveBeenCalledWith(msg);
  });

  it('un duplicado ya terminado se confirma sin repetir el efecto', async () => {
    h.handler.mockReset();
    const channel = fakeChannel();
    const msg = msgWith(envelope);
    const db = fakeDb({ existing: { _id: 'e1', status: 'done' } });
    await _internal.handle(channel, msg, fakeRegistry(), db);
    expect(h.handler).not.toHaveBeenCalled();
    expect(channel.ack).toHaveBeenCalledWith(msg);
  });

  it('si otra entrega tiene el lease vigente, reencola sin ejecutar el handler', async () => {
    h.handler.mockReset();
    const channel = fakeChannel();
    const msg = msgWith(envelope);
    const future = new Date(Date.now() + 30_000);
    const db = fakeDb({ existing: { _id: 'e1', status: 'processing', leaseUntil: future } });
    await _internal.handle(channel, msg, fakeRegistry(), db);
    expect(h.handler).not.toHaveBeenCalled();
    expect(channel.nack).toHaveBeenCalledWith(msg, false, true);
  });

  it('si el proceso murió tras reclamar (lease vencido), el reintento retoma el evento y no lo pierde', async () => {
    h.handler.mockReset().mockResolvedValue();
    const channel = fakeChannel();
    const msg = msgWith(envelope);
    const past = new Date(Date.now() - 1000);
    const db = fakeDb({ existing: { _id: 'e1', status: 'processing', leaseUntil: past } });
    await _internal.handle(channel, msg, fakeRegistry(), db);
    expect(h.handler).toHaveBeenCalledTimes(1);
    expect(db.store.doc.status).toBe('done');
    expect(channel.ack).toHaveBeenCalledWith(msg);
  });

  it('al terminar el handler marca el evento como done', async () => {
    h.handler.mockReset().mockResolvedValue();
    const db = fakeDb();
    await _internal.handle(fakeChannel(), msgWith(envelope), fakeRegistry(), db);
    expect(db.store.doc.status).toBe('done');
  });

  it('una falla transitoria se reintenta por inbox.retry', async () => {
    h.handler.mockReset().mockRejectedValue(new Error('mongo caído'));
    const channel = fakeChannel();
    channel.sendToQueue.mockImplementation((q, c, o, cb) => cb(null));
    const msg = msgWith(envelope);
    await _internal.handle(channel, msg, fakeRegistry(), fakeDb());
    expect(channel.sendToQueue.mock.calls[0][0]).toBe('analytics-service.inbox.retry');
    expect(channel.sendToQueue.mock.calls[0][2].headers['x-retry-count']).toBe(1);
    expect(channel.ack).toHaveBeenCalledWith(msg);
  });

  it('el reintento conserva el exchange de origen y al volver por la cola de reintento sí se procesa', async () => {
    const channel = fakeChannel();
    channel.sendToQueue.mockImplementation((q, c, o, cb) => cb(null));
    const first = msgWith(envelope);
    await _internal.handle(channel, first, fakeRegistry(), fakeDb());
    const [, content, options] = channel.sendToQueue.mock.calls[0];
    expect(options.headers['x-origin-exchange']).toBe('festa.auth');

    // Lo que ve la inbox en la segunda entrega: el dead-letter por el exchange por defecto deja `fields.exchange` vacío.
    h.handler.mockReset();
    const second = { content, properties: options, fields: { exchange: '' } };
    const channel2 = fakeChannel();
    await _internal.handle(channel2, second, fakeRegistry(), fakeDb());
    expect(h.handler).toHaveBeenCalledTimes(1);
    expect(channel2.ack).toHaveBeenCalledWith(second);
    expect(channel2.nack).not.toHaveBeenCalled();
  });

  it('un x-origin-exchange forjado en un mensaje que llegó directo no cambia el exchange verificado', async () => {
    const channel = fakeChannel();
    const msg = msgWith(envelope, 'festa.otro', { 'x-origin-exchange': 'festa.auth' });
    await _internal.handle(channel, msg, fakeRegistry(), fakeDb());
    expect(channel.nack).toHaveBeenCalledWith(msg, false, false);
    expect(h.handler).not.toHaveBeenCalled();
  });

  it('con exchange vacío, un x-origin-exchange sin firma válida va a la DLQ', async () => {
    // Quien publica directo en la inbox (credenciales de RabbitMQ compartidas) llega con exchange vacío.
    for (const headers of [
      { 'x-origin-exchange': 'festa.auth' },
      { 'x-origin-exchange': 'festa.auth', 'x-origin-signature': 'ab'.repeat(32) },
    ]) {
      h.handler.mockReset();
      const channel = fakeChannel();
      const msg = msgWith(envelope, '', headers);
      await _internal.handle(channel, msg, fakeRegistry(), fakeDb());
      expect(channel.nack).toHaveBeenCalledWith(msg, false, false);
      expect(h.handler).not.toHaveBeenCalled();
    }
  });

  it('la firma de un reintento no sirve para otro mensaje', async () => {
    const channel = fakeChannel();
    channel.sendToQueue.mockImplementation((q, c, o, cb) => cb(null));
    h.handler.mockReset().mockRejectedValue(new Error('falla'));
    await _internal.handle(channel, msgWith(envelope), fakeRegistry(), fakeDb());
    const [, , options] = channel.sendToQueue.mock.calls[0];

    h.handler.mockReset();
    const other = { ...envelope, eventId: 'otro-evento' };
    const forged = { content: Buffer.from(JSON.stringify(other)), properties: options, fields: { exchange: '' } };
    const channel2 = fakeChannel();
    await _internal.handle(channel2, forged, fakeRegistry(), fakeDb());
    expect(channel2.nack).toHaveBeenCalledWith(forged, false, false);
    expect(h.handler).not.toHaveBeenCalled();
  });

  it('agotados los reintentos va a la DLQ', async () => {
    const channel = fakeChannel();
    const msg = msgWith(envelope, 'festa.auth', { 'x-retry-count': 3 });
    await _internal.handleFailure(channel, msg, envelope, new Error('x'));
    expect(channel.nack).toHaveBeenCalledWith(msg, false, false);
  });
});
