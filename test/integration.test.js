// analytics-service contra MongoDB real, alimentado con los ejemplos de cada contrato que consume.
// Se salta sin TEST_MONGO_URL (p. ej. mongodb://localhost:27018/analytics_test).
import fs from 'node:fs';
import path from 'node:path';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import request from 'supertest';
import jwt from 'jsonwebtoken';
import { jwtPrivateKeyPem } from './fixtures/jwtKeys.js';

const MONGO = process.env.TEST_MONGO_URL;
if (MONGO) process.env.MONGO_URL = MONGO;
const contractsDir = process.env.FESTA_CONTRACTS_DIR ?? path.resolve('../festa-infra/contracts');
const hasContracts = fs.existsSync(path.join(contractsDir, 'events'));

function consumedExamples() {
  const out = [];
  for (const domain of fs.readdirSync(path.join(contractsDir, 'events'))) {
    for (const file of fs.readdirSync(path.join(contractsDir, 'events', domain))) {
      const schema = JSON.parse(fs.readFileSync(path.join(contractsDir, 'events', domain, file), 'utf8'));
      if (schema['x-consumers']?.['analytics-service']) out.push({ eventType: schema['x-routing-key'], data: schema.examples[0] });
    }
  }
  return out;
}

const ORGANIZER = '1c2d3e4f-5a6b-4c7d-8e9f-0a1b2c3d4e5f'; // organizerId de los ejemplos de eventos
const EVENT_ID = '6f1c2b8e-3a1d-4c3e-9b0a-1f2e3d4c5b6a';
const sign = (sub, role) => jwt.sign({ role }, jwtPrivateKeyPem, { algorithm: 'RS256', issuer: process.env.JWT_ISSUER, subject: sub, expiresIn: '5m' });

describe.skipIf(!MONGO || !hasContracts)('analytics-service con MongoDB', () => {
  let db;
  let mongo;
  let handlers;
  let createApp;

  beforeAll(async () => {
    mongo = await import('../src/db/mongo.js');
    db = await mongo.connectMongo();
    await db.dropDatabase();
    ({ handlers } = await import('../src/messaging/handlers.js'));
    ({ createApp } = await import('../src/app.js'));
  });
  afterAll(async () => {
    await db?.dropDatabase();
    await mongo?.closeMongo();
  });

  it('tiene handler para cada evento que el contrato dice que consume', () => {
    const missing = consumedExamples().map((x) => x.eventType).filter((t) => !handlers[t]);
    expect(missing).toEqual([]);
  });

  it('procesa el ejemplo de cada contrato sin errores', async () => {
    let i = 0;
    for (const { eventType, data } of consumedExamples()) {
      if (eventType === 'account.deleted') continue; // se prueba aparte
      await handlers[eventType](data, { eventId: `ex-${i++}`, eventType }, db);
    }
    const stats = await db.collection('event_stats').findOne({ _id: EVENT_ID });
    expect(stats.title).toBe('Noche de jazz en La Candelaria');
    expect(stats.ticketsSold).toBe(2);
    expect(stats.views.total).toBe(1);
  });

  it('un event.updated viejo no pisa uno más nuevo', async () => {
    const base = consumedExamples().find((x) => x.eventType === 'event.published').data;
    await handlers['event.updated']({ ...base, title: 'v5', version: 5 }, { eventType: 'event.updated' }, db);
    await handlers['event.updated']({ ...base, title: 'v3', version: 3 }, { eventType: 'event.updated' }, db);
    expect((await db.collection('event_stats').findOne({ _id: EVENT_ID })).title).toBe('v5');
  });

  it('RF15: el organizador ve su panel y nadie más ve el detalle de su evento', async () => {
    const app = createApp();
    const mine = await request(app).get('/analytics/merchants/me').set('Authorization', `Bearer ${sign(ORGANIZER, 'merchant')}`);
    expect(mine.status).toBe(200);
    expect(mine.body.totals.ticketsSold).toBe(2);
    expect(mine.body.events[0].eventId).toBe(EVENT_ID);

    const other = await request(app).get(`/analytics/events/${EVENT_ID}`).set('Authorization', `Bearer ${sign('2c2d3e4f-5a6b-4c7d-8e9f-0a1b2c3d4e5f', 'user')}`);
    expect(other.status).toBe(404);
  });

  it('RN4: el admin ve los KPIs y un usuario normal no', async () => {
    const app = createApp();
    const admin = await request(app).get('/analytics/admin/overview?days=365').set('Authorization', `Bearer ${sign(ORGANIZER, 'admin')}`);
    expect(admin.status).toBe(200);
    expect(admin.body.users.registrations).toBe(1);
    expect(admin.body.sales.ticketsSold).toBe(2);
    const user = await request(app).get('/analytics/admin/overview').set('Authorization', `Bearer ${sign(ORGANIZER, 'user')}`);
    expect(user.status).toBe(403);
  });

  it('dos entregas simultáneas del mismo mensaje cuentan una sola vez', async () => {
    const { _internal } = await import('../src/messaging/inboxConsumer.js');
    const registry = {
      validateEnvelope: () => true,
      getExpectedProducer: () => null,
      getExpectedExchange: () => null,
      getDataValidator: () => null,
    };
    const example = consumedExamples().find((e) => e.eventType === 'account.registered');
    const env = {
      eventId: '0b0c0d0e-0f10-4a11-8b12-131415161718',
      eventType: 'account.registered',
      producer: 'auth-service',
      occurredAt: '2099-03-03T12:00:00.000Z',
      data: { ...example.data, userId: '7a7b7c7d-7e7f-4a80-8b81-828384858687', registeredAt: '2099-03-03T12:00:00.000Z' },
    };
    const channel = { ack: () => {}, nack: () => {}, sendToQueue: () => {} };
    const msg = () => ({ content: Buffer.from(JSON.stringify(env)), properties: { headers: {} }, fields: { exchange: 'festa.auth' } });
    const before = await db.collection('platform_daily').find().toArray();
    const sum = (rows) => rows.reduce((acc, r) => acc + (r.registrations ?? 0), 0);

    await Promise.all([_internal.handle(channel, msg(), registry, db), _internal.handle(channel, msg(), registry, db)]);
    await _internal.handle(channel, msg(), registry, db);

    expect(sum(await db.collection('platform_daily').find().toArray())).toBe(sum(before) + 1);
  });

  it('RN4: un usuario con días activos viejos no cuenta como recurrente en el periodo', async () => {
    const { adminOverview } = await import('../src/stats/queries.js');
    const now = new Date();
    const daysAgo = (n) => new Date(now.getTime() - n * 86_400_000).toISOString().slice(0, 10);
    const before = (await adminOverview({ days: 30 }, db)).users.recurring;
    await db.collection('user_activity').insertMany([
      { _id: 'recurrente-viejo', activeDays: ['2020-01-01', '2020-01-02', '2020-01-03', daysAgo(0)], lastSeen: now },
      { _id: 'recurrente-real', activeDays: [daysAgo(1), daysAgo(2), daysAgo(3)], lastSeen: now },
    ]);
    expect((await adminOverview({ days: 30 }, db)).users.recurring).toBe(before + 1);
    await db.collection('user_activity').deleteMany({ _id: { $in: ['recurrente-viejo', 'recurrente-real'] } });
  });

  it('account.deleted quita el userId de los agregados', async () => {
    await handlers['account.deleted']({ userId: ORGANIZER, deletedAt: new Date().toISOString() }, { eventType: 'account.deleted' }, db);
    expect(await db.collection('event_stats').countDocuments({ organizerId: ORGANIZER })).toBe(0);
    expect(await db.collection('merchant_stats').countDocuments({ _id: ORGANIZER })).toBe(0);
  });
});
