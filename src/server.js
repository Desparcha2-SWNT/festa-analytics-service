import { createApp } from './app.js';
import { config } from './config.js';
import { logger } from './logger.js';
import { closeMongo, connectMongo } from './db/mongo.js';
import { startMessaging } from './messaging/supervisor.js';

async function ensureIndexes(db) {
  await db.collection('event_stats').createIndex({ organizerId: 1, startsAt: -1 });
  await db.collection('event_stats').createIndex({ startsAt: -1, attendeeCount: -1 });
  await db.collection('user_activity').createIndex({ lastSeen: -1 });
  await db.collection('merchant_stats').createIndex({ status: 1 });
  await db.collection('reports').createIndex({ resolvedAt: 1 });
}

async function main() {
  await connectMongo({ ensureIndexes });
  const messaging = await startMessaging();

  const server = createApp().listen(config.port, () => {
    logger.info({ port: config.port }, 'festa-analytics-service arriba');
  });

  const shutdown = async () => {
    logger.info('apagando...');
    server.close();
    await messaging.stop();
    await closeMongo();
    process.exit(0);
  };
  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}

main().catch((err) => {
  logger.error(err, 'no se pudo arrancar el servicio');
  process.exit(1);
});
