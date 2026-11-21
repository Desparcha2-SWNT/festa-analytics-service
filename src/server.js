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

  let stopping = false;
  const shutdown = async () => {
    if (stopping) return;
    stopping = true;
    logger.info('apagando...');
    // Red de seguridad: si algo deja el event loop vivo, se sale igual a los 10 s (ECS mata a los 30 s).
    setTimeout(() => process.exit(0), 10_000).unref();
    // server.close no devuelve promesa: se espera a que terminen las peticiones abiertas antes de
    // cerrar el consumidor y Mongo. Luego el proceso sale solo (sin process.exit), así los logs se vacían.
    await new Promise((resolve) => server.close(resolve));
    await messaging.stop();
    await closeMongo();
    logger.info('apagado');
  };
  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}

main().catch((err) => {
  logger.error(err, 'no se pudo arrancar el servicio');
  process.exit(1);
});
