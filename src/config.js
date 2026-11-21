import path from 'node:path';

function required(name) {
  const value = process.env[name];
  if (!value) throw new Error(`Falta la variable de entorno ${name}`);
  return value;
}

export function parseTrustProxy(value) {
  if (value === undefined || value === '') return 1;
  if (/^\d+$/.test(value)) return Number(value);
  if (value === 'true') return true;
  if (value === 'false') return false;
  return value;
}

const MIN_RETRY_KEY_LENGTH = 16;

/**
 * Clave que firma el origen de los reintentos de la inbox. Es OBLIGATORIA y debe ser estable: con una distinta en
 * cada arranque, un reintento pendiente que sobreviva a un reinicio (o que otra réplica reciba) llegaría con una firma
 * que ya no vale y se iría a la DLQ. Tampoco puede ser vacía, corta ni un valor de ejemplo (`change-me…`): una clave
 * conocida dejaría la firma sin ningún efecto, sin que nadie lo notara. Se genera con `openssl rand -hex 32`.
 */
export function resolveInboxRetryKey(value) {
  if (value === undefined) throw new Error('Falta la variable de entorno INBOX_RETRY_KEY (openssl rand -hex 32)');
  const key = value.trim();
  if (key.length < MIN_RETRY_KEY_LENGTH || /^change-?me/i.test(key)) {
    throw new Error(
      `INBOX_RETRY_KEY está vacía, tiene menos de ${MIN_RETRY_KEY_LENGTH} caracteres o es un valor de ejemplo: define una clave real (openssl rand -hex 32)`,
    );
  }
  return value;
}

export const config = {
  serviceName: 'analytics-service',
  port: Number(process.env.PORT ?? 8088),
  trustProxy: parseTrustProxy(process.env.TRUST_PROXY),
  logLevel: process.env.LOG_LEVEL ?? 'info',
  mongoUrl: required('MONGO_URL'),
  amqpUrl: required('AMQP_URL'),
  jwtIssuer: required('JWT_ISSUER'),
  jwtPublicKeyPath: required('JWT_PUBLIC_KEY_PATH'),
  contractsDir: process.env.FESTA_CONTRACTS_DIR ?? path.resolve('../festa-infra/contracts'),
  // Firma el origen de los reintentos (ver inboxConsumer.originExchange): quien publique directo en la inbox sin
  // conocer la clave no puede falsificarlo. Con varias réplicas, todas deben compartir la misma (ver resolveInboxRetryKey).
  inboxRetryKey: resolveInboxRetryKey(process.env.INBOX_RETRY_KEY),
  inboxRetryKeyFromEnv: Boolean(process.env.INBOX_RETRY_KEY),
};
