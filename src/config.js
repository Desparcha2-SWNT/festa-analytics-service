import { randomBytes } from 'node:crypto';
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
 * Clave que firma el origen de los reintentos. Si la variable no existe se genera una por proceso (sirve con una
 * réplica). Si existe pero está vacía o es demasiado corta se detiene el arranque: una clave vacía es conocida por
 * todos y dejaría la firma sin ningún efecto, sin que nadie lo notara.
 */
export function resolveInboxRetryKey(value) {
  if (value === undefined) return randomBytes(32).toString('hex');
  if (value.trim().length < MIN_RETRY_KEY_LENGTH) {
    throw new Error(`INBOX_RETRY_KEY está vacía o tiene menos de ${MIN_RETRY_KEY_LENGTH} caracteres: define una clave real (openssl rand -hex 32)`);
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
