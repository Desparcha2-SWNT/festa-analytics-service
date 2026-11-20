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
  // Firma el origen de los reintentos (ver inboxConsumer.originExchange). Solo este servicio la conoce: quien
  // publique directo en la inbox con las credenciales compartidas de RabbitMQ no puede falsificarla. Sin la
  // variable se genera una por proceso (sirve con una réplica; con varias, todas deben compartir la misma).
  inboxRetryKey: process.env.INBOX_RETRY_KEY ?? randomBytes(32).toString('hex'),
  inboxRetryKeyFromEnv: Boolean(process.env.INBOX_RETRY_KEY),
};
