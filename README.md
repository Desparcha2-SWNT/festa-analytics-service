# festa-analytics-service

RF15, RN4 y RN5: panel del comercio para medir su retorno y KPIs del piloto en la ECI. Node 20 / Express con MongoDB
(`analytics_db`). Puerto 8088.

No llama a ningún servicio: construye sus agregados consumiendo los 28 eventos que el contrato le asigna
(`festa-infra/contracts/README.md` § analytics).

## Qué mide

| Colección | Contenido | Fuente |
|---|---|---|
| `event_stats` | Por evento: vistas (por origen), compartidos, asistentes, ingresos reales (check-in), tasa de asistencia, veces recomendado por la IA, alcance de push, embudo de compra, boletas vendidas, ingresos, reembolsos, intentos de QR rechazados | `event.*`, `engagement.*`, `attendance.*`, `pass.*`, `order.*`, `recommendation.served`, `push.sent` |
| `merchant_stats` | Por comercio: estado de verificación, liquidaciones recibidas, descripciones generadas con IA | `merchant.*`, `payout.recorded`, `description.generated` |
| `platform_daily` | Por día: registros, eventos publicados, ventas, comisión, reportes y tiempo de resolución, tokens de IA, push | todo lo anterior |
| `user_activity` | Días activos por usuario, para separar usuarios recurrentes de los que solo descargan (RN4) | registros, vistas, asistencias, chat de IA |

`account.deleted` (Ley 1581) elimina al usuario de `user_activity` y `merchant_stats` y quita su id de `event_stats`.

## API

Todas con JWT. El cliente las llama con el prefijo `/api`.

| Método y ruta | Quién | Qué devuelve |
|---|---|---|
| `GET /analytics/merchants/me` | Cualquier organizador | Panel RF15: totales y detalle por evento (conteos simples) |
| `GET /analytics/events/{id}` | Organizador del evento o admin | Detalle de un evento |
| `GET /analytics/admin/overview?days=30` | Admin | KPIs RN4: usuarios activos (7 y 30 días) y recurrentes (3+ días), comercios pendientes y verificados, ventas y comisión, moderación, costo de IA, top de eventos |
| `GET /health`, `GET /metrics` | — | Estado y métricas |

## Correr en local

```bash
cp .env.example .env
npm ci
TEST_MONGO_URL=mongodb://localhost:27018/analytics_test npm test   # la integración usa los ejemplos de los contratos
npm start
```

Con el resto del stack: `docker compose --profile analytics up -d --build` desde `festa-infra`.
