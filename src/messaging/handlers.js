// Agregados de analytics a partir de los eventos del contrato (contracts/README.md § analytics).
// Firma: (data, envelope, db). Los contadores usan $inc; el inbox deduplica por eventId.

const day = (iso) => new Date(iso ?? Date.now()).toISOString().slice(0, 10);

// Ledger durable de efectos ya aplicados: mismo TTL que processed_events (30 días, ver db/mongo.js).
export const APPLIED_EFFECTS = 'applied_effects';

/**
 * Aplica $inc/$set a UN documento sin transacciones, exactamente una vez por (mensaje, documento).
 *
 * 1. Si el ledger `applied_effects` ya tiene (colección, documento, mensaje), el efecto está aplicado.
 * 2. Si no, el incremento y el id del mensaje en `pendingEffects` van en la MISMA escritura atómica,
 *    con un filtro que excluye el id: un reintento tras una caída aquí no vuelve a sumar.
 * 3. Se registra en el ledger y 4. se saca el id de `pendingEffects`.
 *
 * El arreglo solo guarda efectos en curso (entre 2 y 4), no los últimos N: no hay ventana que se
 * llene y deje pasar un reintento tardío. Ese era el límite de la versión anterior (500 ids por documento).
 * Una caída en cualquier punto se corrige al reintentar: 2 no se repite porque el id sigue en el
 * arreglo, y 3 y 4 son idempotentes.
 */
async function applyOnce(db, collection, id, inc, set = {}) {
  const col = db.collection(collection);
  const now = new Date();
  const effectId = db.effectId;
  if (!effectId) {
    return col.updateOne({ _id: id }, { $inc: inc, $set: { ...set, updatedAt: now } }, { upsert: true });
  }
  const ledger = db.collection(APPLIED_EFFECTS);
  const key = `${collection}:${id}:${effectId}`;
  if (!(await ledger.findOne({ _id: key }))) {
    try {
      await col.updateOne({ _id: id }, { $setOnInsert: { pendingEffects: [] } }, { upsert: true });
    } catch (err) {
      if (err?.code !== 11000) throw err; // otra entrega creó el documento a la vez
    }
    await col.updateOne(
      { _id: id, pendingEffects: { $ne: effectId } },
      { $inc: inc, $set: { ...set, updatedAt: now }, $push: { pendingEffects: effectId } },
    );
    await ledger.updateOne({ _id: key }, { $setOnInsert: { appliedAt: now } }, { upsert: true });
  }
  // También cuando el ledger ya existía: una caída entre 3 y 4 dejó el id en el arreglo.
  return col.updateOne({ _id: id }, { $pull: { pendingEffects: effectId } });
}

const incEvent = (db, eventId, inc, set = {}) => applyOnce(db, 'event_stats', eventId, inc, set);

const incDaily = (db, date, inc) => applyOnce(db, 'platform_daily', date, inc);

/** RN4: actividad por usuario y día (usuarios recurrentes vs. que solo descargan). */
/**
 * Reclama el tiempo de resolución de un reporte: solo una de las dos entregas (created o resolved) puede
 * medirlo, porque el cambio a `resolutionCounted` es una escritura condicional y atómica por documento.
 *
 * El reclamo guarda qué mensaje lo hizo (`resolutionCountedBy`). Si el proceso cae entre el reclamo y el
 * incremento, la redelivery del MISMO mensaje vuelve a ganar el reclamo y repite el incremento, que es
 * idempotente por mensaje (applyOnce): el KPI no se pierde ni se cuenta dos veces.
 */
async function claimResolutionTime(db, reportId) {
  const effectId = db.effectId;
  if (!effectId) {
    const res = await db
      .collection('reports')
      .updateOne({ _id: reportId, resolutionCounted: { $ne: true } }, { $set: { resolutionCounted: true } });
    return res.modifiedCount === 1;
  }
  const res = await db
    .collection('reports')
    .updateOne(
      { _id: reportId, $or: [{ resolutionCounted: { $ne: true } }, { resolutionCountedBy: effectId }] },
      { $set: { resolutionCounted: true, resolutionCountedBy: effectId } },
    );
  return res.matchedCount === 1;
}

// Una cuenta borrada (lápida) no vuelve a tener actividad: una entrega tardía no recrea su registro.
const isDeleted = async (db, userId) => Boolean(userId) && (await db.collection('deleted_accounts').findOne({ _id: userId })) !== null;

/**
 * Escribe un documento ligado a una cuenta sin recrear actividad de una cuenta borrada. Se comprueba la
 * lápida antes y también después de escribir: si account.deleted corrió justo entre la comprobación y la
 * escritura (entrega concurrente o tardía), el documento recién creado se borra de nuevo.
 */
async function writeUnlessDeleted(db, collection, userId, write) {
  if (!userId || (await isDeleted(db, userId))) return undefined;
  const result = await write();
  if (await isDeleted(db, userId)) await db.collection(collection).deleteOne({ _id: userId });
  return result;
}

const touchUser = (db, userId, iso) =>
  writeUnlessDeleted(db, 'user_activity', userId, () =>
    db.collection('user_activity').updateOne(
      { _id: userId },
      { $addToSet: { activeDays: day(iso) }, $min: { firstSeen: new Date(iso ?? Date.now()) }, $max: { lastSeen: new Date(iso ?? Date.now()) } },
      { upsert: true },
    ),
  );

/**
 * attendeeCount es el total vigente, no un delta: un mensaje atrasado no puede pisar uno más nuevo. Booking sube
 * attendanceVersion en 1 con cada cambio (contrato x-version 2) y solo se guarda una versión mayor a la guardada:
 * una sola escritura condicional por documento (Mongo corre sin transacciones).
 */
async function setAttendeeCount(db, eventId, attendeeCount, attendanceVersion) {
  const filter = { _id: eventId };
  if (Number.isInteger(attendanceVersion)) {
    filter.$or = [{ attendeeCountVersion: { $exists: false } }, { attendeeCountVersion: { $lt: attendanceVersion } }];
  }
  await db.collection('event_stats').updateOne(filter, {
    $set: { attendeeCount, ...(Number.isInteger(attendanceVersion) ? { attendeeCountVersion: attendanceVersion } : {}) },
  });
}

/**
 * Escritura versionada de event_stats: solo se aplica si `version` es mayor que la guardada. Un único upsert
 * condicional; si dos versiones llegan a la vez y el documento no existe, la que pierde la carrera recibe
 * 11000 y repite la escritura condicional (así la versión mayor siempre gana).
 */
async function setEventVersioned(db, eventId, version, fields) {
  const filter = { _id: eventId, $or: [{ version: { $exists: false } }, { version: { $lt: version } }] };
  try {
    await db.collection('event_stats').updateOne(filter, { $set: { ...fields, version }, $setOnInsert: { createdAt: new Date() } }, { upsert: true });
  } catch (err) {
    if (err?.code !== 11000) throw err;
    await db.collection('event_stats').updateOne(filter, { $set: { ...fields, version } });
  }
}

/** event.published / event.updated: metadatos del evento (snapshot completo, se ignoran versiones viejas). */
async function eventSnapshot(d, e, db) {
  const meta = {
    organizerId: d.organizerId,
    merchantId: d.merchantId ?? null,
    title: d.title,
    category: d.category,
    startsAt: new Date(d.startsAt),
    endsAt: new Date(d.endsAt),
    capacity: d.capacity,
    ticketed: d.ticketed,
    priceCents: d.priceCents,
    isLive: d.isLive,
    status: d.status,
    version: d.version,
  };
  // Un único upsert condicional: si dos versiones llegan a la vez y el documento no existe, la que pierde
  // la carrera recibe 11000 y repite la escritura condicional (así la versión mayor siempre gana).
  const filter = { _id: d.eventId, $or: [{ version: { $exists: false } }, { version: { $lt: d.version } }] };
  try {
    await db.collection('event_stats').updateOne(filter, { $set: meta, $setOnInsert: { createdAt: new Date() } }, { upsert: true });
  } catch (err) {
    if (err?.code !== 11000) throw err;
    await db.collection('event_stats').updateOne(filter, { $set: meta });
  }
  // Si el organizador ya se borró (lápida de account.deleted), el snapshot que llegó tarde no debe dejar su id.
  // Se comprueba después de escribir: cubre tanto el orden snapshot→borrado como borrado→snapshot.
  if (d.organizerId && (await db.collection('deleted_accounts').findOne({ _id: d.organizerId }))) {
    await db.collection('event_stats').updateOne({ _id: d.eventId }, { $set: { organizerId: null } });
  }
  if (e.eventType === 'event.published') {
    await incDaily(db, day(d.updatedAt), { eventsPublished: 1, ...(d.isLive ? { liveEventsPublished: 1 } : {}) });
  }
}

export const handlers = {
  // ---------------------------------------------------------------- cuentas
  'account.registered': async (d, e, db) => {
    await touchUser(db, d.userId, d.registeredAt);
    await incDaily(db, day(d.registeredAt), { registrations: 1, [`registrationsBy.${d.provider}`]: 1, [`roles.${d.role}`]: 1 });
  },

  // Ley 1581: se elimina el userId de los agregados.
  'account.deleted': async (d, e, db) => {
    // Lápida primero: un snapshot de evento que llegue tarde ya no puede volver a escribir este organizerId.
    await db.collection('deleted_accounts').updateOne(
      { _id: d.userId },
      { $setOnInsert: { deletedAt: new Date(d.deletedAt ?? Date.now()) } },
      { upsert: true },
    );
    await db.collection('user_activity').deleteOne({ _id: d.userId });
    await db.collection('merchant_stats').deleteOne({ _id: d.userId });
    await db.collection('event_stats').updateMany({ organizerId: d.userId }, { $set: { organizerId: null } });
    await incDaily(db, day(d.deletedAt), { accountsDeleted: 1 });
  },

  // ---------------------------------------------------------------- comercios y reportes
  'merchant.submitted': async (d, e, db) => {
    await writeUnlessDeleted(db, 'merchant_stats', d.userId, () =>
      db.collection('merchant_stats').updateOne(
        { _id: d.userId },
        { $set: { merchantId: d.merchantId, businessName: d.businessName, category: d.category, status: 'pending', submittedAt: new Date(d.submittedAt) } },
        { upsert: true },
      ),
    );
    await incDaily(db, day(d.submittedAt), { merchantsSubmitted: 1 });
  },
  'merchant.verified': async (d, e, db) => {
    await writeUnlessDeleted(db, 'merchant_stats', d.userId, () =>
      db.collection('merchant_stats').updateOne(
        { _id: d.userId },
        { $set: { merchantId: d.merchantId, businessName: d.businessName, category: d.category, status: 'verified', verifiedAt: new Date(d.verifiedAt) } },
        { upsert: true },
      ),
    );
    await incDaily(db, day(d.verifiedAt), { merchantsVerified: 1 });
  },
  'report.created': async (d, e, db) => {
    // resolvedAt va en $setOnInsert: si report.resolved llegó antes, la resolución ya registrada no se borra.
    await db.collection('reports').updateOne(
      { _id: d.reportId },
      { $set: { createdAt: new Date(d.createdAt), targetType: d.targetType, reason: d.reason }, $setOnInsert: { resolvedAt: null } },
      { upsert: true },
    );
    await incDaily(db, day(d.createdAt), { reports: 1, [`reportReasons.${d.reason}`]: 1 });
    // Si report.resolved llegó antes, no pudo medir el tiempo (no conocía la creación): se mide aquí.
    const after = await db.collection('reports').findOne({ _id: d.reportId });
    if (after?.resolvedAt && (await claimResolutionTime(db, d.reportId))) {
      await incDaily(db, day(after.resolvedAt), { reportResolutionMs: new Date(after.resolvedAt) - new Date(d.createdAt) });
    }
  },
  'report.resolved': async (d, e, db) => {
    const report = await db.collection('reports').findOneAndUpdate(
      { _id: d.reportId },
      { $set: { resolvedAt: new Date(d.resolvedAt), outcome: d.outcome } },
      { upsert: true, returnDocument: 'before', includeResultMetadata: false },
    );
    const inc = { reportsResolved: 1, [`reportOutcomes.${d.outcome}`]: 1 };
    if (report?.createdAt && (await claimResolutionTime(db, d.reportId))) {
      inc.reportResolutionMs = new Date(d.resolvedAt) - report.createdAt;
    }
    await incDaily(db, day(d.resolvedAt), inc);
  },

  // ---------------------------------------------------------------- eventos
  'event.published': eventSnapshot,
  'event.updated': eventSnapshot,
  'event.rescheduled': (d, e, db) => incEvent(db, d.eventId, { rescheduledCount: 1 }),
  // Versionado como el snapshot: un event.updated atrasado (versión menor) no reabre un evento cancelado.
  'event.cancelled': (d, e, db) => setEventVersioned(db, d.eventId, d.version, { status: 'cancelled', cancelledAt: new Date(d.cancelledAt) }),
  'event.sold_out': (d, e, db) => incEvent(db, d.eventId, {}, { soldOutAt: new Date(d.soldOutAt) }),
  'event.expired': (d, e, db) => incEvent(db, d.eventId, {}, { status: 'expired', expiredAt: new Date(d.expiredAt) }),
  // Orden: primero lo idempotente (touchUser), al final el único $inc. Si falla algo a mitad, el
  // reintento repite touchUser sin efecto extra y el incremento se aplica una sola vez.
  'engagement.viewed': async (d, e, db) => {
    await touchUser(db, d.viewerId, d.viewedAt);
    await incEvent(db, d.eventId, { 'views.total': 1, [`views.${d.source}`]: 1 });
  },
  'engagement.shared': async (d, e, db) => {
    await touchUser(db, d.sharedBy, d.sharedAt);
    await incEvent(db, d.eventId, { shares: 1 });
  },

  // ---------------------------------------------------------------- asistencia
  // attendeeCount trae el total vigente: se guarda solo si su attendanceVersion es mayor (ver setAttendeeCount);
  // además se cuentan las altas y las bajas.
  'attendance.confirmed': async (d, e, db) => {
    await touchUser(db, d.userId, d.confirmedAt);
    await incEvent(db, d.eventId, { [d.source === 'ticket' ? 'confirmations.ticket' : 'confirmations.rsvp']: 1 });
    await setAttendeeCount(db, d.eventId, d.attendeeCount, d.attendanceVersion);
  },
  'attendance.cancelled': async (d, e, db) => {
    await incEvent(db, d.eventId, { cancellations: 1, [`cancellationReasons.${d.reason}`]: 1 });
    await setAttendeeCount(db, d.eventId, d.attendeeCount, d.attendanceVersion);
  },
  'pass.checked_in': async (d, e, db) => {
    await touchUser(db, d.userId, d.checkedInAt);
    await incEvent(db, d.eventId, { checkIns: 1 });
  },
  'pass.check_in_rejected': (d, e, db) => incEvent(db, d.eventId, { 'checkInRejected.total': 1, [`checkInRejected.${d.reason}`]: 1 }),

  // ---------------------------------------------------------------- pagos (embudo e ingresos)
  'order.created': (d, e, db) => incEvent(db, d.eventId, { 'orders.created': 1 }),
  'order.approved': async (d, e, db) => {
    await incEvent(db, d.eventId, {
      'orders.approved': 1,
      ticketsSold: d.quantity,
      grossCents: d.subtotalCents,
      commissionCents: d.commissionCents,
      [`paymentMethods.${d.paymentMethod}`]: 1,
    });
    await incDaily(db, day(d.approvedAt), { ticketsSold: d.quantity, grossCents: d.subtotalCents, commissionCents: d.commissionCents });
  },
  'order.failed': (d, e, db) => incEvent(db, d.eventId, { 'orders.failed': 1 }),
  'order.expired': (d, e, db) => incEvent(db, d.eventId, { 'orders.expired': 1 }),
  'order.refund_requested': (d, e, db) => incEvent(db, d.eventId, { 'refunds.requested': 1, 'refunds.pendingCents': d.amountCents }),
  'order.refunded': async (d, e, db) => {
    await incEvent(db, d.eventId, { 'refunds.done': 1, 'refunds.refundedCents': d.amountCents, 'refunds.pendingCents': -d.amountCents });
    await incDaily(db, day(d.refundedAt), { refundedCents: d.amountCents });
  },
  'payout.recorded': async (d, e, db) => {
    await writeUnlessDeleted(db, 'merchant_stats', d.organizerId, () =>
      applyOnce(db, 'merchant_stats', d.organizerId, { payoutsCents: d.amountCents, payoutCount: 1 }, { merchantId: d.merchantId, lastPayoutAt: new Date(d.paidAt) }),
    );
  },

  // ---------------------------------------------------------------- IA y push
  'description.generated': async (d, e, db) => {
    await incDaily(db, day(d.generatedAt), { aiDescriptions: 1, aiInputTokens: d.inputTokens, aiOutputTokens: d.outputTokens });
    await writeUnlessDeleted(db, 'merchant_stats', d.requestedBy, () =>
      applyOnce(db, 'merchant_stats', d.requestedBy, { aiDescriptions: 1, aiTokens: d.inputTokens + d.outputTokens }),
    );
  },
  'recommendation.served': async (d, e, db) => {
    await incDaily(db, day(d.servedAt), { aiRecommendations: 1, aiInputTokens: d.inputTokens, aiOutputTokens: d.outputTokens });
    for (const eventId of d.eventIds) await incEvent(db, eventId, { recommended: 1 });
    await touchUser(db, d.userId, d.servedAt);
  },
  'push.sent': async (d, e, db) => {
    if (d.eventId) await incEvent(db, d.eventId, { pushReach: d.recipientCount, [`pushByKind.${d.kind}`]: d.recipientCount });
    await incDaily(db, day(d.sentAt), { pushSent: d.recipientCount, pushFailed: d.failedCount });
  },
};
