// Agregados de analytics a partir de los eventos del contrato (contracts/README.md § analytics).
// Firma: (data, envelope, db). Los contadores usan $inc; el inbox deduplica por eventId.

const day = (iso) => new Date(iso ?? Date.now()).toISOString().slice(0, 10);

const incEvent = (db, eventId, inc, set = {}) =>
  db.collection('event_stats').updateOne(
    { _id: eventId },
    { $inc: inc, $set: { ...set, updatedAt: new Date() } },
    { upsert: true },
  );

const incDaily = (db, date, inc) => db.collection('platform_daily').updateOne({ _id: date }, { $inc: inc }, { upsert: true });

/** RN4: actividad por usuario y día (usuarios recurrentes vs. que solo descargan). */
const touchUser = (db, userId, iso) =>
  userId
    ? db.collection('user_activity').updateOne(
        { _id: userId },
        { $addToSet: { activeDays: day(iso) }, $min: { firstSeen: new Date(iso ?? Date.now()) }, $max: { lastSeen: new Date(iso ?? Date.now()) } },
        { upsert: true },
      )
    : undefined;

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
  const res = await db.collection('event_stats').updateOne(
    { _id: d.eventId, $or: [{ version: { $exists: false } }, { version: { $lt: d.version } }] },
    { $set: meta, $setOnInsert: { createdAt: new Date() } },
  );
  if (res.matchedCount === 0) {
    // No existe todavía, o existe con una versión >= (mensaje viejo): se inserta solo si no existe.
    await db.collection('event_stats').updateOne({ _id: d.eventId }, { $setOnInsert: { ...meta, createdAt: new Date() } }, { upsert: true });
  }
  if (e.eventType === 'event.published') {
    await incDaily(db, day(d.updatedAt), { eventsPublished: 1, ...(d.isLive ? { liveEventsPublished: 1 } : {}) });
  }
}

export const handlers = {
  // ---------------------------------------------------------------- cuentas
  'account.registered': async (d, e, db) => {
    await incDaily(db, day(d.registeredAt), { registrations: 1, [`registrationsBy.${d.provider}`]: 1, [`roles.${d.role}`]: 1 });
    await touchUser(db, d.userId, d.registeredAt);
  },

  // Ley 1581: se elimina el userId de los agregados.
  'account.deleted': async (d, e, db) => {
    await db.collection('user_activity').deleteOne({ _id: d.userId });
    await db.collection('merchant_stats').deleteOne({ _id: d.userId });
    await db.collection('event_stats').updateMany({ organizerId: d.userId }, { $set: { organizerId: null } });
    await incDaily(db, day(d.deletedAt), { accountsDeleted: 1 });
  },

  // ---------------------------------------------------------------- comercios y reportes
  'merchant.submitted': async (d, e, db) => {
    await db.collection('merchant_stats').updateOne(
      { _id: d.userId },
      { $set: { merchantId: d.merchantId, businessName: d.businessName, category: d.category, status: 'pending', submittedAt: new Date(d.submittedAt) } },
      { upsert: true },
    );
    await incDaily(db, day(d.submittedAt), { merchantsSubmitted: 1 });
  },
  'merchant.verified': async (d, e, db) => {
    await db.collection('merchant_stats').updateOne(
      { _id: d.userId },
      { $set: { merchantId: d.merchantId, businessName: d.businessName, category: d.category, status: 'verified', verifiedAt: new Date(d.verifiedAt) } },
      { upsert: true },
    );
    await incDaily(db, day(d.verifiedAt), { merchantsVerified: 1 });
  },
  'report.created': async (d, e, db) => {
    await db.collection('reports').updateOne({ _id: d.reportId }, { $set: { createdAt: new Date(d.createdAt), targetType: d.targetType, reason: d.reason, resolvedAt: null } }, { upsert: true });
    await incDaily(db, day(d.createdAt), { reports: 1, [`reportReasons.${d.reason}`]: 1 });
  },
  'report.resolved': async (d, e, db) => {
    const report = await db.collection('reports').findOneAndUpdate(
      { _id: d.reportId },
      { $set: { resolvedAt: new Date(d.resolvedAt), outcome: d.outcome } },
      { upsert: true, returnDocument: 'before' },
    );
    const inc = { reportsResolved: 1, [`reportOutcomes.${d.outcome}`]: 1 };
    if (report?.createdAt) inc.reportResolutionMs = new Date(d.resolvedAt) - report.createdAt;
    await incDaily(db, day(d.resolvedAt), inc);
  },

  // ---------------------------------------------------------------- eventos
  'event.published': eventSnapshot,
  'event.updated': eventSnapshot,
  'event.rescheduled': (d, e, db) => incEvent(db, d.eventId, { rescheduledCount: 1 }),
  'event.cancelled': (d, e, db) => incEvent(db, d.eventId, {}, { status: 'cancelled', cancelledAt: new Date(d.cancelledAt) }),
  'event.sold_out': (d, e, db) => incEvent(db, d.eventId, {}, { soldOutAt: new Date(d.soldOutAt) }),
  'event.expired': (d, e, db) => incEvent(db, d.eventId, {}, { status: 'expired', expiredAt: new Date(d.expiredAt) }),
  'engagement.viewed': async (d, e, db) => {
    await incEvent(db, d.eventId, { 'views.total': 1, [`views.${d.source}`]: 1 });
    await touchUser(db, d.viewerId, d.viewedAt);
  },
  'engagement.shared': async (d, e, db) => {
    await incEvent(db, d.eventId, { shares: 1 });
    await touchUser(db, d.sharedBy, d.sharedAt);
  },

  // ---------------------------------------------------------------- asistencia
  // attendeeCount trae el total vigente; se guarda tal cual (no se suma) y además se cuentan las altas.
  'attendance.confirmed': async (d, e, db) => {
    await incEvent(
      db,
      d.eventId,
      { [d.source === 'ticket' ? 'confirmations.ticket' : 'confirmations.rsvp']: 1 },
      { attendeeCount: d.attendeeCount },
    );
    await touchUser(db, d.userId, d.confirmedAt);
  },
  'attendance.cancelled': (d, e, db) => incEvent(db, d.eventId, { cancellations: 1, [`cancellationReasons.${d.reason}`]: 1 }, { attendeeCount: d.attendeeCount }),
  'pass.checked_in': async (d, e, db) => {
    await incEvent(db, d.eventId, { checkIns: 1 });
    await touchUser(db, d.userId, d.checkedInAt);
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
    await db.collection('merchant_stats').updateOne(
      { _id: d.organizerId },
      { $inc: { payoutsCents: d.amountCents, payoutCount: 1 }, $set: { merchantId: d.merchantId, lastPayoutAt: new Date(d.paidAt) } },
      { upsert: true },
    );
  },

  // ---------------------------------------------------------------- IA y push
  'description.generated': async (d, e, db) => {
    await incDaily(db, day(d.generatedAt), { aiDescriptions: 1, aiInputTokens: d.inputTokens, aiOutputTokens: d.outputTokens });
    await db.collection('merchant_stats').updateOne(
      { _id: d.requestedBy },
      { $inc: { aiDescriptions: 1, aiTokens: d.inputTokens + d.outputTokens } },
      { upsert: true },
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
