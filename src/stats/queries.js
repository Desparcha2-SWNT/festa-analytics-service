import { getDb } from '../db/mongo.js';

const n = (v) => v ?? 0;

/** Vista de un evento para su organizador (RF15: conteos simples, no un dashboard con gráficas). */
export function toEventSummary(s) {
  const views = s.views ?? {};
  const orders = s.orders ?? {};
  return {
    eventId: s._id,
    title: s.title ?? null,
    category: s.category ?? null,
    startsAt: s.startsAt?.toISOString() ?? null,
    status: s.status ?? null,
    isLive: Boolean(s.isLive),
    capacity: s.capacity ?? null,
    attendees: n(s.attendeeCount),
    checkIns: n(s.checkIns),
    // Tasa de asistencia real: ingresos / confirmados.
    showUpRate: s.attendeeCount ? Math.round((n(s.checkIns) / s.attendeeCount) * 100) / 100 : null,
    views: n(views.total),
    viewsBySource: Object.fromEntries(Object.entries(views).filter(([k]) => k !== 'total')),
    shares: n(s.shares),
    recommendedByAi: n(s.recommended),
    pushReach: n(s.pushReach),
    ticketsSold: n(s.ticketsSold),
    grossCents: n(s.grossCents),
    refundedCents: n(s.refunds?.refundedCents),
    orders: { created: n(orders.created), approved: n(orders.approved), failed: n(orders.failed), expired: n(orders.expired) },
    checkInRejected: n(s.checkInRejected?.total),
    soldOutAt: s.soldOutAt?.toISOString() ?? null,
  };
}

/** RF15 / RN5: panel del organizador (comercio o usuario) con totales y detalle por evento. */
export async function organizerDashboard(userId, db = getDb()) {
  const events = await db.collection('event_stats').find({ organizerId: userId }).sort({ startsAt: -1 }).limit(200).toArray();
  const merchant = await db.collection('merchant_stats').findOne({ _id: userId });
  const items = events.map(toEventSummary);
  const sum = (key) => items.reduce((acc, e) => acc + e[key], 0);
  return {
    merchant: merchant
      ? {
          merchantId: merchant.merchantId ?? null,
          businessName: merchant.businessName ?? null,
          status: merchant.status ?? null,
          payoutsCents: n(merchant.payoutsCents),
          payoutCount: n(merchant.payoutCount),
          aiDescriptions: n(merchant.aiDescriptions),
        }
      : null,
    totals: {
      events: items.length,
      attendees: sum('attendees'),
      checkIns: sum('checkIns'),
      views: sum('views'),
      shares: sum('shares'),
      recommendedByAi: sum('recommendedByAi'),
      pushReach: sum('pushReach'),
      ticketsSold: sum('ticketsSold'),
      grossCents: sum('grossCents'),
    },
    events: items,
  };
}

export async function eventStats(eventId, db = getDb()) {
  return db.collection('event_stats').findOne({ _id: eventId });
}

/**
 * "Usan la app de forma recurrente": activos en 3 o más días distintos DENTRO del periodo. activeDays
 * guarda días de cualquier época ('YYYY-MM-DD', se comparan como texto), así que se filtran al rango.
 */
async function recurringUsers(activity, since, sinceDay) {
  const [row] = await activity
    .aggregate([
      { $match: { lastSeen: { $gte: since } } },
      {
        $project: {
          days: { $size: { $filter: { input: { $ifNull: ['$activeDays', []] }, cond: { $gte: ['$$this', sinceDay] } } } },
        },
      },
      { $match: { days: { $gte: 3 } } },
      { $count: 'n' },
    ])
    .toArray();
  return row?.n ?? 0;
}

/** RN4: KPIs del piloto para el equipo (admin). */
export async function adminOverview({ days = 30 } = {}, db = getDb()) {
  const since = new Date(Date.now() - days * 86_400_000);
  const sinceDay = since.toISOString().slice(0, 10);

  const daily = await db.collection('platform_daily').find({ _id: { $gte: sinceDay } }).toArray();
  const totals = {};
  for (const d of daily) {
    for (const [k, v] of Object.entries(d)) if (typeof v === 'number') totals[k] = (totals[k] ?? 0) + v;
  }

  const activity = db.collection('user_activity');
  const weekAgo = new Date(Date.now() - 7 * 86_400_000);
  const [active7d, active30d, recurring] = await Promise.all([
    activity.countDocuments({ lastSeen: { $gte: weekAgo } }),
    activity.countDocuments({ lastSeen: { $gte: since } }),
    recurringUsers(activity, since, sinceDay),
  ]);

  const merchants = db.collection('merchant_stats');
  const [merchantsPending, merchantsVerified, openReports] = await Promise.all([
    merchants.countDocuments({ status: 'pending' }),
    merchants.countDocuments({ status: 'verified' }),
    db.collection('reports').countDocuments({ resolvedAt: null }),
  ]);

  const topEvents = await db.collection('event_stats').find({ startsAt: { $gte: since } }).sort({ attendeeCount: -1 }).limit(10).toArray();

  return {
    periodDays: days,
    users: { registrations: n(totals.registrations), active7d, active30d, recurring },
    merchants: { pending: merchantsPending, verified: merchantsVerified, submitted: n(totals.merchantsSubmitted) },
    events: { published: n(totals.eventsPublished), liveEventsPublished: n(totals.liveEventsPublished) },
    sales: {
      ticketsSold: n(totals.ticketsSold),
      grossCents: n(totals.grossCents),
      commissionCents: n(totals.commissionCents),
      refundedCents: n(totals.refundedCents),
    },
    moderation: {
      reports: n(totals.reports),
      resolved: n(totals.reportsResolved),
      open: openReports,
      avgResolutionHours: totals.reportsResolved ? Math.round((n(totals.reportResolutionMs) / totals.reportsResolved / 3_600_000) * 10) / 10 : null,
    },
    ai: {
      descriptions: n(totals.aiDescriptions),
      recommendations: n(totals.aiRecommendations),
      inputTokens: n(totals.aiInputTokens),
      outputTokens: n(totals.aiOutputTokens),
    },
    push: { sent: n(totals.pushSent), failed: n(totals.pushFailed) },
    topEvents: topEvents.map(toEventSummary),
  };
}
