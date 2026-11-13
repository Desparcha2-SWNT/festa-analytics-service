import { Router } from 'express';
import rateLimit from 'express-rate-limit';
import { z } from 'zod';
import { requireAdmin, requireAuth } from '../middleware/auth.js';
import { adminOverview, eventStats, organizerDashboard, toEventSummary } from './queries.js';

export const analyticsRouter = Router();
analyticsRouter.use(rateLimit({ windowMs: 60_000, limit: 120, standardHeaders: true, legacyHeaders: false }));
analyticsRouter.use(requireAuth);

// RF15: panel del comercio u organizador autenticado.
analyticsRouter.get('/merchants/me', async (req, res, next) => {
  try {
    res.json(await organizerDashboard(req.auth.userId));
  } catch (err) {
    next(err);
  }
});

// Detalle de un evento: solo su organizador o un admin.
analyticsRouter.get('/events/:id', async (req, res, next) => {
  if (!z.uuid().safeParse(req.params.id).success) return res.status(400).json({ error: 'id inválido' });
  try {
    const stats = await eventStats(req.params.id);
    if (!stats || (stats.organizerId !== req.auth.userId && req.auth.role !== 'admin')) {
      return res.status(404).json({ error: 'evento no encontrado' });
    }
    res.json(toEventSummary(stats));
  } catch (err) {
    next(err);
  }
});

// RN4: KPIs del piloto.
analyticsRouter.get('/admin/overview', requireAdmin, async (req, res, next) => {
  const days = z.coerce.number().int().min(1).max(365).default(30).safeParse(req.query.days);
  if (!days.success) return res.status(400).json({ error: 'days inválido' });
  try {
    res.json(await adminOverview({ days: days.data }));
  } catch (err) {
    next(err);
  }
});
