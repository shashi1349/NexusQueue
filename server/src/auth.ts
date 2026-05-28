import type { Request, Response, NextFunction } from 'express';
import crypto from 'node:crypto';
import jwt from 'jsonwebtoken';

/**
 * Constant-time string comparison. Returns true if a === b without leaking
 * timing information about which characters differ.
 */
function timingSafeEqual(a: string, b: string): boolean {
  const bufA = Buffer.from(a);
  const bufB = Buffer.from(b);
  if (bufA.length !== bufB.length) {
    // Compare against self to keep constant time regardless of length mismatch
    crypto.timingSafeEqual(bufA, bufA);
    return false;
  }
  return crypto.timingSafeEqual(bufA, bufB);
}

/**
 * API Key middleware. If API_KEYS env var is set, requires a valid key
 * in Authorization: Bearer <key> or X-API-Key header.
 * If API_KEYS is not set, auth is disabled and requests pass through.
 */
export function apiKeyMiddleware(req: Request, res: Response, next: NextFunction): void {
  const apiKeysRaw = process.env.API_KEYS;
  if (!apiKeysRaw) {
    next();
    return;
  }

  const validKeys = apiKeysRaw.split(',').map((k) => k.trim()).filter(Boolean);
  if (validKeys.length === 0) {
    next();
    return;
  }

  const authHeader = req.headers.authorization;
  const xApiKey = req.headers['x-api-key'] as string | undefined;

  let key: string | undefined;
  if (authHeader?.startsWith('Bearer ')) {
    key = authHeader.slice(7);
  } else if (xApiKey) {
    key = xApiKey;
  }

  if (!key) {
    res.status(401).json({ error: 'unauthorized', message: 'Invalid or missing API key' });
    return;
  }

  let matched = false;
  for (const validKey of validKeys) {
    if (timingSafeEqual(key, validKey)) {
      matched = true;
    }
  }

  if (!matched) {
    res.status(401).json({ error: 'unauthorized', message: 'Invalid or missing API key' });
    return;
  }

  next();
}

/**
 * JWT middleware. If JWT_SECRET env var is set, requires a valid JWT
 * in Authorization: Bearer <token> header.
 * If JWT_SECRET is not set, auth is disabled and requests pass through.
 */
export function jwtMiddleware(req: Request, res: Response, next: NextFunction): void {
  const secret = process.env.JWT_SECRET;
  if (!secret) {
    next();
    return;
  }

  const authHeader = req.headers.authorization;
  if (!authHeader?.startsWith('Bearer ')) {
    res.status(401).json({ error: 'unauthorized', message: 'Missing or invalid token' });
    return;
  }

  const token = authHeader.slice(7);
  try {
    jwt.verify(token, secret, { algorithms: ['HS256'] });
    next();
  } catch {
    res.status(401).json({ error: 'unauthorized', message: 'Invalid token' });
  }
}

/**
 * POST /auth/login handler.
 * Validates { username, password } against DASHBOARD_USER and DASHBOARD_PASSWORD env vars.
 * Returns a signed JWT with 24h expiry.
 * If env vars are not set, returns 501.
 */
export function loginHandler(req: Request, res: Response): void {
  const secret = process.env.JWT_SECRET;
  const dashUser = process.env.DASHBOARD_USER;
  const dashPass = process.env.DASHBOARD_PASSWORD;

  if (!secret || !dashUser || !dashPass) {
    res.status(501).json({ error: 'not_configured', message: 'Auth is not configured' });
    return;
  }

  const { username, password } = req.body as { username?: string; password?: string };
  if (!username || !password) {
    res.status(400).json({ error: 'invalid_request', message: 'username and password required' });
    return;
  }

  if (!timingSafeEqual(username, dashUser) || !timingSafeEqual(password, dashPass)) {
    res.status(401).json({ error: 'unauthorized', message: 'Invalid credentials' });
    return;
  }

  const token = jwt.sign({ sub: username }, secret, { expiresIn: '24h' });
  res.json({ token });
}
