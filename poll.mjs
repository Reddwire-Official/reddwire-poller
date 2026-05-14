/**
 * Reddwire polling agent — runs as a GitHub Actions cron.
 *
 * Why this exists: Cloudflare Workers can't fetch reddit.com from the CF edge
 * (CF IPs blanket-blocked). GitHub Actions IPs USED to work but Reddit ramped
 * up bot detection through 2026 — anonymous requests from cloud IPs now 403
 * on both www.reddit.com and old.reddit.com. The durable fix is OAuth, which
 * Reddit's rate limit is generous on (60 req/min, 600 req/10min).
 *
 * Auth modes (in order of preference):
 *   1. OAuth (script app, password grant) — preferred. Requires
 *      REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USERNAME, REDDIT_PASSWORD.
 *      Reddit returns Bearer tokens valid for ~1 hour. We cache in-memory
 *      across the per-monitor loop within a single workflow run.
 *   2. Anonymous — fallback if no OAuth creds set. Works locally; usually
 *      403s from GitHub Actions runners. Last-resort only.
 *
 * Flow per tick:
 *   1. GET  api.reddwire.dev/api/internal/poll-queue   → due monitors
 *   2. Dedup subreddits (8 monitors on r/news = 1 Reddit call, not 8)
 *   3. Fetch each unique subreddit's /new feed
 *   4. POST api.reddwire.dev/api/internal/poll-result  → batched raw posts
 */

import { setTimeout as sleep } from 'node:timers/promises';

const API = process.env.REDDWIRE_API_URL || 'https://api.reddwire.dev';
const SECRET = process.env.REDDWIRE_INTERNAL_SECRET;

const REDDIT_CLIENT_ID = process.env.REDDIT_CLIENT_ID;
const REDDIT_CLIENT_SECRET = process.env.REDDIT_CLIENT_SECRET;
const REDDIT_USERNAME = process.env.REDDIT_USERNAME;
const REDDIT_PASSWORD = process.env.REDDIT_PASSWORD;
const HAS_OAUTH =
	!!REDDIT_CLIENT_ID && !!REDDIT_CLIENT_SECRET && !!REDDIT_USERNAME && !!REDDIT_PASSWORD;

// Reddit's docs require a unique, descriptive UA that identifies who you
// are and how to reach you. Required for OAuth, recommended for anonymous.
const USER_AGENT = 'web:reddwire-poller:0.2.0 (by /u/reddwire)';
const REDDIT_FETCH_LIMIT = 25;
const INTER_REQUEST_DELAY_MS = 500;
const ANON_REDDIT_HOSTS = ['www.reddit.com', 'old.reddit.com', 'reddit.com'];

if (!SECRET) {
	console.error('REDDWIRE_INTERNAL_SECRET env var is required');
	process.exit(1);
}

const ts = () => new Date().toISOString();

// ─── Reddwire Worker API (Bearer INTERNAL_SECRET) ────────────────────────

async function authJson(url, init = {}) {
	const headers = {
		Authorization: `Bearer ${SECRET}`,
		'Content-Type': 'application/json',
		...(init.headers ?? {}),
	};
	const response = await fetch(url, { ...init, headers });
	const text = await response.text();
	if (!response.ok) {
		throw new Error(`${init.method ?? 'GET'} ${url} → ${response.status}: ${text.slice(0, 200)}`);
	}
	return text ? JSON.parse(text) : null;
}

// ─── Reddit OAuth ────────────────────────────────────────────────────────

let cachedOauthToken = null;
let cachedOauthExpiresAt = 0;

async function getRedditOauthToken() {
	if (cachedOauthToken && Date.now() < cachedOauthExpiresAt - 60_000) {
		return cachedOauthToken;
	}
	const basic = Buffer.from(`${REDDIT_CLIENT_ID}:${REDDIT_CLIENT_SECRET}`).toString('base64');
	const response = await fetch('https://www.reddit.com/api/v1/access_token', {
		method: 'POST',
		headers: {
			Authorization: `Basic ${basic}`,
			'Content-Type': 'application/x-www-form-urlencoded',
			'User-Agent': USER_AGENT,
		},
		body: new URLSearchParams({
			grant_type: 'password',
			username: REDDIT_USERNAME,
			password: REDDIT_PASSWORD,
		}),
	});
	const text = await response.text();
	if (!response.ok) {
		throw new Error(`Reddit OAuth → ${response.status}: ${text.slice(0, 200)}`);
	}
	const data = JSON.parse(text);
	if (!data.access_token) throw new Error(`Reddit OAuth: no token in response`);
	cachedOauthToken = data.access_token;
	// expires_in is seconds; default ~3600. Subtract 60s grace.
	cachedOauthExpiresAt = Date.now() + (data.expires_in ?? 3600) * 1000;
	return cachedOauthToken;
}

// ─── Subreddit fetchers ──────────────────────────────────────────────────

async function fetchSubredditOauth(subreddit) {
	const token = await getRedditOauthToken();
	const url = `https://oauth.reddit.com/r/${encodeURIComponent(subreddit)}/new?limit=${REDDIT_FETCH_LIMIT}&raw_json=1`;
	const response = await fetch(url, {
		headers: {
			Authorization: `Bearer ${token}`,
			'User-Agent': USER_AGENT,
		},
	});
	if (!response.ok) {
		const text = await response.text();
		throw new Error(`oauth.reddit.com → ${response.status}: ${text.slice(0, 120)}`);
	}
	const data = await response.json();
	const children = data?.data?.children ?? [];
	return children.map((c) => c?.data).filter((p) => p && typeof p.id === 'string');
}

async function fetchSubredditAnonymous(subreddit) {
	let lastError;
	for (const host of ANON_REDDIT_HOSTS) {
		const url = `https://${host}/r/${encodeURIComponent(subreddit)}/new.json?limit=${REDDIT_FETCH_LIMIT}&raw_json=1`;
		try {
			const response = await fetch(url, {
				headers: {
					'User-Agent': USER_AGENT,
					Accept: 'application/json, text/javascript, */*; q=0.01',
					'Accept-Language': 'en-US,en;q=0.9',
				},
			});
			if (!response.ok) {
				lastError = new Error(`${host} → ${response.status}`);
				continue;
			}
			const data = await response.json();
			const children = data?.data?.children ?? [];
			return children.map((c) => c?.data).filter((p) => p && typeof p.id === 'string');
		} catch (err) {
			lastError = err instanceof Error ? err : new Error(String(err));
		}
	}
	throw new Error(`All hosts failed for r/${subreddit}: ${lastError?.message ?? 'unknown'}`);
}

async function fetchSubreddit(subreddit) {
	const clean = subreddit
		.trim()
		.replace(/^\/?r\//i, '')
		.replace(/^\/+|\/+$/g, '');
	return HAS_OAUTH ? fetchSubredditOauth(clean) : fetchSubredditAnonymous(clean);
}

// ─── Main ────────────────────────────────────────────────────────────────

async function main() {
	console.log(`[${ts()}] Reddwire poll tick — auth: ${HAS_OAUTH ? 'OAuth' : 'anonymous'}`);

	const queue = await authJson(`${API}/api/internal/poll-queue`);
	const monitors = Array.isArray(queue?.monitors) ? queue.monitors : [];
	console.log(`  Queue: ${monitors.length} monitor(s) due`);

	if (monitors.length === 0) return;

	// Dedup by subreddit: many monitors may watch the same subreddit. Fetch
	// each once and distribute. Cuts request count + reduces 403 risk.
	const uniqueSubs = [...new Set(monitors.map((m) => m.subreddit))];
	console.log(`  Unique subreddits: ${uniqueSubs.length}`);
	const fetchCache = {};
	for (const sub of uniqueSubs) {
		try {
			fetchCache[sub] = { posts: await fetchSubreddit(sub) };
			console.log(`  ✓ r/${sub}: ${fetchCache[sub].posts.length} posts`);
		} catch (err) {
			const message = err instanceof Error ? err.message : String(err);
			fetchCache[sub] = { error: message };
			console.error(`  ✗ r/${sub}: ${message}`);
		}
		await sleep(INTER_REQUEST_DELAY_MS);
	}

	const results = monitors.map((m) => {
		const cached = fetchCache[m.subreddit];
		if (cached.error) return { monitor_id: m.id, error: cached.error };
		return { monitor_id: m.id, posts: cached.posts };
	});

	const summary = await authJson(`${API}/api/internal/poll-result`, {
		method: 'POST',
		body: JSON.stringify({ results }),
	});

	console.log(
		`  Summary: delivered=${summary.delivered}, deduped=${summary.deduped}, errors=${summary.errors}, webhookFailures=${summary.webhookFailures}`,
	);
}

main().catch((err) => {
	console.error('FATAL:', err);
	process.exit(1);
});
