/**
 * Reddwire polling agent — runs as a GitHub Actions cron.
 *
 * Why this exists: Cloudflare Workers can't fetch reddit.com/.json from the
 * Cloudflare edge — Reddit blanket-blocks Cloudflare's datacenter IPs with a
 * 403. GitHub Actions runners use Azure IPs that Reddit accepts (millions of
 * legitimate workflows hit Reddit from GitHub every day; blocking them would
 * break too much of the open-source ecosystem).
 *
 * Flow per tick:
 *   1. GET  api.reddwire.dev/api/internal/poll-queue   → due monitors
 *   2. For each: fetch reddit.com/r/{sub}/new.json
 *   3. POST api.reddwire.dev/api/internal/poll-result  → batched raw posts
 *      The Worker handles dedup, keyword filtering, and webhook delivery.
 *
 * No user data (emails, webhook URLs, keywords) is ever exposed to this
 * runtime. Public repo, no leakage. Authentication via REDDWIRE_INTERNAL_SECRET
 * (GitHub Actions secret matching the Worker's wrangler secret).
 */

import { setTimeout as sleep } from 'node:timers/promises';

const API = process.env.REDDWIRE_API_URL || 'https://api.reddwire.dev';
const SECRET = process.env.REDDWIRE_INTERNAL_SECRET;
const USER_AGENT = 'Reddwire/0.1.0 (+https://reddwire.dev)';
const REDDIT_FETCH_LIMIT = 25;
const INTER_REQUEST_DELAY_MS = 500;

if (!SECRET) {
	console.error('REDDWIRE_INTERNAL_SECRET env var is required');
	process.exit(1);
}

const ts = () => new Date().toISOString();

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

async function fetchSubreddit(subreddit) {
	const clean = subreddit
		.trim()
		.replace(/^\/?r\//i, '')
		.replace(/^\/+|\/+$/g, '');
	const url = `https://www.reddit.com/r/${encodeURIComponent(clean)}/new.json?limit=${REDDIT_FETCH_LIMIT}`;
	const response = await fetch(url, { headers: { 'User-Agent': USER_AGENT } });
	if (!response.ok) {
		throw new Error(`Reddit returned ${response.status} for r/${clean}`);
	}
	const data = await response.json();
	const children = data?.data?.children ?? [];
	return children
		.map((c) => c?.data)
		.filter((p) => p && typeof p.id === 'string');
}

async function main() {
	console.log(`[${ts()}] Reddwire poll tick`);

	const queue = await authJson(`${API}/api/internal/poll-queue`);
	const monitors = Array.isArray(queue?.monitors) ? queue.monitors : [];
	console.log(`  Queue: ${monitors.length} monitor(s) due`);

	if (monitors.length === 0) return;

	const results = [];
	for (const monitor of monitors) {
		try {
			const posts = await fetchSubreddit(monitor.subreddit);
			results.push({ monitor_id: monitor.id, posts });
			console.log(`  ✓ r/${monitor.subreddit}: ${posts.length} posts`);
		} catch (err) {
			const message = err instanceof Error ? err.message : String(err);
			results.push({ monitor_id: monitor.id, error: message });
			console.error(`  ✗ r/${monitor.subreddit}: ${message}`);
		}
		// Be polite to Reddit, even though we're well under the rate limit.
		await sleep(INTER_REQUEST_DELAY_MS);
	}

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
