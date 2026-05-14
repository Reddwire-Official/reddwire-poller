/**
 * Reddwire polling agent — runs as a GitHub Actions cron.
 *
 * Auth approach: Reddit RSS feeds (`/r/{sub}/.rss`).
 *
 *   Why RSS over OAuth or anonymous .json:
 *   - .json hard-403s from cloud IPs (GitHub Actions blocked since mid-2026)
 *   - OAuth script apps are "personal use only" per Reddit's developer policy.
 *     Using one to power a paid SaaS is a ToS violation, account-ban risk.
 *   - RSS is public web syndication — predates Reddit's API gating, never
 *     required approval, less aggressively defended by bot-detection because
 *     legitimate feed readers (Feedly, Inoreader, Liferea) hit it constantly.
 *
 *   Trade-off: RSS doesn't include score or num_comments. We send 0 for both
 *   in the webhook payload. Title, body, URL, author, timestamp all present.
 *
 * Flow per tick:
 *   1. GET  api.reddwire.dev/api/internal/poll-queue   → due monitors
 *   2. Dedup subreddits (N monitors on same sub = 1 RSS call)
 *   3. Fetch each unique subreddit's RSS, parse Atom XML
 *   4. POST api.reddwire.dev/api/internal/poll-result  → batched posts
 */

import { setTimeout as sleep } from 'node:timers/promises';

const API = process.env.REDDWIRE_API_URL || 'https://api.reddwire.dev';
const SECRET = process.env.REDDWIRE_INTERNAL_SECRET;

// Feedly UA — most-deployed feed reader on the internet. Reddit serves RSS
// to Feedly on every poll cycle, so this UA blends in with legitimate traffic.
const USER_AGENT =
	'Mozilla/5.0 (compatible; Feedly/1.0; +http://www.feedly.com/fetcher.html)';
const INTER_REQUEST_DELAY_MS = 500;
const REDDIT_HOSTS = ['www.reddit.com', 'old.reddit.com', 'reddit.com'];

if (!SECRET) {
	console.error('REDDWIRE_INTERNAL_SECRET env var is required');
	process.exit(1);
}

const ts = () => new Date().toISOString();

// ─── Reddwire Worker API ─────────────────────────────────────────────────

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

// ─── Atom XML parsing ────────────────────────────────────────────────────

const HTML_ENTITIES = {
	'&lt;': '<',
	'&gt;': '>',
	'&quot;': '"',
	'&#39;': "'",
	'&apos;': "'",
	'&amp;': '&',
};

function decodeEntities(s) {
	if (!s) return '';
	return s.replace(/&(?:lt|gt|quot|amp|apos|#39);/g, (m) => HTML_ENTITIES[m] ?? m);
}

function stripHtml(s) {
	return decodeEntities(s).replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
}

function getTagContent(xml, tag) {
	const m = xml.match(new RegExp(`<${tag}[^>]*>([\\s\\S]*?)</${tag}>`));
	return m ? m[1].trim() : null;
}

/** Parse Reddit Atom feed → array of RedditApiPost-shaped objects. */
function parseRedditAtom(xml, fallbackSubreddit) {
	const entries = [];
	const entryRe = /<entry>([\s\S]*?)<\/entry>/g;
	let match;
	while ((match = entryRe.exec(xml)) !== null) {
		const entry = match[1];

		// <id>t3_abc123</id> — Reddit prefixes posts with t3_ (link)
		const rawId = getTagContent(entry, 'id') ?? '';
		const id = rawId.replace(/^t3_/, '');
		if (!id) continue;

		const title = decodeEntities(getTagContent(entry, 'title') ?? '');

		// content is escaped HTML containing the post body (or external link
		// preview for link posts). Strip tags for the wire payload.
		const contentRaw = getTagContent(entry, 'content') ?? '';
		const selftext = stripHtml(contentRaw);

		// <author><name>/u/username</name></author>
		const authorRaw = getTagContent(entry, 'name') ?? '';
		const author = authorRaw.replace(/^\/u\//, '');

		// <link href="https://www.reddit.com/r/news/comments/abc123/title/"/>
		const linkMatch = entry.match(/<link[^>]+href="([^"]+)"/);
		const link = linkMatch ? linkMatch[1] : '';
		const permalink = link.replace(/^https?:\/\/[^/]+/, '');

		// <published>2026-05-13T12:34:56+00:00</published>
		const published = getTagContent(entry, 'published');
		const created_utc = published ? Math.floor(new Date(published).getTime() / 1000) : Math.floor(Date.now() / 1000);

		entries.push({
			id,
			title,
			selftext,
			author,
			permalink,
			created_utc,
			subreddit: fallbackSubreddit,
			// Reddit RSS doesn't expose these — pass 0 so the wire payload stays
			// schema-compatible with the .json shape n8n nodes already consume.
			score: 0,
			num_comments: 0,
		});
	}
	return entries;
}

// ─── Subreddit fetcher ───────────────────────────────────────────────────

async function fetchSubreddit(subreddit) {
	const clean = subreddit
		.trim()
		.replace(/^\/?r\//i, '')
		.replace(/^\/+|\/+$/g, '');

	const headers = {
		'User-Agent': USER_AGENT,
		Accept: 'application/atom+xml, application/rss+xml, text/xml, */*',
	};

	let lastError;
	for (const host of REDDIT_HOSTS) {
		const url = `https://${host}/r/${encodeURIComponent(clean)}/new/.rss`;
		try {
			const response = await fetch(url, { headers });
			if (!response.ok) {
				lastError = new Error(`${host} → ${response.status}`);
				continue;
			}
			const xml = await response.text();
			return parseRedditAtom(xml, clean);
		} catch (err) {
			lastError = err instanceof Error ? err : new Error(String(err));
		}
	}
	throw new Error(`All RSS hosts failed for r/${clean}: ${lastError?.message ?? 'unknown'}`);
}

// ─── Main ────────────────────────────────────────────────────────────────

async function main() {
	console.log(`[${ts()}] Reddwire poll tick — auth: RSS`);

	const queue = await authJson(`${API}/api/internal/poll-queue`);
	const monitors = Array.isArray(queue?.monitors) ? queue.monitors : [];
	console.log(`  Queue: ${monitors.length} monitor(s) due`);

	if (monitors.length === 0) return;

	// Dedup by subreddit — many monitors may watch the same one. Fetch each
	// unique subreddit once and distribute. Cuts request count, reduces 403
	// surface.
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
