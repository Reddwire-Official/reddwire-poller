# Reddwire Poller

> Public polling engine for the [Reddwire](https://reddwire.dev) hosted SaaS. Runs as a GitHub Actions cron every 5 minutes.

## Why this exists

Cloudflare Workers can't fetch `reddit.com/.json` — Reddit blanket-blocks Cloudflare's datacenter IPs with a 403 to deter scrapers. GitHub Actions runners use Azure IPs that Reddit accepts (millions of legitimate dev workflows hit Reddit from GitHub every day; blocking them would break too much of the open-source ecosystem).

By running the polling here instead of in our Cloudflare Worker, Reddwire's hosted SaaS works on free tier infrastructure end-to-end — no proxy, no VPS, no Reddit API approval.

## How it works

Every 5 minutes:

1. The Action pulls the list of monitors due for polling from `api.reddwire.dev/api/internal/poll-queue`.
2. For each monitor it fetches `reddit.com/r/{subreddit}/new.json` from a GitHub Actions runner.
3. The raw posts are POSTed to `api.reddwire.dev/api/internal/poll-result`.
4. The Cloudflare Worker handles dedup, keyword filtering, and webhook delivery to user n8n instances.

The Worker holds all sensitive state (user emails, webhook URLs, keyword configs). This runtime only sees `{ monitor_id, subreddit }` per poll request and the raw public Reddit JSON it fetches. **No user data flows through this repo's code or logs.**

## Authentication

The Worker's internal endpoints are gated by a bearer secret (`REDDWIRE_INTERNAL_SECRET`). This Action provides it via a GitHub Actions repository secret of the same name. The matching value lives in Cloudflare as the Worker secret `INTERNAL_SECRET`.

## Public on purpose

Public repos get unlimited GitHub Actions minutes. That's what keeps Reddwire's free tier actually free.

## License

[FSL-1.1-MIT](LICENSE) — same as the [n8n node](https://github.com/Reddwire-Official/n8n-nodes-reddwire). Free for self-hosters, internal use, education, and any non-competing purpose. Converts to MIT two years after each release.
