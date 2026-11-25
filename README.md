# Data pipeline web scraping: how do you keep a daily run from writing duplicate rows and empty pages? (Five layers, idempotent loads, and validation that fails loudly)

Most scraping pipelines don't break loudly. A target site ships a layout change, the fetch still returns HTTP 200, the parser matches nothing, and the loader writes an empty batch. The run history stays green. Three weeks later someone notices a column has been null since the change.

Three decisions determine whether a pipeline survives that Tuesday: where you draw the boundary between collection and processing, whether writes are idempotent, and whether validation is explicit enough to fail on the same day a selector drifts. Everything else (queue vendor, warehouse choice, dashboard tool) is downstream of those three.

> **If you'd rather not operate the fetch layer:** put a managed scraping API at ingest and keep the queue, workers, storage and monitoring in your own code. Ingest becomes one HTTP call per URL plus ordinary retry logic; proxies, browser rendering and block handling stay outside your repo. ScraperAPI is one option at that layer, and its plan sizes and rates are worth reading before you size anything else: 👉 [see ScraperAPI's current plans and request limits](https://www.scraperapi.com/?fp_ref=coupons).

## The five layers, and what breaks when you skip one

The split below is the one that keeps showing up across production write-ups, and it holds up because each boundary maps to a different failure mode.

| Layer | What belongs there | What breaks if you skip it |
| --- | --- | --- |
| Ingest | API call, basic retry, push raw payload to a queue | A slow site stalls your whole job; retries pile up in memory |
| Queue | Durable buffer between fetching and processing | A traffic spike or one bad worker takes down collection |
| Process | Stateless workers: parse, clean, validate, upsert | Re-runs duplicate rows; a crashed worker loses work |
| Store | Relational for current state, object storage for raw payloads | You can't reprocess after a schema change |
| Monitor | Per-layer metrics plus business-level freshness checks | Selector drift goes unnoticed for weeks |

## Ingest: thin enough that you can throw it away

An ingester does three things: fetch, retry transient failures, push the payload into a queue. Parsing, business rules and database writes belong somewhere else, because the collection layer is the part most likely to be replaced (self-hosted to managed, one vendor to another) and you don't want a rewrite when it changes.

Retry selectively. A 429 with a `Retry-After` header means back off and slow down. A 503 or rising latency means the origin is under pressure; pause that domain rather than hammering it. A 404 will still be a 404 on the third attempt, and retrying it just burns request budget. Keep a per-URL retry cap and log the final reason each URL failed.

## Queue: pick by ops maturity, not by benchmark

A durable buffer decouples spiky ingest from steady processing, and there are three reasonable choices:

- **Redis lists.** Simple and fast, with blocking pops (`BRPOP`) for workers. Good when you want a queue running this afternoon and your volume fits in memory.
- **RabbitMQ.** Richer routing and dead-letter exchanges, so failed jobs land somewhere you can inspect and replay instead of disappearing.
- **Kafka.** Worth it when you need replayability and multiple consumers reading the same stream.

The failure this layer prevents is mundane: one domain gets slow, ingest keeps pushing, and workers drain the backlog while the site recovers. Without a buffer, that same slowdown shows up as a stalled cron job and a partial dataset.

## Process: stateless workers and one stable key

Workers pull a payload, clean it, and write it. They hold no state between messages, so you can run five of them or fifty without coordination.

The flow that works: pop from the queue, run optional extraction, coerce fields to native types (price to float, date to ISO 8601), drop records missing required fields, then upsert on a stable key.

That upsert is the whole trick. Loading the same batch twice has to leave the table in the same state, because schedulers retry failed runs and webhook deliveries get retried too. A pipeline built on plain inserts will eventually produce duplicate rows, and deduplicating them afterwards is more work than writing `ON CONFLICT ... DO UPDATE` on day one.

A working example from a public project: unique key on `(source, external_id)`, and a repeated run over unchanged pages correctly reported 1,000 unchanged records with no duplicates written.

## Store: three destinations, one retention rule

Match storage to the query pattern rather than to fashion:

- **PostgreSQL or MySQL** for current state, transactional updates, joins against your own reference tables.
- **Snowflake or BigQuery** for analytics over large, flat event tables.
- **S3 or GCS** for raw HTML and cheap long-term archive.

**Keep raw payloads for at least 30 days.** When you add a field or fix a parser, you want to reprocess from stored HTML instead of re-crawling thousands of pages. That single retention decision turns a schema change from a re-scrape project into a backfill.

A data model that covers most cases is three tables: a runs table (timestamp, engine, pages or records, new and changed counts, error), a current-items table with the unique key, and an append-only history table that records `new` or `changed` per snapshot. The history table is what makes "what changed on the site last Tuesday" answerable.

## Monitor: two thresholds and three business metrics

Instrument each layer with whatever your metrics stack already speaks. The useful alert lines are few. **Queue depth above 100k** and **worker error rate above 5%** catch most incidents before a human notices missing data.

On top of infrastructure metrics, three business-level numbers do the real work:

- **Volume.** Rows per run. A sudden drop means collection broke, not that the site had a quiet day.
- **Freshness.** Age of the newest `collected_at` timestamp. Alert when data goes stale.
- **Failure rate.** ok versus failed per run. A creeping increase usually means a target site is changing before it breaks entirely.

## Sync or async: choose by URL count

Synchronous retrieval (send a URL, wait for the page) is right for a few hundred known URLs: price checks, monitoring jobs, anything a downstream service is waiting on. It stops scaling somewhere in the low thousands, because your code blocks on every call.

Asynchronous collection flips the model. You push URLs into a named crawler, get a request ID back immediately, and results arrive as POSTs to your webhook as pages finish. Tens of thousands of URLs stop being a scheduling problem and become a queue-draining problem.

One implementation detail bites people: **the webhook handler must return 200 fast and process the payload out of band.** Do the parsing and database write inside the handler and a slow batch will hit the delivery timeout, get marked failed, and be retried, which is exactly when non-idempotent writes produce duplicates.

## Validation: the part that catches selector rot the same day

A page returning zero records is almost never an empty page. It's a selector that no longer matches. Treat it as an error, not as a valid empty result:

- Coerce every field to its native type and reject the record if conversion fails.
- Drop records missing required fields, and count them.
- **Raise when an entire page yields zero records.** This is the difference between finding a broken selector the day it breaks and finding it weeks later in a report.

Add a second line of defense for messy sources. Typed contracts (Pydantic models, for instance) between stages mean each handoff is validated once, and results that fail or look suspicious go to a **review queue** instead of the catalog. Three statuses cover it: `success`, `needs_review`, `failed`. Keep a record per item with its current stage, source URL, confidence and error message, so you can see exactly where a given product stopped.

Worth doing before extraction as well: strip scripts, navigation, footers, forms and comment blocks from the fetched HTML. If an LLM is doing the extraction, that cuts tokens and reduces the chance it latches onto the wrong block of text.

## Two open-source pipelines that show the shape

**ScrapeFlow** collects 1,000 products across 50 paginated pages, with both a Requests/BeautifulSoup engine and a Playwright engine verified against the same source. State and history live in PostgreSQL, change detection classifies each record as `new`, `changed` or `unchanged`, a webhook fires only when something changed, and it ships three passing tests. Scheduling is APScheduler with an interval set by an environment variable, plus a GitHub Actions workflow for hosted runs. It deliberately excludes CAPTCHA solving, login bypasses, proxy rotation to evade blocks and fingerprint spoofing, and the README tells you to check terms, `robots.txt`, rate limits and whether an official API exists before pointing it at a new domain.

**USCF Tournament Ingest** handles a source with genuinely inconsistent structure: chess tournament listings where entry fees, prize funds and schedules have no common format. It runs a fixed sequence (crawl, scrape, deduplicate, refine), uses a local JSON cache to skip URLs it has already fetched, so a daily update is just a re-run of the same script, and passes the messy HTML text through Gemini for structuring. Token usage and projected cost are logged to a file, which is the boring habit that keeps LLM extraction from becoming an unexplained line item.

Both patterns are worth copying: cache-based delta scraping for anything incremental, and a cost log for anything that bills per token.

## Cost and compliance: render on demand, and know the four lines

Rendering is the expensive part of collection, so gate it. Start with plain HTTP; route a page to a browser only when a plain fetch returns an empty shell. Waiting roughly three seconds for client-side content to settle covers most JavaScript-rendered pages, and you pay the render cost only for the pages that need it.

The same logic applies to LLM extraction. Deterministic parsing handles structured pages; the model is for text that has no reliable shape. Latency profiles of these pipelines usually show fetch and extraction dominating everything else, which means speed comes from running work concurrently, not from optimizing local Python.

Compliance boundaries worth writing into the design document: no CAPTCHA solving, no login bypass, no proxy rotation to evade blocks, no fingerprint spoofing. Check the site's terms, its `robots.txt`, the published rate limits, and whether an official API already does the job. A managed collection API handles the proxy and rendering mechanics; it doesn't change what you're allowed to collect.

## Sizing the collection layer before you commit

Whatever you pick at ingest, size it with arithmetic instead of a guess. Monthly requests ≈ unique URLs × runs per day × 30, then add headroom for retries and for pages that need rendering, since those usually bill differently from plain requests. Two thousand product URLs checked twice a day is 120,000 requests a month before a single retry, which is the number that should drive your plan choice.

For ScraperAPI specifically, plan tiers, request volumes and rates sit on their pricing page and change over time, so read them there rather than from a comparison post, including this one: 👉 [check what ScraperAPI's API returns and how it's billed](https://www.scraperapi.com/?fp_ref=coupons).

## Picking your setup

- **A few hundred known URLs, results needed immediately** → synchronous fetches, thin ingester, cron every few hours.
- **Tens of thousands of URLs, minutes of latency acceptable** → async collection with webhook delivery, durable queue, workers that upsert.
- **Steps with dependencies, retries and backfills** → Airflow or Prefect around the same components; cron stops being enough the moment you need run history and dependency ordering.
- **Messy, semi-structured sources** → deterministic parse first, LLM plus a `needs_review` queue for what's left, token cost logged from day one.

Before you buy anything, confirm three things that plan pages tend to bury: what the billing unit actually is (per request, per credit, per rendered page), what overage costs once you exceed the plan, and how cancellation works. Those three answers matter more than the headline monthly number.

If you want to test the architecture without owning the fetch layer, wire your ingester to a managed API, keep the queue and the upsert, and see whether the pipeline survives its first selector drift. 👉 [start from ScraperAPI's current plans](https://www.scraperapi.com/?fp_ref=coupons) and size it against your own request estimate.
