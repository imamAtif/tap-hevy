# tap-hevy

Singer tap for the [Hevy Public API](https://api.hevyapp.com/docs/), built with the [Meltano Singer SDK](https://sdk.meltano.com).

Extracts all documented read-only GET data from Hevy, including workouts, routines, exercise templates, routine folders, exercise history, body measurements, and user info. Supports incremental replication via `/v1/workouts/events`, robust pagination, and production-grade rate-limit handling.

> **Requires Hevy Pro.** The Hevy Public API is only available to Hevy Pro subscribers. Obtain an API key at https://hevy.com/settings?developer

## Installation

```bash
pipx install tap-hevy
# or with pip
pip install tap-hevy

# with uv
uv pip install tap-hevy
```

Development install from source:

```bash
git clone <repo>
cd tap-hevy
uv pip install -e .
```

Check installation:

```bash
tap-hevy --help
tap-hevy --discover --config config.json
```

## Configuration

### Obtaining an API Key

1. Subscribe to **Hevy Pro** (required for API access).
2. Go to https://hevy.com/settings?developer
3. Generate and copy your API key (UUID).

### Meltano `meltano.yml`

```yaml
plugins:
  extractors:
    - name: tap-hevy
      variant: original
      pip_url: tap-hevy
      config:
        api_key: ${HEVY_API_KEY}  # set via env var
        start_date: '2024-01-01T00:00:00Z'
      select:
        - user_info.*
        - workouts.*
        - workout_events.*
        - routines.*
        - exercise_templates.*
        - routine_folders.*
        - body_measurements.*
        - exercise_history.*
```

### Standalone `config.json`

```json
{
  "api_key": "00000000-0000-0000-0000-000000000000",
  "start_date": "2024-01-01T00:00:00Z",
  "api_url": "https://api.hevyapp.com",
  "request_timeout": 30,
  "max_retries": 5,
  "page_size": 10
}
```

| Setting | Required | Default | Secret | Description |
|---|---|---|---|---|
| `api_key` | Yes | - | Yes | Hevy API key (UUID) from https://hevy.com/settings?developer |
| `api_url` | No | `https://api.hevyapp.com` | No | Base URL for Hevy API |
| `start_date` | No | - | No | Earliest date for incremental streams (`workout_events`). ISO-8601. Also used as `since` fallback. |
| `request_timeout` | No | `30` | No | HTTP timeout in seconds (default 30, not a hard limit - configurable) |
| `max_retries` | No | `5` | No | Max retries for 429 / 5xx / connection errors |
| `page_size` | No | `10` | No | Records per page. Configurable, clamped to Hevy hard limits: 10 for most streams, 100 for `exercise_templates` |
| `user_agent` | No | `singer-sdk/<version>` | No | Custom User-Agent |

Run with Meltano:

```bash
meltano config tap-hevy set api_key <uuid>
meltano config tap-hevy set start_date 2024-01-01T00:00:00Z
meltano run tap-hevy target-jsonl
```

## Available Streams

| Stream | Endpoint | Primary Key | Replication Key | Replication Method | Pagination | Parent |
|---|---|---|---|---|---|---|
| `user_info` | `GET /v1/user/info` | `id` | None | FULL_TABLE | single page | - |
| `workouts` | `GET /v1/workouts` | `id` | None | FULL_TABLE | `page` / `page_count` (pageSize 10) | - |
| `workout_events` | `GET /v1/workouts/events?since=` | `id` + `event_timestamp` | `event_timestamp` | INCREMENTAL | `page` / `page_count` + `since` | - |
| `routines` | `GET /v1/routines` | `id` | None | FULL_TABLE | `page` / `page_count` (pageSize 10) | - |
| `exercise_templates` | `GET /v1/exercise_templates` | `id` | None | FULL_TABLE | `page` / `page_count` (pageSize 10, max 100) | - |
| `routine_folders` | `GET /v1/routine_folders` | `id` | None | FULL_TABLE | `page` / `page_count` (pageSize 10) | - |
| `body_measurements` | `GET /v1/body_measurements` | `date` | None | FULL_TABLE | `page` / `page_count` (pageSize 10) | - |
| `exercise_history` | `GET /v1/exercise_history/{exerciseTemplateId}` | `exercise_template_id` + `workout_id` + `workout_start_time` | None | FULL_TABLE | single page (supports `start_date`/`end_date` server filter) | `exercise_templates` |

* `workouts` detail (`/v1/workouts/{id}`) is **not** fetched separately: the list endpoint already returns the full `Workout` object (exercises + sets), so N+1 requests are avoided.
* Similarly, `routines/{id}`, `exercise_templates/{id}`, `routine_folders/{id}` are not fetched individually; list payloads are complete.
* `/v1/workouts/count` is not exposed as a stream (single integer); use `workouts` stream for data.
* `exercise_history` is a child stream: the tap automatically iterates `exercise_templates` and fetches history for each template id.

## Replication Behaviour

* **Incremental**: `workout_events` only. Uses `since` query param derived from state bookmark (`event_timestamp`) or `start_date`. `event_timestamp` is `workout.updated_at` for `updated` events and `deleted_at` for `deleted` events. The stream handles both `updated` (with nested `workout` object) and `deleted` (with `id` + `deleted_at`) events, paginated newest-to-oldest. State bookmark is the max `event_timestamp` seen.
* **Full Table**: All other streams. They fetch all pages each run. Hevy does not provide server-side `since` filters for these endpoints; client-side filtering would still require fetching every page, so full-table is the honest mode. For large accounts with many workouts, prefer `workout_events` for efficient incremental sync and treat `workouts` as occasional full refresh.
* **Body measurements** keyed by `date` (YYYY-MM-DD), one record per day.
* **Exercise history** currently full-table per template. The API supports optional `start_date`/`end_date` filters but they are not yet wired to incremental bookmarks; the stream will fetch full history per template each run.

To reset incremental state:

```bash
meltano run tap-hevy target-jsonl --full-refresh
# or meltano state clear --state-id <id>
```

## Discovery & Catalog Selection

```bash
# Discover
tap-hevy --config config.json --discover > catalog.json

# Inspect streams
cat catalog.json | jq '.streams[].tap_stream_id'

# Select streams (Meltano)
meltano select tap-hevy --list
meltano select tap-hevy 'workouts.*' 'workout_events.*'
meltano select tap-hevy --exclude 'exercise_history.*'

# Singer catalog with --catalog
tap-hevy --config config.json --catalog catalog.json
```

State handling is automatic with `meltano run`. For bare Singer:

```bash
tap-hevy --config config.json --catalog catalog.json --state state.json > out.jsonl
# next run
tap-hevy --config config.json --catalog catalog.json --state state.json > out2.jsonl
```

## Rate Limit & Backoff Behaviour

* Retries on **429 Too Many Requests**, **5xx**, and connection/timeout errors with exponential backoff (factor 2, random jitter, up to `max_retries` attempts).
* Respects `Retry-After` header (seconds or HTTP-date) and `X-RateLimit-Reset` when present; otherwise falls back to exponential.
* Does **not** retry permanent client errors: 400, 401, 403, 404 are raised as `FatalAPIError` immediately.
* Pagination terminates when `page >= page_count` (from response) or when response lacks `page`/`page_count`; no extra requests.
* Jitter adds randomness to avoid synchronized bursts.
* **Operational note per Hevy docs**: If you run integrations hourly/daily, **do not schedule exactly at `xx:00`**; use a random minute (e.g., `07`, `23`) to avoid thundering herd. The tap itself does not add an artificial sleep - schedule jitter is the operator's responsibility.

## Running the Tap

```bash
# Discovery
tap-hevy --config config.json --discover

# Sync with inline config
tap-hevy --config config.json --catalog catalog.json

# With Meltano
meltano invoke tap-hevy --discover
meltano elt tap-hevy target-jsonl
meltano run tap-hevy target-postgres
```

Example output (`workout_events`):

```json
{"type":"RECORD","stream":"workout_events","record":{"type":"updated","id":"b459cba5...","event_timestamp":"2024-08-14T12:30:00Z","workout":{...}}}
{"type":"RECORD","stream":"workout_events","record":{"type":"deleted","id":"efe6801c...","deleted_at":"2024-08-15T09:00:00Z","event_timestamp":"2024-08-15T09:00:00Z","workout":null}}
{"type":"STATE","value":{"bookmarks":{"workout_events":{"replication_key":"event_timestamp","replication_key_value":"2024-08-15T09:00:00Z"}}}}
```

## Hevy API Limitations / Discoveries

* **Pro only**: All `/v1/*` endpoints require Hevy Pro + API key via `api-key` header (UUID).
* **No list filter except `workout_events`**: Only `/v1/workouts/events?since=` supports server-side incremental filtering. Other list endpoints only support `page`/`pageSize`; no `updated_at` filter, so full scan is required for refresh.
* **`pageSize` caps differ (hard limits)**: 10 for workouts, routines, routine folders, body measurements, workout events; **100** for exercise templates. Tap default is 10 and configurable via `page_size` (clamped to hard limits), so `30` is the default timeout and `10` is the default page size - neither is a hard limit.
* **`workouts` pagination order**: `GET /v1/workouts` returns oldest→newest; `GET /v1/workouts/events` returns newest→oldest. Events feed is intended for delta sync.
* **`exercise_history` quirks**: Returns array `exercise_history` with per-set rows (no per-entry id, no pagination metadata, no `page_count`); each entry is a set within a workout, identified by `workout_id` + `workout_start_time` + metrics. No natural primary key - tap uses composite `exercise_template_id`/`workout_id`/`workout_start_time` (duplicates possible if same workout has identical sets).
* **Detail endpoints not needed**: List payloads already contain full nested objects; no N+1 calls for `workouts/{id}` etc.
* **Schemas**: OpenAPI declares many fields `nullable:true` (e.g., `weight_kg`, `reps`, `rpe`, `description`, `folder_id`). Tap marks optional fields nullable via Singer SDK (optional → nullable automatically).
* **Rate limit headers**: API may return `Retry-After` (seconds or HTTP-date). Tap respects it; Hevy asks not to burst at `xx:00`.
* **Base URL**: `https://api.hevyapp.com` (not `api.hevy.com`).

## Development

```bash
# Install dev deps
uv pip install -e ".[test]"

# Lint
uv run ruff check tap_hevy tests
uv run ruff format tap_hevy tests

# Type check
uv run mypy tap_hevy tests  # if configured

# Tests (mocked, no real account)
pytest tests/ -v

# Discovery smoke test
tap-hevy --config tests/config.json --discover | jq '.streams[].tap_stream_id'
```

## Architecture

```
tap_hevy/
  tap.py      # TapHevy definition, config schema, discover_streams
  client.py   # HevyStream base (auth, pagination, backoff, Retry-After)
  streams.py  # 8 streams, schemas, incremental/child logic
```

Shared behaviour (base URL, `api-key` header, `Retry-After`/`5xx` backoff, `page`/`page_count` paginator, timeout) lives in `client.py`.

## License

Apache-2.0
