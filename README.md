<img src="src/logo.svg" width="72" alt="Webhook Buffer logo">

# webhook-buffer

A small buffer server that receives webhooks and forwards them as-is to the destinations you register.
So that webhooks aren't lost when a destination is briefly down or returning errors, failed deliveries are stored and retried until they succeed.
A web panel lets you manage endpoints and destinations and inspect received webhooks and their delivery status.

## How it works

- **Endpoint**: The address that receives webhooks, in the form `http://server/h/{slug}`. Created from the panel.
- **Destination**: Each endpoint can have multiple destinations. A webhook received on an endpoint is sent to every enabled destination.

When a webhook arrives, it is handled like this:

1. The method, headers, and body are first saved to disk (SQLite), and `202 Accepted` is returned to the sender immediately.
2. It is then delivered to each destination independently. Once a destination responds with 2xx, that destination is done.
3. Failures are handled differently depending on the cause:
   - **No response at all** (connection failure, timeout): It is not retried. The destination is marked "unreachable", and webhooks arriving afterwards are queued instead of sent. When the destination sends a [recovery notification](#recovery-notification), all queued webhooks are sent at once.
   - **Error response** (any non-2xx response): It is retried up to the destination's configured retry count. The interval starts at `MIN_BACKOFF_MS`, doubles on each failure, and caps at `MAX_BACKOFF_SECS`. Once the retries are used up, it stops as "failed", and you can retry or discard it from the panel.

One destination being down does not affect the others. Because webhooks are saved before being sent, they survive even if the buffer server dies mid-delivery, and delivery resumes when it restarts.

Any trailing path and query string from the received request are appended to the destination URL.
With a destination of `https://ci.example.com/hook?k=1`, a webhook received as `POST /h/github/push?x=2` is sent to `POST https://ci.example.com/hook/push?k=1&x=2`.

## Running

```sh
ADMIN_TOKEN=some-secret-value cargo run --release
```

Open `http://localhost:8080/` in a browser and sign in with your `ADMIN_TOKEN`.
If `ADMIN_TOKEN` is not set, a temporary token is generated on each start and printed to the log.

### Docker

```sh
docker run -d --name webhook-buffer -p 8080:8080 \
  -e ADMIN_TOKEN=some-secret-value \
  -v webhook-buffer-data:/data \
  ghcr.io/pixelzx0/webhook-buffer:latest
```

The DB is stored at `/data` inside the container, so be sure to mount a volume. The container runs as uid 65532.
If you mount a host directory instead of a volume, that directory must be writable by this uid.

The health check is `GET /healthz`. It requires no authentication and returns `200 ok` if the DB responds, or `503` otherwise.
The image includes a `HEALTHCHECK`, so `docker ps` shows `healthy`/`unhealthy`.
Since the image has no curl, the check runs the binary as `webhook-buffer healthcheck`.

Images are built by GitHub Actions on every push to `master` and published to `ghcr.io`.
Supported architectures: `linux/amd64`, `arm64`, `arm/v7`, `arm/v6`, `386`, `ppc64le`, `riscv64`, `loong64`, `s390x`.

| Tag | When |
| --- | --- |
| `latest` | Latest commit on `master` |
| `1.2.3`, `1.2`, `1` | When a `v1.2.3` tag is pushed |
| `sha-abc1234` | Every commit |

### Behind a reverse proxy

You can run it behind a reverse proxy such as nginx or Caddy. Since the admin token travels in a header, it's best to terminate HTTPS at the proxy.
It can be mounted at the root (`https://hooks.example.com/`) or under a subpath (`https://example.com/buffer/`).
When mounting under a subpath, the proxy must strip the prefix before forwarding. The webhook URLs and recovery notification URLs shown in the panel are built from the address the panel was opened at.

```nginx
location /buffer/ {
    proxy_pass http://127.0.0.1:8080/;   # the trailing / strips the prefix
    client_max_body_size 10m;            # match MAX_BODY_BYTES (nginx default is 1m)
}
```

- If the proxy's body size limit is smaller than `MAX_BODY_BYTES`, large webhooks are rejected before reaching the buffer and are never stored.
- To prevent bypassing the proxy, setting `LISTEN_ADDR=127.0.0.1:8080` (or `-p 127.0.0.1:8080:8080` with Docker) is recommended.
- Headers added by the proxy, such as `X-Forwarded-For`, are forwarded to destinations like any other header.

## Recovery notification

Each destination has its own unique recovery notification URL, which you can copy from under the destination in the panel.

```sh
curl -X POST http://buffer-server/d/{recovery-token}/alive
# {"status":"ok","queued":3}
```

If the destination server calls this URL once on startup, it immediately receives the webhooks that piled up while it was down.
No admin token is needed; the token in the URL identifies the destination. GET works too.
If you can't modify the destination server, pressing the "Mark alive" button in the panel does the same thing.

## Web panel

- **Summary**: Number of endpoints and destinations, webhooks received and successful deliveries in the last 24 hours, pending / retrying / failed deliveries, and unreachable destinations
- **Endpoints**: Add/delete endpoints, copy webhook URLs, add/delete/disable/enable destinations, per-destination retry count (default 10, leave empty for unlimited), destination status (Healthy/Unreachable), and recovery notification URLs
- **Recent webhooks**: List of received webhooks. Click one to see its headers, body, and per-destination delivery results (attempts, response code, error), and to resend it to all destinations.
- **Retry queue**: Deliveries not yet completed (waiting to retry, held because the destination is unreachable, failed after exceeding retries). You can retry or discard them.

When a destination is disabled, new webhooks are not sent to it, and deliveries already queued for it are paused until it is re-enabled.

The API used by the panel lives under `/api` and requires an `Authorization: Bearer {ADMIN_TOKEN}` header.

```sh
# Create an endpoint (a random slug is generated if omitted)
curl -X POST localhost:8080/api/endpoints -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H 'content-type: application/json' -d '{"name":"GitHub","slug":"github"}'

# Add a destination (max_retries defaults to 10 if omitted; null means unlimited)
curl -X POST localhost:8080/api/endpoints/1/destinations -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H 'content-type: application/json' -d '{"url":"https://ci.example.com/hook","max_retries":5}'
```

## Configuration (environment variables)

| Variable | Default | Description |
| --- | --- | --- |
| `ADMIN_TOKEN` | (generated) | Token for the web panel and admin API |
| `LISTEN_ADDR` | `0.0.0.0:8080` | Listen address |
| `DB_PATH` | `webhook-buffer.db` | SQLite file. Parent directories are created automatically; if a directory (`/data/`) is given, `webhook-buffer.db` is created inside it |
| `REQUEST_TIMEOUT_SECS` | `10` | Timeout for requests to destinations |
| `MIN_BACKOFF_MS` | `1000` | Delay before the first retry |
| `MAX_BACKOFF_SECS` | `300` | Maximum retry interval |
| `MAX_CONCURRENCY` | `16` | Maximum number of concurrent outgoing requests |
| `RETENTION_DAYS` | `7` | How long to keep records of fully delivered webhooks (undelivered ones are never deleted) |
| `MAX_BODY_BYTES` | `10485760` | Maximum accepted body size |
| `RUST_LOG` | `info` | Log level |

## Things to know

- Senders never see the destination's response; they always get `202`. This keeps senders from timing out when a destination is slow.
- Anything other than 2xx is treated as an error response. With unlimited retries, a webhook that a destination keeps rejecting with 400 is retried until you discard it in the panel.
- Timeouts also count as "no response". Even if a destination is merely slow but alive, nothing is sent until a recovery notification arrives, so set `REQUEST_TIMEOUT_SECS` generously.
- The unreachable state is cleared only by a recovery notification or the panel's "Mark alive". The buffer does not re-check on its own.
- Attempts with no response do not count toward the retry limit.
- Webhooks to the same destination are sent concurrently, so they may arrive in a different order than they were received.
- If the buffer server dies mid-delivery, the same webhook may be sent again after restart (at-least-once delivery).
- Redirects (3xx) are not followed and are treated as failures.
- The `TARGET_URL` setting from earlier versions has been removed. Create endpoints and destinations from the panel instead.
