# Bluesky Jetstream WebSocket API Reference

Source: https://github.com/bluesky-social/jetstream

## Public Endpoints

| Hostname | Region |
|----------|--------|
| `jetstream1.us-east.bsky.network` | US-East |
| `jetstream2.us-east.bsky.network` | US-East |
| `jetstream1.us-west.bsky.network` | US-West |
| `jetstream2.us-west.bsky.network` | US-West |

Connection: `wss://<hostname>/subscribe`

## /subscribe Query Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `wantedCollections` | string[] | all | NSIDs to filter (max 100). Supports prefixes: `app.bsky.graph.*` |
| `wantedDids` | string[] | all | DIDs to filter (max 10,000) |
| `cursor` | int64 | none | Unix microseconds timestamp for replay. Absent/future = live-tail |
| `maxMessageSizeBytes` | int64 | unlimited | Max payload size |
| `compress` | bool | false | Enable zstd compression (~56% smaller) |
| `requireHello` | bool | false | Pause until first options_update received |

## Event Schema

All events share: `did` (string), `time_us` (int64, unix microseconds), `kind` (string)

### Commit Event (kind: "commit")
```json
{
  "did": "did:plc:...",
  "time_us": 1725911162329308,
  "kind": "commit",
  "commit": {
    "rev": "string",
    "operation": "create|update|delete",
    "collection": "app.bsky.feed.post",
    "rkey": "string",
    "record": { ... },
    "cid": "string"
  }
}
```

### Identity Event (kind: "identity")
```json
{
  "did": "did:plc:...",
  "time_us": 1725516665234703,
  "kind": "identity",
  "identity": { "did": "...", "handle": "...", "seq": 123, "time": "ISO-8601" }
}
```

### Account Event (kind: "account")
```json
{
  "did": "did:plc:...",
  "time_us": 1725516665333808,
  "kind": "account",
  "account": { "active": true, "did": "...", "seq": 123, "time": "ISO-8601" }
}
```

## Collections (for wantedCollections)

- `app.bsky.feed.post` — posts
- `app.bsky.feed.like` — likes
- `app.bsky.feed.repost` — reposts
- `app.bsky.graph.follow` — follows
- `app.bsky.graph.block` — blocks
- `app.bsky.actor.profile` — profile updates

## Cursor / Reconnection

- Cursor is `time_us` from most recently processed event
- Subtract a few seconds buffer when reconnecting for gapless playback
- Same cursor works across any instance (time-based, not sequence-based)
- Multiple connections allowed

## Subscriber Messages

```json
{
  "type": "options_update",
  "payload": {
    "wantedCollections": ["app.bsky.feed.post"],
    "wantedDids": [],
    "maxMessageSizeBytes": 0
  }
}
```

Max 10MB message size. Invalid updates disconnect the client.
