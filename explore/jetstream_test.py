"""Quick test: connect to Bluesky Jetstream and print events."""
import json
import websocket

url = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"

def on_message(ws, message):
    event = json.loads(message)
    commit = event.get("commit", {})
    record = commit.get("record", {})

    print(json.dumps({
        "did": event.get("did"),
        "time_us": event.get("time_us"),
        "kind": event.get("kind"),
        "operation": commit.get("operation"),
        "collection": commit.get("collection"),
        "text": record.get("text", "")[:200] if record else "",
        "langs": record.get("langs"),
        "created_at": record.get("createdAt"),
        "has_embed": "embed" in record if record else False,
        "has_facets": "facets" in record if record else False,
        "reply_to": bool(record.get("reply")),
    }, indent=2, ensure_ascii=False))
    print("---")

def on_error(ws, error):
    print(f"Error: {error}")

def on_open(ws):
    print("Connected to Jetstream. Listening for posts...\n")

ws = websocket.WebSocketApp(url, on_message=on_message, on_open=on_open, on_error=on_error)
ws.run_forever()
