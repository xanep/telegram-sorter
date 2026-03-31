import httpx
import yaml

cfg = yaml.safe_load(open("config.yaml"))
token = cfg["telegram"]["bot_token"]

# Test copyMessage: copy the latest message from @stranaua to the Ukraine channel
source_id = -1001092413834
dest_id    = -1003827611915

# First grab the latest message ID from the source channel
r = httpx.get(f"https://api.telegram.org/bot{token}/getUpdates")
print("getUpdates:", r.json().get("ok"), r.json().get("description", ""))

# Try copying a recent known message
for msg_id in [231101, 231100, 231099]:
    r = httpx.post(
        f"https://api.telegram.org/bot{token}/copyMessage",
        json={"chat_id": dest_id, "from_chat_id": source_id, "message_id": msg_id},
    )
    j = r.json()
    print(f"copyMessage msg={msg_id}: ok={j.get('ok')}  {j.get('description', j.get('result', ''))}")


