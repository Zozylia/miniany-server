"""Сигнальный сервер MiniAnyDesk: только знакомство устройств по коду. Видео через него не идёт."""
import json, os, secrets, time
from collections import defaultdict, deque
from pathlib import Path
from aiohttp import web, WSMsgType

WEB = Path(__file__).parent / "web"
hosts, peers = {}, {}
hits = defaultdict(deque)


def limited(ip):
    q, now = hits[ip], time.time()
    while q and now - q[0] > 60:
        q.popleft()
    q.append(now)
    return len(q) > 30  # не больше 30 попыток входа в минуту с одного IP


async def ws_handler(req):
    ws = web.WebSocketResponse(heartbeat=20, max_msg_size=256 * 1024)
    await ws.prepare(req)
    ip = req.headers.get("X-Forwarded-For", req.remote or "").split(",")[0].strip()
    uid, my_host, joined = secrets.token_hex(6), None, None
    async for msg in ws:
        if msg.type != WSMsgType.TEXT:
            continue
        try:
            m = json.loads(msg.data)
        except ValueError:
            continue
        t = m.get("type")
        if t == "host":
            hid = str(m.get("id", ""))
            if len(hid) == 6 and hid.isdigit():
                old = hosts.get(hid)
                if old is not None and not old.closed and old is not ws:
                    await ws.send_json({"type": "error", "reason": "id-busy"})
                    continue
                hosts[hid], my_host = ws, hid
        elif t == "join":
            if limited(ip):
                await ws.send_json({"type": "denied", "reason": "rate"})
                continue
            hid = str(m.get("id", ""))
            h = hosts.get(hid)
            if h is None or h.closed:
                await ws.send_json({"type": "denied", "reason": "no-host"})
                continue
            joined, peers[uid] = hid, ws
            await h.send_json({"type": "join", "pin": str(m.get("pin", ""))[:16], "peer": uid})
        elif t in ("offer", "bye") and joined:
            h = hosts.get(joined)
            if h and not h.closed:
                await h.send_json({"type": t, "sdp": m.get("sdp"), "peer": uid})
        elif t in ("ok", "denied", "answer") and my_host:
            p = peers.get(m.get("to"))
            if p and not p.closed:
                m.pop("to", None)
                await p.send_json(m)
    if my_host and hosts.get(my_host) is ws:
        hosts.pop(my_host, None)
    peers.pop(uid, None)
    if joined:
        h = hosts.get(joined)
        if h and not h.closed:
            await h.send_json({"type": "bye", "peer": uid})
    return ws


async def index(_):
    return web.FileResponse(WEB / "index.html")


async def health(_):
    return web.Response(text="ok")


app = web.Application()
app.add_routes([web.get("/", index), web.get("/health", health), web.get("/ws", ws_handler)])

if __name__ == "__main__":
    web.run_app(app, port=int(os.environ.get("PORT", 8080)))
