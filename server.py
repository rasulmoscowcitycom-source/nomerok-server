# pip install fastapi uvicorn python-telegram-bot
# Запуск: uvicorn server:app --host 0.0.0.0 --port 8000   (нужен https-домен)
import asyncio, random, math, hashlib, hmac, json, sqlite3, time, urllib.parse, urllib.request
from fastapi import FastAPI, HTTPException, Request
import os
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

BOT_TOKEN = os.environ.get("BOT_TOKEN", "PASTE_BOT_TOKEN_FROM_BOTFATHER")
DB_PATH = os.environ.get("DB_PATH", "game.db")
# Каналы для призов за подписку: id (как в игре, CHS в html) -> @канал. Бот должен быть АДМИНОМ канала.
CHANNELS = {"c1": os.environ.get("CHANNEL1", "@your_channel"), "c2": os.environ.get("CHANNEL2", "@your_channel2")}
REQUIRED_CHANNEL = os.environ.get("REQUIRED_CHANNEL", "@your_channel")           # обязательная подписка, чтобы играть ("" = выключить)
REQUIRED_URL = os.environ.get("REQUIRED_URL", "https://t.me/your_channel")   # ссылка на этот канал
BOT_USERNAME = os.environ.get("BOT_USERNAME", "your_bot")                    # логин бота без @ (для ссылок-приглашений)
REF_MILESTONES = [(1, 3000000), (3, 10000000), (5, 25000000), (10, 100000000)]  # друзья -> приз (сум)
app = FastAPI()

def db():
    c = sqlite3.connect(DB_PATH); c.row_factory = sqlite3.Row; c.execute("PRAGMA journal_mode=WAL")
    c.executescript("""
    CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY,name TEXT,first_seen INT,last_seen INT,state TEXT);
    CREATE TABLE IF NOT EXISTS gifts(id INTEGER PRIMARY KEY AUTOINCREMENT,uid INT,data TEXT);
    CREATE TABLE IF NOT EXISTS promos(code TEXT PRIMARY KEY,reward TEXT,max_uses INT,used INT DEFAULT 0);
    CREATE TABLE IF NOT EXISTS redeemed(code TEXT,uid INT,PRIMARY KEY(code,uid));
    CREATE TABLE IF NOT EXISTS offers(id INTEGER PRIMARY KEY AUTOINCREMENT,frm INT,to_id INT,item TEXT,price INT,status TEXT DEFAULT 'open',ts INT);
    CREATE TABLE IF NOT EXISTS refpending(uid INTEGER PRIMARY KEY,ref INT);
    CREATE TABLE IF NOT EXISTS referrals(uid INTEGER PRIMARY KEY,ref INT);
    CREATE TABLE IF NOT EXISTS events(name TEXT,luck REAL,sell REAL,xp REAL,until INT);
    CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY,v TEXT);
    CREATE TABLE IF NOT EXISTS auctions(id INTEGER PRIMARY KEY AUTOINCREMENT,seller INT,item TEXT,start INT,top INT DEFAULT 0,topuid INT,end INT,status TEXT DEFAULT 'open');
    """)
    return c

def auth(init):  # проверка подписи Telegram initData
    d = dict(urllib.parse.parse_qsl(init)); h = d.pop("hash", "")
    s = "\n".join(f"{k}={v}" for k, v in sorted(d.items()))
    key = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
    if not hmac.compare_digest(hmac.new(key, s.encode(), hashlib.sha256).hexdigest(), h):
        raise HTTPException(401, "Нет доступа")
    u = json.loads(d["user"]); return u["id"], u.get("first_name", "")

def gift(c, uid, d): c.execute("INSERT INTO gifts(uid,data) VALUES(?,?)", (uid, json.dumps(d)))

@app.get("/")
def index(): return FileResponse("uz-nomerok-tg.html")

@app.post("/api/load")
async def load(r: Request):
    b = await r.json(); uid, name = auth(b["initData"]); now = int(time.time()); c = db()
    existed = c.execute("SELECT 1 FROM users WHERE id=?", (uid,)).fetchone()
    c.execute("INSERT INTO users(id,name,first_seen,last_seen) VALUES(?,?,?,?) ON CONFLICT(id) DO UPDATE SET name=?,last_seen=?", (uid, name, now, now, name, now))
    if not existed: ref_credit(c, uid)
    row = c.execute("SELECT state FROM users WHERE id=?", (uid,)).fetchone()
    g = [{"id": x["id"], **json.loads(x["data"])} for x in c.execute("SELECT * FROM gifts WHERE uid=?", (uid,))]
    n = c.execute("SELECT COUNT(*) FROM referrals WHERE ref=?", (uid,)).fetchone()[0]
    c.commit()
    return {"uid": uid, "state": json.loads(row["state"]) if row["state"] else None, "gifts": g, "ref": {"n": n, "bot": BOT_USERNAME}, "rid": get_rid(c)}

@app.post("/api/save")
async def save(r: Request):
    b = await r.json(); uid, _ = auth(b["initData"]); c = db()
    if str(b["state"].get("rid", 0)) != str(get_rid(c)): return {"stale": 1}   # игра из до сброса прогресса - не сохраняем
    c.execute("UPDATE users SET state=?,last_seen=? WHERE id=?", (json.dumps(b["state"]), int(time.time()), uid)); c.commit()
    return {"ok": 1}

@app.post("/api/gifts/clear")
async def clear(r: Request):
    b = await r.json(); uid, _ = auth(b["initData"]); ids = [int(i) for i in b.get("ids", [])]; c = db()
    if ids: c.execute("DELETE FROM gifts WHERE uid=? AND id IN (%s)" % ",".join("?" * len(ids)), [uid, *ids]); c.commit()
    return {"ok": 1}

@app.post("/api/promo")
async def promo(r: Request):
    b = await r.json(); uid, _ = auth(b["initData"]); code = b["code"].strip().upper(); c = db()
    p = c.execute("SELECT * FROM promos WHERE code=?", (code,)).fetchone()
    if not p: raise HTTPException(400, "Промокод не найден")
    if p["used"] >= p["max_uses"]: raise HTTPException(400, "Промокод закончился")
    try: c.execute("INSERT INTO redeemed VALUES(?,?)", (code, uid))
    except sqlite3.IntegrityError: raise HTTPException(400, "Вы уже активировали этот код")
    c.execute("UPDATE promos SET used=used+1 WHERE code=?", (code,)); c.commit()
    return {"reward": json.loads(p["reward"])}

@app.post("/api/offer/create")
async def offer_create(r: Request):
    b = await r.json(); uid, _ = auth(b["initData"]); c = db(); to = int(b["to"]); price = int(b["price"])
    if price < 0 or not c.execute("SELECT 1 FROM users WHERE id=?", (to,)).fetchone():
        raise HTTPException(400, "Игрок с таким ID не найден")
    c.execute("INSERT INTO offers(frm,to_id,item,price,ts) VALUES(?,?,?,?,?)", (uid, to, json.dumps(b["item"]), price, int(time.time()))); c.commit()
    return {"ok": 1}

@app.post("/api/offer/list")
async def offer_list(r: Request):
    b = await r.json(); uid, _ = auth(b["initData"]); c = db()
    def rows(w): return [{"id": x["id"], "frm": x["frm"], "to_id": x["to_id"], "item": json.loads(x["item"]), "price": x["price"]}
                         for x in c.execute("SELECT * FROM offers WHERE status='open' AND " + w, (uid,))]
    return {"incoming": rows("to_id=?"), "outgoing": rows("frm=?")}

@app.post("/api/offer/accept")
async def offer_accept(r: Request):
    b = await r.json(); uid, _ = auth(b["initData"]); c = db()
    o = c.execute("SELECT * FROM offers WHERE id=? AND status='open'", (int(b["id"]),)).fetchone()
    if not o or o["to_id"] != uid: raise HTTPException(400, "Предложение недоступно")
    if c.execute("UPDATE offers SET status='done' WHERE id=? AND status='open'", (o["id"],)).rowcount != 1:
        raise HTTPException(400, "Предложение недоступно")
    gift(c, uid, json.loads(o["item"])); gift(c, o["frm"], {"t": "bal", "v": o["price"]}); c.commit()
    return {"ok": 1}

@app.post("/api/offer/cancel")
async def offer_cancel(r: Request):
    b = await r.json(); uid, _ = auth(b["initData"]); c = db()
    o = c.execute("SELECT * FROM offers WHERE id=? AND status='open'", (int(b["id"]),)).fetchone()
    if not o or o["frm"] != uid: raise HTTPException(400, "Предложение недоступно")
    if c.execute("UPDATE offers SET status='cancelled' WHERE id=? AND status='open'", (o["id"],)).rowcount != 1:
        raise HTTPException(400, "Предложение недоступно")
    gift(c, uid, json.loads(o["item"])); c.commit()
    return {"ok": 1}

os.makedirs("cars", exist_ok=True)
app.mount("/cars", StaticFiles(directory="cars"), name="cars")  # фото машин: cars/имя.jpg

@app.post("/api/sub")
async def sub(r: Request):
    b = await r.json(); uid, _ = auth(b["initData"]); cid = b.get("id"); c = db()
    ch = CHANNELS.get(cid)
    if not ch: raise HTTPException(400, "Канал не найден")
    c.execute("CREATE TABLE IF NOT EXISTS claims(uid INT,id TEXT,PRIMARY KEY(uid,id))")
    if c.execute("SELECT 1 FROM claims WHERE uid=? AND id=?", (uid, cid)).fetchone(): raise HTTPException(400, "Уже получено")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/getChatMember?chat_id={urllib.parse.quote(ch)}&user_id={uid}"
    try: res = json.load(urllib.request.urlopen(url, timeout=8))
    except Exception: raise HTTPException(400, "Не удалось проверить подписку")
    if res.get("result", {}).get("status") not in ("member", "administrator", "creator"):
        raise HTTPException(400, "Сначала подпишитесь на канал")
    c.execute("INSERT INTO claims VALUES(?,?)", (uid, cid)); c.commit()
    return {"ok": 1}

def ref_credit(c, uid):  # засчитать приглашение, когда новый игрок впервые зашёл в игру
    p = c.execute("SELECT ref FROM refpending WHERE uid=?", (uid,)).fetchone()
    if not p or p["ref"] == uid or not c.execute("SELECT 1 FROM users WHERE id=?", (p["ref"],)).fetchone(): return
    c.execute("INSERT OR IGNORE INTO referrals VALUES(?,?)", (uid, p["ref"]))
    n = c.execute("SELECT COUNT(*) FROM referrals WHERE ref=?", (p["ref"],)).fetchone()[0]
    for need, reward in REF_MILESTONES:
        if n == need: gift(c, p["ref"], {"t": "bal", "v": reward})

@app.post("/api/gate")
async def gate(r: Request):
    b = await r.json(); uid, _ = auth(b["initData"])
    if not REQUIRED_CHANNEL: return {"ok": True}
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/getChatMember?chat_id={urllib.parse.quote(REQUIRED_CHANNEL)}&user_id={uid}"
    try: res = json.load(urllib.request.urlopen(url, timeout=8))
    except Exception: return {"ok": True}   # не удалось проверить (бот не админ?) - не блокируем игру
    ok = res.get("result", {}).get("status") in ("member", "administrator", "creator")
    return {"ok": ok, "url": REQUIRED_URL}

@app.post("/api/event")
async def event(r: Request):
    b = await r.json(); auth(b["initData"]); c = db()
    e = c.execute("SELECT * FROM events WHERE until>? ORDER BY until DESC LIMIT 1", (int(time.time()),)).fetchone()
    return {**(dict(e) if e else {}), "rid": get_rid(c)}

REGS = ["01", "10", "20", "25", "30", "40", "50", "60", "70", "75", "80", "85", "90", "95"]
LETTERS = set("ABCDEFGHJKLMNPRSTUVWXYZ")
BLOCK = {"PAA", "PSS", "PPP", "APC", "TSS", "TST", "ZDF", "DXA", "FVV", "VSB", "VSC", "VSF", "VSD", "VSG", "VSV", "PSF",
         "TTA", "TTB", "TTC", "TTD", "TTE", "TTF", "TTG", "UZB", "AAA", "SBA", "AUB"}
STAR_ITEMS = {  # item: (звёзды, название, описание)
    "b50": (50, "Удача x2 на 2 часа", "Повышенный шанс на хорошие номера на 2 часа"),
    "b100": (100, "Набор бустов на 2 часа", "Удача x2, опыт x2 и бонус продажи +25% на 2 часа"),
    "noads": (150, "Отключить рекламу", "Убрать рекламный баннер в игре навсегда"),
    "p500": (500, "Любой номер на выбор", "Номер, который вы выберете сами"),
}

def check_plate(p):  # проверка номера, который игрок хочет купить за звёзды
    try:
        k = int(p["k"]); r = str(p["r"]); d = str(p["d"]); z = str(p["z"]).upper(); a = str(p.get("a", "")).upper()
    except Exception: raise HTTPException(400, "Неверный номер")
    bad = r not in REGS or not (d.isdigit() and len(d) == 3) or d == "000" or not z or not set(z) <= LETTERS
    if k == 1: bad = bad or len(z) != 3
    else: bad = bad or len(z) != 2 or len(a) != 1 or a not in LETTERS
    if bad or (a + z) in BLOCK or z in BLOCK or (k == 0 and r == "01" and a == "R" and d == "700" and z == "TT"):
        raise HTTPException(400, "Этот номер купить нельзя")
    return {"k": k, "r": r, "d": d, "z": z, **({"a": a} if k == 0 else {})}

@app.post("/api/invoice")
async def invoice(r: Request):
    b = await r.json(); uid, _ = auth(b["initData"]); item = b.get("item")
    if item not in STAR_ITEMS: raise HTTPException(400, "Товар не найден")
    amount, title, desc = STAR_ITEMS[item]; payload = {"u": uid, "i": item}
    if item == "p500": payload["p"] = check_plate(b.get("plate") or {})
    pl = json.dumps(payload, separators=(",", ":"))
    data = json.dumps({"title": title, "description": desc, "payload": pl, "provider_token": "", "currency": "XTR",
                       "prices": [{"label": title, "amount": amount}]}).encode()
    req = urllib.request.Request(f"https://api.telegram.org/bot{BOT_TOKEN}/createInvoiceLink", data=data, headers={"Content-Type": "application/json"})
    try: res = json.load(urllib.request.urlopen(req, timeout=10))
    except Exception: raise HTTPException(400, "Не удалось создать счёт")
    if not res.get("ok"): raise HTTPException(400, "Не удалось создать счёт")
    return {"url": res["result"]}

# ---------- аукцион между игроками (в реальном времени, опрос раз в 2 сек) ----------
def settle(c):  # завершить аукционы, у которых вышло время
    now = int(time.time())
    for a in c.execute("SELECT * FROM auctions WHERE status='open' AND end<=?", (now,)).fetchall():
        if c.execute("UPDATE auctions SET status='done' WHERE id=? AND status='open'", (a["id"],)).rowcount != 1: continue
        item = json.loads(a["item"])
        if a["topuid"]:
            gift(c, a["topuid"], item)
            if a["seller"]: gift(c, a["seller"], {"t": "bal", "v": int(a["top"] * 0.95)})  # продавец получает 95%
        elif a["seller"]:
            gift(c, a["seller"], item)   # ставок не было - предмет возвращается
    c.commit()

@app.on_event("startup")
async def _auction_loop():
    async def loop():
        while True:
            await asyncio.sleep(5)
            try: c = db(); settle(c); system_lot(c)
            except Exception: pass
    asyncio.create_task(loop())

def next_price(a): return a["start"] if not a["topuid"] else math.ceil(a["top"] * 1.05)

@app.post("/api/gifts")
async def gifts(r: Request):
    b = await r.json(); uid, _ = auth(b["initData"]); c = db()
    return {"gifts": [{"id": x["id"], **json.loads(x["data"])} for x in c.execute("SELECT * FROM gifts WHERE uid=?", (uid,))]}

@app.post("/api/auc/create")
async def auc_create(r: Request):
    b = await r.json(); uid, _ = auth(b["initData"]); c = db(); item = b.get("item") or {}
    start = int(b.get("start", 0)); mins = int(b.get("mins", 0))
    if item.get("t") not in ("plate", "car") or not isinstance(item.get("v"), dict) or mins not in (1, 3, 10) or not (1000 <= start <= 10**10):
        raise HTTPException(400, "Неверные данные лота")
    if c.execute("SELECT COUNT(*) FROM auctions WHERE seller=? AND status='open'", (uid,)).fetchone()[0] >= 5:
        raise HTTPException(400, "Не больше 5 лотов одновременно")
    c.execute("INSERT INTO auctions(seller,item,start,end) VALUES(?,?,?,?)", (uid, json.dumps(item), start, int(time.time()) + mins * 60)); c.commit()
    return {"ok": 1}

@app.post("/api/auc/list")
async def auc_list(r: Request):
    b = await r.json(); auth(b["initData"]); c = db(); settle(c)
    rows = c.execute("SELECT * FROM auctions WHERE status='open' ORDER BY end LIMIT 50").fetchall()
    return {"now": int(time.time()), "list": [{"id": a["id"], "seller": a["seller"], "item": json.loads(a["item"]), "start": a["start"],
            "top": a["top"], "topuid": a["topuid"], "end": a["end"], "next": next_price(a)} for a in rows]}

@app.post("/api/auc/bid")
async def auc_bid(r: Request):
    b = await r.json(); uid, _ = auth(b["initData"]); c = db(); settle(c); now = int(time.time())
    a = c.execute("SELECT * FROM auctions WHERE id=? AND status='open' AND end>?", (int(b["id"]), now)).fetchone()
    if not a: raise HTTPException(400, "Торги уже закончились")
    if a["seller"] == uid: raise HTTPException(400, "Нельзя ставить на свой лот")
    if a["topuid"] == uid: raise HTTPException(400, "Вы уже лидер")
    price = next_price(a)
    if price != int(b.get("exp", -1)): raise HTTPException(400, "Цена изменилась, попробуйте ещё раз")
    ok = c.execute("UPDATE auctions SET top=?,topuid=?,end=MAX(end,?) WHERE id=? AND status='open' AND end>? AND top=? AND IFNULL(topuid,0)=?",
                   (price, uid, now + 15, a["id"], now, a["top"], a["topuid"] or 0)).rowcount
    if ok != 1: raise HTTPException(400, "Вашу ставку перебили, попробуйте ещё раз")
    if a["topuid"]: gift(c, a["topuid"], {"t": "bal", "v": a["top"]})   # возврат прошлому лидеру
    c.commit()
    return {"ok": 1, "price": price}

def get_rid(c):  # номер "сезона": при сбросе прогресса увеличивается
    r = c.execute("SELECT v FROM meta WHERE k='rid'").fetchone()
    return int(r[0]) if r else 0

# эксклюзивные машины: их нельзя выбить в рулетке, они появляются только как системные лоты аукциона
EXC = [("Rolls-Royce Spectre", 3.5), ("Lamborghini Aventador SVJ", 3.2), ("Ferrari 812 Superfast", 3), ("Bugatti Veyron", 4),
       ("Porsche Carrera GT", 3.8), ("McLaren P1", 4.5), ("Lamborghini Revuelto", 4)]
ULTRA = [("Mercedes G63 «Аюб»", {"k": 1, "r": "01", "d": "007", "z": "AUB"}), ("Zeekr 009 «Бибиша»", {"k": 0, "r": "01", "a": "A", "d": "777", "z": "AC"})]
SYSLOT_EVERY = 1800   # новый эксклюзивный лот не чаще раза в 30 минут (торги идут 10 минут)

def system_lot(c):
    now = int(time.time())
    if c.execute("SELECT 1 FROM auctions WHERE seller=0 AND status='open'").fetchone(): return
    r = c.execute("SELECT v FROM meta WHERE k='syslot'").fetchone()
    if r and now - int(r[0]) < SYSLOT_EVERY: return
    if random.random() < 0.25:   # супер-лот ~1 млрд: Аюб или Бибиша
        n, pl = random.choice(ULTRA)
        item = {"t": "car", "v": {"n": n, "e": "🚙", "t": 4, "m": 2, "pl": pl}}; start = 600_000_000
    else:
        n, m = random.choice(EXC)
        item = {"t": "car", "v": {"n": n, "e": "🏎️", "t": 3, "m": m, "pl": None}}; start = int(30000000 * m * 0.5 / 1000) * 1000
    c.execute("INSERT INTO auctions(seller,item,start,end) VALUES(0,?,?,?)", (json.dumps(item, ensure_ascii=False), start, now + 600))
    c.execute("INSERT OR REPLACE INTO meta(k,v) VALUES('syslot',?)", (str(now),)); c.commit()
