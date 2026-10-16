# UZ Номерок: сервер игры (FastAPI + SQLite). Запускается через start.py вместе с ботом.
# Запросы к базе идут в потоках (сайт не «замирает»), соединения с базой постоянные, есть индексы,
# проверка подписки кэшируется, база сама чистится и раз в сутки копируется в папку backups.
import asyncio, gzip, hashlib, hmac, json, logging, math, os, random, shutil, sqlite3, threading, time
import urllib.error, urllib.parse, urllib.request
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response
from fastapi.staticfiles import StaticFiles

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger("uzn")

BOT_TOKEN = os.environ.get("BOT_TOKEN", "PASTE_BOT_TOKEN_FROM_BOTFATHER")
DB_PATH = os.environ.get("DB_PATH", "game.db")
TG_API = os.environ.get("TG_API", "https://api.telegram.org").rstrip("/")
# Каналы для призов за подписку: id (как в игре, CHS в html) -> @канал. Бот должен быть АДМИНОМ канала.
CHANNELS = {"c1": os.environ.get("CHANNEL1", "@your_channel"), "c2": os.environ.get("CHANNEL2", "@your_channel2")}
REQUIRED_CHANNEL = os.environ.get("REQUIRED_CHANNEL", "@your_channel")           # обязательная подписка, чтобы играть ("" = выключить)
REQUIRED_URL = os.environ.get("REQUIRED_URL", "https://t.me/your_channel")   # ссылка на этот канал
BOT_USERNAME = os.environ.get("BOT_USERNAME", "your_bot")                    # логин бота без @ (для ссылок-приглашений)
REF_MILESTONES = [(1, 3000000), (3, 10000000), (5, 25000000), (10, 100000000)]  # друзья -> приз (сум)
HERE = os.path.dirname(os.path.abspath(__file__))
BK_DIR = os.environ.get("BACKUP_DIR") or os.path.join(os.path.dirname(os.path.abspath(DB_PATH)), "backups")
BK_KEEP = int(os.environ.get("BACKUP_KEEP", "5"))
APP_URL = os.environ.get("APP_URL", "")   # адрес игры (для кнопки «Открыть игру» в сообщениях бота)
MAX_BODY = 3_000_000   # больше 3 МБ от игрока не принимаем

app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

# ---------------------------------------------------------------- база данных
SCHEMA = """
CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY,name TEXT,first_seen INT,last_seen INT,state TEXT);
CREATE TABLE IF NOT EXISTS gifts(id INTEGER PRIMARY KEY AUTOINCREMENT,uid INT,data TEXT);
CREATE TABLE IF NOT EXISTS promos(code TEXT PRIMARY KEY,reward TEXT,max_uses INT,used INT DEFAULT 0);
CREATE TABLE IF NOT EXISTS redeemed(code TEXT,uid INT,PRIMARY KEY(code,uid));
CREATE TABLE IF NOT EXISTS offers(id INTEGER PRIMARY KEY AUTOINCREMENT,frm INT,to_id INT,item TEXT,price INT,status TEXT DEFAULT 'open',ts INT);
CREATE TABLE IF NOT EXISTS refpending(uid INTEGER PRIMARY KEY,ref INT);
CREATE TABLE IF NOT EXISTS referrals(uid INTEGER PRIMARY KEY,ref INT);
CREATE TABLE IF NOT EXISTS events(name TEXT,luck REAL,sell REAL,xp REAL,until INT);
CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY,v TEXT);
CREATE TABLE IF NOT EXISTS uniq(key TEXT PRIMARY KEY,owner INT,ts INT);
CREATE TABLE IF NOT EXISTS auctions(id INTEGER PRIMARY KEY AUTOINCREMENT,seller INT,item TEXT,start INT,top INT DEFAULT 0,topuid INT,end INT,status TEXT DEFAULT 'open');
CREATE TABLE IF NOT EXISTS shop(id INTEGER PRIMARY KEY AUTOINCREMENT,seller INT,item TEXT,price INT,ts INT,status TEXT DEFAULT 'open',buyer INT);
CREATE TABLE IF NOT EXISTS duels(id INTEGER PRIMARY KEY AUTOINCREMENT,a INT,aplate TEXT,stake INT,ts INT,status TEXT DEFAULT 'open',b INT,bplate TEXT,winner INT);
CREATE TABLE IF NOT EXISTS claims(uid INT,id TEXT,PRIMARY KEY(uid,id));
CREATE TABLE IF NOT EXISTS payments(charge TEXT PRIMARY KEY,uid INT,item TEXT,stars INT,ts INT);
CREATE INDEX IF NOT EXISTS ix_gifts_uid ON gifts(uid);
CREATE INDEX IF NOT EXISTS ix_offers_to ON offers(to_id,status);
CREATE INDEX IF NOT EXISTS ix_offers_frm ON offers(frm,status);
CREATE INDEX IF NOT EXISTS ix_ref_ref ON referrals(ref);
CREATE INDEX IF NOT EXISTS ix_auc_open ON auctions(status,end);
CREATE INDEX IF NOT EXISTS ix_auc_seller ON auctions(seller,status);
CREATE INDEX IF NOT EXISTS ix_shop_open ON shop(status,id);
CREATE INDEX IF NOT EXISTS ix_shop_seller ON shop(seller,status);
CREATE INDEX IF NOT EXISTS ix_duel_open ON duels(status,id);
CREATE INDEX IF NOT EXISTS ix_duel_a ON duels(a,status);
CREATE INDEX IF NOT EXISTS ix_duel_b ON duels(b,status);
CREATE INDEX IF NOT EXISTS ix_uniq_owner ON uniq(owner);
CREATE INDEX IF NOT EXISTS ix_users_seen ON users(last_seen);
"""

def init_db():   # один раз при запуске: таблицы, индексы, режим WAL
    d = os.path.dirname(os.path.abspath(DB_PATH))
    if d: os.makedirs(d, exist_ok=True)
    c = sqlite3.connect(DB_PATH, timeout=30)
    try:
        c.execute("PRAGMA journal_mode=WAL"); c.executescript(SCHEMA)
        try: c.execute("ALTER TABLE users ADD COLUMN bal INT DEFAULT 0")
        except sqlite3.OperationalError: pass
        try: c.execute("ALTER TABLE users ADD COLUMN stars INT DEFAULT 0")   # звёздный кошелёк игрока (хранится на сервере)
        except sqlite3.OperationalError: pass
        c.execute("CREATE TABLE IF NOT EXISTS wlog(id INTEGER PRIMARY KEY AUTOINCREMENT,uid INT,d INT,why TEXT,ts INT)")
        c.execute("CREATE TABLE IF NOT EXISTS star_auc(id INTEGER PRIMARY KEY AUTOINCREMENT,seller INT,item TEXT,start INT,top INT DEFAULT 0,topuid INT,end INT,status TEXT DEFAULT 'open')")
        c.execute("CREATE INDEX IF NOT EXISTS ix_sa_status ON star_auc(status,end)")
        c.execute("CREATE INDEX IF NOT EXISTS ix_users_bal ON users(bal)"); c.execute("CREATE INDEX IF NOT EXISTS ix_pay_uid ON payments(uid)"); c.commit()
    finally: c.close()

init_db()
_tl = threading.local()

def db():   # у каждого потока своё постоянное соединение (не открываем базу заново на каждый запрос)
    c = getattr(_tl, "c", None)
    if c is None:
        c = sqlite3.connect(DB_PATH, timeout=15); c.row_factory = sqlite3.Row
        c.execute("PRAGMA synchronous=NORMAL"); c.execute("PRAGMA temp_store=MEMORY"); c.execute("PRAGMA cache_size=-4000")
        _tl.c = c
    elif c.in_transaction: c.rollback()   # хвост прерванного запроса не должен попасть в чужую запись
    return c

def _end_tx():
    c = getattr(_tl, "c", None)
    if c is not None and c.in_transaction:
        try: c.rollback()
        except Exception: pass

def auth(init):  # проверка подписи Telegram initData
    try:
        d = dict(urllib.parse.parse_qsl(init or "")); h = d.pop("hash", "")
        s = "\n".join(f"{k}={v}" for k, v in sorted(d.items()))
        key = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        ok = hmac.compare_digest(hmac.new(key, s.encode(), hashlib.sha256).hexdigest(), h)
        if ok:
            u = json.loads(d["user"]); return u["id"], u.get("first_name", "")
    except Exception: pass
    raise HTTPException(401, "Нет доступа")

# ---------------------------------------------------------------- как обрабатываются запросы
POOL = ThreadPoolExecutor(max_workers=int(os.environ.get("DB_THREADS", "12")), thread_name_prefix="db")   # запросы к базе
TGPOOL = ThreadPoolExecutor(max_workers=6, thread_name_prefix="tg")                                       # запросы к Telegram (медленные)
_LAT = deque(maxlen=20000)   # (время, путь, мс) - для /stat в боте

def _resp(res, gz):
    if isinstance(res, Response): return res
    raw = bytes(res) if isinstance(res, (bytes, bytearray)) else json.dumps(res, separators=(",", ":")).encode()
    h = {"Cache-Control": "no-store"}
    if gz and len(raw) > 1200:
        raw = gzip.compress(raw, 5); h["Content-Encoding"] = "gzip"; h["Vary"] = "Accept-Encoding"
    return Response(raw, media_type="application/json", headers=h)

def api(path, method="post", pool=None):
    """Регистрирует обработчик fn(b): читает JSON, выполняет fn в потоке, отдаёт сжатый ответ."""
    def deco(fn):
        async def h(r: Request):
            p = pool or POOL
            if p._work_queue.qsize() > 400: raise HTTPException(503, "Сервер занят, попробуйте ещё раз")
            try:
                if int(r.headers.get("content-length") or 0) > MAX_BODY: raise HTTPException(413, "Слишком большой запрос")
            except ValueError: pass
            body = await r.body()
            if len(body) > MAX_BODY: raise HTTPException(413, "Слишком большой запрос")
            gz = "gzip" in r.headers.get("accept-encoding", "")
            t0 = time.perf_counter()
            def work():
                try:
                    try: b = json.loads(body) if body else {}
                    except ValueError: raise HTTPException(400, "Неверный запрос")
                    if not isinstance(b, dict): raise HTTPException(400, "Неверный запрос")
                    return _resp(fn(b), gz)
                finally: _end_tx()
            try: return await asyncio.get_running_loop().run_in_executor(p, work)
            finally:
                dt = time.perf_counter() - t0; _LAT.append((time.time(), path, dt * 1000))
                if dt > 2: log.warning("медленный запрос %s: %.1f c", path, dt)
        h.__name__ = fn.__name__
        getattr(app, method)(path)(h)
        return fn
    return deco

def tg_request(method, params=None, timeout=6):   # вызов Telegram Bot API (только из потока TGPOOL)
    url = "%s/bot%s/%s" % (TG_API, BOT_TOKEN, method)
    if params is not None: url = urllib.request.Request(url, data=json.dumps(params).encode(), headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(url, timeout=timeout) as r: return json.load(r)

# ---------------------------------------------------------------- сообщения от бота игрокам (перебили ставку, лот продан и т.д.)
def fnum(n):   # 1 000 000
    try: return "{:,}".format(int(n)).replace(",", " ")
    except Exception: return str(n)

def plate_txt(p):
    try:
        r, d, z, a = str(p.get("r", "")), str(p.get("d", "")), str(p.get("z", "")), str(p.get("a") or "")
        return ("%s %s %s" % (r, d, z)) if p.get("k") else ("%s %s %s %s" % (r, a, d, z))
    except Exception: return "номер"

def item_txt(it):
    try:
        v = it.get("v") or {}
        if it.get("t") == "car":
            return "🚗 %s%s" % (str(v.get("n", "машина"))[:40], (" + номер " + plate_txt(v["pl"])) if isinstance(v.get("pl"), dict) else "")
        return "🔢 номер " + plate_txt(v)
    except Exception: return "предмет"

def left_txt(sec):
    sec = max(0, int(sec))
    if sec >= 3600: return "%d ч %d мин" % (sec // 3600, sec % 3600 // 60)
    if sec >= 60: return "%d мин" % (sec // 60)
    return "%d сек" % sec

def notify(uid, text, btn="🎰 Открыть игру"):
    """Отправить игроку сообщение от бота (в фоне; если игрок заблокировал бота - молча пропускаем)."""
    try: uid = int(uid)
    except (TypeError, ValueError): return
    if uid <= 0 or TGPOOL._work_queue.qsize() > 3000: return
    def job():
        try:
            m = {"chat_id": uid, "text": text[:3500], "disable_web_page_preview": True}
            if APP_URL.startswith("https://"): m["reply_markup"] = {"inline_keyboard": [[{"text": btn, "web_app": {"url": APP_URL}}]]}
            tg_request("sendMessage", m, timeout=8)
        except Exception as e: log.debug("сообщение игроку %s не доставлено: %s", uid, e)
    try: TGPOOL.submit(job)
    except Exception: pass

# ---------------------------------------------------------------- единственные экземпляры: Мифик и Секретный номера есть только у одного игрока
SPSER = {"UZB", "AAA", "SBA"}
SPX = [(1, "879", "MEA", ""), (1, "762", "MMA", ""), (1, "500", "SOO", ""), (0, "777", "ZZ", "Z"), (0, "001", "OO", "O")]
SSX = {"PAA": 4, "PSS": 4, "PPP": 3, "APC": 3, "TSS": 3, "TST": 3, "ZDF": 3, "DXA": 2, "FVV": 2, "VSB": 2, "VSC": 2, "VSF": 2, "VSD": 2, "VSG": 2, "VSV": 2, "PSF": 2,
       "TTA": 1, "TTB": 1, "TTC": 1, "TTD": 1, "TTE": 1, "TTF": 1, "TTG": 1}

def tier_py(p):  # то же, что tier() в игре
    try: k = int(p["k"]); r = str(p["r"]); d = str(p["d"]); z = str(p["z"]); a = str(p.get("a") or "")
    except Exception: return 0
    if r == "01" and any(x[0] == k and x[1] == d and x[2] == z and x[3] == a for x in SPX): return 5
    if k and (z in SPSER or (r == "01" and d == "007" and z == "AUB")): return 5
    if k and z in SSX: return SSX[z]
    if len(d) != 3 or len(z) < 2: return 0
    sc = 0
    if d[0] == d[1] == d[2]: sc += 4
    elif d in ("123", "321", "456", "789", "007", "100") or d[1:] == "00": sc += 3
    elif d[0] == d[2]: sc += 1
    if z[0] == z[1]: sc += 1
    if len(z) > 2 and z[1] == z[2]: sc += 1
    if r == "01": sc += 1
    return 4 if sc >= 6 else 3 if sc >= 5 else 2 if sc >= 3 else 1 if sc >= 2 else 0

def ukey(p): return "%s|%s|%s|%s|%s" % (p.get("k"), p.get("r"), p.get("a") or "", p.get("d"), p.get("z"))
def is_uniq(p): return isinstance(p, dict) and tier_py(p) >= 4

def need_own(c, uid, v):
    """Единственный номер (Мифик/Секретный) можно выставить/поставить в дуэль только если он ваш по базе:
    иначе можно подделать чужой или несуществующий номер."""
    pl = v.get("pl") if isinstance(v, dict) and "pl" in v else v
    if not isinstance(pl, dict): return
    try: p = clean_plate(pl)
    except HTTPException: raise HTTPException(400, "Неверный номер")
    if tier_py(p) >= 4:
        row = c.execute("SELECT owner FROM uniq WHERE key=?", (ukey(p),)).fetchone()
        if not row or row["owner"] != uid: raise HTTPException(400, "Этот единственный номер вам не принадлежит")

def uniq_set(c, uid, p):   # передать номер новому владельцу
    c.execute("INSERT OR REPLACE INTO uniq(key,owner,ts) VALUES(?,?,?)", (ukey(p), uid, int(time.time())))

def gift(c, uid, d):
    c.execute("INSERT INTO gifts(uid,data) VALUES(?,?)", (uid, json.dumps(d)))
    try:   # номер/машина с номером: единственный экземпляр переходит получателю
        if d.get("t") == "plate" and is_uniq(d.get("v")): uniq_set(c, uid, d["v"])
        elif d.get("t") == "car" and is_uniq((d.get("v") or {}).get("pl")): uniq_set(c, uid, d["v"]["pl"])
    except Exception: pass

def clean_plate(p):
    try: k = int(p["k"]); r = str(p["r"]); d = str(p["d"]); z = str(p["z"]).upper(); a = str(p.get("a") or "").upper()
    except Exception: raise HTTPException(400, "Неверный номер")
    return {"k": k, "r": r, "d": d, "z": z, **({"a": a} if k == 0 else {})}

@api("/api/uclaim")
def uclaim(b):   # игрок выбил номер: если это Мифик/Секретный - закрепить его за собой, если свободен
    uid, _ = auth(b.get("initData")); p = clean_plate(b.get("p") or {}); c = db()
    if tier_py(p) < 4: return {"ok": 1, "free": 1}
    c.execute("INSERT OR IGNORE INTO uniq(key,owner,ts) VALUES(?,?,?)", (ukey(p), uid, int(time.time()))); c.commit()   # кто успел первым - тот и владелец
    row = c.execute("SELECT owner FROM uniq WHERE key=?", (ukey(p),)).fetchone()
    if row and row["owner"] != uid: return {"ok": 0, "taken": 1}
    return {"ok": 1}

@api("/api/urel")
def urel(b):   # игрок продал уникальный номер магазину - он снова может выпасть
    uid, _ = auth(b.get("initData")); p = clean_plate(b.get("p") or {}); c = db()
    c.execute("UPDATE uniq SET owner=0 WHERE key=? AND owner=?", (ukey(p), uid)); c.commit()   # номер one-of-one больше никогда не выпадет
    return {"ok": 1}

@api("/api/ucheck")
def ucheck(b):   # проверить номер: редкость, занят ли
    uid, _ = auth(b.get("initData")); p = clean_plate(b.get("p") or {}); c = db(); t = tier_py(p)
    row = c.execute("SELECT owner FROM uniq WHERE key=?", (ukey(p),)).fetchone() if t >= 4 else None
    own = None
    if row:
        u = c.execute("SELECT name FROM users WHERE id=?", (row["owner"],)).fetchone(); own = ("убран из игры" if row["owner"] == 0 else (u["name"] if u else "игрок")) if row["owner"] != uid else "вы"
    return {"tier": t, "unique": t >= 4, "taken": bool(row), "owner": own}

@api("/api/ustats", method="get")
def ustats(b):
    c = db(); n = {4: 0, 5: 0}
    for x in c.execute("SELECT key FROM uniq").fetchall():
        k, r, a, d, z = x["key"].split("|"); t = tier_py({"k": int(k), "r": r, "a": a, "d": d, "z": z})
        if t in n: n[t] += 1
    return {"myth": n[4], "secret": n[5]}

def uniq_backfill():   # один раз при запуске: записать за игроками их Мифик/Секретные номера
    c = db()
    if c.execute("SELECT 1 FROM meta WHERE k='uniq_bf'").fetchone(): return
    for (uid,) in c.execute("SELECT id FROM users WHERE state IS NOT NULL").fetchall():
        row = c.execute("SELECT state FROM users WHERE id=?", (uid,)).fetchone()
        try: st = json.loads(row["state"])
        except Exception: continue
        pls = list(st.get("col") or []) + [x.get("pl") for x in (st.get("cars") or []) if x.get("pl")]
        for p in pls:
            if is_uniq(p) and not c.execute("SELECT 1 FROM uniq WHERE key=?", (ukey(p),)).fetchone(): uniq_set(c, uid, p)
    c.execute("INSERT OR REPLACE INTO meta(k,v) VALUES('uniq_bf','1')"); c.commit()

# ---------------------------------------------------------------- главная страница (сжатая копия в памяти + ETag)
_IDX = {}
def _index_entry():
    path = os.path.join(HERE, "uz-nomerok-tg.html"); mt = os.stat(path).st_mtime_ns
    e = _IDX.get("e")
    if e and e["mt"] == mt: return e
    raw = open(path, "rb").read()
    e = {"mt": mt, "raw": raw, "gz": gzip.compress(raw, 9), "etag": '"%s"' % hashlib.md5(raw).hexdigest()}
    _IDX["e"] = e; return e

@app.get("/")
async def index(r: Request):
    e = _index_entry()
    h = {"ETag": e["etag"], "Cache-Control": "no-cache", "Vary": "Accept-Encoding"}
    if r.headers.get("if-none-match") == e["etag"]: return Response(status_code=304, headers=h)
    if "gzip" in r.headers.get("accept-encoding", ""):
        h["Content-Encoding"] = "gzip"; return Response(e["gz"], media_type="text/html; charset=utf-8", headers=h)
    return Response(e["raw"], media_type="text/html; charset=utf-8", headers=h)

@app.get("/healthz")
async def healthz(): return Response('{"ok":1}', media_type="application/json", headers={"Cache-Control": "no-store"})

# ---------------------------------------------------------------- общие запросы
def get_rid(c):  # номер "сезона": при сбросе прогресса увеличивается
    r = c.execute("SELECT v FROM meta WHERE k='rid'").fetchone()
    return int(r[0]) if r else 0

def gifts_of(c, uid):
    return [{"id": x["id"], **json.loads(x["data"])} for x in c.execute("SELECT id,data FROM gifts WHERE uid=? ORDER BY id LIMIT 500", (uid,))]

def ref_count(c, uid): return c.execute("SELECT COUNT(*) FROM referrals WHERE ref=?", (uid,)).fetchone()[0]

def offers_of(c, uid):
    def rows(w): return [{"id": x["id"], "frm": x["frm"], "to_id": x["to_id"], "item": json.loads(x["item"]), "price": x["price"]}
                         for x in c.execute("SELECT * FROM offers WHERE status='open' AND " + w + " ORDER BY id LIMIT 100", (uid,))]
    return {"incoming": rows("to_id=?"), "outgoing": rows("frm=?")}

_EVC = [0, None]
def event_of(c):   # текущий ивент (кэш 5 сек), общий для всех игроков
    t = time.time()
    if _EVC[1] is None or t - _EVC[0] > 5:
        e = c.execute("SELECT * FROM events WHERE until>? ORDER BY until DESC LIMIT 1", (int(t),)).fetchone()
        _EVC[0] = t; _EVC[1] = {**(dict(e) if e else {}), "rid": get_rid(c)}
    return _EVC[1]

def ref_credit(c, uid):  # засчитать приглашение, когда новый игрок впервые зашёл в игру
    p = c.execute("SELECT ref FROM refpending WHERE uid=?", (uid,)).fetchone()
    if not p or p["ref"] == uid or not c.execute("SELECT 1 FROM users WHERE id=?", (p["ref"],)).fetchone(): return
    if c.execute("INSERT OR IGNORE INTO referrals VALUES(?,?)", (uid, p["ref"])).rowcount != 1: return   # уже засчитан
    n = c.execute("SELECT COUNT(*) FROM referrals WHERE ref=?", (p["ref"],)).fetchone()[0]
    for need, reward in REF_MILESTONES:
        if n == need: gift(c, p["ref"], {"t": "bal", "v": reward})

def _bal_of(st):
    try: v = int(float(st.get("bal") or 0))
    except Exception: v = 0
    return max(0, min(v, 9 * 10 ** 15))

def _hidden_ids():   # админы (ADMINS) и HIDE_IDS не показываются в рейтинге
    out = set()
    for x in (os.environ.get("ADMINS", "") + "," + os.environ.get("HIDE_IDS", "")).replace(";", ",").replace(" ", ",").split(","):
        if x.strip().lstrip("-").isdigit(): out.add(int(x))
    return out
HIDDEN = _hidden_ids()
BAL_JUMP = int(os.environ.get("BAL_JUMP", 3 * 10 ** 9))   # скачок баланса за одно сохранение, после которого админы получают оповещение
_JUMP = {}

def bal_backfill():   # один раз: заполнить рейтинг по уже сохранённым игрокам
    c = db()
    if c.execute("SELECT 1 FROM meta WHERE k='bal_bf'").fetchone(): return
    for r in c.execute("SELECT id,state FROM users WHERE state IS NOT NULL").fetchall():
        try: c.execute("UPDATE users SET bal=? WHERE id=?", (_bal_of(json.loads(r["state"])), r["id"]))
        except Exception: pass
    c.execute("INSERT OR REPLACE INTO meta(k,v) VALUES('bal_bf','1')"); c.commit()

_TOP = {"ts": 0, "list": []}
@api("/api/top")
def top(b):
    uid, _ = auth(b.get("initData")); c = db(); now = time.time()
    if now - _TOP["ts"] > 15:
        hid = sorted(HIDDEN); q = ",".join("?" * len(hid))
        rows = c.execute("SELECT id,name,bal FROM users WHERE bal>0 " + ("AND id NOT IN (%s) " % q if hid else "") + "ORDER BY bal DESC LIMIT 20", hid).fetchall()
        _TOP["list"] = [{"id": r["id"], "name": (r["name"] or "Игрок")[:18], "bal": r["bal"]} for r in rows]; _TOP["ts"] = now
    out = [{"n": i + 1, "name": x["name"], "bal": x["bal"], "me": x["id"] == uid} for i, x in enumerate(_TOP["list"])]
    me = None
    if uid not in HIDDEN:
        r = c.execute("SELECT bal FROM users WHERE id=?", (uid,)).fetchone(); mb = (r["bal"] if r else 0) or 0
        hid = sorted(HIDDEN); q = ",".join("?" * len(hid))
        rk = c.execute("SELECT COUNT(*) FROM users WHERE bal>?" + (" AND id NOT IN (%s)" % q if hid else ""), [mb] + hid).fetchone()[0] + 1
        me = {"rank": rk, "bal": mb}
    return {"list": out, "me": me}

def wallet_of(c, uid):
    r = c.execute("SELECT COALESCE(stars,0) FROM users WHERE id=?", (uid,)).fetchone()
    return int(r[0]) if r else 0

def wadd(c, uid, n, why):
    c.execute("UPDATE users SET stars=COALESCE(stars,0)+? WHERE id=?", (int(n), uid))
    c.execute("INSERT INTO wlog(uid,d,why,ts) VALUES(?,?,?,?)", (uid, int(n), why, int(time.time())))

def wspend(c, uid, n, why):   # списать звёзды атомарно: не получится - ничего не меняется
    n = int(n)
    if n <= 0 or c.execute("UPDATE users SET stars=stars-? WHERE id=? AND COALESCE(stars,0)>=?", (n, uid, n)).rowcount != 1:
        c.rollback(); raise HTTPException(400, "Не хватает ⭐ на звёздном балансе. Пополните баланс")
    c.execute("INSERT INTO wlog(uid,d,why,ts) VALUES(?,?,?,?)", (uid, -n, why, int(time.time())))

def stars_of(c, uid): return wallet_of(c, uid)

@api("/api/load")
def load(b):
    uid, name = auth(b.get("initData")); now = int(time.time()); c = db()
    existed = c.execute("SELECT 1 FROM users WHERE id=?", (uid,)).fetchone()
    c.execute("INSERT INTO users(id,name,first_seen,last_seen) VALUES(?,?,?,?) ON CONFLICT(id) DO UPDATE SET name=?,last_seen=?", (uid, name, now, now, name, now))
    if not existed: ref_credit(c, uid)
    row = c.execute("SELECT state FROM users WHERE id=?", (uid,)).fetchone()
    head = json.dumps({"uid": uid, "gifts": gifts_of(c, uid), "ref": {"n": ref_count(c, uid), "bot": BOT_USERNAME}, "rid": get_rid(c), "stars": stars_of(c, uid)}, separators=(",", ":"))
    c.commit()
    state = row["state"] if row and row["state"] else "null"   # состояние уже хранится как JSON - вставляем как есть
    if state != "null" and ('"z"' in state):
        try:
            st = json.loads(state); uniq_guard(c, uid, st); st.pop("_own", None)
            ns = json.dumps(st, separators=(",", ":"))
            if ns != state:
                c.execute("UPDATE users SET state=?,bal=? WHERE id=?", (ns, _bal_of(st), uid)); c.commit(); state = ns
        except Exception: log.exception("load: чистка дублей")
    return ('{"state":%s,%s' % (state, head[1:])).encode()

_SEEN = {}
@api("/api/poll")
def poll(b):   # один лёгкий запрос вместо четырёх: подарки, обмены, друзья, ивент
    uid, _ = auth(b.get("initData")); c = db(); t = time.time()
    if t - _SEEN.get(uid, 0) > 120:
        _SEEN[uid] = t
        if len(_SEEN) > 100000: _SEEN.clear()
        c.execute("UPDATE users SET last_seen=? WHERE id=?", (int(t), uid)); c.commit()
    return {"gifts": gifts_of(c, uid), "offers": offers_of(c, uid), "ref": {"n": ref_count(c, uid), "bot": BOT_USERNAME}, "ev": event_of(c), "stars": stars_of(c, uid)}

def uniq_guard(c, uid, st):
    """Единственные номера (Мифик/Секретный): у каждого - ровно один владелец.
    Чужой/уже занятый номер в состоянии игрока удаляется, свободный - закрепляется за ним."""
    dups = []; seen = set(); st["_own"] = 0
    def chk(p):
        if not isinstance(p, dict): return True
        try: pp = clean_plate(p)
        except HTTPException: return True
        if tier_py(pp) < 4: return True
        k = ukey(pp)
        c.execute("INSERT OR IGNORE INTO uniq(key,owner,ts) VALUES(?,?,?)", (k, uid, int(time.time())))
        row = c.execute("SELECT owner FROM uniq WHERE key=?", (k,)).fetchone()
        if row and row["owner"] != uid:
            dups.append(pp); return False
        if k in seen:            # второй такой же номер у самого владельца
            st["_own"] += 1; return False
        seen.add(k)
        return True
    col = st.get("col")
    if isinstance(col, list):
        st["col"] = [p for p in col if chk(p)]
    cars = st.get("cars")
    if isinstance(cars, list):
        for car in cars:
            if isinstance(car, dict) and car.get("pl") is not None and not chk(car["pl"]): car["pl"] = None
    if dups:
        log.warning("uniq: дубликаты у %s: %s", uid, [ukey(p) for p in dups])
    if st.get("_own"): log.warning("uniq: двойники у владельца %s: %s", uid, st["_own"])
    return dups

@api("/api/save")
def save(b):
    uid, _ = auth(b.get("initData")); c = db(); st = b.get("state")
    if not isinstance(st, dict): raise HTTPException(400, "Неверное состояние")
    if str(st.get("rid", 0)) != str(get_rid(c)): return {"stale": 1}   # игра из до сброса прогресса - не сохраняем
    dups = uniq_guard(c, uid, st); own = st.pop("_own", 0)
    try:    # подозрительный скачок баланса за одно сохранение - сообщаем админам (читеры правят состояние игры)
        old = c.execute("SELECT bal FROM users WHERE id=?", (uid,)).fetchone(); nb = _bal_of(st)
        if old and nb - (old["bal"] or 0) > BAL_JUMP and uid not in HIDDEN and time.time() - _JUMP.get(uid, 0) > 3600:
            _JUMP[uid] = time.time(); log.warning("ПОДОЗРЕНИЕ: баланс %s: %s -> %s", uid, old["bal"], nb)
            for a in HIDDEN: notify(a, "⚠️ Подозрительный скачок баланса\nИгрок ID %s: %s → %s сум" % (uid, fnum(old["bal"] or 0), fnum(nb)), "🎰 Открыть игру")
    except Exception: log.exception("jump check")
    c.execute("UPDATE users SET state=?,last_seen=?,bal=? WHERE id=?", (json.dumps(st, separators=(",", ":")), int(time.time()), _bal_of(st), uid)); c.commit()
    r = {"ok": 1}
    if dups: r["dup"] = dups
    if own: r["own"] = own
    return r

@api("/api/gifts/clear")
def clear(b):
    uid, _ = auth(b.get("initData")); c = db()
    try: ids = [int(i) for i in (b.get("ids") or [])][:500]
    except (TypeError, ValueError): raise HTTPException(400, "Неверные данные")
    if ids: c.execute("DELETE FROM gifts WHERE uid=? AND id IN (%s)" % ",".join("?" * len(ids)), [uid, *ids]); c.commit()
    return {"ok": 1}

@api("/api/gifts")
def gifts(b):
    uid, _ = auth(b.get("initData")); return {"gifts": gifts_of(db(), uid)}

@api("/api/promo")
def promo(b):
    uid, _ = auth(b.get("initData")); code = str(b.get("code", "")).strip().upper(); c = db()
    p = c.execute("SELECT * FROM promos WHERE code=?", (code,)).fetchone()
    if not p: raise HTTPException(400, "Промокод не найден")
    if p["used"] >= p["max_uses"]: raise HTTPException(400, "Промокод закончился")
    try: c.execute("INSERT INTO redeemed VALUES(?,?)", (code, uid))
    except sqlite3.IntegrityError: raise HTTPException(400, "Вы уже активировали этот код")
    if c.execute("UPDATE promos SET used=used+1 WHERE code=? AND used<max_uses", (code,)).rowcount != 1:   # последний код забрали в ту же секунду
        c.rollback(); raise HTTPException(400, "Промокод закончился")
    c.commit()
    return {"reward": json.loads(p["reward"])}

@api("/api/offer/create")
def offer_create(b):
    uid, uname = auth(b.get("initData")); c = db()
    try: to = int(b["to"]); price = int(b["price"])
    except (KeyError, TypeError, ValueError): raise HTTPException(400, "Неверные данные")
    item = b.get("item")
    if price < 0 or not c.execute("SELECT 1 FROM users WHERE id=?", (to,)).fetchone():
        raise HTTPException(400, "Игрок с таким ID не найден")
    if not isinstance(item, dict) or item.get("t") not in ("plate", "car") or not isinstance(item.get("v"), dict):
        raise HTTPException(400, "Неверные данные предложения")
    need_own(c, uid, item["v"])
    c.execute("INSERT INTO offers(frm,to_id,item,price,ts) VALUES(?,?,?,?,?)", (uid, to, json.dumps(item), price, int(time.time()))); c.commit()
    notify(to, "🤝 %s предлагает вам обмен:\n%s\nЦена: %s" % (str(uname)[:30] or ("ID %d" % uid), item_txt(item), (fnum(price) + " сум") if price else "бесплатно (подарок)"), "🤝 Открыть предложение")
    return {"ok": 1}

@api("/api/offer/list")
def offer_list(b):
    uid, _ = auth(b.get("initData")); return offers_of(db(), uid)

@api("/api/offer/accept")
def offer_accept(b):
    uid, _ = auth(b.get("initData")); c = db()
    o = c.execute("SELECT * FROM offers WHERE id=? AND status='open'", (int(b.get("id", 0)),)).fetchone()
    if not o or o["to_id"] != uid: raise HTTPException(400, "Предложение недоступно")
    if c.execute("UPDATE offers SET status='done' WHERE id=? AND status='open'", (o["id"],)).rowcount != 1:
        raise HTTPException(400, "Предложение недоступно")
    gift(c, uid, json.loads(o["item"])); gift(c, o["frm"], {"t": "bal", "v": o["price"]}); c.commit()
    notify(o["frm"], "✅ Ваше предложение принято!\n%s\nВам начислено: %s сум" % (item_txt(json.loads(o["item"])), fnum(o["price"])))
    return {"ok": 1}

@api("/api/offer/cancel")
def offer_cancel(b):
    uid, _ = auth(b.get("initData")); c = db()
    o = c.execute("SELECT * FROM offers WHERE id=? AND status='open'", (int(b.get("id", 0)),)).fetchone()
    if not o or o["frm"] != uid: raise HTTPException(400, "Предложение недоступно")
    if c.execute("UPDATE offers SET status='cancelled' WHERE id=? AND status='open'", (o["id"],)).rowcount != 1:
        raise HTTPException(400, "Предложение недоступно")
    gift(c, uid, json.loads(o["item"])); c.commit()
    return {"ok": 1}

os.makedirs(os.path.join(HERE, "cars"), exist_ok=True)

class CarFiles(StaticFiles):   # фото машин кэшируются в телефоне на неделю
    def file_response(self, *a, **k):
        r = super().file_response(*a, **k); r.headers["Cache-Control"] = "public, max-age=604800"; return r

app.mount("/cars", CarFiles(directory=os.path.join(HERE, "cars")), name="cars")  # фото машин: cars/имя.jpg
_CI = [0, []]

@api("/api/carimgs", method="get")
def carimgs(b):   # какие фото машин реально лежат в папке cars (игра не просит несуществующие)
    t = time.time()
    if t - _CI[0] > 60:
        try: _CI[1] = sorted(f[:-4] for f in os.listdir(os.path.join(HERE, "cars")) if f.lower().endswith(".jpg"))
        except OSError: _CI[1] = []
        _CI[0] = t
    return {"list": _CI[1]}

# ---------------------------------------------------------------- подписка на каналы (Telegram отвечает медленно: отдельные потоки + кэш)
_GATE = {}   # (канал, игрок) -> (до какого времени верить, подписан, когда проверяли)

def member_ok(chat, uid, force=False):
    key = (chat, uid); t = time.time(); e = _GATE.get(key)
    if e and e[0] > t and not (force and not e[1] and t - e[2] > 3): return e[1]
    try:
        res = tg_request("getChatMember?chat_id=%s&user_id=%d" % (urllib.parse.quote(chat), uid))
        ok = res.get("result", {}).get("status") in ("member", "administrator", "creator"); ttl = 600 if ok else 15
    except Exception as ex:
        log.warning("getChatMember не ответил: %s", type(ex).__name__)
        ok, ttl = True, 60   # не удалось проверить (бот не админ?) - не блокируем игру
    if len(_GATE) > 50000: _GATE.clear()
    _GATE[key] = (t + ttl, ok, t); return ok

@api("/api/gate", pool=TGPOOL)
def gate(b):
    uid, _ = auth(b.get("initData"))
    if not REQUIRED_CHANNEL: return {"ok": True}
    return {"ok": member_ok(REQUIRED_CHANNEL, uid, bool(b.get("force"))), "url": REQUIRED_URL}

@api("/api/sub", pool=TGPOOL)
def sub(b):
    uid, _ = auth(b.get("initData")); cid = b.get("id"); c = db()
    ch = CHANNELS.get(cid)
    if not ch: raise HTTPException(400, "Канал не найден")
    if c.execute("SELECT 1 FROM claims WHERE uid=? AND id=?", (uid, cid)).fetchone(): raise HTTPException(400, "Уже получено")
    try: res = tg_request("getChatMember?chat_id=%s&user_id=%d" % (urllib.parse.quote(ch), uid), timeout=8)
    except Exception: raise HTTPException(400, "Не удалось проверить подписку")
    if res.get("result", {}).get("status") not in ("member", "administrator", "creator"):
        raise HTTPException(400, "Сначала подпишитесь на канал")
    try: c.execute("INSERT INTO claims VALUES(?,?)", (uid, cid)); c.commit()
    except sqlite3.IntegrityError: raise HTTPException(400, "Уже получено")
    return {"ok": 1}

@api("/api/event")
def event(b):
    auth(b.get("initData")); return event_of(db())

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

# ---------------------------------------------------------------- звёзды: пополнение баланса и «звёздная крутка» (единственные номера)
STAR_RATE = int(os.environ.get("STAR_RATE", "1000000"))   # сколько сум даём за 1 ⭐ (можно поменять в настройках Render: STAR_RATE)
STAR_MIN, STAR_MAX = 50, 5000
WALLET_MIN, WALLET_MAX = 10, 10000                         # пополнение звёздного баланса
SA_FEE, SA_MIN = 5, 10                                    # комиссия звёздного аукциона (%) и минимальная стартовая ставка (⭐)                              # пополнение баланса: от 50 ⭐
STAR_BONUS = [(1000, 25), (250, 10)]                       # от 250 ⭐ +10%, от 1000 ⭐ +25%
SPIN_PRICE = 15                                            # одна звёздная крутка = 15 ⭐
SPIN_PACKS = (1, 5, 10)
SPIN_MYTHIC = 0.10                                         # шанс Мифика в крутке за звёзды, остальное - Секретный
SPIN_PAA = 0.03                                            # шанс номера PAA в крутке за звёзды
SPIN_SAME = 0.05                                           # шанс одинаковых цифр (777) у Секретной спецсерии - такие выпадают только за звёзды
SPIN_SECRET = 0.10                                         # шанс Секретного номера; иначе Мифик (оба - единственные в игре)

def star_sum(stars):
    pct = next((p for lim, p in STAR_BONUS if stars >= lim), 0)
    return int(stars * STAR_RATE * (100 + pct) / 100)

@api("/api/stars")
def stars_info(b):
    auth(b.get("initData"))
    return {"rate": STAR_RATE, "min": STAR_MIN, "max": STAR_MAX, "bonus": [[lim, p] for lim, p in STAR_BONUS], "spin": SPIN_PRICE,
            "packs": list(SPIN_PACKS), "secret": 100 - int(SPIN_MYTHIC * 100) - int(SPIN_PAA * 100), "mythic": int(SPIN_MYTHIC * 100), "paa": int(SPIN_PAA * 100), "wmin": WALLET_MIN, "wmax": WALLET_MAX, "samin": SA_MIN, "safee": SA_FEE}

_PLR = ["01", "10", "20", "25", "30", "40", "50", "60", "70", "75", "80", "85", "90", "95"]
_ULTRA = [{"k": 1, "r": "01", "d": "879", "z": "MEA"}, {"k": 1, "r": "01", "d": "762", "z": "MMA"}, {"k": 1, "r": "01", "d": "500", "z": "SOO"},
          {"k": 0, "r": "01", "a": "Z", "d": "777", "z": "ZZ"}, {"k": 0, "r": "01", "a": "O", "d": "001", "z": "OO"}, {"k": 1, "r": "01", "d": "007", "z": "AUB"}]

def _claim(c, uid, p):   # атомарно занять единственный номер: получится только у одного
    return c.execute("INSERT OR IGNORE INTO uniq(key,owner,ts) VALUES(?,?,?)", (ukey(p), uid, int(time.time()))).rowcount == 1

def _pretty(d): return d[0] == d[1] == d[2] or d in ("123", "321", "456", "789", "007", "100") or d[1:] == "00"

SER_REG = {"SBA": ("25", "40"), "AAA": ("25", "40"), "UZB": ("25", "40", "01")}   # где выпадают спецсерии (Джизак, Фергана; UZB ещё Ташкент)
PAA_REG = ("25", "40", "01")

def roll_unique(c, uid, reg=""):
    """Крутка за звёзды. Регион можно выбрать (reg), пусто - любой.
    PAA (3%) и Секретный (спецсерии SBA/AAA/UZB) - только в своих регионах: SBA/AAA на 25 и 40, UZB и PAA на 25, 40 и 01.
    Мифик (тройные цифры) - в любом регионе. Нет подходящего - Мифик; свободных нет вовсе - None (сумы)."""
    if reg not in _PLR: reg = ""
    if _RND.random() < SPIN_PAA and (not reg or reg in PAA_REG):
        for _ in range(400):
            p = {"k": 1, "r": reg or _RND.choice(PAA_REG), "d": "%03d" % _RND.randint(1, 999), "z": "PAA"}
            if tier_py(p) >= 4 and _claim(c, uid, p): return p
    sers = [z for z in ("SBA", "AAA", "UZB") if not reg or reg in SER_REG[z]]
    want_sec = bool(sers) and _RND.random() >= SPIN_MYTHIC
    if want_sec:
        w = {"SBA": 40, "AAA": 40, "UZB": 20}
        for _ in range(800):
            z = _RND.choices(sers, [w[x] for x in sers])[0]
            r = reg or _RND.choice(SER_REG[z])
            if _RND.random() < SPIN_SAME: d = str(_RND.randint(1, 9)) * 3
            else:
                d = "%03d" % _RND.randint(1, 999)
                if _pretty(d): continue
            p = {"k": 1, "r": r, "d": d, "z": z}
            if tier_py(p) == 5 and _claim(c, uid, p): return p
    for _ in range(800):   # Мифик (или запасной вариант)
        x = str(_RND.randint(1, 9)); b = _RND.choice(sorted(LETTERS))
        if reg not in ("", "01") or _RND.random() < 0.5: p = {"k": 1, "r": reg or _RND.choice(_PLR), "d": x * 3, "z": b * 3}
        else: p = {"k": 0, "r": "01", "a": _RND.choice(sorted(LETTERS)), "d": x * 3, "z": b * 2}
        if tier_py(p) == 4 and _claim(c, uid, p): return p
    return None

def apply_payment(c, d, total, charge):
    """Выдать оплаченное. Возвращает текст для игрока или None, если платёж уже обработан/неверный."""
    uid = int(d["u"]); item = d.get("i")
    try: c.execute("INSERT INTO payments VALUES(?,?,?,?,?)", (charge, uid, item, total, int(time.time())))
    except sqlite3.IntegrityError: return None
    msg = "⭐ Оплата получена! Откройте игру: подарок уже там."
    if item == "b50": gift(c, uid, {"t": "boost", "k": "bl", "v": 120})
    elif item == "b100":
        for k in ("bl", "bx", "bs"): gift(c, uid, {"t": "boost", "k": k, "v": 120})
    elif item == "noads": gift(c, uid, {"t": "noads"})
    elif item == "wallet":
        st = int(d.get("s", 0))
        if st != total or not (WALLET_MIN <= st <= WALLET_MAX): log.error("платёж wallet: сумма не сходится %s %s", st, total); c.commit(); return None
        wadd(c, uid, st, "topup"); msg = "⭐ Звёздный баланс пополнен на %d ⭐. Тратьте их в игре." % st
    elif item == "p500": gift(c, uid, {"t": "plate", "v": d["p"]})
    elif item == "bal":
        st = int(d.get("s", 0))
        if st != total or not (STAR_MIN <= st <= STAR_MAX): log.error("платёж bal: сумма не сходится %s %s", st, total); c.commit(); return None
        gift(c, uid, {"t": "bal", "v": star_sum(st)}); msg = "⭐ Оплата получена! На баланс придёт %s сум. Откройте игру." % fnum(star_sum(st))
    elif item == "spin":
        n = int(d.get("n", 0))
        if n not in SPIN_PACKS or total < n * SPIN_PRICE: log.error("платёж spin: неверная сумма %s %s", n, total); c.commit(); return None
        got = []
        for _ in range(n):
            p = roll_unique(c, uid, str(d.get("g") or ""))
            if p: gift(c, uid, {"t": "plate", "v": p, "src": "spin"}); got.append("%s (%s)" % (plate_txt(p), "Секретный" if tier_py(p) >= 5 else "PAA" if p.get("z") == "PAA" else "Мифик"))
            else: gift(c, uid, {"t": "bal", "v": SPIN_PRICE * STAR_RATE})   # единственные номера закончились - вернём стоимость сумами
        msg = "🎰 Звёздная крутка! Ваши единственные номера:\n" + "\n".join("🔢 " + x for x in got) + "\nОни уже в игре."
    c.commit()
    return msg

def check_plate(p):  # проверка номера, который игрок хочет купить за звёзды
    try:
        k = int(p["k"]); r = str(p["r"]); d = str(p["d"]); z = str(p["z"]).upper(); a = str(p.get("a", "")).upper()
    except Exception: raise HTTPException(400, "Неверный номер")
    bad = r not in REGS or not (d.isdigit() and len(d) == 3) or d == "000" or not z or not set(z) <= LETTERS
    if k == 1: bad = bad or len(z) != 3
    else: bad = bad or len(z) != 2 or len(a) != 1 or a not in LETTERS
    if not bad and tier_py({"k": k, "r": r, "d": d, "z": z, "a": a}) >= 4: bad = True   # Мифик и Секретные номера купить нельзя - они единственные
    if bad or (a + z) in BLOCK or z in BLOCK or (k == 0 and r == "01" and a == "R" and d == "700" and z == "TT"):
        raise HTTPException(400, "Этот номер купить нельзя")
    return {"k": k, "r": r, "d": d, "z": z, **({"a": a} if k == 0 else {})}

@api("/api/invoice", pool=TGPOOL)
def invoice(b):
    uid, _ = auth(b.get("initData")); item = b.get("item")
    if item == "bal":
        try: stars = int(b.get("s", 0))
        except (TypeError, ValueError): raise HTTPException(400, "Неверная сумма")
        if not (STAR_MIN <= stars <= STAR_MAX): raise HTTPException(400, "Пополнение от %d до %d ⭐" % (STAR_MIN, STAR_MAX))
        amount, title, desc = stars, "Пополнение баланса", "%s сум на игровой баланс" % fnum(star_sum(stars)); payload = {"u": uid, "i": item, "s": stars}
    elif item == "wallet":
        try: stars = int(b.get("s", 0))
        except (TypeError, ValueError): raise HTTPException(400, "Неверная сумма")
        if not (WALLET_MIN <= stars <= WALLET_MAX): raise HTTPException(400, "Пополнение от %d до %d ⭐" % (WALLET_MIN, WALLET_MAX))
        amount, title, desc = stars, "Звёздный баланс", "Пополнение звёздного баланса на %d ⭐ — тратьте в игре" % stars; payload = {"u": uid, "i": item, "s": stars}
    elif item == "spin":
        try: n = int(b.get("n", 0))
        except (TypeError, ValueError): raise HTTPException(400, "Неверное количество")
        if n not in SPIN_PACKS: raise HTTPException(400, "Неверное количество")
        reg = str(b.get("reg") or "")
        if reg and reg not in _PLR: raise HTTPException(400, "Неверный регион")
        amount, title, desc = n * SPIN_PRICE, "Звёздная крутка x%d" % n, "Выпадает единственный в игре номер: Мифик или Секретный"; payload = {"u": uid, "i": item, "n": n}
        if reg: payload["g"] = reg
    elif item not in STAR_ITEMS: raise HTTPException(400, "Товар не найден")
    else: amount, title, desc = STAR_ITEMS[item]; payload = {"u": uid, "i": item}
    if item == "p500": payload["p"] = check_plate(b.get("plate") or {})
    pl = json.dumps(payload, separators=(",", ":"))
    try: res = tg_request("createInvoiceLink", {"title": title, "description": desc, "payload": pl, "provider_token": "", "currency": "XTR",
                                                "prices": [{"label": title, "amount": amount}]}, timeout=10)
    except urllib.error.HTTPError as e:   # Telegram отказал: покажем точную причину (и в логах Render)
        try: desc = json.load(e).get("description", "")
        except Exception: desc = ""
        log.error("createInvoiceLink %s отклонён: %s %s", item, e.code, desc)
        raise HTTPException(400, "Telegram: %s" % (desc[:90] or "счёт не создан"))
    except Exception as e:
        log.error("createInvoiceLink %s ошибка: %r", item, e)
        raise HTTPException(400, "Не удалось создать счёт, попробуйте ещё раз")
    if not res.get("ok"):
        log.error("createInvoiceLink %s: %s", item, res)
        raise HTTPException(400, "Telegram: %s" % str(res.get("description", "счёт не создан"))[:90])
    return {"url": res["result"]}

# ---------------------------------------------------------------- аукцион между игроками (в реальном времени, опрос раз в 2 сек)
def settle(c):  # завершить аукционы, у которых вышло время
    now = int(time.time()); notes = []
    for a in c.execute("SELECT * FROM auctions WHERE status='open' AND end<=?", (now,)).fetchall():
        if c.execute("UPDATE auctions SET status='done' WHERE id=? AND status='open'", (a["id"],)).rowcount != 1: continue
        item = json.loads(a["item"]); nm = item_txt(item)
        if a["topuid"]:
            gift(c, a["topuid"], item)
            notes.append((a["topuid"], "🏆 Вы выиграли аукцион!\n%s\nВаша ставка: %s сум. Предмет уже в вашей коллекции." % (nm, fnum(a["top"])), "🏆 Забрать"))
            if a["seller"]:
                got = int(a["top"] * 0.95); gift(c, a["seller"], {"t": "bal", "v": got})  # продавец получает 95%
                notes.append((a["seller"], "✅ Ваш лот продан!\n%s\nЦена: %s сум. Вам начислено %s сум (95%%)." % (nm, fnum(a["top"]), fnum(got)), "💰 Открыть игру"))
        elif a["seller"]:
            gift(c, a["seller"], item)   # ставок не было - предмет возвращается
            notes.append((a["seller"], "⌛ Торги закончились без ставок.\n%s\nПредмет вернулся в вашу коллекцию." % nm, "Открыть игру"))
    c.commit()
    for n in notes: notify(*n)

def next_price(a): return a["start"] if not a["topuid"] else math.ceil(a["top"] * 1.05)

_AUC = [0, []]
def auc_snapshot(c):   # список лотов: один раз в секунду на всех игроков сразу
    t = time.time()
    if t - _AUC[0] < 1.0: return _AUC[1]
    if c.execute("SELECT 1 FROM auctions WHERE status='open' AND end<=? LIMIT 1", (int(t),)).fetchone(): settle(c)
    rows = c.execute("SELECT * FROM auctions WHERE status='open' ORDER BY end LIMIT 100").fetchall()
    _AUC[1] = [{"id": a["id"], "seller": a["seller"], "item": json.loads(a["item"]), "start": a["start"],
                "top": a["top"], "topuid": a["topuid"], "end": a["end"], "next": next_price(a)} for a in rows]
    _AUC[0] = t; return _AUC[1]

@api("/api/auc/create")
def auc_create(b):
    uid, _ = auth(b.get("initData")); c = db(); item = b.get("item") or {}
    try: start = int(b.get("start", 0)); mins = int(b.get("mins", 0))
    except (TypeError, ValueError): raise HTTPException(400, "Неверные данные лота")
    if item.get("t") not in ("plate", "car") or not isinstance(item.get("v"), dict) or mins not in (1, 3, 10, 60, 360, 1440) or not (1000 <= start <= 10**10):
        raise HTTPException(400, "Неверные данные лота")
    need_own(c, uid, item["v"])
    if c.execute("SELECT COUNT(*) FROM auctions WHERE seller=? AND status='open'", (uid,)).fetchone()[0] >= 10:
        raise HTTPException(400, "Не больше 10 лотов одновременно")
    c.execute("INSERT INTO auctions(seller,item,start,end) VALUES(?,?,?,?)", (uid, json.dumps(item), start, int(time.time()) + mins * 60)); c.commit()
    _AUC[0] = 0
    return {"ok": 1}

@api("/api/auc/list")
def auc_list(b):   # список лотов + подарки игрока одним запросом
    uid, _ = auth(b.get("initData")); c = db()
    return {"now": int(time.time()), "list": auc_snapshot(c), "gifts": gifts_of(c, uid)}

@api("/api/auc/bid")
def auc_bid(b):
    uid, _ = auth(b.get("initData")); c = db(); settle(c); now = int(time.time())
    try: aid = int(b["id"])
    except (KeyError, TypeError, ValueError): raise HTTPException(400, "Неверные данные")
    a = c.execute("SELECT * FROM auctions WHERE id=? AND status='open' AND end>?", (aid, now)).fetchone()
    if not a: raise HTTPException(400, "Торги уже закончились")
    if a["seller"] == uid: raise HTTPException(400, "Нельзя ставить на свой лот")
    if a["topuid"] == uid: raise HTTPException(400, "Вы уже лидер")
    price = next_price(a)
    if price != int(b.get("exp", -1)): raise HTTPException(400, "Цена изменилась, попробуйте ещё раз")
    ok = c.execute("UPDATE auctions SET top=?,topuid=?,end=MAX(end,?) WHERE id=? AND status='open' AND end>? AND top=? AND IFNULL(topuid,0)=?",
                   (price, uid, now + 15, a["id"], now, a["top"], a["topuid"] or 0)).rowcount
    if ok != 1: raise HTTPException(400, "Вашу ставку перебили, попробуйте ещё раз")
    if a["topuid"]: gift(c, a["topuid"], {"t": "bal", "v": a["top"]})   # возврат прошлому лидеру
    c.commit(); _AUC[0] = 0
    if a["topuid"]:
        notify(a["topuid"], "🔨 Вашу ставку перебили!\n%s\nНовая ставка: %s сум. Ваши %s сум уже вернулись на баланс.\nДо конца торгов: %s" % (
            item_txt(json.loads(a["item"])), fnum(price), fnum(a["top"]), left_txt(max(a["end"], now + 15) - now)), "🔨 Перебить ставку")
    return {"ok": 1, "price": price}

@api("/api/auc/cancel")
def auc_cancel(b):   # продавец снимает свой лот, если ставок ещё нет
    uid, _ = auth(b.get("initData")); c = db()
    try: aid = int(b["id"])
    except (KeyError, TypeError, ValueError): raise HTTPException(400, "Неверные данные")
    a = c.execute("SELECT * FROM auctions WHERE id=? AND status='open'", (aid,)).fetchone()
    if not a or a["seller"] != uid: raise HTTPException(400, "Лот недоступен")
    if c.execute("UPDATE auctions SET status='cancelled' WHERE id=? AND status='open' AND topuid IS NULL", (aid,)).rowcount != 1:
        raise HTTPException(400, "На лот уже сделали ставку, снять нельзя")
    gift(c, uid, json.loads(a["item"])); c.commit(); _AUC[0] = 0
    return {"ok": 1}

# ---------------------------------------------------------------- покупки за звёздный баланс
@api("/api/wbuy")
def wbuy(b):
    """Купить что-то за ⭐ со звёздного баланса (пополнение баланса - отдельная оплата через Telegram)."""
    uid, _ = auth(b.get("initData")); c = db(); item = b.get("item"); d = {"u": uid, "i": item}
    if item == "spin":
        try: n = int(b.get("n", 0))
        except (TypeError, ValueError): raise HTTPException(400, "Неверное количество")
        if n not in SPIN_PACKS: raise HTTPException(400, "Неверное количество")
        reg = str(b.get("reg") or "")
        if reg and reg not in _PLR: raise HTTPException(400, "Неверный регион")
        price = n * SPIN_PRICE; d["n"] = n
        if reg: d["g"] = reg
    elif item == "p500": d["p"] = check_plate(b.get("plate") or {}); price = STAR_ITEMS["p500"][0]
    elif item in ("b50", "b100", "noads"): price = STAR_ITEMS[item][0]
    elif item == "bal":
        try: st = int(b.get("s", 0))
        except (TypeError, ValueError): raise HTTPException(400, "Неверная сумма")
        if not (STAR_MIN <= st <= STAR_MAX): raise HTTPException(400, "Обмен от %d до %d ⭐" % (STAR_MIN, STAR_MAX))
        price = st; d["s"] = st
    else: raise HTTPException(400, "Товар не найден")
    wspend(c, uid, price, "buy:" + item)
    msg = apply_payment(c, d, price, "wallet:" + os.urandom(8).hex())
    if msg is None:
        wadd(c, uid, price, "refund:" + item); c.commit(); raise HTTPException(400, "Не удалось выдать покупку, звёзды вернулись на баланс")
    c.commit()
    return {"ok": 1, "stars": wallet_of(c, uid), "msg": msg, "item": item}

# ---------------------------------------------------------------- аукцион за звёзды: только Мифик и Секретные номера
def sa_next(a): return a["start"] if not a["topuid"] else a["top"] + max(1, math.ceil(a["top"] * 0.05))

def sa_settle(c):
    now = int(time.time()); notes = []
    for a in c.execute("SELECT * FROM star_auc WHERE status='open' AND end<=?", (now,)).fetchall():
        if c.execute("UPDATE star_auc SET status='done' WHERE id=? AND status='open'", (a["id"],)).rowcount != 1: continue
        p = json.loads(a["item"]); nm = plate_txt(p)
        if a["topuid"]:
            fee = a["top"] * SA_FEE // 100; got = a["top"] - fee
            gift(c, a["topuid"], {"t": "plate", "v": p}); wadd(c, a["seller"], got, "sa-sold:%d" % a["id"])
            notes.append((a["topuid"], "🏆 Вы выиграли аукцион за ⭐!\n🔢 %s\nСтавка: %d ⭐. Номер уже в вашем гараже." % (nm, a["top"]), "Открыть игру"))
            notes.append((a["seller"], "✅ Ваш номер продан за ⭐!\n🔢 %s\nЦена: %d ⭐. Вам зачислено %d ⭐ (комиссия %d%%)." % (nm, a["top"], got, SA_FEE), "Открыть игру"))
        else:
            gift(c, a["seller"], {"t": "plate", "v": p})
            notes.append((a["seller"], "⌛ Торги за ⭐ закончились без ставок.\n🔢 %s\nНомер вернулся в ваш гараж." % nm, "Открыть игру"))
    c.commit()
    for n in notes: notify(*n)

_SA = [0, []]
def sa_snapshot(c):
    t = time.time()
    if t - _SA[0] < 1.0: return _SA[1]
    rows = c.execute("SELECT a.*,u.name sn FROM star_auc a LEFT JOIN users u ON u.id=a.seller WHERE a.status='open' ORDER BY a.end LIMIT 150").fetchall()
    _SA[1] = [{"id": r["id"], "seller": r["seller"], "sn": str(r["sn"] or "")[:24], "plate": json.loads(r["item"]), "start": r["start"], "top": r["top"], "lead": r["topuid"] or 0,
               "end": r["end"], "next": sa_next(r)} for r in rows]
    _SA[0] = t; return _SA[1]

@api("/api/sa/list")
def sa_list(b):
    uid, _ = auth(b.get("initData")); c = db()
    if c.execute("SELECT 1 FROM star_auc WHERE status='open' AND end<=? LIMIT 1", (int(time.time()),)).fetchone(): sa_settle(c); _SA[0] = 0
    return {"now": int(time.time()), "list": sa_snapshot(c), "stars": wallet_of(c, uid), "gifts": gifts_of(c, uid)}

@api("/api/sa/create")
def sa_create(b):
    uid, _ = auth(b.get("initData")); c = db(); p = clean_plate(b.get("plate") or {})
    try: start = int(b.get("start", 0)); mins = int(b.get("mins", 0))
    except (TypeError, ValueError): raise HTTPException(400, "Неверные данные лота")
    if tier_py(p) < 4: raise HTTPException(400, "На звёздный аукцион можно выставить только Мифик и Секретные номера")
    if mins not in (10, 60, 360, 1440) or not (SA_MIN <= start <= 1000000): raise HTTPException(400, "Стартовая ставка от %d ⭐" % SA_MIN)
    if c.execute("SELECT COUNT(*) FROM star_auc WHERE seller=? AND status='open'", (uid,)).fetchone()[0] >= 5: raise HTTPException(400, "Не больше 5 лотов одновременно")
    if c.execute("UPDATE uniq SET owner=-1 WHERE key=? AND owner=?", (ukey(p), uid)).rowcount != 1:   # номер уходит «в залог» аукциона
        c.rollback(); raise HTTPException(400, "Этот единственный номер вам не принадлежит")
    c.execute("INSERT INTO star_auc(seller,item,start,end) VALUES(?,?,?,?)", (uid, json.dumps(p), start, int(time.time()) + mins * 60)); c.commit(); _SA[0] = 0
    return {"ok": 1}

@api("/api/sa/bid")
def sa_bid(b):
    uid, _ = auth(b.get("initData")); c = db(); sa_settle(c); now = int(time.time())
    try: aid = int(b["id"]); exp = int(b.get("exp", -1))
    except (KeyError, TypeError, ValueError): raise HTTPException(400, "Неверные данные")
    a = c.execute("SELECT * FROM star_auc WHERE id=? AND status='open' AND end>?", (aid, now)).fetchone()
    if not a: raise HTTPException(400, "Торги уже закончились")
    if a["seller"] == uid: raise HTTPException(400, "Нельзя ставить на свой лот")
    if a["topuid"] == uid: raise HTTPException(400, "Вы уже лидер")
    price = sa_next(a)
    if price != exp: raise HTTPException(400, "Ставка изменилась, попробуйте ещё раз")
    wspend(c, uid, price, "sa-bid:%d" % aid)
    if c.execute("UPDATE star_auc SET top=?,topuid=?,end=MAX(end,?) WHERE id=? AND status='open' AND end>? AND top=? AND IFNULL(topuid,0)=?",
                 (price, uid, now + 30, aid, now, a["top"], a["topuid"] or 0)).rowcount != 1:
        c.rollback(); raise HTTPException(400, "Вашу ставку перебили, попробуйте ещё раз")
    if a["topuid"]: wadd(c, a["topuid"], a["top"], "sa-refund:%d" % aid)
    c.commit(); _SA[0] = 0
    if a["topuid"]: notify(a["topuid"], "🔨 Вашу ставку на аукционе за ⭐ перебили!\n🔢 %s\nНовая ставка: %d ⭐. Ваши %d ⭐ вернулись на баланс." % (plate_txt(json.loads(a["item"])), price, a["top"]), "🔨 Перебить")
    return {"ok": 1, "price": price, "stars": wallet_of(c, uid)}

@api("/api/sa/cancel")
def sa_cancel(b):
    uid, _ = auth(b.get("initData")); c = db()
    try: aid = int(b["id"])
    except (KeyError, TypeError, ValueError): raise HTTPException(400, "Неверные данные")
    a = c.execute("SELECT * FROM star_auc WHERE id=? AND status='open'", (aid,)).fetchone()
    if not a or a["seller"] != uid: raise HTTPException(400, "Лот недоступен")
    if c.execute("UPDATE star_auc SET status='cancelled' WHERE id=? AND status='open' AND topuid IS NULL", (aid,)).rowcount != 1:
        raise HTTPException(400, "На лот уже сделали ставку, снять нельзя")
    gift(c, uid, {"t": "plate", "v": json.loads(a["item"])}); c.commit(); _SA[0] = 0
    return {"ok": 1}

# ---------------------------------------------------------------- витрина: игроки продают номера и машины по фиксированной цене
_SHOP = [0, []]
def shop_snapshot(c):   # список витрины: один раз в 2 секунды на всех игроков сразу
    t = time.time()
    if t - _SHOP[0] < 2.0: return _SHOP[1]
    rows = c.execute("SELECT s.id,s.seller,s.item,s.price,s.ts,u.name FROM shop s LEFT JOIN users u ON u.id=s.seller WHERE s.status='open' ORDER BY s.id DESC LIMIT 150").fetchall()
    _SHOP[1] = [{"id": r["id"], "seller": r["seller"], "sn": str(r["name"] or "")[:24], "item": json.loads(r["item"]), "price": r["price"], "ts": r["ts"]} for r in rows]
    _SHOP[0] = t; return _SHOP[1]

@api("/api/shop/list")
def shop_list(b):
    uid, _ = auth(b.get("initData")); return {"list": shop_snapshot(db())}

@api("/api/shop/create")
def shop_create(b):
    uid, _ = auth(b.get("initData")); c = db(); item = b.get("item") or {}
    try: price = int(b.get("price", 0))
    except (TypeError, ValueError): raise HTTPException(400, "Неверная цена")
    if not isinstance(item, dict) or item.get("t") not in ("plate", "car") or not isinstance(item.get("v"), dict) or not (1000 <= price <= 10**10):
        raise HTTPException(400, "Неверные данные: цена от 1 000")
    need_own(c, uid, item["v"])
    if c.execute("SELECT COUNT(*) FROM shop WHERE seller=? AND status='open'", (uid,)).fetchone()[0] >= 20:
        raise HTTPException(400, "Не больше 20 предметов на витрине")
    c.execute("INSERT INTO shop(seller,item,price,ts) VALUES(?,?,?,?)", (uid, json.dumps(item), price, int(time.time()))); c.commit()
    _SHOP[0] = 0
    return {"ok": 1}

@api("/api/shop/buy")
def shop_buy(b):
    uid, uname = auth(b.get("initData")); c = db()
    try: sid = int(b["id"]); exp = int(b.get("exp", -1))
    except (KeyError, TypeError, ValueError): raise HTTPException(400, "Неверные данные")
    o = c.execute("SELECT * FROM shop WHERE id=? AND status='open'", (sid,)).fetchone()
    if not o: raise HTTPException(400, "Уже куплено или снято с продажи")
    if o["seller"] == uid: raise HTTPException(400, "Это ваш предмет")
    if o["price"] != exp: raise HTTPException(400, "Цена изменилась, обновите список")
    if c.execute("UPDATE shop SET status='sold',buyer=? WHERE id=? AND status='open'", (uid, sid)).rowcount != 1:
        raise HTTPException(400, "Уже куплено или снято с продажи")
    item = json.loads(o["item"]); got = int(o["price"] * 0.95)
    gift(c, uid, item); gift(c, o["seller"], {"t": "bal", "v": got}); c.commit(); _SHOP[0] = 0
    notify(o["seller"], "💰 У вас купили!\n%s\nЦена: %s сум. Вам начислено %s сум (95%%). Покупатель: %s" % (item_txt(item), fnum(o["price"]), fnum(got), str(uname)[:30] or ("ID %d" % uid)), "💰 Открыть игру")
    return {"ok": 1}

@api("/api/shop/cancel")
def shop_cancel(b):
    uid, _ = auth(b.get("initData")); c = db()
    try: sid = int(b["id"])
    except (KeyError, TypeError, ValueError): raise HTTPException(400, "Неверные данные")
    o = c.execute("SELECT * FROM shop WHERE id=? AND status='open'", (sid,)).fetchone()
    if not o or o["seller"] != uid: raise HTTPException(400, "Предмет недоступен")
    if c.execute("UPDATE shop SET status='cancelled' WHERE id=? AND status='open'", (sid,)).rowcount != 1:
        raise HTTPException(400, "Предмет уже куплен")
    gift(c, uid, json.loads(o["item"])); c.commit(); _SHOP[0] = 0
    return {"ok": 1}

# ---------------------------------------------------------------- дуэли между игроками (ставка в сумах, исход по классу номеров)
DUEL_STAKES = (100000, 500000, 2000000, 10000000)
DUEL_W = [1, 1.6, 2.6, 4.2, 6.5, 9]   # «сила» класса номера: одинаковые классы = 50 на 50, выше класс = больше шанс
DUEL_TTL = 86400                        # вызов висит сутки, потом ставка возвращается
_RND = random.SystemRandom()
_DU = [0, []]

def duel_p(ta, tb):
    wa, wb = DUEL_W[min(5, max(0, ta))], DUEL_W[min(5, max(0, tb))]
    return wa / (wa + wb)

def duel_snapshot(c):
    t = time.time()
    if t - _DU[0] < 2.0: return _DU[1]
    rows = c.execute("SELECT d.id,d.a,d.aplate,d.stake,d.ts,u.name FROM duels d LEFT JOIN users u ON u.id=d.a WHERE d.status='open' ORDER BY d.id DESC LIMIT 60").fetchall()
    _DU[1] = [{"id": r["id"], "a": r["a"], "an": str(r["name"] or "")[:24], "plate": json.loads(r["aplate"]), "stake": r["stake"], "ts": r["ts"]} for r in rows]
    _DU[0] = t; return _DU[1]

@api("/api/duel/list")
def duel_list(b):
    uid, _ = auth(b.get("initData")); c = db()
    mine = []
    for r in c.execute("SELECT d.*,ua.name an,ub.name bn FROM duels d LEFT JOIN users ua ON ua.id=d.a LEFT JOIN users ub ON ub.id=d.b "
                       "WHERE d.status='done' AND (d.a=? OR d.b=?) ORDER BY d.id DESC LIMIT 8", (uid, uid)):
        me_a = r["a"] == uid
        mine.append({"id": r["id"], "win": r["winner"] == uid, "stake": r["stake"], "payout": int(r["stake"] * 2 * 0.95),
                     "opp": json.loads(r["bplate"] if me_a else r["aplate"]), "on": str((r["bn"] if me_a else r["an"]) or "")[:24], "ts": r["ts"]})
    return {"list": duel_snapshot(c), "mine": mine, "stakes": list(DUEL_STAKES)}

@api("/api/duel/create")
def duel_create(b):
    uid, _ = auth(b.get("initData")); c = db(); plate = clean_plate(b.get("plate") or {})
    try: stake = int(b.get("stake", 0))
    except (TypeError, ValueError): raise HTTPException(400, "Неверная ставка")
    if stake not in DUEL_STAKES: raise HTTPException(400, "Неверная ставка")
    need_own(c, uid, plate)
    if c.execute("SELECT COUNT(*) FROM duels WHERE a=? AND status='open'", (uid,)).fetchone()[0] >= 3:
        raise HTTPException(400, "Не больше 3 открытых вызовов")
    c.execute("INSERT INTO duels(a,aplate,stake,ts) VALUES(?,?,?,?)", (uid, json.dumps(plate), stake, int(time.time()))); c.commit(); _DU[0] = 0
    return {"ok": 1}

@api("/api/duel/accept")
def duel_accept(b):
    uid, uname = auth(b.get("initData")); c = db(); bplate = clean_plate(b.get("plate") or {})
    try: did = int(b["id"])
    except (KeyError, TypeError, ValueError): raise HTTPException(400, "Неверные данные")
    need_own(c, uid, bplate)
    d = c.execute("SELECT * FROM duels WHERE id=? AND status='open'", (did,)).fetchone()
    if not d: raise HTTPException(400, "Вызов уже принят или отменён")
    if d["a"] == uid: raise HTTPException(400, "Нельзя принять свой вызов")
    if c.execute("UPDATE duels SET status='done',b=?,bplate=? WHERE id=? AND status='open'", (uid, json.dumps(bplate), did)).rowcount != 1:
        raise HTTPException(400, "Вызов уже принят или отменён")
    aplate = json.loads(d["aplate"]); pa = duel_p(tier_py(aplate), tier_py(bplate))
    a_wins = _RND.random() < pa; winner = d["a"] if a_wins else uid; payout = int(d["stake"] * 2 * 0.95)
    c.execute("UPDATE duels SET winner=? WHERE id=?", (winner, did))
    gift(c, winner, {"t": "bal", "v": payout}); c.commit(); _DU[0] = 0
    me = str(uname)[:30] or ("ID %d" % uid)
    notify(d["a"], "⚔️ Ваш вызов на дуэль принят!\nСтавка: %s сум\nВаш номер %s против %s (%s)\n%s" % (
        fnum(d["stake"]), plate_txt(aplate), plate_txt(bplate), me,
        ("🏆 Вы победили! Вам начислено %s сум." % fnum(payout)) if a_wins else ("💥 Вы проиграли, ставка %s сум пропала." % fnum(d["stake"]))), "⚔️ Открыть игру")
    return {"ok": 1, "win": not a_wins, "payout": payout, "opp": aplate, "chance": round((1 - pa) * 100)}

@api("/api/duel/cancel")
def duel_cancel(b):
    uid, _ = auth(b.get("initData")); c = db()
    try: did = int(b["id"])
    except (KeyError, TypeError, ValueError): raise HTTPException(400, "Неверные данные")
    d = c.execute("SELECT * FROM duels WHERE id=? AND status='open'", (did,)).fetchone()
    if not d or d["a"] != uid: raise HTTPException(400, "Вызов недоступен")
    if c.execute("UPDATE duels SET status='cancelled' WHERE id=? AND status='open'", (did,)).rowcount != 1:
        raise HTTPException(400, "Вызов уже принят")
    gift(c, uid, {"t": "bal", "v": d["stake"]}); c.commit(); _DU[0] = 0
    return {"ok": 1}

def duel_expire(c):   # вызовы, которые никто не принял за сутки: ставка возвращается
    notes = []
    for d in c.execute("SELECT * FROM duels WHERE status='open' AND ts<?", (int(time.time()) - DUEL_TTL,)).fetchall():
        if c.execute("UPDATE duels SET status='expired' WHERE id=? AND status='open'", (d["id"],)).rowcount != 1: continue
        gift(c, d["a"], {"t": "bal", "v": d["stake"]})
        notes.append((d["a"], "⌛ Ваш вызов на дуэль никто не принял за сутки.\nСтавка %s сум вернулась на баланс." % fnum(d["stake"]), "⚔️ Открыть игру"))
    if notes: c.commit(); _DU[0] = 0
    for n in notes: notify(*n)

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
    free = [u for u in ULTRA if not is_uniq(u[1]) or not c.execute("SELECT 1 FROM uniq WHERE key=?", (ukey(u[1]),)).fetchone()]
    if free and random.random() < 0.25:   # супер-лот ~1 млрд: Аюб или Бибиша (если их номер ещё ни у кого нет)
        n, pl = random.choice(free)
        item = {"t": "car", "v": {"n": n, "e": "🚙", "t": 4, "m": 2, "pl": pl}}; start = 600_000_000
    else:
        n, m = random.choice(EXC)
        item = {"t": "car", "v": {"n": n, "e": "🏎️", "t": 3, "m": m, "pl": None}}; start = int(30000000 * m * 0.5 / 1000) * 1000
    c.execute("INSERT INTO auctions(seller,item,start,end) VALUES(0,?,?,?)", (json.dumps(item, ensure_ascii=False), start, now + 600))
    c.execute("INSERT OR REPLACE INTO meta(k,v) VALUES('syslot',?)", (str(now),)); c.commit(); _AUC[0] = 0

# ---------------------------------------------------------------- уход за базой: чистка, копии, проверка, отчёт
def cleanup(c):   # раз в час: убрать старые закрытые обмены и торги, не раздувать базу
    old = int(time.time()) - 14 * 86400; n = 0
    n += c.execute("DELETE FROM offers WHERE id IN (SELECT id FROM offers WHERE status!='open' AND ts<? LIMIT 5000)", (old,)).rowcount
    n += c.execute("DELETE FROM auctions WHERE id IN (SELECT id FROM auctions WHERE status!='open' AND end<? LIMIT 5000)", (old,)).rowcount
    n += c.execute("DELETE FROM duels WHERE id IN (SELECT id FROM duels WHERE status!='open' AND ts<? LIMIT 5000)", (old,)).rowcount
    n += c.execute("DELETE FROM shop WHERE id IN (SELECT id FROM shop WHERE status!='open' AND ts<? LIMIT 5000)", (old,)).rowcount
    c.commit()
    c.execute("PRAGMA wal_checkpoint(PASSIVE)")
    if n: log.info("чистка базы: удалено старых записей: %d", n)

def _backups():
    try: return sorted(f for f in os.listdir(BK_DIR) if f.startswith("game-") and f.endswith(".db.gz"))
    except OSError: return []

def backup_db(tag="auto"):
    """Копия базы (безопасная, пока игроки играют) со сжатием в папку backups рядом с базой. Возвращает путь к файлу."""
    os.makedirs(BK_DIR, exist_ok=True)
    size = os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0
    free = shutil.disk_usage(BK_DIR).free
    if free < size * 2.2 + 50e6: raise RuntimeError("мало места на диске (свободно %d МБ)" % (free // 1048576))
    stamp = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
    tmp = os.path.join(BK_DIR, "tmp-%s.db" % stamp); out = os.path.join(BK_DIR, "game-%s-%s.db.gz" % (stamp, tag))
    src = sqlite3.connect(DB_PATH, timeout=30); dst = sqlite3.connect(tmp)
    try: src.backup(dst, pages=2000, sleep=0.001)
    finally: dst.close(); src.close()
    try:
        with open(tmp, "rb") as f, gzip.open(out + ".part", "wb", 6) as g: shutil.copyfileobj(f, g, 1 << 20)
        os.replace(out + ".part", out)
    finally:
        for x in (tmp, out + ".part"):
            try: os.remove(x)
            except OSError: pass
    for tagged, keep in ((True, BK_KEEP), (False, 5)):   # автокопий храним BK_KEEP, ручных и перед сбросом - 5
        fs = [f for f in _backups() if f.endswith("-auto.db.gz") == tagged]
        for f in fs[:-keep]:
            try: os.remove(os.path.join(BK_DIR, f))
            except OSError: pass
    return out

def auto_backup():   # не чаще раза в ~20 часов
    fs = [f for f in _backups() if f.endswith("-auto.db.gz")]
    if fs and time.time() - os.path.getmtime(os.path.join(BK_DIR, fs[-1])) < 20 * 3600: return
    c = db()
    if not c.execute("SELECT 1 FROM users LIMIT 1").fetchone(): return
    p = backup_db("auto"); log.info("копия базы: %s (%d КБ)", os.path.basename(p), os.path.getsize(p) // 1024)

DB_CHECK = ["ещё не проверялась", 0]
def integrity():
    r = db().execute("PRAGMA quick_check").fetchone()[0]
    DB_CHECK[0] = r; DB_CHECK[1] = time.time()
    if r != "ok": log.error("ПРОВЕРКА БАЗЫ НАШЛА ПРОБЛЕМУ: %s", r)
    return r

def _mb(n): return "%.1f МБ" % (n / 1048576)

def db_report():   # текст для команды /stat в боте
    c = db(); now = int(time.time()); out = []
    one = lambda q, a=(): c.execute(q, a).fetchone()[0]
    tot = one("SELECT COUNT(*) FROM users")
    out.append("👥 Игроков: %d · за 24 ч: %d · за 7 дней: %d" % (tot, one("SELECT COUNT(*) FROM users WHERE last_seen>?", (now - 86400,)), one("SELECT COUNT(*) FROM users WHERE last_seen>?", (now - 7 * 86400,))))
    sz = os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0; wal = os.path.getsize(DB_PATH + "-wal") if os.path.exists(DB_PATH + "-wal") else 0
    try: free = shutil.disk_usage(os.path.dirname(os.path.abspath(DB_PATH))).free
    except OSError: free = 0
    out.append("💾 База: %s (журнал %s) · свободно на диске: %s" % (_mb(sz), _mb(wal), _mb(free)))
    big = c.execute("SELECT id,length(CAST(state AS BLOB)) l FROM users WHERE state IS NOT NULL ORDER BY l DESC LIMIT 1").fetchone()
    avg = one("SELECT IFNULL(AVG(length(CAST(state AS BLOB))),0) FROM users WHERE state IS NOT NULL")
    out.append("📦 Сохранение игрока: в среднем %d КБ, самое большое %d КБ (ID %s)" % (avg // 1024, (big["l"] if big else 0) // 1024, big["id"] if big else "-"))
    out.append("🎁 Подарков в очереди: %d · 🤝 открытых обменов: %d · 🔨 открытых лотов: %d · 🔒 единственных номеров: %d · 🛒 на витрине: %d · ⚔️ открытых дуэлей: %d" % (
        one("SELECT COUNT(*) FROM gifts"), one("SELECT COUNT(*) FROM offers WHERE status='open'"), one("SELECT COUNT(*) FROM auctions WHERE status='open'"), one("SELECT COUNT(*) FROM uniq"), one("SELECT COUNT(*) FROM shop WHERE status='open'"), one("SELECT COUNT(*) FROM duels WHERE status='open'")))
    out.append("🧪 Проверка целостности: %s%s" % (DB_CHECK[0], (" (" + time.strftime("%d.%m %H:%M", time.gmtime(DB_CHECK[1])) + " UTC)") if DB_CHECK[1] else ""))
    fs = _backups()
    if fs:
        p = os.path.join(BK_DIR, fs[-1]); out.append("🗄 Копий базы: %d · последняя: %s UTC (%s)" % (len(fs), time.strftime("%d.%m %H:%M", time.gmtime(os.path.getmtime(p))), _mb(os.path.getsize(p))))
    else: out.append("🗄 Копий базы пока нет (первая создаётся автоматически; сделать сейчас: /backup)")
    rows = [x for x in list(_LAT) if x[0] > time.time() - 300]
    if rows:
        ms = sorted(x[2] for x in rows); n = len(ms); pc = lambda q: ms[min(n - 1, int(n * q))]
        out.append("⚡ За 5 минут: запросов %d (%.1f/сек) · ответ в среднем %d мс, 95%% быстрее %d мс, самый долгий %d мс, дольше 1 сек: %d" % (n, n / 300, sum(ms) / n, pc(.95), ms[-1], sum(1 for m in ms if m > 1000)))
        by = {}
        for _, p, m in rows: by.setdefault(p, []).append(m)
        top = sorted(by.items(), key=lambda kv: -len(kv[1]))[:5]
        out.append("   чаще всего: " + ", ".join("%s ×%d (%d мс)" % (p.replace("/api/", ""), len(v), sum(v) / len(v)) for p, v in top))
    else: out.append("⚡ За 5 минут запросов не было")
    return "\n".join(out)

def _bg():   # фоновый поток: торги, системные лоты, чистка и копии (не зависит от нагрузки на сайт)
    try: bal_backfill()
    except Exception: log.exception("bal_backfill")
    try: uniq_backfill()
    except Exception: log.exception("uniq_backfill")
    for f in os.listdir(BK_DIR) if os.path.isdir(BK_DIR) else []:
        if f.startswith("tmp-") or f.endswith(".part"):
            try: os.remove(os.path.join(BK_DIR, f))
            except OSError: pass
    last_clean = last_bk = last_chk = 0
    while True:
        try: c = db(); settle(c); sa_settle(c); duel_expire(c); system_lot(c)
        except Exception: log.exception("торги")
        t = time.time()
        if t - last_clean > 3600:
            last_clean = t
            try: cleanup(db())
            except Exception: log.exception("чистка базы")
        if t - last_bk > 3600:
            last_bk = t
            try: auto_backup()
            except Exception: log.exception("копия базы")
        if t - last_chk > 86400:
            last_chk = t
            try: integrity()
            except Exception: log.exception("проверка базы")
        time.sleep(5)

if not os.environ.get("NO_BG"): threading.Thread(target=_bg, daemon=True, name="bg").start()
