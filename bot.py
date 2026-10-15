# Запуск (в той же папке, что server.py):  python bot.py
import json, time, sqlite3, shutil
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo, MenuButtonWebApp, BotCommand
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, PreCheckoutQueryHandler, MessageHandler, filters
import os
from server import db, gift, get_rid, BOT_TOKEN, DB_PATH, REQUIRED_CHANNEL, REQUIRED_URL

ADMINS = {int(x) for x in os.environ.get("ADMINS", "123456789").split(",") if x.strip()}              # ваш Telegram ID (узнать: @userinfobot)
URL = os.environ.get("APP_URL", "https://YOUR-DOMAIN/")      # https-адрес, где запущен server.py
CHANNEL = os.environ.get("CHANNEL_URL", "https://t.me/")         # ссылка на ваш канал

def admin(f):
    async def w(u: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if u.effective_user.id in ADMINS: await f(u, ctx)
    return w

def play_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎰 Играть", web_app=WebAppInfo(url=URL))],
        [InlineKeyboardButton("📖 Как играть", callback_data="how"), InlineKeyboardButton("📢 Канал", url=CHANNEL)]])

def welcome_text(name, known):
    if known:
        return f"👋 С возвращением, <b>{name}</b>!\n\nТвои номера и машины ждут в гараже. Продолжим? 🎰"
    return (f"👋 Привет, <b>{name}</b>!\n\nДобро пожаловать в <b>UZ Номерок</b> 🇺🇿\n\n"
            "🎰 Крути номерные знаки Узбекистана\n💎 Ищи редкие и секретные номера\n"
            "🚗 Выбивай машины и ставь на них номера\n🤝 Меняйся с другими игроками\n"
            "🎁 Активируй промокоды\n\nНажми «Играть», чтобы начать!")

async def is_sub(bot, uid):  # подписан ли игрок на обязательный канал
    if not REQUIRED_CHANNEL: return True
    try:
        m = await bot.get_chat_member(REQUIRED_CHANNEL, uid)
        return m.status in ("member", "administrator", "creator")
    except Exception:
        return True   # не удалось проверить (бот не админ канала) - не блокируем

async def start(u: Update, ctx):
    user = u.effective_user; c = db()
    known = c.execute("SELECT 1 FROM users WHERE id=?", (user.id,)).fetchone()
    if ctx.args and ctx.args[0].startswith("ref_") and not known:   # ссылка-приглашение
        try: c.execute("INSERT OR REPLACE INTO refpending VALUES(?,?)", (user.id, int(ctx.args[0][4:]))); c.commit()
        except ValueError: pass
    name = user.first_name or "игрок"
    if not await is_sub(ctx.bot, user.id):
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("📢 Подписаться", url=REQUIRED_URL)],
                                   [InlineKeyboardButton("✅ Я подписался", callback_data="chk")]])
        return await u.message.reply_text(f"👋 Привет, <b>{name}</b>!\n\nЧтобы начать играть, подпишитесь на наш канал и нажмите «Я подписался».",
                                          reply_markup=kb, parse_mode="HTML")
    await u.message.reply_text(welcome_text(name, known), reply_markup=play_kb(), parse_mode="HTML")

async def chk(u: Update, ctx):
    q = u.callback_query; user = q.from_user
    if not await is_sub(ctx.bot, user.id):
        return await q.answer("Вы ещё не подписаны на канал", show_alert=True)
    await q.answer("Готово!")
    known = db().execute("SELECT 1 FROM users WHERE id=?", (user.id,)).fetchone()
    await q.message.reply_text(welcome_text(user.first_name or "игрок", known), reply_markup=play_kb(), parse_mode="HTML")

async def precheckout(u: Update, ctx):
    await u.pre_checkout_query.answer(ok=True)

async def paid(u: Update, ctx):  # оплата звёздами прошла - выдаём подарок
    pay = u.message.successful_payment; d = json.loads(pay.invoice_payload); uid = d["u"]
    if uid != u.effective_user.id: return
    c = db()
    c.execute("CREATE TABLE IF NOT EXISTS payments(charge TEXT PRIMARY KEY,uid INT,item TEXT,stars INT,ts INT)")
    try: c.execute("INSERT INTO payments VALUES(?,?,?,?,?)", (pay.telegram_payment_charge_id, uid, d["i"], pay.total_amount, int(time.time())))
    except sqlite3.IntegrityError: return
    if d["i"] == "b50": gift(c, uid, {"t": "boost", "k": "bl", "v": 120})
    elif d["i"] == "b100":
        for k in ("bl", "bx", "bs"): gift(c, uid, {"t": "boost", "k": k, "v": 120})
    elif d["i"] == "noads": gift(c, uid, {"t": "noads"})
    elif d["i"] == "p500": gift(c, uid, {"t": "plate", "v": d["p"]})
    c.commit()
    await u.message.reply_text("⭐ Оплата получена! Откройте игру: подарок уже там.")

@admin
async def stars(u, ctx):  # /stars - сколько звёзд получено
    rows = db().execute("SELECT item, COUNT(*) n, SUM(stars) s FROM payments GROUP BY item").fetchall()
    await u.message.reply_text("\n".join(f'{r["item"]}: {r["n"]} шт · {r["s"]} ⭐' for r in rows) or "Платежей пока нет")

async def how(u: Update, ctx):
    await u.callback_query.answer()
    await u.callback_query.message.reply_text(
        "📖 <b>Как играть</b>\n\n1. Крути номера: Обычная, а с 5 и 7 уровня Улучшенная и Премиум.\n"
        "2. Каждый номер можно продать или оставить в гараже.\n3. Повышай уровень, открывай новые крутки и машины.\n"
        "4. Машины выбиваются в рулетке «Машины», на них можно поставить номер.\n"
        "5. Вкладка «Задания»: промокоды и обмен с игроками (нужен ID).", parse_mode="HTML")

SUPPORT = os.environ.get("SUPPORT", "@your_username")  # <- впиши свой @username для поддержки

async def paysupport(u, c):
    await u.message.reply_text("По вопросам оплаты Stars пишите: " + SUPPORT)

async def terms(u, c):
    await u.message.reply_text("Покупки за Telegram Stars — виртуальные игровые предметы. Возврат возможен по запросу в поддержку: " + SUPPORT)

async def post_init(app):
    await app.bot.set_my_commands([BotCommand("start", "Начать игру"), BotCommand("paysupport", "Поддержка по оплате"), BotCommand("terms", "Условия покупок")])
    await app.bot.set_chat_menu_button(menu_button=MenuButtonWebApp("🎰 Играть", WebAppInfo(url=URL)))
    await app.bot.set_my_short_description("Собирай номера и машины Узбекистана 🇺🇿")
    await app.bot.set_my_description("🎰 UZ Номерок: крути номерные знаки Узбекистана, выбивай редкие номера и машины, меняйся с игроками. Нажми «Запустить»!")

@admin
async def players(u, ctx):
    rows = db().execute("SELECT id,name,state FROM users ORDER BY last_seen DESC").fetchall()
    out = [f"Игроков: {len(rows)}"]
    for r in rows[:40]:
        s = json.loads(r["state"] or "{}")
        out.append(f'{r["id"]} {r["name"]} · ур.{s.get("lv",1)} · {s.get("bal",0):,} сум · машин {len(s.get("cars",[]))}')
    await u.message.reply_text("\n".join(out))

def parse_gift(a):  # a = слова после ID игрока
    kind = a[0]
    if kind == "bal": return {"t": "bal", "v": int(a[1])}
    if kind == "plate": return {"t": "plate", "v": {"k": 0, "r": a[1], "a": a[2].upper(), "d": a[3], "z": a[4].upper()}}
    if kind == "firm": return {"t": "plate", "v": {"k": 1, "r": a[1], "d": a[2], "z": a[3].upper()}}
    if kind == "car": return {"t": "car", "v": {"n": " ".join(a[2:]), "e": "🚗", "t": int(a[1]), "m": 1, "pl": None}}
    if kind == "carplate":
        nm = " ".join(a[5:])
        return {"t": "car", "v": {"n": nm, "e": "🚙", "t": 3, "m": .8 if "9X" in nm else 1,
                "pl": {"k": 0, "r": a[1], "a": a[2].upper(), "d": a[3], "z": a[4].upper()}}}
    if kind == "boost": return {"t": "boost", "k": {"luck": "bl", "sell": "bs", "xp": "bx"}[a[1]], "v": int(a[2])}
    raise ValueError

GIVE_HELP = ("Выдача: /give ID что-то (вместо ID можно all - всем игрокам)\n"
             "/give ID bal 1000000\n/give ID plate 01 A 777 AC\n/give ID firm 01 007 AUB\n"
             "/give ID car 3 Название машины (0-4: обычная...секретная)\n/give ID carplate 01 R 700 TT Zeekr 9X\n"
             "/give ID boost luck 120 (luck/sell/xp, минуты)\n\nНайти ID: /find имя, /players")

@admin
async def give(u, ctx):
    a = ctx.args
    try: g = parse_gift(a[1:]); target = a[0]
    except Exception: return await u.message.reply_text(GIVE_HELP)
    c = db()
    if target == "all":
        ids = [r["id"] for r in c.execute("SELECT id FROM users")]; who = f"всем игрокам ({len(ids)})"
    else:
        row = c.execute("SELECT name FROM users WHERE id=?", (int(target),)).fetchone()
        if not row: return await u.message.reply_text("Игрок с таким ID не найден (он должен хотя бы раз открыть игру)")
        ids = [int(target)]; who = f"игроку {row['name']} ({target})"
    for i in ids: gift(c, i, g)
    c.commit()
    await u.message.reply_text(f"Выдано {who}. Игрок получит это при следующем заходе (до 45 сек).")

@admin
async def addbal(u, ctx):  # /addbal ID СУММА - пополнить баланс игрока
    try: uid = int(ctx.args[0]); n = int(ctx.args[1])
    except Exception: return await u.message.reply_text("Использование: /addbal ID СУММА\nНайти ID: /find имя")
    c = db()
    if not c.execute("SELECT 1 FROM users WHERE id=?", (uid,)).fetchone(): return await u.message.reply_text("Игрок с таким ID не найден")
    gift(c, uid, {"t": "bal", "v": n}); c.commit()
    await u.message.reply_text(f"Баланс игрока {uid} пополнен на {n:,} сум (придёт при следующем заходе).")

@admin
async def broadcast(u, ctx):  # /broadcast текст - рассылка всем игрокам
    import asyncio
    parts = (u.message.text or "").split(None, 1)
    text = parts[1] if len(parts) > 1 else ""
    if not text: return await u.message.reply_text("Использование: /broadcast текст сообщения")
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🎰 Играть", web_app=WebAppInfo(url=URL))]])
    ids = [r["id"] for r in db().execute("SELECT id FROM users")]
    ok = bad = 0
    for i in ids:
        try: await ctx.bot.send_message(i, text, reply_markup=kb); ok += 1
        except Exception: bad += 1
        await asyncio.sleep(0.06)
    await u.message.reply_text(f"Рассылка: доставлено {ok}, не доставлено {bad}")

@admin
async def uniq(u, ctx):  # /uniq - список единственных номеров; /uniq free ID - освободить все номера игрока
    c = db()
    if ctx.args[:1] == ["free"] and len(ctx.args) == 2:
        n = c.execute("DELETE FROM uniq WHERE owner=?", (int(ctx.args[1]),)).rowcount; c.commit()
        return await u.message.reply_text(f"Освобождено единственных номеров: {n}")
    rows = c.execute("SELECT u.key,u.owner,x.name FROM uniq u LEFT JOIN users x ON x.id=u.owner ORDER BY u.ts DESC LIMIT 25").fetchall()
    tot = c.execute("SELECT COUNT(*) FROM uniq").fetchone()[0]
    def fmt(k):
        kk, r, a, d, z = k.split("|"); return f"{r} {a+' ' if a else ''}{d} {z}" + (" (фирма)" if kk == "1" else "")
    await u.message.reply_text(f"Единственных номеров занято: {tot}\n" + "\n".join(f"{fmt(x['key'])} - {x['name'] or ''} ({x['owner']})" for x in rows) +
                               "\n\n/uniq free ID - освободить все номера игрока")

@admin
async def promo(u, ctx):
    a = ctx.args; c = db()
    if a and a[0] == "add" and len(a) == 4:      # /promo add CODE СУММА КОЛ-ВО
        c.execute("INSERT OR REPLACE INTO promos(code,reward,max_uses,used) VALUES(?,?,?,0)",
                  (a[1].upper(), json.dumps({"t": "bal", "v": int(a[2])}), int(a[3]))); c.commit()
        await u.message.reply_text(f"Промокод {a[1].upper()} создан")
    elif a and a[0] == "del" and len(a) == 2:
        c.execute("DELETE FROM promos WHERE code=?", (a[1].upper(),)); c.commit(); await u.message.reply_text("Удалён")
    elif a and a[0] == "list":
        rows = c.execute("SELECT * FROM promos").fetchall()
        await u.message.reply_text("\n".join(f'{r["code"]}: {json.loads(r["reward"])["v"]:,} сум · {r["used"]}/{r["max_uses"]}' for r in rows) or "Нет промокодов")
    else:
        await u.message.reply_text("/promo add КОД СУММА ЛИМИТ\n/promo list\n/promo del КОД")

@admin
async def find(u, ctx):  # /find имя или ID - найти игрока и его ID
    q = " ".join(ctx.args).lower()
    rows = [r for r in db().execute("SELECT id,name,state FROM users") if q and (q in (r["name"] or "").lower() or q == str(r["id"]))]
    out = []
    for r in rows[:20]:
        s = json.loads(r["state"] or "{}")
        out.append(f'{r["id"]} {r["name"]} · ур.{s.get("lv",1)} · {s.get("bal",0):,} сум · номеров {len(s.get("col",[]))} · машин {len(s.get("cars",[]))}')
    await u.message.reply_text("\n".join(out) or "Не найдено. Использование: /find имя или ID")

@admin
async def resetall(u, ctx):  # /resetall CONFIRM - сбросить прогресс ВСЕХ игроков
    if ctx.args != ["CONFIRM"]:
        return await u.message.reply_text("⚠️ Это сотрёт прогресс ВСЕХ игроков (баланс, номера, машины, лоты, подарки, обмены, приглашения, использованные промокоды). "
                                          "Игроки, их ID, промокоды и платежи останутся. Перед сбросом делается копия базы.\n\nПодтвердите: /resetall CONFIRM")
    shutil.copy(DB_PATH, f"game-backup-{int(time.time())}.db")
    c = db(); rid = get_rid(c) + 1
    c.execute("CREATE TABLE IF NOT EXISTS claims(uid INT,id TEXT,PRIMARY KEY(uid,id))")
    for q in ("UPDATE users SET state=NULL", "DELETE FROM uniq", "DELETE FROM gifts", "DELETE FROM offers", "DELETE FROM auctions", "DELETE FROM referrals",
              "DELETE FROM refpending", "DELETE FROM redeemed", "UPDATE promos SET used=0", "DELETE FROM claims", "DELETE FROM events"):
        c.execute(q)
    c.execute("INSERT OR REPLACE INTO meta(k,v) VALUES('rid',?)", (str(rid),)); c.commit()
    await u.message.reply_text(f"✅ Прогресс всех игроков сброшен (сезон {rid}). Копия базы сохранена. У всех игра начнётся заново при следующем открытии.")

@admin
async def reset(u, ctx):  # /reset ID - сбросить прогресс одного игрока
    try: uid = int(ctx.args[0])
    except Exception: return await u.message.reply_text("Использование: /reset ID\nНайти ID: /find имя")
    c = db()
    row = c.execute("SELECT name FROM users WHERE id=?", (uid,)).fetchone()
    if not row: return await u.message.reply_text("Игрок с таким ID не найден")
    c.execute("CREATE TABLE IF NOT EXISTS claims(uid INT,id TEXT,PRIMARY KEY(uid,id))")
    c.execute("DELETE FROM uniq WHERE owner=?", (uid,))
    c.execute("DELETE FROM gifts WHERE uid=?", (uid,))
    c.execute("DELETE FROM offers WHERE frm=? OR to_id=?", (uid, uid))
    c.execute("DELETE FROM auctions WHERE seller=? AND status='open'", (uid,))
    c.execute("DELETE FROM claims WHERE uid=?", (uid,))
    c.execute("DELETE FROM redeemed WHERE uid=?", (uid,))
    c.execute("UPDATE users SET state=NULL WHERE id=?", (uid,))
    gift(c, uid, {"t": "reset"}); c.commit()
    await u.message.reply_text(f"✅ Прогресс игрока {row['name']} ({uid}) сброшен. У него игра начнётся заново при следующем заходе (до 15 сек).")

@admin
async def event(u, ctx):
    a = ctx.args; c = db()
    if a and a[0] == "stop":
        c.execute("DELETE FROM events"); c.commit(); return await u.message.reply_text("Ивент остановлен")
    plain = [x for x in a if "=" not in x]
    if not plain or not plain[0].isdigit():
        return await u.message.reply_text("/event luck=3 sell=2 xp=2 МИНУТЫ [название]\nнапример: /event luck=3 60 Выходные\n/event stop")
    kv = {x.split("=")[0]: float(x.split("=")[1]) for x in a if "=" in x}
    c.execute("DELETE FROM events")
    c.execute("INSERT INTO events(name,luck,sell,xp,until) VALUES(?,?,?,?,?)",
              (" ".join(plain[1:]) or "Ивент", kv.get("luck", 1), kv.get("sell", 1), kv.get("xp", 1), int(time.time()) + int(plain[0]) * 60)); c.commit()
    await u.message.reply_text("Ивент запущен")

app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()
app.add_handler(CallbackQueryHandler(how, pattern="^how$"))
app.add_handler(CallbackQueryHandler(chk, pattern="^chk$"))
app.add_handler(PreCheckoutQueryHandler(precheckout))
app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, paid))
for n, f in [("start", start), ("players", players), ("give", give), ("promo", promo), ("event", event), ("find", find), ("stars", stars), ("resetall", resetall), ("paysupport", paysupport), ("terms", terms), ("addbal", addbal), ("reset", reset), ("broadcast", broadcast), ("uniq", uniq)]:
    app.add_handler(CommandHandler(n, f))
app.run_polling()
