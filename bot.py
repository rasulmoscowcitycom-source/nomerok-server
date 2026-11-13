# Запуск (в той же папке, что server.py):  python bot.py
import json, time, sqlite3, os
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo, MenuButtonWebApp, BotCommand
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, PreCheckoutQueryHandler, MessageHandler, filters
import os
from server import db, gift, wadd, notify, raw_addr, ton_of, tadd, NANO, get_rid, BOT_TOKEN, DB_PATH, REQUIRED_CHANNEL, REQUIRED_URL, BOT_USERNAME, backup_db, db_report, launch_ultra, ads_is_off, ads_cfg, income_cfg, income_save, restore_list, restore_missing, stars_audit, stars_audit_fix, apply_payment, plate_txt, ukey, clean_plate, season_plan, season_apply, SEASON_BAL, COSM, COSM_NAME, BPC

ADMINS = {int(x) for x in os.environ.get("ADMINS", "123456789").split(",") if x.strip()}              # ваш Telegram ID (узнать: @userinfobot)
URL = os.environ.get("APP_URL", "https://YOUR-DOMAIN/")      # https-адрес, где запущен server.py
CHANNEL = os.environ.get("CHANNEL_URL", "https://t.me/")         # ссылка на ваш канал

def admin(f):
    async def w(u: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if u.effective_user.id in ADMINS: await f(u, ctx)
    return w

def SB(text, style=None, **kw):   # кнопка с цветом (Bot API: style = primary синяя / success зелёная / danger красная)
    return InlineKeyboardButton(text, api_kwargs={"style": style} if style else None, **kw)

FX_FIRE, FX_PARTY = "5104841245755180586", "5046509860389126442"   # анимация-эффект сообщения: 🔥 и 🎉

async def send_fx(msg, text, kb, fx=None):
    """Отправить с цветными кнопками и анимацией; если Telegram что-то не принял - обычным способом."""
    try: return await msg.reply_text(text, reply_markup=kb(True), parse_mode="HTML", **({"message_effect_id": fx} if fx else {}))
    except Exception: return await msg.reply_text(text, reply_markup=kb(False), parse_mode="HTML")

def play_kb(styled=True):
    S = (lambda t, s, **k: SB(t, s, **k)) if styled else (lambda t, s, **k: InlineKeyboardButton(t, **k))
    return InlineKeyboardMarkup([
        [S("🎰 Играть", "success", web_app=WebAppInfo(url=URL))],
        [S("📖 Как играть", "primary", callback_data="how"), InlineKeyboardButton("📢 Канал", url=CHANNEL)]])

def gate_kb(styled=True):
    S = (lambda t, s, **k: SB(t, s, **k)) if styled else (lambda t, s, **k: InlineKeyboardButton(t, **k))
    return InlineKeyboardMarkup([[S("📢 Подписаться на канал", "primary", url=REQUIRED_URL)], [S("✅ Я подписался", "success", callback_data="chk")]])

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
        return await send_fx(u.message, f"👋 Привет, <b>{name}</b>!\n\n🔒 Чтобы начать играть, подпишитесь на наш канал и нажмите «✅ Я подписался».", gate_kb, FX_FIRE)
    await send_fx(u.message, welcome_text(name, known), play_kb, FX_PARTY if not known else None)

async def chk(u: Update, ctx):
    q = u.callback_query; user = q.from_user
    if not await is_sub(ctx.bot, user.id):
        return await q.answer("❌ Вы ещё не подписаны на канал. Подпишитесь и нажмите снова", show_alert=True)
    await q.answer("✅ Подписка найдена!")
    known = db().execute("SELECT 1 FROM users WHERE id=?", (user.id,)).fetchone()
    try: await q.message.delete()   # убрать окно с подпиской, чтобы не мешало
    except Exception: pass
    await send_fx(q.message, "✅ <b>Подписка подтверждена!</b>\n\n" + welcome_text(user.first_name or "игрок", known), play_kb, FX_PARTY)

async def precheckout(u: Update, ctx):
    await u.pre_checkout_query.answer(ok=True)

async def paid(u: Update, ctx):  # оплата звёздами прошла - выдаём подарок (вся логика в server.apply_payment)
    pay = u.message.successful_payment; d = json.loads(pay.invoice_payload)
    if int(d["u"]) != u.effective_user.id: return
    try: msg = apply_payment(db(), d, pay.total_amount, pay.telegram_payment_charge_id)
    except Exception as e:   # деньги получены, а выдача упала - не молчим: пишем игроку и админам, в логах Render будет подробность
        import logging, traceback; logging.getLogger("uzn").error("ОПЛАТА НЕ ВЫДАНА uid=%s item=%s charge=%s\n%s", d.get("u"), d.get("i"), pay.telegram_payment_charge_id, traceback.format_exc())
        await u.message.reply_text("⚠️ Оплата получена, но выдача не удалась. Мы уже знаем и всё исправим — напишите администратору. Код платежа: %s" % pay.telegram_payment_charge_id)
        for a in ADMINS:
            try: await ctx.bot.send_message(a, "🚨 Оплата не выдана: игрок %s, товар %s, %s ⭐, код %s\n%r" % (d.get("u"), d.get("i"), pay.total_amount, pay.telegram_payment_charge_id, e))
            except Exception: pass
        return
    if msg: await u.message.reply_text(msg)
    try:   # уведомление админам: игрок заплатил звёздами
        c = db(); tu = u.effective_user; row = c.execute("SELECT name FROM users WHERE id=?", (tu.id,)).fetchone()
        nm = (row["name"] if row and row["name"] else tu.full_name) or str(tu.id)
        tot = c.execute("SELECT COALESCE(SUM(stars),0) FROM payments WHERE charge NOT LIKE 'wallet:%'").fetchone()[0]
        txt = "💫 Донат звёздами!\n%s%s · ID %s\nТовар: %s\nСумма: %s ⭐\nВсего получено: %s ⭐" % (nm, (" (@%s)" % tu.username) if tu.username else "", tu.id, d.get("i"), pay.total_amount, tot)
        for a in ADMINS:
            try: await ctx.bot.send_message(a, txt)
            except Exception as e: import logging; logging.getLogger("uzn").error("уведомление о донате не доставлено админу %s: %r", a, e)
    except Exception as e:
        import logging; logging.getLogger("uzn").error("ошибка уведомления о донате: %r", e)

@admin
async def notifytest(u, ctx):  # /notifytest - проверка: придёт ли уведомление о донате всем админам
    ok = []; bad = []
    for a in ADMINS:
        try: await ctx.bot.send_message(a, "💫 Тест уведомления о донате: если вы это видите, то при оплате звёздами придёт такое же сообщение."); ok.append(a)
        except Exception as e: bad.append("%s (%s)" % (a, e))
    await u.message.reply_text("Админы из настроек (ADMINS): %s\nДоставлено: %s\nНе доставлено: %s\nВаш ID: %s" % (", ".join(str(x) for x in ADMINS), ok or "никому", bad or "нет", u.effective_user.id))

@admin
async def stars(u, ctx):  # /stars - сколько звёзд получено
    c = db(); rows = c.execute("SELECT item, COUNT(*) n, SUM(stars) s FROM payments WHERE charge NOT LIKE 'wallet:%' GROUP BY item").fetchall()   # реальные платежи Telegram (покупки со звёздного баланса не считаем)
    tot = sum(r["s"] or 0 for r in rows); held = c.execute("SELECT COALESCE(SUM(stars),0) FROM users").fetchone()[0]
    await u.message.reply_text(("\n".join(f'{r["item"]}: {r["n"]} шт · {r["s"]} ⭐' for r in rows) or "Платежей пока нет") + f"\n\nВсего получено: {tot} ⭐\nСейчас лежит на звёздных балансах игроков: {held} ⭐")

@admin
async def lastpay(u, ctx):  # /lastpay - последние платежи и сколько подарков ещё ждут игрока (диагностика оплаты)
    c = db(); rows = c.execute("SELECT charge,uid,item,stars,ts FROM payments WHERE charge NOT LIKE 'wallet:%' ORDER BY ts DESC LIMIT 8").fetchall()
    if not rows: await u.message.reply_text("Платежей пока нет: бот ещё ни разу не получал оплату."); return
    out = []
    for r in rows:
        w = c.execute("SELECT COUNT(*) FROM gifts WHERE uid=?", (r["uid"],)).fetchone()[0]
        out.append("%s · ID %s · %s · %s ⭐ · подарков в очереди: %s" % (time.strftime("%d.%m %H:%M", time.gmtime(r["ts"] + 5 * 3600)), r["uid"], r["item"], r["stars"], w))
    await u.message.reply_text("Последние платежи (время Ташкента):\n" + "\n".join(out) + "\n\n«В очереди» = подарок выдан, но игра его ещё не забрала. Если там 0 и подарка нет в игре, значит он уже забран.")

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
    c = db()
    total = c.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    rows = c.execute("SELECT id,name,state FROM users ORDER BY last_seen DESC LIMIT 40").fetchall()
    out = [f"Игроков: {total} (показаны последние 40)"]
    for r in rows:
        s = json.loads(r["state"] or "{}")
        out.append(f'{r["id"]} {r["name"]} · ур.{s.get("lv",1)} · {s.get("bal",0):,} сум · машин {len(s.get("cars",[]))}')
    await u.message.reply_text("\n".join(out))

def parse_gift(a):  # a = слова после ID игрока
    kind = a[0]
    if kind == "noads": return {"t": "noads"}
    if kind == "bal": return {"t": "bal", "v": int(a[1])}
    if kind == "plate": return {"t": "plate", "v": {"k": 0, "r": a[1], "a": a[2].upper(), "d": a[3], "z": a[4].upper()}}
    if kind == "firm": return {"t": "plate", "v": {"k": 1, "r": a[1], "d": a[2], "z": a[3].upper()}}
    if kind == "car": return {"t": "car", "v": {"n": " ".join(a[2:]), "e": "🚗", "t": int(a[1]), "m": 1, "pl": None}}
    if kind == "carplate":
        nm = " ".join(a[5:])
        return {"t": "car", "v": {"n": nm, "e": "🚙", "t": 3, "m": .8 if "9X" in nm else 1,
                "pl": {"k": 0, "r": a[1], "a": a[2].upper(), "d": a[3], "z": a[4].upper()}}}
    if kind == "boost":
        g = {"t": "boost", "k": {"luck": "bl", "sell": "bs", "xp": "bx"}[a[1]], "v": int(a[2])}
        if len(a) > 3 and a[1] == "luck":   # множитель удачи: /give ID boost luck 120 5
            g["x"] = max(2, min(50, int(a[3])))
        return g
    if kind == "stars": return {"t": "stars", "v": max(1, min(100000, int(a[1])))}
    if kind in ("paint", "flame"):   # краски и цвет пламени: /give ID paint gold, /give ID flame blue, /give ID paint all
        w = a[1].lower(); pool = [k for k in list(COSM) + list(BPC) if not k.startswith(("frm_", "ex_")) and (k.startswith("fl_") or k in ("bp_ice", "bp_fire")) == (kind == "flame")]
        if w == "all": return {"t": "cosm", "v": pool}
        k = next((x for x in pool if x == w or x == "fl_" + w or x == "bp_" + w), None)
        if not k: raise ValueError
        return {"t": "cosm", "v": [k]}
    raise ValueError

GIVE_HELP = ("Выдача: /give ID что-то (вместо ID можно all - всем игрокам)\n"
             "/give ID noads (отключить рекламу навсегда)\n/give ID bal 1000000\n/give ID plate 01 A 777 AC\n/give ID firm 01 007 AUB\n"
             "/give ID car 3 Название машины (0-4: обычная...секретная)\n/give ID carplate 01 R 700 TT Zeekr 9X\n"
             "/give ID stars 100 (звёзды ⭐ на звёздный баланс)\n/give ID paint gold (краски: gold chrome matte pearl candy neon royal, или all)\n/give ID flame blue (пламя: blue green pink white gold ice fire, или all)\n/give ID boost luck 120 (luck/sell/xp, минуты)\n/give ID boost luck 120 10 (удача ×10 на 120 минут, множитель 2-50)\n\nНайти ID: /find имя, /players")

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
    if g["t"] == "stars":
        for i in ids:
            wadd(c, i, g["v"], "admin-gift"); notify(i, "🎁 Вам начислено %d ⭐ на звёздный баланс!" % g["v"])
        c.commit(); return await u.message.reply_text(f"Выдано {g['v']} ⭐ {who}. Они уже на звёздном балансе.")
    if g["t"] == "cosm":
        for i in ids:
            for k in g["v"]: c.execute("INSERT OR IGNORE INTO cosm(uid,k,ts) VALUES(?,?,?)", (i, k, int(time.time())))
            notify(i, "🎁 Вам выдали: " + ", ".join(COSM_NAME.get(k, k) for k in g["v"][:6]) + (" и ещё" if len(g["v"]) > 6 else "") + ". Они уже в Тюнинге машины!")
        c.commit(); return await u.message.reply_text(f"Выдано {who}: " + ", ".join(COSM_NAME.get(k, k) for k in g["v"]) + ". Уже в Тюнинге.")
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
async def takebal(u, ctx):  # /takebal ID СУММА (или all) - списать деньги у игрока
    try: uid = int(ctx.args[0]); w = ctx.args[1].lower().replace(" ", "")
    except Exception: return await u.message.reply_text("Использование: /takebal ID СУММА\n/takebal ID all - забрать всё\nНайти ID: /find имя")
    c = db(); row = c.execute("SELECT name,bal FROM users WHERE id=?", (uid,)).fetchone()
    if not row: return await u.message.reply_text("Игрок с таким ID не найден")
    have = int(row["bal"] or 0)
    try: n = have if w in ("all", "все", "всё") else int(w)
    except ValueError: return await u.message.reply_text("Сумма должна быть числом или all")
    if n <= 0: return await u.message.reply_text("Сумма должна быть больше нуля")
    gift(c, uid, {"t": "bal", "v": -n}); c.commit()
    await u.message.reply_text(f"У игрока {row['name']} ({uid}) спишется {n:,} сум (сейчас у него {have:,}). Спишется при его следующем заходе (до 45 сек), баланс не уйдёт ниже нуля.")

@admin
async def addstars(u, ctx):  # /addstars ID N - выдать (или списать, если N<0) звёзды на звёздном балансе игрока
    try: uid = int(ctx.args[0]); n = int(ctx.args[1])
    except Exception: return await u.message.reply_text("Использование: /addstars ID N (N со знаком минус - списать)\nНайти ID: /find имя")
    c = db(); row = c.execute("SELECT name,COALESCE(stars,0) s FROM users WHERE id=?", (uid,)).fetchone()
    if not row: return await u.message.reply_text("Игрок с таким ID не найден")
    new = max(0, row["s"] + n); c.execute("UPDATE users SET stars=? WHERE id=?", (new, uid))
    c.execute("INSERT INTO wlog(uid,d,why,ts) VALUES(?,?,?,?)", (uid, new - row["s"], "admin", int(time.time()))); c.commit()
    await u.message.reply_text(f"Звёздный баланс игрока {row['name']} ({uid}): {row['s']} → {new} ⭐")

@admin
async def post(u, ctx):   # /post текст - опубликовать сообщение в канале игры (бот должен быть админом канала)
    parts = (u.message.text or "").split(None, 1)
    text = parts[1] if len(parts) > 1 else ""
    if not text: return await u.message.reply_text("Использование: /post текст сообщения (уйдёт в канал)")
    if not REQUIRED_CHANNEL: return await u.message.reply_text("Канал не задан: добавьте REQUIRED_CHANNEL в Render (например @название_канала)")
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🎰 Играть", url="https://t.me/" + BOT_USERNAME)]]) if BOT_USERNAME else None
    try: await ctx.bot.send_message(REQUIRED_CHANNEL, text, reply_markup=kb)
    except Exception as e: return await u.message.reply_text("Не получилось отправить в канал: %s\nПроверьте, что бот добавлен в канал админом с правом публикации." % e)
    await u.message.reply_text("✅ Опубликовано в канале")

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
    if not q: return await u.message.reply_text("Использование: /find имя или ID")
    like = "%" + q.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
    c = db(); c.create_function("pylower", 1, lambda x: (x or "").lower())  # LOWER() в SQLite не знает кириллицу
    rows = c.execute("SELECT id,name,state FROM users WHERE CAST(id AS TEXT)=? OR pylower(name) LIKE ? ESCAPE '\\' LIMIT 20", (q, like)).fetchall()
    out = []
    for r in rows:
        s = json.loads(r["state"] or "{}")
        out.append(f'{r["id"]} {r["name"]} · ур.{s.get("lv",1)} · {s.get("bal",0):,} сум · номеров {len(s.get("col",[]))} · машин {len(s.get("cars",[]))}')
    await u.message.reply_text("\n".join(out) or "Не найдено. Использование: /find имя или ID")

def _keep_plan(c):
    """Какие единственные номера оставить при сбросе: выпавшие в звёздной крутке + 001 UZB. Возвращает ({ключ: (владелец, номер, ts)}, [потерянные ключи])."""
    import bisect
    pts = sorted(r[0] for r in c.execute("SELECT ts FROM payments WHERE item='spin'"))
    own = {}; keys = set()
    for r in c.execute("SELECT key,owner,ts FROM uniq WHERE owner!=0"):
        own[r[0]] = (r[1], r[2]); parts = r[0].split("|")
        i = bisect.bisect_right(pts, r[2]) - 1                                  # ближайшая оплата звёздной крутки не позже появления номера
        star = i >= 0 and 0 <= r[2] - pts[i] <= 5
        if star or (len(parts) == 5 and parts[3] == "001" and parts[4] == "UZB"): keys.add(r[0])
    try:   # номера, выигранные за ⭐ на аукционе, тоже остаются
        for r in c.execute("SELECT item FROM star_auc WHERE status='done' AND topuid IS NOT NULL").fetchall():
            try: keys.add(ukey(clean_plate(json.loads(r[0]))))
            except Exception: pass
    except Exception: pass
    plan = {}
    def take(uid, p, esc=False):
        if not isinstance(p, dict): return
        try: pp = clean_plate(p)
        except Exception: return
        k = ukey(pp); ow = own.get(k, (None,))[0]
        if k in keys and k not in plan and (ow == uid or (esc and ow in (-1, -3))): plan[k] = (uid, pp, own[k][1])
    for r in c.execute("SELECT id,state FROM users WHERE state IS NOT NULL"):
        try: st = json.loads(r[1])
        except Exception: continue
        for p in (st.get("col") or []): take(r[0], p)
        for car in (st.get("cars") or []):
            if isinstance(car, dict): take(r[0], car.get("pl"))
    for r in c.execute("SELECT uid,data FROM gifts"):
        try: g = json.loads(r[1])
        except Exception: continue
        if g.get("t") == "plate": take(r[0], g.get("v"))
    listed = set(); lkeys = set()
    for t, q in (("shop", "SELECT id,seller,item FROM shop WHERE status='open'"), ("star_auc", "SELECT id,seller,item FROM star_auc WHERE status='open'"),
                 ("ton_lot", "SELECT id,seller,plate FROM ton_lot WHERE status='open'"), ("auctions", "SELECT id,seller,item FROM auctions WHERE status='open' AND seller>0"),
                 ("duels", "SELECT id,a,aplate FROM duels WHERE status='open'")):   # номера, выставленные на витрину/аукцион/лоты
        try: rows = c.execute(q).fetchall()
        except Exception: continue
        for lid, uid, it in rows:
            try: it = json.loads(it)
            except Exception: continue
            if not isinstance(it, dict): continue
            for cand in (it, it.get("pl"), it.get("v")):
                if not isinstance(cand, dict): continue
                try: k = ukey(clean_plate(cand))
                except Exception: continue
                if k in keys and own.get(k, (None,))[0] in (uid, -1, -3):
                    if t == "duels": take(uid, cand, True)
                    else: listed.add((t, lid)); lkeys.add(k)
                    break
    _keep_plan.listed = listed; _keep_plan.lkeys = lkeys
    return plan, sorted(keys - set(plan) - lkeys)

@admin
async def resetall(u, ctx):  # /resetall CONFIRM - сбросить всех, оставив номера из звёздной крутки и 001 UZB; /resetall CONFIRM ALL - сбросить вообще всё
    full = ctx.args == ["CONFIRM", "ALL"]
    if ctx.args != ["CONFIRM"] and not full:
        c = db(); plan, lost = _keep_plan(c)
        lst = "\n".join("• %s — игрок %s" % (plate_txt(p), uid) for uid, p, _ in list(plan.values())[:40]) or "— таких номеров нет"
        extra = ("\n…и ещё %d" % (len(plan) - 40)) if len(plan) > 40 else ""
        return await u.message.reply_text("⚠️ Сброс сотрёт у ВСЕХ игроков баланс, обычные номера, машины, лоты, подарки, обмены, приглашения и использованные промокоды.\n"
                                          "Останутся: ID игроков, промокоды, платежи, звёздный и TON-баланс и эти номера (вернутся владельцам в гараж):\n" + lst + extra +
                                          ("\n\n⚠️ Не найдено в гаражах владельцев (не сохранятся): %d" % len(lost) if lost else "") +
                                          "\n\nПеред сбросом делается копия базы.\nПодтвердить: /resetall CONFIRM\nСбросить вообще всё, без исключений: /resetall CONFIRM ALL")
    try: bk = backup_db("resetall")
    except Exception as e: return await u.message.reply_text(f"❌ Не удалось сделать копию базы ({e}). Сброс отменён.")
    c = db(); rid = get_rid(c) + 1; plan = {} if full else _keep_plan(c)[0]
    c.execute("CREATE TABLE IF NOT EXISTS claims(uid INT,id TEXT,PRIMARY KEY(uid,id))")
    listed = set() if full else set(getattr(_keep_plan, "listed", ())); lkeys = set() if full else set(getattr(_keep_plan, "lkeys", ()))
    try:   # ставки на ⭐ аукционе возвращаются на звёздный баланс (у снимаемых лотов); все остальные лоты снимаются
        for a in c.execute("SELECT id,top,topuid FROM star_auc WHERE status='open' AND topuid IS NOT NULL").fetchall():
            if ("star_auc", a[0]) not in listed: wadd(c, a[2], a[1], "reset-refund:%d" % a[0])
    except Exception: pass
    for t in ("shop", "star_auc", "ton_lot", "duels", "auctions"):
        try:
            for r in c.execute("SELECT id FROM " + t + " WHERE status='open'").fetchall():
                if (t, r[0]) not in listed: c.execute("DELETE FROM " + t + " WHERE id=?", (r[0],))
        except Exception: pass
    for lid in [x[1] for x in listed if x[0] == "auctions"]: c.execute("UPDATE auctions SET top=0,topuid=NULL WHERE id=?", (lid,))
    for q in ("UPDATE users SET state=NULL, bal=0", "DELETE FROM uniq WHERE (owner>=0 OR owner IN (-1,-3))" + (" AND key NOT IN (%s)" % ",".join("?" * len(lkeys)) if lkeys else ""), "DELETE FROM gifts", "DELETE FROM offers", "DELETE FROM referrals",
              "DELETE FROM refpending", "DELETE FROM redeemed", "UPDATE promos SET used=0", "DELETE FROM claims", "DELETE FROM events"):
        c.execute(q, sorted(lkeys) if q.startswith("DELETE FROM uniq") else ())
    for k, (uid, p, ts) in plan.items():
        c.execute("INSERT OR REPLACE INTO uniq(key,owner,ts) VALUES(?,?,?)", (k, uid, ts)); gift(c, uid, {"t": "plate", "v": p})
    c.execute("INSERT OR REPLACE INTO meta(k,v) VALUES('rid',?)", (str(rid),)); c.execute("INSERT OR REPLACE INTO meta(k,v) VALUES('season_end',?)", (str(int(time.time()) + 14 * 86400),)); c.commit()
    await u.message.reply_text(f"✅ Прогресс всех игроков сброшен (сезон {rid}). Сохранено номеров: {len(plan)}. Копия базы сохранена (/backup пришлёт свежую). У всех игра начнётся заново при следующем открытии.")

@admin
async def season(u, ctx):  # /season Название ДНИ - новый сезон (Battle Pass всех сбрасывается), напр. /season Beta 3 14
    if len(ctx.args) < 2 or not ctx.args[-1].isdigit():
        c = db(); n = c.execute("SELECT v FROM meta WHERE k='season_name'").fetchone(); e = c.execute("SELECT v FROM meta WHERE k='season_end'").fetchone()
        left = max(0, int(e[0]) - int(time.time())) if e else 0
        return await u.message.reply_text(f"Сейчас сезон: {n[0] if n else 'Beta 2'}, осталось ~{left // 86400} дн.\nНовый сезон: /season Название ДНИ\nНапример: /season Beta 3 14\n(Battle Pass у всех начнётся заново. Прогресс игроков при этом НЕ сбрасывается — для этого есть /resetall CONFIRM.)")
    days = max(1, min(120, int(ctx.args[-1]))); name = " ".join(ctx.args[:-1])[:24]
    c = db(); c.execute("INSERT OR REPLACE INTO meta(k,v) VALUES('season_name',?)", (name,)); c.execute("INSERT OR REPLACE INTO meta(k,v) VALUES('season_end',?)", (str(int(time.time()) + days * 86400),)); c.commit()
    await u.message.reply_text(f"✅ Сезон «{name}» начался на {days} дн.")

@admin
async def stat(u, ctx):  # /stat - здоровье сервера и базы
    await u.message.reply_text(db_report())

@admin
async def backup(u, ctx):  # /backup - сделать копию базы и прислать файл сюда
    try: path = backup_db("manual")
    except Exception as e: return await u.message.reply_text(f"❌ Копия не создалась: {e}")
    size = os.path.getsize(path)
    if size > 49 * 1024 * 1024:
        return await u.message.reply_text(f"✅ Копия сохранена на сервере ({size // 1048576} МБ), но для Telegram слишком большая: {path}")
    with open(path, "rb") as f:
        await u.message.reply_document(f, filename=os.path.basename(path), caption="Копия базы игры (gzip). Сохраните этот файл.")

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
        return await u.message.reply_text("/event luck=3 sell=2 xp=2 upg=1 МИНУТЫ [название]\nнапример: /event luck=3 60 Выходные\n/event stop")
    kv = {x.split("=")[0]: float(x.split("=")[1]) for x in a if "=" in x}
    c.execute("DELETE FROM events")
    c.execute("INSERT INTO events(name,luck,sell,xp,until,upg) VALUES(?,?,?,?,?,?)",
              (" ".join(plain[1:]) or "Ивент", kv.get("luck", 1), kv.get("sell", 1), kv.get("xp", 1), int(time.time()) + int(plain[0]) * 60, kv.get("upg"))); c.commit()
    await u.message.reply_text("Ивент запущен")

@admin
async def nftlist(u, ctx):  # /nftlist - необработанные заявки на NFT
    rows = db().execute("SELECT * FROM nft WHERE status='pending' ORDER BY id LIMIT 30").fetchall()
    if not rows: return await u.message.reply_text("Заявок на NFT нет")
    await u.message.reply_text("\n".join("#%d  %s  игрок %s\n%s" % (r["id"], json.loads(r["plate"])["d"] + " " + json.loads(r["plate"])["z"] + " " + json.loads(r["plate"])["r"], r["uid"], r["addr"]) for r in rows))

@admin
async def nftdone(u, ctx):  # /nftdone ID АДРЕС_NFT [ссылка] - NFT выпущен на кошелёк игрока (адрес нужен для возврата в игру)
    try: nid = int(ctx.args[0]); rest = ctx.args[1:]
    except Exception: return await u.message.reply_text("Использование: /nftdone ID АДРЕС_NFT [ссылка]\nАдрес NFT-предмета нужен, чтобы игрок мог вернуть номер в игру")
    item = next((x for x in rest if raw_addr(x)), ""); link = next((x for x in rest if x != item), "")
    c = db(); r = c.execute("SELECT * FROM nft WHERE id=? AND status='pending'", (nid,)).fetchone()
    if not r: return await u.message.reply_text("Заявка не найдена или уже обработана")
    c.execute("UPDATE nft SET status='minted',link=?,item=? WHERE id=?", (link, item, nid)); c.commit()
    notify(r["uid"], "💎 Ваш NFT выпущен и отправлен на кошелёк!" + ("\n" + link if link else ""))
    await u.message.reply_text("Готово, игрок уведомлён" + ("" if item else "\n⚠️ Адрес NFT не указан — вернуть в игру он не сможет, пока не добавите"))

@admin
async def nftitem(u, ctx):  # /nftitem ID АДРЕС_NFT - дописать адрес NFT к уже выпущенному
    try: nid = int(ctx.args[0]); item = ctx.args[1]
    except Exception: return await u.message.reply_text("Использование: /nftitem ID АДРЕС_NFT")
    if not raw_addr(item): return await u.message.reply_text("Это не адрес TON")
    c = db(); c.execute("UPDATE nft SET item=? WHERE id=?", (item, nid)); c.commit(); await u.message.reply_text("Сохранено")

@admin
async def addton(u, ctx):  # /addton ID N - выдать TON на баланс (минус - забрать)
    try: uid = int(ctx.args[0]); n = int(round(float(ctx.args[1]) * NANO))
    except Exception: return await u.message.reply_text("Использование: /addton ID N (например /addton 123 1.5; с минусом - забрать)")
    c = db()
    if not c.execute("SELECT 1 FROM users WHERE id=?", (uid,)).fetchone(): return await u.message.reply_text("Игрок не найден")
    if n < 0: n = -min(-n, ton_of(c, uid))
    tadd(c, uid, n, "admin"); c.commit(); await u.message.reply_text("Теперь у игрока %.3f TON" % (ton_of(c, uid) / NANO))

@admin
async def tonwd(u, ctx):  # /tonwd - заявки на вывод TON
    rows = db().execute("SELECT * FROM ton_wd WHERE status='pending' ORDER BY id LIMIT 30").fetchall()
    if not rows: return await u.message.reply_text("Заявок на вывод нет")
    await u.message.reply_text("\n\n".join("#%d  %.3f TON  игрок %s\n%s" % (r["id"], r["amount"] / NANO, r["uid"], r["addr"]) for r in rows) + "\n\n/tonpaid ID — выплачено, /tonno ID — отклонить")

@admin
async def tonpaid(u, ctx):  # /tonpaid ID - вы отправили TON игроку вручную
    try: i = int(ctx.args[0])
    except Exception: return await u.message.reply_text("Использование: /tonpaid ID")
    c = db(); r = c.execute("SELECT * FROM ton_wd WHERE id=? AND status='pending'", (i,)).fetchone()
    if not r: return await u.message.reply_text("Заявка не найдена или уже обработана")
    c.execute("UPDATE ton_wd SET status='paid' WHERE id=?", (i,)); c.commit(); notify(r["uid"], "💸 Вывод %.3f TON выплачен на ваш кошелёк" % (r["amount"] / NANO)); await u.message.reply_text("Отмечено как выплаченное")

@admin
async def tonno(u, ctx):  # /tonno ID - отклонить вывод, TON возвращаются на баланс
    try: i = int(ctx.args[0])
    except Exception: return await u.message.reply_text("Использование: /tonno ID")
    c = db(); r = c.execute("SELECT * FROM ton_wd WHERE id=? AND status='pending'", (i,)).fetchone()
    if not r: return await u.message.reply_text("Заявка не найдена или уже обработана")
    c.execute("UPDATE ton_wd SET status='rejected' WHERE id=?", (i,)); tadd(c, r["uid"], r["amount"], "wd-refund"); c.commit(); notify(r["uid"], "Вывод отклонён, TON вернулись на баланс"); await u.message.reply_text("Отклонено, TON возвращены")

@admin
async def tonstat(u, ctx):  # /tonstat - сколько TON игроки держат на балансах (ваш долг) и ждут на вывод
    c = db(); held = c.execute("SELECT COALESCE(SUM(ton),0) FROM users").fetchone()[0]; wd = c.execute("SELECT COALESCE(SUM(amount),0) FROM ton_wd WHERE status='pending'").fetchone()[0]
    dep = c.execute("SELECT COALESCE(SUM(amount),0) FROM ton_tx").fetchone()[0]
    await u.message.reply_text("Принято депозитов: %.3f TON\nНа балансах игроков: %.3f TON\nЖдёт вывода: %.3f TON\nНа кошельке игры должно быть не меньше суммы балансов и заявок." % (dep / NANO, held / NANO, wd / NANO))

@admin
async def nftno(u, ctx):  # /nftno ID - отклонить заявку, номер возвращается игроку
    try: nid = int(ctx.args[0])
    except Exception: return await u.message.reply_text("Использование: /nftno ID")
    c = db(); r = c.execute("SELECT * FROM nft WHERE id=? AND status='pending'", (nid,)).fetchone()
    if not r: return await u.message.reply_text("Заявка не найдена или уже обработана")
    p = json.loads(r["plate"]); c.execute("UPDATE nft SET status='rejected' WHERE id=?", (nid,))
    c.execute("UPDATE uniq SET owner=? WHERE key=? AND owner=-2", (r["uid"], r["key"])); gift(c, r["uid"], {"t": "plate", "v": p}); c.commit()
    notify(r["uid"], "Заявка на NFT отклонена, номер вернулся в гараж.")
    await u.message.reply_text("Отклонено, номер возвращён")

@admin
async def ultra(u, ctx):  # /ultra 1 (Аюб) или /ultra 2 (Бибиша)
    try: i = int(ctx.args[0])
    except Exception: i = 0
    await u.message.reply_text(launch_ultra(db(), i))

@admin
async def ads(u, ctx):  # /ads off - выключить рекламу всем, /ads on - включить
    a = (ctx.args[0].lower() if ctx.args else "")
    c = db()
    if a in ("off", "on"):
        c.execute("INSERT OR REPLACE INTO meta(k,v) VALUES('ads_off',?)", ("1" if a == "off" else "0",)); c.commit()
    await u.message.reply_text("Реклама сейчас: %s\n/ads off - выключить всем\n/ads on - включить" % ("ВЫКЛЮЧЕНА" if ads_is_off(c) else "включена"))

@admin
async def adsint(u, ctx):  # /adsint 60 или /adsint 60 10 (секунды между рекламой, сколько штук подряд)
    c = db()
    try:
        i = int(ctx.args[0]); f = int(ctx.args[1]) if len(ctx.args) > 1 else ads_cfg(c)[1]
        if not (20 <= i <= 3600 and 1 <= f <= 100): raise ValueError
        c.execute("INSERT OR REPLACE INTO meta(k,v) VALUES('ads_cfg',?)", ("%d,%d" % (i, f),)); c.commit()
    except IndexError: pass
    except ValueError: return await u.message.reply_text("Секунды от 20 до 3600, штук подряд от 1 до 100. Пример: /adsint 60 10")
    i, f = ads_cfg(c)
    await u.message.reply_text("Реклама: раз в %d сек, до %d штук подряд, потом пауза ~6 минут.\nИзменить: /adsint секунды штук\nНапример /adsint 60 10\nДействует у игроков при следующем заходе в игру." % (i, f))

@admin
async def restore(u, ctx):   # /restore ID - снимки коллекции; /restore ID N - вернуть пропавшие номера из снимка N
    c = db(); a = ctx.args
    try: uid = int(a[0])
    except Exception: return await u.message.reply_text("Формат: /restore ID  или  /restore ID НОМЕР_СНИМКА")
    if len(a) < 2:
        rows = restore_list(c, uid)
        if not rows: return await u.message.reply_text("Снимков коллекции этого игрока нет (они начнут копиться после обновления).")
        out = []
        for r in rows:
            n_ = len(json.loads(r["col"])) + sum(1 for x in json.loads(r["cars"]) if isinstance(x, dict) and x.get("pl"))
            out.append("№%d · %s · номеров %d" % (r["id"], time.strftime("%d.%m %H:%M", time.localtime(r["ts"] + 5 * 3600)), n_))
        return await u.message.reply_text("Снимки игрока %d (новые сверху):\n%s\n\nВернуть пропавшие: /restore %d НОМЕР" % (uid, "\n".join(out), uid))
    back = restore_missing(c, uid, int(a[1]))
    if back is None: return await u.message.reply_text("Снимок не найден.")
    if not back: return await u.message.reply_text("Всё из этого снимка уже есть у игрока, возвращать нечего.")
    for p in back: gift(c, uid, {"t": "plate", "v": p})
    c.commit(); notify(uid, "🎁 Мы вернули вам пропавшие номера: %d шт. Зайдите в игру." % len(back))
    await u.message.reply_text("Возвращено номеров: %d. Игрок получит их при следующем заходе (до 45 сек)." % len(back))

@admin
async def audit(u, ctx):   # /audit - сверка звёзд всех игроков с журналом; /audit fix - принять старые расхождения как стартовые
    c = db()
    if ctx.args and ctx.args[0] == "fix":
        n = stars_audit_fix(c); return await u.message.reply_text("Принято как стартовые значения: %d игроков. Теперь любое новое расхождение будет тревогой." % n)
    bad = stars_audit(c)
    if not bad: return await u.message.reply_text("✅ Звёзды у всех игроков сходятся с журналом. Накруток нет.")
    await u.message.reply_text("Не сходится у %d игроков:\n%s\n\nЕсли это старые расхождения (до журнала) — /audit fix" % (len(bad), "\n".join("ID %s: баланс %d ⭐, журнал %d ⭐" % x for x in bad[:30])))

@admin
async def income(u, ctx):   # /income - показать; /income on|off; /income rate ТИР СУМ; /income car ТИП МНОЖ; /income cap ЧАСЫ
    c = db(); cf = income_cfg(c); a = [x.lower() for x in ctx.args]; err = ""
    try:
        if a and a[0] in ("on", "off"): cf["on"] = 1 if a[0] == "on" else 0
        elif a and a[0] == "rate": i = int(a[1]); assert 0 <= i <= 5; cf["r"][i] = int(a[2].replace("_", "")); assert 0 <= cf["r"][i] <= 10**9
        elif a and a[0] == "car": i = int(a[1]); assert 0 <= i <= 4; cf["c"][i] = float(a[2].replace(",", ".")); assert 0.1 <= cf["c"][i] <= 100
        elif a and a[0] == "cap": cf["cap"] = int(a[1]); assert 1 <= cf["cap"] <= 72
        elif a and a[0] == "reset": cf = income_cfg.__globals__["INC_DEF"].copy(); cf = json.loads(json.dumps(cf))
        elif a: err = "Не понял команду.\n\n"
        if a and not err: income_save(c, cf)
    except Exception: err = "Неверные числа.\n\n"
    NMS = ["Обычный", "Редкий", "Эпик", "Легендарный", "Мифик", "Секретный"]; CNS = ["Обычная", "Редкая", "Мифическая", "Легендарная", "Секретная"]
    txt = err + "💰 Пассивный доход: %s\nНомер на машине приносит сум в минуту (копится до %d ч, игрок забирает в гараже).\n\nПо редкости номера (сум/мин):\n" % ("ВКЛЮЧЁН" if cf["on"] else "ВЫКЛЮЧЕН", cf["cap"])
    txt += "\n".join("%d · %s: %s" % (i, NMS[i], "{:,}".format(cf["r"][i]).replace(",", " ")) for i in range(6))
    txt += "\n\nМножитель по машине:\n" + "\n".join("%d · %s: ×%s" % (i, CNS[i], cf["c"][i]) for i in range(5))
    txt += "\n\n/income on | off\n/income rate 4 80000 (номер Мифик = 80 000 в минуту)\n/income car 4 3 (Секретная машина ×3)\n/income cap 8 (часов копится)\n/income reset (вернуть стандарт)\nДействует у игроков в течение минуты."
    await u.message.reply_text(txt)

@admin
async def newseason(u, ctx):   # /newseason Beta 3 14 - показать итоги и план; /newseason Beta 3 14 CONFIRM - начать новый сезон
    a = list(ctx.args); conf = bool(a) and a[-1] == "CONFIRM"
    if conf: a = a[:-1]
    if len(a) < 2 or not a[-1].isdigit():
        return await u.message.reply_text("Новый сезон: /newseason Название ДНИ\nНапример: /newseason Beta 3 14\nБот покажет итоги и что получит каждый. Начать: добавьте CONFIRM в конце.\n\n"
                                          "Что произойдёт: у ВСЕХ сумы станут %s (номера и машины остаются), топ по победам в гонках, топ по уровню и все, кто играл, получат награды, Battle Pass начнётся заново." % f"{SEASON_BAL:,}".replace(",", " "))
    days = max(1, min(120, int(a[-1]))); name = " ".join(a[:-1])[:24]
    c = db(); old, P = season_plan(c)
    tw = sorted([i for i in P if P[i]["wr"]], key=lambda i: P[i]["wr"])[:10]; tl = sorted([i for i in P if P[i]["lr"]], key=lambda i: P[i]["lr"])[:10]
    ts_ = sorted([i for i in P if P[i]["sr"]], key=lambda i: P[i]["sr"])[:10]; tt_ = sorted([i for i in P if P[i]["tr"]], key=lambda i: P[i]["tr"])[:10]
    act = sum(1 for x in P.values() if x["act"]); part = sum(1 for x in P.values() if x["rew"] and not x["wr"] and not x["lr"] and not x["sr"] and not x["tr"])
    tot_m = sum(x["rew"].get("m", 0) for x in P.values()); tot_s = sum(x["rew"].get("s", 0) for x in P.values())
    txt = "Сезон «%s» → «%s» на %d дн.\nИгроков: %d, играли в сезоне: %d\n\n🏁 Топ по победам в гонках:\n%s\n\n📈 Топ по уровню:\n%s\n\n💸 Топ по тратам сумов:\n%s\n\n⭐ Топ по трате звёзд:\n%s\n\nЗа участие наград: %d игрокам\nВсего сумов призами: %s, звёзд: %d ⭐\nСумы после сброса: %s у каждого" % (
        old, name, days, len(P), act,
        "\n".join("%d. %s — %d побед из %d" % (P[i]["wr"], P[i]["name"], P[i]["w"], P[i]["p"]) for i in tw) or "— пока никто не набрал %d побед" % SEASON_MIN_WINS,
        "\n".join("%d. %s — уровень %d" % (P[i]["lr"], P[i]["name"], P[i]["lv"]) for i in tl) or "—",
        "\n".join("%d. %s — %s сум" % (P[i]["sr"], P[i]["name"], f"{P[i]['sps']:,}".replace(",", " ")) for i in ts_) or "—",
        "\n".join("%d. %s — %d ⭐" % (P[i]["tr"], P[i]["name"], P[i]["stp"]) for i in tt_) or "—",
        part, f"{tot_m:,}".replace(",", " "), tot_s, f"{SEASON_BAL:,}".replace(",", " "))
    if not conf:
        return await u.message.reply_text(txt + "\n\nЭто только план, ничего не изменено.\nНачать сезон: /newseason %s %d CONFIRM" % (name, days))
    try: bk = backup_db("newseason")
    except Exception as e: return await u.message.reply_text(f"❌ Не удалось сделать копию базы ({e}). Новый сезон не начат.")
    season_apply(c, P, old, name, days)
    await u.message.reply_text("✅ Сезон «%s» начался на %d дн.\n\n%s\n\nИгроки получат сумы и награды при заходе в игру (до 45 сек). Лучшим игрокам бот отправил личные сообщения.\nКопия базы сохранена: /backup\n\nОбъявите игрокам: /broadcast 🏁 Начался сезон %s! У всех %s сум, номера и машины остались. Новый Battle Pass, задания и награды ждут вас!" % (
        name, days, txt, name, f"{SEASON_BAL:,}".replace(",", " ")))

async def mute(u, ctx):  # /mute - отключить рассылки игры
    c = db(); c.execute("UPDATE users SET mute=1 WHERE id=?", (u.effective_user.id,)); c.commit(); await u.message.reply_text("🔕 Уведомления выключены. Включить: /unmute")

async def unmute(u, ctx):
    c = db(); c.execute("UPDATE users SET mute=0 WHERE id=?", (u.effective_user.id,)); c.commit(); await u.message.reply_text("🔔 Уведомления включены")

app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()
app.add_handler(CallbackQueryHandler(how, pattern="^how$"))
app.add_handler(CallbackQueryHandler(chk, pattern="^chk$"))
app.add_handler(PreCheckoutQueryHandler(precheckout))
app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, paid))
for n, f in [("start", start), ("players", players), ("give", give), ("ultra", ultra), ("ads", ads), ("adsint", adsint), ("promo", promo), ("event", event), ("find", find), ("stars", stars), ("notifytest", notifytest), ("newseason", newseason), ("income", income), ("restore", restore), ("audit", audit), ("post", post), ("lastpay", lastpay), ("resetall", resetall), ("paysupport", paysupport), ("terms", terms), ("addbal", addbal), ("takebal", takebal), ("addstars", addstars), ("reset", reset), ("broadcast", broadcast), ("uniq", uniq), ("stat", stat), ("season", season), ("backup", backup), ("nftlist", nftlist), ("nftdone", nftdone), ("nftno", nftno), ("nftitem", nftitem), ("addton", addton), ("tonwd", tonwd), ("tonpaid", tonpaid), ("tonno", tonno), ("tonstat", tonstat), ("mute", mute), ("unmute", unmute)]:
    app.add_handler(CommandHandler(n, f))
app.run_polling()
