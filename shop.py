import asyncio, json, os, re, threading, time, requests

try:
    from splusthon import SoroushClient
    from splusthon.sessions import StringSession
    HAS_LIB = True
except Exception:
    HAS_LIB = False

# ================== تنظیمات ==================
TOKEN = os.environ.get("BOT_TOKEN", "70498658:Rq6lJ93XiuT8xETSN8bR9tCa0QkhkA4HPrI")   # توکن بازسازی‌شده رو اینجا بذار
BASE = ""              # خالی بذار تا خودکار پیدا بشه؛ اگه از مستندات آدرس دقیق رو دیدی اینجا بنویس، مثلا "https://xxx/bot"
BASE_CANDIDATES = ["https://botapi.splus.ir/bot", "https://api.splus.ir/bot", "https://bot.splus.ir/bot",
                   "https://soroushplus.com/bot", "https://api.soroushplus.com/bot"]
ADMIN_ID = 0           # آیدی عددی ادمین؛ اگه نمی‌دونی 0 بذار و توی ربات بزن: /admin رمز
ADMIN_PASS = "ARSHYA1389ASHRAF"   # رمز دستور /admin (حتما عوضش کن)
ADMIN_USERNAME = "@vitenna"
CARD = "6219861967814582"
CARD_NAME = "به نام اشرف"
PRICE = 40            # قیمت هر الماس (تومان)
ACT_FEE = 30          # هزینه فعالسازی اولیه (الماس)
HOUR_FEE = 10         # هزینه هر ساعت (الماس)
STEP = 100            # گام دکمه + و -
RUNNER_URL = ""       # اختیاری: آدرسی که ربات اجرای سلف اطلاعات رو ازش می‌گیره (POST)
SESSIONS_FILE = "sessions.json"   # ربات اجرای سلف از همین فایل سشن مشتری‌ها رو می‌خونه
API_ID = None         # اگه کتابخونه splusthon خواست، پر کن (طبق README لازم نیست)
API_HASH = None
# =============================================

S = requests.Session()
DB_FILE = "db.json"
DB = json.load(open(DB_FILE, encoding="utf-8")) if os.path.exists(DB_FILE) else {}
for k in ("users", "orders", "files"):
    DB.setdefault(k, {})
STATE = {}  # وضعیت موقت هر کاربر
ADMIN_ID = ADMIN_ID or DB.get("admin", 0)


def find_base():
    global BASE
    cands = [BASE] if BASE else BASE_CANDIDATES
    for c in cands:
        try:
            r = S.get(f"{c}{TOKEN}/getMe", timeout=15).json()
            if r.get("ok"):
                BASE = c
                print("API OK:", c, r["result"].get("username"))
                return True
        except Exception as e:
            print("try", c, "->", type(e).__name__)
    return False


def save():
    json.dump(DB, open(DB_FILE, "w", encoding="utf-8"), ensure_ascii=False)


# ---------- ورود به اکانت مشتری (کتابخونه async توی یه thread جدا) ----------
LOOP = asyncio.new_event_loop()
threading.Thread(target=LOOP.run_forever, daemon=True).start()
LOGIN = {}   # uid -> {"client", "phone", "hash"}


def run_async(coro, timeout=90):
    return asyncio.run_coroutine_threadsafe(coro, LOOP).result(timeout)


def fa2en(t):
    return t.translate(str.maketrans("۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩", "01234567890123456789"))


def norm_phone(p):
    p = re.sub(r"[^\d+]", "", fa2en(p))
    if p.startswith("00"):
        p = "+" + p[2:]
    elif p.startswith("09"):
        p = "+98" + p[1:]
    elif p.startswith("9") and len(p) == 10:
        p = "+98" + p
    elif p.startswith("98"):
        p = "+" + p
    return p


def load_sessions():
    try:
        with open(SESSIONS_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_session(uid, sess, phone):
    d = load_sessions()
    if sess is None:
        d.pop(str(uid), None)
    else:
        d[str(uid)] = {"session": sess, "phone": phone, "ts": int(time.time())}
    with open(SESSIONS_FILE + ".tmp", "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False)
    os.replace(SESSIONS_FILE + ".tmp", SESSIONS_FILE)
    try:
        os.chmod(SESSIONS_FILE, 0o600)
    except Exception:
        pass


def has_session(uid):
    return bool(load_sessions().get(str(uid), {}).get("session"))


def new_client():
    a = [StringSession()]
    if API_ID:
        a += [API_ID, API_HASH]
    return SoroushClient(*a)


async def _login_send(phone):
    c = new_client()
    await c.connect()
    sent = await c.send_code_request(phone)
    return c, sent.phone_code_hash


async def _login_code(c, phone, code, h):
    await c.sign_in(phone, code, phone_code_hash=h)


async def _login_pw(c, pw):
    await c.sign_in(password=pw)


async def _login_done(c):
    sess = c.session.save()
    await c.disconnect()
    return sess


def cancel_login(uid):
    lg = LOGIN.pop(uid, None)
    if lg:
        try:
            run_async(lg["client"].disconnect(), 15)
        except Exception:
            pass


def api(method, data=None, files=None):
    try:
        return S.post(f"{BASE}{TOKEN}/{method}", data=data, files=files, timeout=70).json()
    except Exception as e:
        print("api error:", method, e)
        return {}


def kbd(rows):
    return json.dumps({"inline_keyboard": rows})


def b(text, data):
    return {"text": text, "callback_data": data}


def send(chat, text, kb=None):
    d = {"chat_id": chat, "text": text}
    if kb:
        d["reply_markup"] = kbd(kb)
    return api("sendMessage", d)


def photo(chat, key, caption, kb=None):
    """عکس images/<key>.jpg رو می‌فرسته. بار اول آپلود می‌شه و بعدش با file_id."""
    path = f"images/{key}.jpg"
    d = {"chat_id": chat, "caption": caption}
    if kb:
        d["reply_markup"] = kbd(kb)
    if not os.path.exists(path):
        return send(chat, caption, kb)
    ck = f"{key}:{int(os.path.getmtime(path))}"
    if ck in DB["files"]:
        r = api("sendPhoto", {**d, "photo": DB["files"][ck]})
        if r.get("ok"):
            return r
    with open(path, "rb") as f:
        r = api("sendPhoto", d, files={"photo": f})
    if r.get("ok"):
        DB["files"][ck] = r["result"]["photo"][-1]["file_id"]
        save()
    return r


def edit(m, text, kb):
    d = {"chat_id": m["chat"]["id"], "message_id": m["message_id"], "reply_markup": kbd(kb)}
    if m.get("photo"):
        api("editMessageCaption", {**d, "caption": text})
    else:
        api("editMessageText", {**d, "text": text})


def user(u):
    k = str(u["id"])
    if k not in DB["users"]:
        DB["users"][k] = {"name": u.get("first_name", ""), "username": u.get("username", ""),
                          "diamonds": 0, "exempt": None}
        save()
    return DB["users"][k]


def is_exempt(us):
    e = us.get("exempt")
    return e == "forever" or (isinstance(e, (int, float)) and e > time.time())


def queue_runner(uid, until):
    """until = None یعنی همیشگی، وگرنه timestamp پایان"""
    item = {"user_id": uid, "until": until, "ts": int(time.time())}
    with open("runner_queue.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps(item) + "\n")
    if RUNNER_URL:
        try:
            S.post(RUNNER_URL, json=item, timeout=15)
        except Exception as e:
            print("runner error:", e)


# ================== صفحه‌ها ==================
HOME = [[b("🛍 خرید الماس", "shop")],
        [b("👤 حساب کاربری", "acc"), b("🆓 الماس رایگان", "free")],
        [b("⚡ فعالسازی سلف", "act"), b("🤖 سلف چیست", "about")],
        [b("📖 راهنما", "help")]]

FREE_TXT = f"""💎 بخش دریافت الماس رایگان گابلین

🎁 به ازای هر عضو جدیدی که از طریق شما عضو کانال بشه، ۱ الماس رایگان دریافت می‌کنید!

📩 مراحل دریافت الماس:
1️⃣ لینک کانال رو برای دوستانتون ارسال کنید.
2️⃣ از عضویت هر فرد، اسکرین‌شات (شات) تهیه کنید.
3️⃣ اسکرین‌شات رو به پیوی ادمین ارسال کنید.

👑 پس از بررسی و تأیید ادمین، به ازای هر عضو تأییدشده، ۱ الماس به حساب شما اضافه می‌شه.

⚠️ عضویت‌های تکراری یا تأییدنشده شامل دریافت الماس نمی‌شن.

📨 ادمین: {ADMIN_USERNAME}

💚 Goblin | بیشتر دعوت کن، بیشتر الماس بگیر!"""

ABOUT_TXT = """🤖 سلف چیست؟

✨ سلف یک ربات است که قابلیت‌های زیادی به اکانت شما اضافه می‌کند.

📸 سیو عکس و ویدیوهای زمان‌دار

⌚️ ساعت در بیو و اسم

👩‍💻 منشی خودکار

🔞 دوستی و دشمنی

⚡ و ده‌ها قابلیت جذاب دیگر..."""


HELP_TXT = """📖 راهنمای دستورات سلف
(دستورها رو توی هر چتِ اکانتت بنویس؛ همه با نقطه شروع می‌شن)

⌚ ساعت (به وقت ایران، هر دقیقه آپدیت)
.ساعت نام روشن  |  .ساعت نام خاموش
.ساعت بیو روشن  |  .ساعت بیو خاموش
.فونت  ← لیست فونت‌ها  |  .فونت 3

📝 بیو
.بیو متن دلخواه  ← جای 00:00 توی متن، ساعت می‌شینه
.بیو  ← بدون متن: فقط ساعت نوشته می‌شه

✍️ حالت متن (همه‌ی پیام‌هات ادیت می‌شه)
.حالت بولد | ایتالیک | زیرخط | خط‌خورده | اسپویلر | تکفاصله | نقل‌قول | تدریجی | خاموش

💾 ذخیره محتوا (روی پیام ریپلای کن)
.کپی  ← متن توی Saved Messages
.ذخیره  ← عکس/ویدیو/فایل توی Saved Messages

💬 پاسخ خودکار (پیوی و گروه)
.پاسخ خودکار روشن | خاموش
.افزودن پاسخ کلمه = متن
.حذف پاسخ کلمه
.نوع تطبیق برابر | شامل
.لیست پاسخ  |  .پاکسازی لیست

👩‍💻 منشی (فقط پیوی)
.منشی روشن | خاموش
.متن منشی متن دلخواه
.تایم منشی 60  ← حداقل فاصله (ثانیه) بین دو پاسخ به یک نفر

🌐 ترجمه
.ترجمه انگلیسی  (روی پیام ریپلای کن)
.ترجمه روسی متن دلخواه

🎲 شیطنت
.تاس 6  |  .بولینگ  |  .دارت  |  .فوتبال  |  .بسکتبال

📢 اسپم (فقط گروه/کانال)
.اسپم 10 سلام  |  .اسپم سریع 30 سلام  |  .اسپم آرام 30 سلام
.پایان اسپم

🛡 نگهبان (ضد حذف / ضد ویرایش)
.نگهبان روشن | خاموش
← هر پیامی که کسی (توی هر چت) حذف یا ویرایش کنه، با مشخصاتش توی Saved Messages میاد

⚔️ دشمن  |  💚 دوست
.دشمن  ← روی پیام فرد ریپلای کن (یا .دشمن @یوزرنیم / آیدی عددی)
.دشمن بنر متن بلند  ← هر وقت دشمن پیام بده این متن ریپلای می‌شه
.دشمن تک کلمه کلمه۱ ، کلمه۲  ← به هر پیامش یکی از کلمه‌ها (تصادفی) ریپلای می‌شه
.حذف دشمن  |  .لیست دشمن  |  .پاکسازی دشمن
(دوست هم دقیقاً همینه: .دوست  |  .دوست بنر  |  .دوست تک کلمه ...)
اگه فقط .دشمن بنر یا .دشمن تک کلمه بنویسی، راهنمای کامل میاد

🧮 ماشین حساب
.c 50+25  |  .c 2*2  |  .c 30÷2"""

HELP_FILE = "help_text.txt"   # متن راهنما؛ هم این ربات و هم ربات اجرای سلف (دستور .راهنما) از همین فایل می‌خونن


def help_text():
    """هر بار از فایل خونده می‌شه (ادیت فایل بدون ری‌استارت اعمال می‌شه). اگه فایل نبود، HELP_TXT قدیمی"""
    try:
        with open(HELP_FILE, encoding="utf-8") as f:
            t = f.read().strip()
        return t or HELP_TXT
    except Exception:
        return HELP_TXT


LOGIN_INFO = ("📱 شماره‌ی اکانتی که می‌خوای سلف روش فعال بشه رو با کد کشور بفرست.\nمثال: +989123456789\n\n"
              "ℹ️ با ورود، به ربات اجازه می‌دی دستورهای سلف رو روی اکانتت اجرا کنه. هر وقت خواستی با /logout "
              "ورودت رو پاک کن و از بخش «دستگاه‌های فعال» اکانتت هم نشست ربات رو ببند.")


def start_login(uid, chat, after=None):
    if not HAS_LIB:
        return send(chat, "❌ ورود به اکانت فعلاً فعال نیست. با پشتیبانی تماس بگیرید.")
    cancel_login(uid)
    STATE.setdefault(uid, {})["login"] = {"step": "phone", "after": after}
    return send(chat, LOGIN_INFO, [[b("❌ انصراف", "lgx")]])


def login_text(uid, chat, st, text, m):
    lg = st["login"]
    cancel_kb = [[b("❌ انصراف", "lgx")]]
    if lg["step"] in ("code", "pw"):   # پیام حاوی کد/رمز رو پاک کن
        api("deleteMessage", {"chat_id": chat, "message_id": m["message_id"]})
    if lg["step"] == "phone":
        phone = norm_phone(text)
        if not re.fullmatch(r"\+\d{8,15}", phone):
            return send(chat, "❌ شماره نامعتبره. مثال: +989123456789", cancel_kb)
        send(chat, "⏳ در حال ارسال کد ورود...")
        try:
            c, h = run_async(_login_send(phone))
        except Exception as e:
            st.pop("login", None)
            return send(chat, f"❌ ارسال کد ناموفق بود: {type(e).__name__}\nدوباره تلاش کن.")
        LOGIN[uid] = {"client": c, "phone": phone, "hash": h}
        lg["step"] = "code"
        return send(chat, "📩 کدی که توی سروش‌پلاس برات اومد رو بفرست، **با فاصله بین رقم‌ها** "
                          "(مثلاً: 1 2 3 4 5) تا مشکلی پیش نیاد.", cancel_kb)
    lgd = LOGIN.get(uid)
    if not lgd:
        st.pop("login", None)
        return send(chat, "⌛ نشست ورود منقضی شد. دوباره /login بزن.")
    try:
        if lg["step"] == "code":
            code = re.sub(r"\D", "", fa2en(text))
            run_async(_login_code(lgd["client"], lgd["phone"], code, lgd["hash"]))
        else:
            run_async(_login_pw(lgd["client"], text))
    except Exception as e:
        name = type(e).__name__
        if name == "SessionPasswordNeededError":
            lg["step"] = "pw"
            return send(chat, "🔐 رمز تایید دومرحله‌ای اکانتت رو بفرست:", cancel_kb)
        if "Code" in name:
            return send(chat, "❌ کد اشتباه یا منقضی شده. کد رو دوباره (با فاصله) بفرست یا /login بزن.", cancel_kb)
        if "Password" in name:
            return send(chat, "❌ رمز اشتباهه. دوباره بفرست:", cancel_kb)
        cancel_login(uid)
        st.pop("login", None)
        return send(chat, f"❌ ورود ناموفق بود: {name}")
    try:
        sess = run_async(_login_done(lgd["client"]))
    except Exception as e:
        cancel_login(uid)
        st.pop("login", None)
        return send(chat, f"❌ ذخیره‌ی ورود ناموفق بود: {type(e).__name__}")
    LOGIN.pop(uid, None)
    save_session(uid, sess, lgd["phone"])
    after = lg.get("after")
    st.pop("login", None)
    send(chat, "✅ ورود انجام شد.")
    if after == "act":
        st["hours"] = st.get("hours", 1)
        t, kb = act_screen(st["hours"])
        return photo(chat, "activate", t, kb)


def shop_screen(qty):
    txt = (f"🛍 فروشگاه الماس\n\n🎰 تعداد انتخابی: {qty:,} الماس\n"
           f"💰 قیمت هر الماس: {PRICE} تومان\n✅ جمع سبد خرید: {qty * PRICE:,} تومان\n\n"
           "تعداد را تنظیم کن و برای ادامه تایید را بزن.")
    kb = [[b("✏️ تنظیم دستی", "q:manual")],
          [b("➖ کمتر", "q:-"), b(f"{qty:,}", "q:noop"), b("➕ بیشتر", "q:+")],
          [b("✅ تایید", "q:ok")],
          [b("↩️ بازگشت", "home")]]
    return txt, kb


def act_screen(h):
    cost = ACT_FEE + HOUR_FEE * h
    txt = (f"⚡ فعالسازی سلف\n\n💎 هزینه فعالسازی اولیه: {ACT_FEE} الماس\n💎 هزینه ساعتی: {HOUR_FEE} الماس\n\n"
           f"⏱ مدت انتخابی: {h} ساعت\n✅ مجموع: {cost} الماس")
    kb = [[b("➖ کمتر", "h:-"), b(f"{h} ساعت", "h:noop"), b("➕ بیشتر", "h:+")],
          [b("✅ تایید", "h:ok")],
          [b("↩️ بازگشت", "home")]]
    return txt, kb


def show_home(chat):
    photo(chat, "start", "سلام 👋\nبه فروشگاه الماس و سلف خوش اومدی. یکی از گزینه‌ها رو انتخاب کن:", HOME)


# ================== پیام‌ها ==================
def on_message(m):
    global ADMIN_ID
    uid = m["from"]["id"]
    chat = m["chat"]["id"]
    us = user(m["from"])
    st = STATE.setdefault(uid, {})
    text = (m.get("text") or "").strip()

    if text.startswith("/start"):
        cancel_login(uid)
        STATE[uid] = {}
        return show_home(chat)

    if text in ("راهنما", "/help", "help"):
        return send(chat, help_text(), [[b("↩️ منو", "home")]])

    if text.startswith("/login"):
        return start_login(uid, chat)

    if text.startswith("/status") or text == "وضعیت":
        sess = has_session(uid)
        until = us.get("self_until")
        if us.get("exempt") == "forever":
            tm = "همیشگی ♾"
        elif until and until > time.time():
            left = int(until - time.time())
            tm = f"{left // 3600} ساعت و {left % 3600 // 60} دقیقه مونده"
        else:
            tm = "فعال نیست"
        try:
            rs = json.load(open("runner_status.json", encoding="utf-8")).get(str(uid))
        except Exception:
            rs = None
        lines = [f"🔑 ورود به اکانت: {'✅' if sess else '❌'}", f"⏱ زمان سلف: {tm}"]
        if rs is None:
            lines.append("🤖 ربات اجرای سلف: اکانت شما رو هنوز اجرا نکرده (اگه زمان فعاله، چند ثانیه صبر کن)")
        else:
            lines.append("🤖 ربات اجرای سلف: " + ("روشن ✅" if rs.get("running") else "❌ خطا"))
            le = rs.get("last_event") or 0
            lines.append("📨 آخرین پیامِ دیده‌شده از اکانتت: " +
                         (f"{int(time.time() - le)} ثانیه پیش" if le else "هنوز هیچ پیامی نرسیده"))
            if rs.get("last_error"):
                lines.append(f"⚠️ آخرین خطا: {rs['last_error']}")
        return send(chat, "\n".join(lines))

    if text.startswith("/logout"):
        cancel_login(uid)
        save_session(uid, None, None)
        queue_runner(uid, 0)     # until=0 یعنی همین الان خاموش شه
        DB["users"][str(uid)]["self_until"] = 0
        save()
        return send(chat, "✅ ورودت پاک شد و سلف خاموش شد. (یادت نره نشست ربات رو از «دستگاه‌های فعال» اکانتت هم ببندی)")

    if st.get("login") and text:
        return login_text(uid, chat, st, text, m)

    # تعیین ادمین: /admin رمز
    if text.startswith("/admin"):
        if text.split()[-1] == ADMIN_PASS:
            ADMIN_ID = uid
            DB["admin"] = uid
            save()
            return send(chat, f"✅ شما ادمین شدید. آیدی عددی: {uid}")
        return send(chat, "❌ رمز اشتباه است.")

    # دستور ادمین: تغییر متن پیام ورود (فقط بالای پیام؛ راهنمای زیرش ثابته)
    if uid == ADMIN_ID and (text.startswith("/entry") or text.startswith("متن ورود")):
        arg = text.split(None, 1)[1].strip() if text.startswith("/entry") and len(text.split(None, 1)) > 1 else \
            (text[len("متن ورود"):].strip() if text.startswith("متن ورود") else "")
        foot = "\n\n📖 برای استفاده از سلف، بخش «راهنما» را چک کنید. روی دکمه‌ی زیر بزنید 👇"
        if not arg:
            cur = DB.get("entry_text") or "✅ سلف شما فعال شد."
            return send(chat, "متن ورود فعلی:\n\n" + cur + foot +
                        "\n\n— برای تغییر:  متن ورود متن جدید\n— برای برگشت به حالت اولیه:  متن ورود پیش‌فرض")
        if arg in ("پیش‌فرض", "پیشفرض", "default"):
            DB.pop("entry_text", None)
            save()
            return send(chat, "✅ متن ورود به حالت اولیه برگشت.")
        DB["entry_text"] = arg[:1500]
        save()
        return send(chat, "✅ متن ورود عوض شد. مشتری‌ها از این به بعد این پیام رو می‌بینن:\n\n" + arg[:1500] + foot)

    # دستور ادمین: /add آیدی تعداد
    if uid == ADMIN_ID and text.startswith("/add"):
        try:
            _, tid, n = text.split()
            DB["users"][tid]["diamonds"] += int(n)
            save()
            send(chat, "✅ انجام شد")
            send(int(tid), f"💎 {n} الماس به حساب شما اضافه شد.")
        except Exception:
            send(chat, "فرمت: /add آیدی_عددی تعداد")
        return

    # ادمین: تعداد روز معافیت
    if uid == ADMIN_ID and st.get("days_for") and text.isdigit():
        o = DB["orders"][st.pop("days_for")]
        until = int(time.time()) + int(text) * 86400
        grant_exempt(o["uid"], until, f"{text} روز")
        return send(chat, f"✅ کاربر {o['uid']} به مدت {text} روز معاف شد.")

    # تعداد دستی الماس
    if st.get("manual") and text.isdigit():
        st["manual"] = False
        st["qty"] = max(1, min(int(text), 1000000))
        t, kb = shop_screen(st["qty"])
        return send(chat, t, kb)

    # رسید
    if st.get("receipt") and m.get("photo"):
        o = st.pop("receipt")
        oid = str(len(DB["orders"]) + 1)
        DB["orders"][oid] = {"uid": uid, "qty": o["qty"], "amount": o["amount"], "status": "pending"}
        save()
        cap = (f"🧾 رسید جدید #{oid}\n👤 {us['name']} (@{us['username']})\n🆔 {uid}\n"
               f"💎 {o['qty']:,} الماس\n💰 {o['amount']:,} تومان")
        api("sendPhoto", {"chat_id": ADMIN_ID, "photo": m["photo"][-1]["file_id"], "caption": cap,
                          "reply_markup": kbd([[b("✅ تایید و شارژ", f"ok:{oid}"), b("❌ رد", f"no:{oid}")],
                                               [b("🎁 معاف", f"ex:{oid}")]])})
        return send(chat, "✅ رسید شما برای ادمین ارسال شد. بعد از بررسی، الماس‌ها به حسابتان اضافه می‌شود.")

    if st.get("receipt"):
        return send(chat, "لطفاً عکس رسید بانکی را ارسال کنید 📷")

    show_home(chat)


def grant_exempt(uid, until, label):
    us = DB["users"][str(uid)]
    us["exempt"] = "forever" if until is None else until
    us["self_until"] = until
    save()
    queue_runner(uid, until)
    if has_session(uid):
        send(uid, f"🎁 سلف شما به صورت رایگان ({label}) فعال شد ✅")
    else:
        send(uid, f"🎁 سلف شما به صورت رایگان ({label}) فعال شد ✅\nبرای شروع، وارد اکانتت شو:",
             [[b("🔑 ورود به اکانت", "login")]])


# ================== دکمه‌ها ==================
def on_callback(cq):
    m = cq["message"]
    uid = cq["from"]["id"]
    chat = m["chat"]["id"]
    d = cq["data"]
    us = user(cq["from"])
    st = STATE.setdefault(uid, {})
    api("answerCallbackQuery", {"callback_query_id": cq["id"]})

    # ---- ادمین ----
    if d.split(":")[0] in ("ok", "no", "ex", "exf", "ext", "exb"):
        if uid != ADMIN_ID:
            return
        act, oid = d.split(":")
        o = DB["orders"].get(oid)
        if not o:
            return
        if act == "ok" and o["status"] == "pending":
            o["status"] = "ok"
            DB["users"][str(o["uid"])]["diamonds"] += o["qty"]
            save()
            send(o["uid"], f"✅ پرداخت شما تایید شد و {o['qty']:,} الماس به حسابتان اضافه شد.")
            send(chat, f"✅ سفارش #{oid} تایید شد.")
        elif act == "no" and o["status"] == "pending":
            o["status"] = "no"
            save()
            send(o["uid"], "❌ رسید شما تایید نشد. در صورت اشتباه با پشتیبانی تماس بگیرید.")
            send(chat, f"❌ سفارش #{oid} رد شد.")
        elif act == "ex":
            api("editMessageReplyMarkup", {"chat_id": chat, "message_id": m["message_id"],
                                           "reply_markup": kbd([[b("♾ معاف همیشگی", f"exf:{oid}"),
                                                                 b("📅 معاف تایمی", f"ext:{oid}")],
                                                                [b("↩️ بازگشت", f"exb:{oid}")]])})
        elif act == "exb":
            api("editMessageReplyMarkup", {"chat_id": chat, "message_id": m["message_id"],
                                           "reply_markup": kbd([[b("✅ تایید و شارژ", f"ok:{oid}"),
                                                                 b("❌ رد", f"no:{oid}")],
                                                                [b("🎁 معاف", f"ex:{oid}")]])})
        elif act == "exf":
            grant_exempt(o["uid"], None, "همیشگی")
            send(chat, f"✅ کاربر {o['uid']} برای همیشه معاف شد.")
        elif act == "ext":
            STATE.setdefault(ADMIN_ID, {})["days_for"] = oid
            send(chat, "چند روز معاف باشه؟ (فقط عدد بفرست)")
        return

    # ---- کاربر ----
    if d == "home":
        cancel_login(uid)
        st.clear()
        return show_home(chat)
    if d == "login":
        return start_login(uid, chat)
    if d == "lgx":
        cancel_login(uid)
        st.pop("login", None)
        return send(chat, "انصراف داده شد.", [[b("↩️ منو", "home")]])
    if d == "acc":
        cap = (f"👤 نام: {us['name']}\n💎 موجودی الماس: {us['diamonds']:,}\n"
               f"💵 معادل تومانی: {us['diamonds'] * PRICE:,} تومان")
        if is_exempt(us):
            cap += "\n🎁 وضعیت سلف: معاف ✅"
        return photo(chat, "account", cap, [[b("↩️ بازگشت", "home")]])
    if d == "free":
        return photo(chat, "free", FREE_TXT, [[b("↩️ بازگشت", "home")]])
    if d == "about":
        return photo(chat, "about", ABOUT_TXT, [[b("↩️ بازگشت", "home")]])
    if d == "help":
        return send(chat, help_text(), [[b("↩️ بازگشت", "home")]])

    if d == "shop":
        st["qty"] = st.get("qty", 100)
        t, kb = shop_screen(st["qty"])
        return photo(chat, "shop", t, kb)
    if d.startswith("q:"):
        a = d[2:]
        qty = st.get("qty", 100)
        if a == "+":
            qty += STEP
        elif a == "-":
            qty = max(STEP, qty - STEP)
        elif a == "manual":
            st["manual"] = True
            return send(chat, "تعداد دلخواه خود را وارد کنید (فقط عدد):")
        elif a == "ok":
            amount = qty * PRICE
            st["receipt"] = {"qty": qty, "amount": amount}
            txt = (f"پیش فاکتور شما با موفقیت ساخته شد ✅\n💶 مبلغ قابل پرداخت : {amount:,}\n\n{CARD}\n\n{CARD_NAME}\n\n"
                   "از ارسال رسید فیک جدّا خودداری فرمایید چرا که منجر به مسدودی دائم شما از ربات میشود‼️\n\n"
                   "🔹 بعد از واریز مبلغ، عکس رسید بانکی را در ادامه ی همین پیام ارسال نمایید")
            return photo(chat, "invoice", txt)
        else:
            return
        st["qty"] = qty
        t, kb = shop_screen(qty)
        return edit(m, t, kb)

    if d == "act":
        if not has_session(uid):
            return start_login(uid, chat, "act")
        st["hours"] = st.get("hours", 1)
        t, kb = act_screen(st["hours"])
        return photo(chat, "activate", t, kb)
    if d.startswith("h:"):
        a = d[2:]
        h = st.get("hours", 1)
        if a == "+":
            h += 1
        elif a == "-":
            h = max(1, h - 1)
        elif a == "ok":
            if not has_session(uid):
                return start_login(uid, chat, "act")
            if us.get("exempt") == "forever":
                return send(chat, "♾ شما معاف همیشگی هستید و نیازی به خرید ساعت نیست.")
            cost = ACT_FEE + HOUR_FEE * h
            if us["diamonds"] < cost:
                return send(chat, f"❌ موجودی کافی نیست.\nنیاز: {cost} الماس | موجودی: {us['diamonds']} الماس",
                            [[b("🛍 خرید الماس", "shop")]])
            us["diamonds"] -= cost
            base = max(time.time(), us.get("self_until") or 0)   # اگه فعاله، به انتهاش اضافه می‌شه
            until = int(base + h * 3600)
            us["self_until"] = until
            save()
            queue_runner(uid, until)
            return send(chat, f"✅ سلف شما برای {h} ساعت فعال شد.\n💎 {cost} الماس کسر شد. موجودی: {us['diamonds']}\n\n"
                              "برای دیدن دستورها روی دکمه‌ی راهنما بزن 👇", [[b("📖 راهنما", "help")]])
        else:
            return
        st["hours"] = h
        t, kb = act_screen(h)
        return edit(m, t, kb)


def handle(u):
    if "callback_query" in u:
        on_callback(u["callback_query"])
    elif "message" in u:
        on_message(u["message"])


if __name__ == "__main__":
    if not find_base():
        print("❌ به API وصل نشد. آدرس رو از مستندات پیدا کن و توی BASE بذار، یا توکن رو چک کن.")
        raise SystemExit(1)
    print("bot started")
    offset = 0
    while True:
        r = api("getUpdates", {"offset": offset, "timeout": 50})
        for u in r.get("result", []):
            offset = u["update_id"] + 1
            try:
                handle(u)
            except Exception as e:
                print("error:", e)
        if not r.get("ok"):
            time.sleep(3)
