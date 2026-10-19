"""
ربات اجرای سلف (ربات دوم)
- از فایل runner_queue.jsonl (که ربات فروشگاه می‌نویسه) می‌فهمه هر کاربر تا کی سلف داره
- از فایل sessions.json (که ربات فروشگاه موقع ورود مشتری می‌سازه) سشن اکانت رو می‌خونه
- وقتی زمان تموم شد اکانت رو از سلف بیرون میاره (اسم و بیو اصلی برمی‌گرده)
نصب:  pip install splusthon deep-translator requests
اجرا:  python self_runner.py
"""
import asyncio, ast, copy, html, json, operator, os, random, re, sys, time, traceback
from collections import OrderedDict, deque
from datetime import datetime, timedelta, timezone

import requests

from splusthon import SoroushClient, events
from splusthon.sessions import StringSession
from splusthon.tl.functions.account import UpdateProfileRequest

try:
    from splusthon.tl.types import InputMediaDice
except Exception:  # اگه کتابخونه تاس رو نداشت، دستورهای شیطنت پیام خطا می‌دن
    InputMediaDice = None

try:
    from splusthon.tl.functions.users import GetFullUserRequest
except Exception:
    GetFullUserRequest = None

# ================== تنظیمات ==================
SESSIONS_FILE = "sessions.json"
QUEUE_FILE = "runner_queue.jsonl"
DATA_DIR = "selfdata"
API_ID = None          # اگه کتابخونه خواست، پر کن (طبق README لازم نیست)
API_HASH = None
NOTIFY_TOKEN = os.environ.get("BOT_TOKEN", "")      # توکن ربات فروشگاه (برای خبر دادن به مشتری)؛ خالی = بدون اعلان
NOTIFY_BASE = "https://api.splus.ir/bot"
MAX_SPAM = 50
PREFIXES = (".", "!")  # دستورها با این‌ها شروع می‌شن
DEBUG = os.environ.get("DEBUG", "0") == "1"           # لاگ کامل برای عیب‌یابی؛ بعد از رفع مشکل False کن
# =============================================

TZ = timezone(timedelta(hours=3, minutes=30))  # ساعت ایران
os.makedirs(DATA_DIR, exist_ok=True)

ACTIVE_SELF_UIDS = set()  # آیدی عددیِ کاربرهایی که الان سلفشون روشنه (برای معافیت گزارش ادیت نگهبان)


def log(*a):
    print(datetime.now(TZ).strftime("%H:%M:%S"), *a, flush=True)


def load_json(path, default):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


# ---------------- فونت‌های ساعت ----------------
def _mk(d):
    assert len(d) == 10
    return str.maketrans("0123456789", d)


FONTS = [
    _mk("0123456789"),
    _mk("۰۱۲۳۴۵۶۷۸۹"),
    _mk("０１２３４５６７８９"),
    _mk("𝟎𝟏𝟐𝟑𝟒𝟓𝟔𝟕𝟖𝟗"),
    _mk("𝟘𝟙𝟚𝟛𝟜𝟝𝟞𝟟𝟠𝟡"),
    _mk("𝟶𝟷𝟸𝟹𝟺𝟻𝟼𝟽𝟾𝟿"),
    _mk("⓪①②③④⑤⑥⑦⑧⑨"),
    _mk("⓿❶❷❸❹❺❻❼❽❾"),
    _mk("𝟢𝟣𝟤𝟥𝟦𝟧𝟨𝟩𝟪𝟫"),
    _mk("𝟬𝟭𝟮𝟯𝟰𝟱𝟲𝟳𝟴𝟵"),
    _mk("⁰¹²³⁴⁵⁶⁷⁸⁹"),
    _mk("₀₁₂₃₄₅₆₇₈₉"),
]


def fmt_time(font=1, now=None):
    now = now or datetime.now(TZ)
    return now.strftime("%H:%M").translate(FONTS[(font - 1) % len(FONTS)])


# ---------------- ماشین حساب امن ----------------
_OPS = {ast.Add: operator.add, ast.Sub: operator.sub, ast.Mult: operator.mul,
        ast.Div: operator.truediv, ast.Mod: operator.mod, ast.FloorDiv: operator.floordiv,
        ast.Pow: operator.pow}


def _ev(n):
    if isinstance(n, ast.Expression):
        return _ev(n.body)
    if isinstance(n, ast.Constant) and isinstance(n.value, (int, float)) and not isinstance(n.value, bool):
        return n.value
    if isinstance(n, ast.UnaryOp) and isinstance(n.op, (ast.USub, ast.UAdd)):
        v = _ev(n.operand)
        return -v if isinstance(n.op, ast.USub) else v
    if isinstance(n, ast.BinOp) and type(n.op) in _OPS:
        l, r = _ev(n.left), _ev(n.right)
        if isinstance(n.op, ast.Pow) and (abs(r) > 100 or abs(l) > 1e6):
            raise ValueError("big")
        return _OPS[type(n.op)](l, r)
    raise ValueError("bad")


def calc(expr):
    e = expr.strip()
    e = e.translate(str.maketrans("۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩", "01234567890123456789"))
    e = e.replace("÷", "/").replace("×", "*").replace("^", "**").replace("،", "").replace("٫", ".")
    if not e or len(e) > 100:
        raise ValueError("len")
    v = _ev(ast.parse(e, mode="eval"))
    if isinstance(v, float):
        v = round(v, 10)
        if v == int(v):
            v = int(v)
    return v


# ---------------- ترجمه ----------------
LANGS = {
    "فارسی": "fa", "انگلیسی": "en", "عربی": "ar", "روسی": "ru", "ترکی": "tr", "آذری": "az",
    "آلمانی": "de", "فرانسه": "fr", "فرانسوی": "fr", "اسپانیایی": "es", "ایتالیایی": "it",
    "چینی": "zh-CN", "ژاپنی": "ja", "کره‌ای": "ko", "کره ای": "ko", "کرهای": "ko", "هندی": "hi",
    "اردو": "ur", "پرتغالی": "pt", "هلندی": "nl", "اوکراینی": "uk", "لهستانی": "pl",
    "عبری": "iw", "یونانی": "el", "سوئدی": "sv", "تایلندی": "th", "ویتنامی": "vi",
    "اندونزیایی": "id", "کردی": "ku",
}

# ---------------- حالت متن ----------------
MODES = {
    "بولد": ("<b>", "</b>"), "ایتالیک": ("<i>", "</i>"), "زیرخط": ("<u>", "</u>"),
    "خط‌خورده": ("<s>", "</s>"), "خطخورده": ("<s>", "</s>"),
    "اسپویلر": ("<tg-spoiler>", "</tg-spoiler>"), "تکفاصله": ("<code>", "</code>"),
    "نقل‌قول": ("<blockquote>", "</blockquote>"), "نقل قول": ("<blockquote>", "</blockquote>"),
    "تدریجی": None,
}

# ---------------- شیطنت (تاس و بازی‌ها) ----------------
GAMES = {"بولینگ": ("🎳", {6}), "دارت": ("🎯", {6}), "فوتبال": ("⚽", {3, 4, 5}),
         "بسکتبال": ("🏀", {4, 5})}

COMMANDS = ["ping", "ساعت نام", "ساعت بیو", "فونت", "بیو", "حالت", "ذخیره", "کپی", "پاسخ خودکار",
            "افزودن پاسخ", "حذف پاسخ", "لیست پاسخ", "پاکسازی لیست", "نوع تطبیق", "ترجمه", "تاس",
            "بولینگ", "دارت", "فوتبال", "بسکتبال", "اسپم سریع", "اسپم آرام", "اسپم", "پایان اسپم",
            "منشی", "متن منشی", "تایم منشی", "c", "نگهبان",
            "دشمن", "دشمن بنر", "دشمن تک کلمه", "حذف دشمن", "لیست دشمن", "پاکسازی دشمن",
            "دوست", "دوست بنر", "دوست تک کلمه", "حذف دوست", "لیست دوست", "پاکسازی دوست"]
COMMANDS.sort(key=len, reverse=True)

DEFAULTS = {
    "clock_name": False, "clock_bio": False, "font": 1,
    "orig_first": None, "orig_last": None, "orig_bio": None,
    "bio_text": "", "mode": None,
    "auto_on": False, "match": "equal", "replies": {},
    "sec_on": False, "sec_text": "✅ مشغولم، بعداً پاسخ می‌دم", "sec_time": 60,
    "guard": False,
    "enemy": {"ids": {}, "banner": "", "words": []},
    "friend": {"ids": {}, "banner": "", "words": []},
}
# ---------------- متن‌های رندومِ پیش‌فرض دشمن/دوست ----------------
# این چهار لیست رو با متن‌های دلخواهت پر کن. به محض اینکه کسی رو با دستور «.دشمن» (یا «.دوست»)
# اضافه کنی، خودِ ربات خودکار از همین لیست‌ها جواب می‌ده — نیازی نیست برای هر نفر جدا بنر یا
# تک‌کلمه تنظیم کنی. (اگه برای یه نفرِ خاص هم جدا با دستور متن ثبت کنی، اون هم به همین لیست‌ها
# اضافه می‌شه، حذفشون نمی‌کنه.)
# بنر = فاصله‌ی ارسال بیشتر (هر BANNER_CD ثانیه یه‌بار) | تک‌کلمه = فاصله‌ی کمتر (هر WORD_CD ثانیه)
ENEMY_BANNERS = [
    "بیناموس کیرم بره تو کس مادرت واس بابات نشآخ مادر کیری کیرم بره تو کس اجدادت کسکش بیناموس کس ول نسل شوتی ابجی کسده کیرم تو کس مادرت بیناموس کیری کیرم تو کس نسلت ابجی کونی کس نسل سگ ممبر کونی ابجی سگ ممبر سگ کونی کیرم تو کس ماورت کیر کابوس ارمیا تو کس مادرت کیرم تو کس نسلت مادر کونی ابجی کونی کیری ناموس ابجیتو گاییدم سگ حرومی خارکسه مادر کیری با کیر بزنم تو رحم مادرت ناموستو بگام لاشی کونی ابجی کس  خیابونی",
"کیرم تو کس نسلت ابجی کونی نسل کونی ابجی کس سگ ممبر لاشی حرومی خارکسه جد سگ ممبر سگ کونی کیرم بره تو کس ابجیت مادر کونی نسل الکسیس کیر ارتش انگلیس تو کس مادرت کفش ورزشی تو کس ابجیت کیروخایم تو کس نسلت ابجی کونی کیرم برع تو کس جدت مادر کونی ابجی سگ ممبر کونی سگ خایمال ابجی جنده مادر کس قشنگ نسل شوتی ابجیتو گاییدم حرومی بیناموس",
"کیرم از پهنا به کس خارت کس مادرت بیناموس مادر کسه حرومی خارکسه کیرم بره تو کس ناموست مادر کس خیابونی کیرم رو کس نسلت ابجی کونی کیرم بره تو کس جدت چی تو خودت دیدی که داری واسه من واق واق میکنی اخ کیرم تا ته تو ناموست مادر کونی بیناموس کیروخایم تو کس خارت ابجی جنده مادر کس اوبی نسل جنده کیرم تو قبر مادر مرده ات بیناموس مادر جنده کیرم بزارم تو شاشدون مادرت حرومی ممبر یتیم ناموس کونی سگ خایمال",
  "کیرم از پهنا تو کس ناموست مادرتو به 10 روش گاییدم سکس ناموس ننه سگ کونی ابجی کیری کیرم تو کس نسلت سکس ناموس کونی ابجی کیری کیروخایم تو کس ناموست کیرم تو کس دختر عموت ناموستو گاییدم کیرم از پهنا تو کس دختر خالت ناموستو بخورم بیناموس مادر حرومی ممبر کیری ننتو گاییدم که ترو بوجود اورد ناموس کیری کیرم تو کس ابجیت سگ  ناموس نبینم واق واق کنی واس بابات با کیر میزنم تو کس اجدادت",
  "کس نسل کونی ابجی کونی مادر خیابونی ابجی کسه کیرم بره تو کس جدت کیرم رو شکم مادرت کیرم رو شکم دختر عموت کیرم رو شکم دختر داییت کیرم رو شکم خاله ات کیرم تو کس نسلت کیرم تو کس مادرت کیرم تو کس ابجیت کیرم تو کس زنداییت کیرم تو کس خاله ات کیرم تو کس خار بابات کیرم رو صورت مادرت کیرم رو صورت ابجیت کیرم رو کس ابجیت کیرم تو قبر مادرت کیرم تو شاشدون خارت کیرم تو رحم خار بابات کیرم تو کس ننه بابات کیرم به قبر مرده هات بیناموس یتیم ممبر کیونی کیرم بره تو کس نوامیست کیری ممبر سگ کونی خایمالی ابجی کس تنگ",
"خارتو گاییدم مادر کس خیابونی ابجی کونی کیرم بره تو کس نسلت ابجی کس تنگ نسل کونی کیرم بره تو کس ابجیت نسل کونی کیرم با خایه هام تو حلق مادر جندت کونی ممبر سگ خایه مال کیرم ابجی کونی کیرم تو کس جدت ابجی کیری نسل شوتی کس نسل کونی خارکسه جد کونی ناموس ولو خارکسه کیرم تو کس نوامیست مادرتو بگام مادر کس قشنگ مادر کونی نسل اوبی  جنده ناموس سکس ممبر کیری ناموس کیرم بره تو کس جدت ابجی سگ ممبر کیری",
]
ENEMY_WORDS = [
    "ننه کونی ",
  "کسننت",
  "کسمادرت",
  "ننه سگ",
  "ننه جنده",  
  "ننه صورتی",
  "ننه کس پاپیونی",
  "کیرم تو شاشدون مادرت",
]
FRIEND_BANNERS = [
    
]
FRIEND_WORDS = [
    "عشق منی💞",
  "فداتشم",
  "دورت بگردم",
  "نانازمن",
  "گوگولیم",
  "کیوتم",
  "خوشگلم"
  "نوکرتم",
  "کونت بلیسم",
]

BANNER_CD = 60      # حداقل فاصله‌ی ارسال بنر برای هر دشمن/دوست (ثانیه)
WORD_CD = 3         # حداقل فاصله‌ی ارسال تک‌کلمه
CACHE_MAX = 100     # آخرین پیام‌های دیده‌نشده‌ی دریافتی که نگهبان نگه می‌داره
REL = {"enemy": "دشمن", "friend": "دوست"}
ENTRY_DEFAULT = "✅ سلف شما فعال شد."
ENTRY_FOOTER = ("\n\n📖 برای دیدن همه‌ی دستورها توی همین ربات بنویس: راهنما\n"
                "(دستورها رو با نقطه توی هر چتِ اکانتت بنویس؛ مثلا: .ساعت نام روشن)")


def entry_message():
    """متن پیام ورود = متن قابل‌تغییر ادمین (db.json) + فوتر ثابت"""
    t = load_json("db.json", {}).get("entry_text") or ENTRY_DEFAULT
    return t + ENTRY_FOOTER


def media_label(m):
    if m is None:
        return ""
    for attr, name in (("sticker", "استیکر"), ("gif", "گیف"), ("voice", "ویس"), ("video_note", "ویدیو مسیج"),
                       ("video", "فیلم"), ("audio", "موزیک"), ("photo", "عکس")):
        try:
            if getattr(m, attr, None):
                return name
        except Exception:
            pass
    if getattr(m, "media", None) is not None:
        return "فایل"
    return ""


def fmt_dt(d=None):
    try:
        d = d.astimezone(TZ) if d else datetime.now(TZ)
    except Exception:
        d = datetime.now(TZ)
    return d.strftime("%Y/%m/%d %H:%M:%S")


def person(e):
    if e is None:
        return "؟"
    name = " ".join(x for x in (getattr(e, "first_name", "") or "", getattr(e, "last_name", "") or "") if x)
    name = name or getattr(e, "title", "") or "؟"
    un = getattr(e, "username", None)
    return name + (f" (@{un})" if un else "")


def norm(t):
    t = t.replace("ي", "ی").replace("ك", "ک").replace("\u200c", "").replace("ـ", "")
    return " ".join(t.split()).lower()


MODES_N = {norm(k): k for k in MODES}
INVIS = "\u200e\u200f\u202a\u202b\u202c\u202d\u202e\u2066\u2067\u2068\u2069\ufeff \t"


def parse_command(body):
    toks = body.split()
    for ph in COMMANDS:
        n = len(ph.split())
        if len(toks) >= n and norm(" ".join(toks[:n])) == ph:
            rest = body.split(None, n)
            return ph, (rest[n].strip() if len(rest) > n else "")
    return None, ""


def on_off(arg):
    a = arg.strip().lower()
    if a in ("روشن", "on", "فعال"):
        return True
    if a in ("خاموش", "off", "غیرفعال"):
        return False
    return None


class UserSelf:
    def __init__(self, uid, session_str):
        self.uid = str(uid)
        self.sess = session_str
        self.name_lim = 62
        self.path = os.path.join(DATA_DIR, f"{uid}.json")
        self.s = {**copy.deepcopy(DEFAULTS), **load_json(self.path, {})}
        for k in ("enemy", "friend"):
            self.s[k] = {**copy.deepcopy(DEFAULTS[k]), **(self.s.get(k) or {})}
        self.cache = OrderedDict()   # پیام‌های دریافتی برای نگهبان
        a = [StringSession(session_str)]
        if API_ID:
            a += [API_ID, API_HASH]
        self.client = SoroushClient(*a)
        self.tasks = []
        self.spam_task = None
        self.last_event = 0
        self.last_error = ""
        self.raw_seen = {}
        self.last_check = 0
        self.fails = 0
        self.handled = deque(maxlen=500)   # (chat, id) پیام‌هایی که قبلاً پردازش شدن
        self.recent = {}          # متن‌هایی که خود ربات فرستاده
        self.cool = {}            # زمان آخرین پاسخ به هر چت
        self.me = None

    # ---------- ذخیره ----------
    def save(self):
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(self.s, f, ensure_ascii=False)

    def mark(self, text):
        self.recent[text] = time.time()
        if len(self.recent) > 200:
            for k in sorted(self.recent, key=self.recent.get)[:100]:
                self.recent.pop(k, None)

    def is_ours(self, text):
        return self.recent.get(text, 0) > time.time() - 10

    # ---------- شروع / پایان ----------
    async def start(self):
        await self.client.connect()
        if not await self.client.is_user_authorized():
            raise RuntimeError("سشن معتبر نیست؛ مشتری باید دوباره وارد شه")
        self.me = await self.client.get_me()
        log(self.uid, "logged in as", getattr(self.me, "first_name", "?"), getattr(self.me, "id", "?"))
        self.client.add_event_handler(self.on_any, events.NewMessage())
        try:
            self.client.add_event_handler(self.on_raw, events.Raw())
        except Exception as e:
            log(self.uid, "raw handler unavailable:", e)
        for nm, h in (("MessageEdited", self.on_edit), ("MessageDeleted", self.on_del), ("MessageRead", self.on_read)):
            cls = getattr(events, nm, None)
            if cls is None:
                log(self.uid, "event unavailable (guard limited):", nm)
                continue
            try:
                self.client.add_event_handler(h, cls())
            except Exception as e:
                log(self.uid, "event register failed:", nm, e)
        self.tasks.append(asyncio.create_task(self.clock_loop()))
        log(self.uid, "started, waiting for messages (type .c 2+2 in any chat)")

    async def healthy(self):
        try:
            await asyncio.wait_for(self.client.get_me(), 25)
            return True
        except Exception as e:
            self.last_error = f"اتصال: {type(e).__name__}"
            return False

    async def drop(self):
        """قطع اتصال بدون برگردوندن اسم/بیو (برای بالا آوردن دوباره)"""
        for t in self.tasks + ([self.spam_task] if self.spam_task else []):
            t.cancel()
        try:
            await self.client.disconnect()
        except Exception:
            pass

    async def stop(self):
        for t in self.tasks + ([self.spam_task] if self.spam_task else []):
            t.cancel()
        try:
            await self.restore_profile()
        except Exception as e:
            log(self.uid, "restore error", e)
        try:
            await self.client.disconnect()
        except Exception:
            pass
        log(self.uid, "stopped")

    # ---------- ساعت ----------
    async def get_bio(self):
        if GetFullUserRequest is None:
            return None
        try:
            r = await self.client(GetFullUserRequest("me"))
            fu = getattr(r, "full_user", r)
            return getattr(fu, "about", None) or ""
        except Exception:
            return None

    async def snapshot_profile(self):
        me = await self.client.get_me()
        if self.s["orig_first"] is None:
            self.s["orig_first"] = me.first_name or ""
            self.s["orig_last"] = me.last_name or ""
        if self.s["orig_bio"] is None:
            b = await self.get_bio()
            self.s["orig_bio"] = b if b is not None else ""
        self.save()

    async def restore_profile(self):
        if self.s["orig_first"] is None and self.s["orig_bio"] is None:
            return
        kw = {}
        if self.s["clock_name"] and self.s["orig_first"] is not None:
            kw["first_name"] = self.s["orig_first"]
            kw["last_name"] = self.s["orig_last"] or ""
        if self.s["clock_bio"] and self.s["orig_bio"] is not None:
            kw["about"] = self.s["orig_bio"]
        if kw:
            await self.client(UpdateProfileRequest(**kw))
        self.s["clock_name"] = self.s["clock_bio"] = False
        self.s["orig_first"] = self.s["orig_last"] = self.s["orig_bio"] = None
        self.save()

    async def apply_clock(self):
        t = fmt_time(self.s["font"])
        about = None
        if self.s["clock_bio"]:
            tpl = self.s["bio_text"]
            about = (tpl.replace("00:00", t) if tpl else t)[:70]
        if not (self.s["clock_name"] and self.s["orig_first"] is not None):
            if about is not None:
                await self.client(UpdateProfileRequest(about=about))
            return
        # اسم‌های طولانی یا با حروف تزئینی ممکنه از حد سرور رد شن؛ کم‌کم کوتاه‌تر امتحان می‌کنیم
        lims = [l for l in (62, 40, 28, 18, 10, 4) if l <= self.name_lim] or [4]
        last = None
        for lim in lims:
            base = self.s["orig_first"][:max(1, lim - len(t))]
            kw = {"first_name": f"{base} {t}", "last_name": self.s["orig_last"] or ""}
            if about is not None:
                kw["about"] = about
            try:
                await self.client(UpdateProfileRequest(**kw))
                if lim != self.name_lim:
                    log(self.uid, "clock: name shortened to", lim, "chars")
                self.name_lim = lim
                return
            except asyncio.CancelledError:
                raise
            except Exception as e:
                last = e
                if any(w in type(e).__name__ for w in ("Flood", "Timeout", "Connection", "Auth")):
                    break                      # مشکل اسم نیست؛ الکی تکرار نکن
        raise last

    async def clock_loop(self):
        while True:
            try:
                if self.s["clock_name"] or self.s["clock_bio"]:
                    await self.apply_clock()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log(self.uid, "clock error", repr(e))
                self.last_error = f"ساعت: {type(e).__name__}: {str(e)[:80]}"
                await asyncio.sleep(30)
            now = datetime.now(TZ)
            await asyncio.sleep(60 - now.second + 0.3)

    # ---------- پاسخ به دستور ----------
    async def say(self, ev, text, delete_after=4):
        self.mark(text)
        try:
            await ev.edit(text)
        except Exception:
            m = await ev.respond(text)
            ev = m
        if delete_after:
            async def _d():
                await asyncio.sleep(delete_after)
                try:
                    await ev.delete()
                except Exception:
                    pass
            asyncio.create_task(_d())

    # ---------- پیام‌های خودِ کاربر ----------
    async def on_raw(self, update):
        """فقط نوع آپدیت‌ها رو (بدون محتوا) برای عیب‌یابی ثبت می‌کنه"""
        n = type(update).__name__
        self.raw_seen[n] = self.raw_seen.get(n, 0) + 1
        if DEBUG and self.raw_seen[n] == 1:
            log(self.uid, "update type:", n)

    async def on_any(self, ev):
        """هر پیامی در هر چتی؛ خودی بره on_out، دریافتی بره on_in"""
        self.last_event = time.time()
        mid = getattr(ev, "id", None)
        if mid is not None:                      # یک پیام فقط یک بار پردازش شه
            key = (getattr(ev, "chat_id", None), mid)
            if key in self.handled:
                return
            self.handled.append(key)
        out = getattr(ev, "out", None)
        if out is None:
            out = getattr(getattr(ev, "message", None), "out", False)
        if DEBUG:
            log(self.uid, "MSG out=", bool(out), "chat", getattr(ev, "chat_id", "?"),
                "len", len(getattr(ev, "raw_text", "") or ""))
        is_priv = getattr(ev, "is_private", False)
        if out:
            if self.s["guard"] and is_priv:
                self.mark_seen(getattr(ev, "chat_id", None))
            await self.on_out(ev)
        else:
            if self.s["guard"] and is_priv:      # نگهبان فقط توی پیوی کار می‌کنه، نه گروه/کانال
                await self.remember(ev)
            await self.on_in(ev)

    async def on_out(self, ev):
        try:
            text = ev.raw_text or ""
            if not text or self.is_ours(text):
                return
            clean = text.lstrip(INVIS)
            if clean[:1] in PREFIXES:
                if DEBUG:
                    log(self.uid, "cmd:", clean[:60])
                ph, arg = parse_command(clean[1:].strip())
                if ph:
                    return await self.command(ev, ph, arg)
                return
            if self.s["mode"] and not getattr(ev, "media", None):
                await self.apply_mode(ev, text)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.last_error = f"{type(e).__name__}: {e}"[:200]
            log(self.uid, "out error", repr(e))
            traceback.print_exc()

    async def apply_mode(self, ev, text):
        mode = self.s["mode"]
        if mode == "تدریجی":
            words = text.split(" ")
            steps = min(5, len(words)) if len(words) > 1 else min(5, len(text))
            if len(words) > 1:
                parts = [" ".join(words[:max(1, round(len(words) * (i + 1) / steps))]) for i in range(steps)]
            else:
                parts = [text[:max(1, round(len(text) * (i + 1) / steps))] for i in range(steps)]
            for p in parts:
                self.mark(p)
                await ev.edit(p)
                await asyncio.sleep(0.35)
            return
        tags = MODES.get(mode)
        if not tags:
            return
        out = f"{tags[0]}{html.escape(text)}{tags[1]}"
        self.mark(text)
        await ev.edit(out, parse_mode="html")

    # ---------- دستورها ----------
    async def command(self, ev, ph, arg):
        s = self.s
        if ph == "ping":
            return await self.say(ev, f"✅ سلف فعاله | ساعت {fmt_time(1)}")
        if ph == "c":
            try:
                v = calc(arg)
                return await self.say(ev, f"🧮 {arg} = {v}", delete_after=0)
            except ZeroDivisionError:
                return await self.say(ev, "❌ تقسیم بر صفر")
            except Exception:
                return await self.say(ev, "❌ عبارت نامعتبر. مثال: .c 50+25")

        if ph in ("ساعت نام", "ساعت بیو"):
            v = on_off(arg)
            key = "clock_name" if ph == "ساعت نام" else "clock_bio"
            if v is None:
                return await self.say(ev, f"❌ بنویس: .{ph} روشن  یا  .{ph} خاموش")
            if v:
                await self.snapshot_profile()
                s[key] = True
                self.save()
                await self.apply_clock()
                return await self.say(ev, f"✅ {ph} روشن شد")
            s[key] = False
            self.save()
            kw = {}
            if key == "clock_name" and s["orig_first"] is not None:
                kw = {"first_name": s["orig_first"], "last_name": s["orig_last"] or ""}
            if key == "clock_bio" and s["orig_bio"] is not None:
                kw = {"about": s["orig_bio"]}
            if kw:
                await self.client(UpdateProfileRequest(**kw))
            if not s["clock_name"] and not s["clock_bio"]:
                s["orig_first"] = s["orig_last"] = s["orig_bio"] = None
                self.save()
            return await self.say(ev, f"✅ {ph} خاموش شد")

        if ph == "فونت":
            if arg.isdigit() and 1 <= int(arg) <= len(FONTS):
                s["font"] = int(arg)
                self.save()
                if s["clock_name"] or s["clock_bio"]:
                    await self.apply_clock()
                return await self.say(ev, f"✅ فونت {arg}: {fmt_time(int(arg))}")
            lines = [f"فونت {i}  |  {fmt_time(i)}" + ("  ✅" if i == s["font"] else "")
                     for i in range(1, len(FONTS) + 1)]
            return await self.say(ev, "🔤 فونت‌ها (با .فونت شماره انتخاب کن):\n" + "\n".join(lines), 15)

        if ph == "بیو":
            s["bio_text"] = arg
            self.save()
            if s["clock_bio"]:
                await self.apply_clock()
            if arg:
                return await self.say(ev, "✅ متن بیو ثبت شد" + ("" if "00:00" in arg else
                                                              "\n(برای نمایش ساعت، 00:00 رو توی متن بنویس)"))
            return await self.say(ev, "✅ متن بیو پاک شد؛ فقط ساعت نوشته می‌شه")

        if ph == "حالت":
            toks = norm(arg).split()
            if not toks or toks[-1] in ("خاموش", "off"):      # .حالت خاموش  یا  .حالت بولد خاموش
                s["mode"] = None
                self.save()
                return await self.say(ev, "✅ حالت متن خاموش شد")
            if toks[-1] in ("روشن", "on") and len(toks) > 1:   # .حالت بولد روشن
                toks = toks[:-1]
            key = MODES_N.get(" ".join(toks))
            if key:
                s["mode"] = key
                self.save()
                return await self.say(ev, f"✅ حالت متن: {key}")
            return await self.say(ev, "❌ حالت‌ها: " + "، ".join(sorted(set(MODES))) + "، خاموش", 8)

        if ph in ("ذخیره", "کپی"):
            r = await ev.get_reply_message()
            if not r:
                return await self.say(ev, "❌ روی پیام مورد نظر ریپلای کن")
            return await self.save_msg(ev, r, ph)

        if ph == "پاسخ خودکار":
            v = on_off(arg)
            if v is None:
                return await self.say(ev, "❌ بنویس: .پاسخ خودکار روشن  یا  خاموش")
            s["auto_on"] = v
            self.save()
            return await self.say(ev, "✅ پاسخ خودکار " + ("روشن شد" if v else "خاموش شد"))
        if ph == "افزودن پاسخ":
            if "=" not in arg:
                return await self.say(ev, "❌ فرمت: .افزودن پاسخ کلمه = متن")
            k, v = [x.strip() for x in arg.split("=", 1)]
            if not k or not v:
                return await self.say(ev, "❌ فرمت: .افزودن پاسخ کلمه = متن")
            s["replies"][k] = v
            self.save()
            return await self.say(ev, f"✅ ثبت شد: «{k}» ← «{v}»")
        if ph == "حذف پاسخ":
            if s["replies"].pop(arg, None) is None:
                return await self.say(ev, "❌ چنین کلمه‌ای نیست")
            self.save()
            return await self.say(ev, "✅ حذف شد")
        if ph == "نوع تطبیق":
            if arg in ("برابر", "شامل"):
                s["match"] = "equal" if arg == "برابر" else "contain"
                self.save()
                return await self.say(ev, f"✅ نوع تطبیق: {arg}")
            return await self.say(ev, "❌ بنویس: .نوع تطبیق برابر  یا  شامل")
        if ph == "لیست پاسخ":
            if not s["replies"]:
                return await self.say(ev, "لیست خالیه")
            body = "\n".join(f"• {k} ← {v}" for k, v in s["replies"].items())
            return await self.say(ev, f"💬 پاسخ‌ها ({'برابر' if s['match'] == 'equal' else 'شامل'}):\n{body}", 15)
        if ph == "پاکسازی لیست":
            s["replies"] = {}
            self.save()
            return await self.say(ev, "✅ لیست پاک شد")

        if ph == "نگهبان":
            v = on_off(arg)
            if v is None:
                return await self.say(ev, "🛡 نگهبان: " + ("روشن ✅" if s["guard"] else "خاموش ❌") +
                                      "\n.نگهبان روشن  |  .نگهبان خاموش\nفقط توی پیوی کار می‌کنه (نه گروه)."
                                      "\nپیامی که کسی حذف کنه با متن اولیه‌ش (قبل از هر ویرایشی) توی Saved Messages میاد."
                                      "\nویرایش هم گزارش می‌شه، مگر اینکه فرستنده خودش الان سلف فعال داشته باشه.", 14)
            s["guard"] = v
            if not v:
                self.cache.clear()
            self.save()
            return await self.say(ev, "✅ نگهبان " + ("روشن شد (از الان پیام‌های دریافتی ثبت می‌شن)" if v else "خاموش شد"))

        for kind, nm in REL.items():
            if ph == nm:
                return await self.rel_add(ev, kind, arg)
            if ph == f"{nm} بنر":
                return await self.rel_banner(ev, kind, arg)
            if ph == f"{nm} تک کلمه":
                return await self.rel_words(ev, kind, arg)
            if ph == f"حذف {nm}":
                return await self.rel_remove(ev, kind, arg)
            if ph == f"لیست {nm}":
                return await self.rel_list(ev, kind)
            if ph == f"پاکسازی {nm}":
                s[kind] = {"ids": {}, "banner": "", "words": []}
                self.save()
                return await self.say(ev, f"✅ همه‌ی تنظیمات {nm} پاک شد")

        if ph == "ترجمه":
            return await self.translate(ev, arg)

        if ph == "تاس":
            if arg.isdigit() and 1 <= int(arg) <= 6:
                return await self.rig(ev, "🎲", {int(arg)})
            return await self.say(ev, "❌ مثال: .تاس 6")
        if ph in GAMES:
            e, ok = GAMES[ph]
            return await self.rig(ev, e, ok)

        if ph in ("اسپم", "اسپم سریع", "اسپم آرام"):
            return await self.spam(ev, ph, arg)
        if ph == "پایان اسپم":
            if self.spam_task and not self.spam_task.done():
                self.spam_task.cancel()
                return await self.say(ev, "✅ اسپم متوقف شد")
            return await self.say(ev, "اسپمی در حال اجرا نیست")

        if ph == "منشی":
            v = on_off(arg)
            if v is None:
                return await self.say(ev, "❌ بنویس: .منشی روشن  یا  خاموش")
            s["sec_on"] = v
            self.save()
            return await self.say(ev, "✅ منشی " + ("روشن شد" if v else "خاموش شد"))
        if ph == "متن منشی":
            if not arg:
                return await self.say(ev, "❌ مثال: .متن منشی مشغولم، بعداً جواب می‌دم")
            s["sec_text"] = arg
            self.save()
            return await self.say(ev, "✅ متن منشی ثبت شد")
        if ph == "تایم منشی":
            if arg.isdigit() and 1 <= int(arg) <= 86400:
                s["sec_time"] = int(arg)
                self.save()
                return await self.say(ev, f"✅ تایم منشی: {arg} ثانیه")
            return await self.say(ev, "❌ مثال: .تایم منشی 60 (ثانیه)")

    # ---------- نگهبان (ضد حذف / ضد ویرایش) ----------
    async def remember(self, ev):
        try:
            if ev.id is None:
                return
            try:
                sender = await asyncio.wait_for(ev.get_sender(), 6)
            except Exception:
                sender = None
            try:
                chat = await asyncio.wait_for(ev.get_chat(), 6)
            except Exception:
                chat = None
            msg = getattr(ev, "message", ev)
            raw = ev.raw_text or ""
            self.cache[(ev.chat_id, ev.id)] = {
                "text": raw, "orig_text": raw,      # orig_text هیچ‌وقت عوض نمی‌شه؛ برای گزارش حذف همیشه از همین استفاده می‌شه
                "label": media_label(msg), "media": getattr(msg, "media", None),
                "sender": person(sender), "sender_id": getattr(ev, "sender_id", None),
                "chat": person(chat) if chat is not None else str(ev.chat_id),
                "time": fmt_dt(getattr(ev, "date", None)),
            }
            while len(self.cache) > CACHE_MAX:
                self.cache.popitem(last=False)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log(self.uid, "remember error", repr(e))

    def mark_seen(self, chat_id, max_id=None):
        """پیام‌هایی که کاربر دیده (توی اون چت چیزی فرستاده یا چت رو خونده) از نگهبان کنار می‌رن"""
        if chat_id is None:
            return
        for k in [k for k in self.cache if k[0] == chat_id and (max_id is None or k[1] <= max_id)]:
            self.cache.pop(k, None)

    async def on_read(self, ev):
        try:
            if getattr(ev, "inbox", True) is False:      # خوندنِ طرف مقابل نیست
                return
            mx = getattr(ev, "max_id", None)
            cid = getattr(ev, "chat_id", None)
            if cid is not None and mx is not None:
                self.mark_seen(cid, mx)
        except Exception:
            pass

    def _find(self, chat_id, mid):
        if chat_id is not None and (chat_id, mid) in self.cache:
            return (chat_id, mid)
        for k in reversed(self.cache):          # حذف توی پیوی ممکنه آیدی چت نداشته باشه
            if k[1] == mid and (chat_id is None or k[0] == chat_id):
                return k
        return None

    def _body(self, text, label):
        t = (f"[{label}]" if label else "") + ((" " if label and text else "") + text if text else "")
        return (t or "[بدون متن]")[:3000]

    async def guard_send(self, kind, e, new_text=None):
        head = "☒ ضد دیلیت / ضد ویرایش | " + ("پیام حذف شد" if kind == "del" else "پیام ویرایش شد")
        lines = [head, "", f"چت: {e['chat']}", f"فرستنده: {e['sender']}", f"زمان ثبت: {e['time']}",
                 f"زمان {'حذف' if kind == 'del' else 'ویرایش'}: {fmt_dt()}", ""]
        if kind == "del":
            lines += ["متن:", self._body(e.get("orig_text", e["text"]), e["label"])]
        else:
            lines += ["متن قبلی:", self._body(e["text"], e["label"]), "", "متن جدید:", self._body(new_text or "", e["label"])]
        report = "\n".join(lines)
        self.mark(report)
        await self.client.send_message("me", report)
        if kind == "del" and e.get("media") is not None:
            try:
                await self.client.send_file("me", e["media"])
            except Exception as ex:
                if DEBUG:
                    log(self.uid, "guard media resend failed", repr(ex))

    async def on_del(self, ev):
        try:
            if not self.s["guard"]:
                return
            ids = list(getattr(ev, "deleted_ids", None) or [])
            if not ids and getattr(ev, "deleted_id", None) is not None:
                ids = [ev.deleted_id]
            for mid in ids:
                k = self._find(getattr(ev, "chat_id", None), mid)
                if not k:
                    continue
                e = self.cache.pop(k)
                await self.guard_send("del", e)
                await asyncio.sleep(0.3)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.last_error = f"نگهبان: {type(e).__name__}: {e}"[:200]
            log(self.uid, "guard del error", repr(e))

    async def on_edit(self, ev):
        try:
            if not self.s["guard"] or getattr(ev, "out", False):
                return
            k = (ev.chat_id, ev.id)
            e = self.cache.get(k)
            new = ev.raw_text or ""
            if e is None:                  # قبلاً ندیدیمش؛ حداقل از این به بعد ثبت می‌شه
                await self.remember(ev)
                return
            if e["text"] == new or e.get("edited"):      # هر پیام فقط برای اولین ویرایشش گزارش می‌شه
                return
            sid = e.get("sender_id")
            # اگه فرستنده خودش الان سلف فعال داره، به این طرف خبر ادیت‌شدن داده نمی‌شه (فقط افراد بدون سلف گزارش می‌شن)
            if sid is None or str(sid) not in ACTIVE_SELF_UIDS:
                await self.guard_send("edit", e, new)
            e["edited"] = True
            e["text"] = new
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.last_error = f"نگهبان: {type(e).__name__}: {e}"[:200]
            log(self.uid, "guard edit error", repr(e))

    # ---------- دشمن / دوست ----------
    REL_ICON = {"enemy": "⚔️", "friend": "💚"}

    def rel_guide(self, kind):
        nm, d = REL[kind], self.s[kind]
        cur = ("\n\nوضعیت فعلی:\n"
               f"• تعداد {nm}: {len(d['ids'])}\n"
               f"• بنر: {d['banner'][:80] + ('…' if len(d['banner']) > 80 else '') if d['banner'] else 'ثبت نشده'}\n"
               f"• تک‌کلمه‌ها: {('، '.join(d['words'])[:120]) if d['words'] else 'ثبت نشده'}")
        return (f"{self.REL_ICON[kind]} {nm}\n"
                f"• روی پیام فرد ریپلای کن و بنویس: .{nm}\n   (یا  .{nm} @یوزرنیم  /  .{nm} آیدی_عددی)\n"
                f"• .{nm} بنر متن بلند دلخواه ← هر وقت پیام بده، این متن بهش ریپلای می‌شه (حداکثر هر {BANNER_CD} ثانیه)\n"
                f"• .{nm} تک کلمه کلمه۱ ، کلمه۲ ، کلمه۳ ← به هر پیامش یکی از این کلمه‌ها (تصادفی) ریپلای می‌شه\n"
                f"• برای پاک کردن بنر یا کلمه‌ها: .{nm} بنر پاک  |  .{nm} تک کلمه پاک\n"
                f"• .حذف {nm} (ریپلای یا آیدی)  |  .لیست {nm}  |  .پاکسازی {nm}" + cur)

    async def resolve_target(self, ev, arg):
        r = await ev.get_reply_message()
        if r is not None and getattr(r, "sender_id", None):
            try:
                snd = await r.get_sender()
            except Exception:
                snd = None
            return str(r.sender_id), person(snd)
        a = (arg or "").strip()
        if not a:
            return None
        if a.lstrip("-").isdigit():
            return a, a
        try:
            e = await self.client.get_entity(a)
            return str(e.id), person(e)
        except Exception:
            return None

    async def rel_add(self, ev, kind, arg):
        nm, other = REL[kind], ("friend" if kind == "enemy" else "enemy")
        t = await self.resolve_target(ev, arg)
        if not t:
            return await self.say(ev, self.rel_guide(kind), 25)
        tid, label = t
        if self.me is not None and tid == str(getattr(self.me, "id", "")):
            return await self.say(ev, "❌ خودت رو نمی‌تونی اضافه کنی")
        self.s[other]["ids"].pop(tid, None)
        self.s[kind]["ids"][tid] = label
        self.save()
        extra = "" if (self.s[kind]["banner"] or self.s[kind]["words"]) else \
            f"\n⚠️ هنوز بنر یا تک‌کلمه‌ای تنظیم نکردی:\n.{nm} بنر متن  |  .{nm} تک کلمه کلمه۱ ، کلمه۲"
        return await self.say(ev, f"{self.REL_ICON[kind]} {label} شد {nm}{extra}", 8)

    async def rel_banner(self, ev, kind, arg):
        nm = REL[kind]
        if not arg:
            return await self.say(ev, self.rel_guide(kind), 25)
        if arg.strip() in ("پاک", "حذف"):
            self.s[kind]["banner"] = ""
            self.save()
            return await self.say(ev, f"✅ بنر {nm} پاک شد")
        self.s[kind]["banner"] = arg.strip()[:3500]
        self.save()
        return await self.say(ev, f"✅ بنر {nm} ثبت شد (هر وقت یکی از {nm}ها پیام بده، این متن ریپلای می‌شه)")

    async def rel_words(self, ev, kind, arg):
        nm = REL[kind]
        if not arg:
            return await self.say(ev, self.rel_guide(kind), 25)
        if arg.strip() in ("پاک", "حذف"):
            self.s[kind]["words"] = []
            self.save()
            return await self.say(ev, f"✅ تک‌کلمه‌های {nm} پاک شد")
        words = []
        for w in re.split(r"[\n،,|]+", arg):
            w = w.strip()[:100]
            if w and w not in words:
                words.append(w)
        if not words:
            return await self.say(ev, self.rel_guide(kind), 25)
        self.s[kind]["words"] = words[:30]
        self.save()
        return await self.say(ev, f"✅ {len(words[:30])} کلمه برای {nm} ثبت شد: " + "، ".join(words[:30]), 10)

    async def rel_remove(self, ev, kind, arg):
        nm = REL[kind]
        t = await self.resolve_target(ev, arg)
        if not t:
            return await self.say(ev, f"❌ روی پیامش ریپلای کن یا بنویس: .حذف {nm} آیدی_عددی")
        if self.s[kind]["ids"].pop(t[0], None) is None:
            return await self.say(ev, f"❌ این فرد توی لیست {nm} نیست")
        self.save()
        return await self.say(ev, f"✅ از لیست {nm} حذف شد")

    async def rel_list(self, ev, kind):
        nm, ids = REL[kind], self.s[kind]["ids"]
        if not ids:
            return await self.say(ev, f"لیست {nm} خالیه\n\n" + self.rel_guide(kind), 25)
        body = "\n".join(f"• {lb}  [{i}]" for i, lb in ids.items())
        return await self.say(ev, f"{self.REL_ICON[kind]} لیست {nm}:\n{body}", 20)

    async def relation_reply(self, ev):
        sid = str(getattr(ev, "sender_id", "") or "")
        if not sid:
            return False
        now = time.time()
        for kind in REL:
            d = self.s[kind]
            if sid not in d["ids"]:
                continue
            banners = (ENEMY_BANNERS if kind == "enemy" else FRIEND_BANNERS)
            words = (ENEMY_WORDS if kind == "enemy" else FRIEND_WORDS)
            banner_pool = ([d["banner"]] if d["banner"] else []) + banners
            word_pool = list(d["words"]) + words
            reply = None
            if banner_pool and now - self.cool.get((kind, "b", sid), 0) > BANNER_CD:
                self.cool[(kind, "b", sid)] = now
                reply = random.choice(banner_pool)
            elif word_pool and now - self.cool.get((kind, "w", sid), 0) > WORD_CD:
                self.cool[(kind, "w", sid)] = now
                reply = random.choice(word_pool)
            if reply:
                self.mark(reply)
                await ev.reply(reply)
                return True
            return False
        return False

    # ---------- ذخیره / کپی ----------
    async def save_msg(self, ev, r, ph):
        txt = r.raw_text or ""
        media = getattr(r, "media", None)
        if media is not None and getattr(media, "ttl_seconds", None):
            return await self.say(ev, "❌ رسانه‌ی زمان‌دار ذخیره نمی‌شه")
        if ph == "کپی" or media is None:
            if not txt:
                return await self.say(ev, "❌ این پیام متن نداره")
            await self.client.send_message("me", txt)
            return await self.say(ev, "✅ متن توی Saved Messages ذخیره شد")
        try:
            await self.client.forward_messages("me", r)
        except Exception:
            data = await r.download_media(bytes)
            await self.client.send_file("me", data, caption=txt or None)
        return await self.say(ev, "✅ توی Saved Messages ذخیره شد")

    # ---------- ترجمه ----------
    async def translate(self, ev, arg):
        parts = arg.split(None, 1)
        if not parts:
            return await self.say(ev, "❌ مثال: .ترجمه انگلیسی  (روی پیام ریپلای کن یا متن بنویس)")
        lang = LANGS.get(parts[0], parts[0] if re.fullmatch(r"[A-Za-z\-]{2,7}", parts[0]) else None)
        if not lang:
            return await self.say(ev, "❌ زبان نامعتبر. مثال: انگلیسی، روسی، عربی، ... یا کد مثل en", 8)
        text = parts[1] if len(parts) > 1 else ""
        if not text:
            r = await ev.get_reply_message()
            text = (r.raw_text if r else "") or ""
        if not text:
            return await self.say(ev, "❌ متنی برای ترجمه نیست")
        try:
            from deep_translator import GoogleTranslator
            out = await asyncio.get_running_loop().run_in_executor(
                None, lambda: GoogleTranslator(source="auto", target=lang).translate(text))
        except Exception as e:
            log(self.uid, "translate error", e)
            return await self.say(ev, "❌ ترجمه انجام نشد")
        return await self.say(ev, f"🌐 {out}", delete_after=0)

    # ---------- شیطنت ----------
    async def rig(self, ev, emoji, ok):
        if InputMediaDice is None:
            return await self.say(ev, "❌ این نسخه‌ی کتابخونه از تاس پشتیبانی نمی‌کنه")
        chat = ev.chat_id
        try:
            await ev.delete()
        except Exception:
            pass
        for _ in range(30):
            m = await self.client.send_file(chat, InputMediaDice(emoji))
            val = getattr(getattr(m, "media", None), "value", None)
            if val in ok:
                return
            try:
                await m.delete()
            except Exception:
                pass
            await asyncio.sleep(0.4)

    # ---------- اسپم (پیوی، گروه، کانال) ----------
    async def spam(self, ev, ph, arg):
        m = re.match(r"(\d+)\s+(.+)", arg, re.S)
        if not m:
            return await self.say(ev, f"❌ مثال: .{ph} 10 سلام")
        n, text = min(int(m.group(1)), MAX_SPAM), m.group(2)
        delay = {"اسپم": 1.0, "اسپم سریع": 0.4, "اسپم آرام": 3.0}[ph]
        if self.spam_task and not self.spam_task.done():
            self.spam_task.cancel()
        chat = ev.chat_id
        try:
            await ev.delete()
        except Exception:
            pass

        async def run():
            self.mark(text)
            for _ in range(n):
                await self.client.send_message(chat, text)
                await asyncio.sleep(delay)

        async def guard():
            try:
                await run()
            except asyncio.CancelledError:
                pass
            except Exception as e:
                log(self.uid, "spam error", e)

        self.spam_task = asyncio.create_task(guard())

    # ---------- پیام‌های دریافتی ----------
    async def on_in(self, ev):
        try:
            s = self.s
            rel = bool(s["enemy"]["ids"] or s["friend"]["ids"])
            if not (s["auto_on"] or s["sec_on"] or rel):
                return
            text = (ev.raw_text or "").strip()
            try:
                sender = await ev.get_sender()
                if getattr(sender, "bot", False):
                    return
            except Exception:
                pass
            if rel and not (getattr(ev, "is_channel", False) and not getattr(ev, "is_group", False)):
                if await self.relation_reply(ev):
                    return
            now = time.time()
            chat = ev.chat_id
            if s["auto_on"] and text and s["replies"]:
                for k, v in s["replies"].items():
                    hit = (text == k) if s["match"] == "equal" else (k in text)
                    if hit and now - self.cool.get(("a", chat), 0) > 2:
                        self.cool[("a", chat)] = now
                        self.mark(v)
                        await ev.reply(v)
                        return
            if s["sec_on"] and getattr(ev, "is_private", False):
                sid = getattr(ev, "sender_id", chat)
                if now - self.cool.get(("s", sid), 0) > s["sec_time"]:
                    self.cool[("s", sid)] = now
                    self.mark(s["sec_text"])
                    await ev.reply(s["sec_text"])
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log(self.uid, "in error", repr(e))
            traceback.print_exc()


# ================== مدیریت کاربران ==================
def read_queue():
    """آخرین زمان پایان هر کاربر: None = همیشگی.
    ابتدا صف (runner_queue.jsonl) خونده می‌شه، بعد db.json ربات فروشگاه که معتبرتره و روش اولویت داره
    (اگه صف قدیمی یا پاک‌شده باشه هم درست کار می‌کنه)."""
    res = {}
    try:
        with open(QUEUE_FILE, encoding="utf-8") as f:
            for line in f:
                try:
                    it = json.loads(line)
                    res[str(it["user_id"])] = it.get("until")
                except Exception:
                    pass
    except FileNotFoundError:
        pass
    users = load_json("db.json", {}).get("users", {})
    for uid, us in users.items():
        try:
            if us.get("exempt") == "forever":
                res[str(uid)] = None
            elif "self_until" in us:
                res[str(uid)] = us.get("self_until") or 0
        except Exception:
            continue
    return res


def notify(uid, text):
    if not NOTIFY_TOKEN:
        return
    try:
        requests.post(f"{NOTIFY_BASE}{NOTIFY_TOKEN}/sendMessage",
                      data={"chat_id": uid, "text": text}, timeout=15)
    except Exception as e:
        log("notify error", e)


class Manager:
    def __init__(self):
        self.users = {}
        self.bad = set()
        self.bad_sess = {}
        self.notified = set()

    async def sync(self):
        sessions = load_json(SESSIONS_FILE, {})
        queue = read_queue()
        now = time.time()
        snap = (sorted((k, v is None or v > now) for k, v in queue.items()), sorted(sessions), sorted(self.users), sorted(self.bad))
        if snap != getattr(self, "_snap", None):
            self._snap = snap
            log("status: فعال-در-صف=", [k for k, v in queue.items() if v is None or v > now],
                "سشن-دارند=", list(sessions), "در-حال-اجرا=", list(self.users), "ناموفق=", list(self.bad))
        for uid, until in queue.items():
            active = until is None or until > now
            sess = sessions.get(uid, {}).get("session")
            if active and sess and uid not in self.users and uid not in self.bad:
                u = UserSelf(uid, sess)
                try:
                    await asyncio.wait_for(u.start(), 45)   # یه اکانت گیرکرده بقیه رو معطل نکنه
                    self.users[uid] = u
                    if u.s.get("welcomed") != sess[-16:]:      # برای هر ورود فقط یک بار (نه با هر ری‌استارت)
                        u.s["welcomed"] = sess[-16:]
                        u.save()
                        notify(uid, entry_message())
                except RuntimeError as e:          # سشن نامعتبر
                    log(uid, "start failed:", e)
                    self.bad.add(uid)
                    self.bad_sess[uid] = sess
                    notify(uid, "❌ ورود به اکانت شما ناموفق بود. دوباره وارد اکانت بشید (/login).")
                except Exception as e:             # مشکل شبکه: ۱۰ ثانیه بعد دوباره تلاش می‌شه
                    log(uid, "start failed (retry):", repr(e))
                    try:
                        await u.drop()
                    except Exception:
                        pass
            elif not active and uid in self.users:
                await self.users.pop(uid).stop()
                notify(uid, "⏰ زمان سلف شما تموم شد. برای تمدید از «فعالسازی سلف» استفاده کنید.")
        # اگه مشتری دوباره وارد شده (سشن جدید)، اتصال قدیمی رو ببند تا با سشن جدید بالا بیاد
        for uid, u in list(self.users.items()):
            new = sessions.get(uid, {}).get("session")
            if new and new != u.sess:
                log(uid, "new session detected, restarting")
                await u.drop()
                self.users.pop(uid, None)
        # بررسی سلامت اتصال (هر ۶۰ ثانیه): اگه قطع بود وصل مجدد، اگه نشد دوباره بالا آوردن
        for uid, u in list(self.users.items()):
            if now - u.last_check < 60:
                continue
            u.last_check = now
            if await u.healthy():
                u.fails = 0
                continue
            u.fails += 1
            log(uid, "connection unhealthy, fails =", u.fails)
            if u.fails >= 3:
                await u.drop()
                self.users.pop(uid, None)     # sync بعدی دوباره بالاش میاره
            else:
                try:
                    await u.client.connect()
                except Exception as e:
                    log(uid, "reconnect error", repr(e))
        global ACTIVE_SELF_UIDS
        ACTIVE_SELF_UIDS = set(self.users.keys())
        self.write_status()
        # سشن جدید بعد از ورود دوباره
        for uid in list(self.bad):
            if sessions.get(uid, {}).get("session") != self.bad_sess.get(uid):
                self.bad.discard(uid)
                self.bad_sess.pop(uid, None)

    def write_status(self):
        st = {uid: {"running": True, "last_event": u.last_event, "last_error": u.last_error}
              for uid, u in self.users.items()}
        for uid in self.bad:
            st.setdefault(uid, {"running": False, "last_error": "ورود به اکانت ناموفق بود"})
        try:
            with open("runner_status.json", "w", encoding="utf-8") as f:
                json.dump(st, f, ensure_ascii=False)
        except Exception:
            pass

    async def run(self):
        log("self runner started")
        while True:
            try:
                await self.sync()
            except Exception as e:
                log("sync error", repr(e))
            await asyncio.sleep(10)


def acquire_lock():
    """اگه یه نسخه‌ی دیگه از رانر هنوز زنده‌ست، خارج شو (دو اتصال با یک سشن، اتصال‌ها رو می‌اندازه)"""
    try:
        with open("runner.lock") as f:
            pid = int(f.read().strip())
        if pid != os.getpid():
            os.kill(pid, 0)
            print(f"❌ یه رانر دیگه (pid {pid}) هنوز اجراست. اول اون رو ببند:  pkill -f self_runner.py")
            sys.exit(1)
    except (FileNotFoundError, ValueError, ProcessLookupError, PermissionError):
        pass
    with open("runner.lock", "w") as f:
        f.write(str(os.getpid()))


if __name__ == "__main__":
    acquire_lock()
    asyncio.run(Manager().run())
