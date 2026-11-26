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

try:
    from splusthon.tl import types as tl_types
except Exception:
    tl_types = None

try:
    from splusthon.tl.functions.account import UpdateStatusRequest
except Exception:
    UpdateStatusRequest = None

try:
    from splusthon.tl.functions.messages import SetTypingRequest
    from splusthon.tl.types import SendMessageTypingAction
except Exception:
    SetTypingRequest = SendMessageTypingAction = None

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
            "سیو", "پخش", "تبچی", "پایان پخش", "خودتخریب", "پایان خودتخریب", "خروج",
            "آنلاین", "تایپ", "راهنما",
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
    "online": False, "typing_pv": False,   # همیشه آنلاین / همیشه در حال تایپ توی پیوی‌های اخیر
    "reply_ents": {}, "sec_ents": [],     # قالب (بولد/نقل‌قول…) متن‌های پاسخ خودکار و منشی
    "enemy": {"ids": {}, "reply_mode": ""},   # reply_mode: "" (خاموش) | "banner" | "word"
    "friend": {"ids": {}, "reply_mode": ""},
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
    # "متن بنر دلخواهت",
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

BANNER_CD = 40      # حداقل فاصله‌ی ارسال بنر برای هر دشمن/دوست (ثانیه)
WORD_CD = 3         # حداقل فاصله‌ی ارسال تک‌کلمه
CACHE_MAX = 100     # آخرین پیام‌های دیده‌نشده‌ی دریافتی که نگهبان نگه می‌داره
REL = {"enemy": "دشمن", "friend": "دوست"}
ENTRY_DEFAULT = "✅ سلف شما فعال شد."
ENTRY_FOOTER = "\n\n📖 برای استفاده از سلف، بخش «راهنما» را چک کنید. روی دکمه‌ی زیر بزنید 👇"
ENTRY_HELP_BUTTON = True     # دکمه‌ی «راهنما» زیر پیام ورود (ربات فروشگاه باید callback «help» رو جواب بده)
HELP_BUTTONS = [[{"text": "📖 راهنما", "callback_data": "help"}]]
HELP_FILE = "help_text.txt"  # متن راهنما؛ هم دکمه‌ی شیشه‌ای ربات و هم دستور .راهنما از همین فایل می‌خونن
HELP_DEFAULT = ("📖 راهنمای سلف\n\n"
                "• .ping — بررسی فعال بودن سلف\n"
                "• (دستور بعدی رو اینجا اضافه کن — مثال: .دستور — توضیح)\n"
                "• (...)")


def help_text():
    """هر بار از فایل خونده می‌شه؛ با ادیت فایل نیازی به ری‌استارت نیست"""
    try:
        with open(HELP_FILE, encoding="utf-8") as f:
            t = f.read().strip()
        return t or HELP_DEFAULT
    except Exception:
        return HELP_DEFAULT


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


# ---------------- تنظیمات دستورهای جدید ----------------
SD_MAX_SECONDS = 365 * 86400   # بیشترین مدت خودتخریب
SD_MAX_PENDING = 5000          # بیشترین تعداد پیامِ در انتظار حذف برای هر کاربر
BC_MAX = 300                   # سقف تعداد ارسال در هر بار اجرای «پخش»
BC_DELAY_MIN, BC_DELAY_MAX = 2.0, 4.0   # فاصله‌ی تصادفی بین هر ارسال (ثانیه)
BC_MAX_FLOOD = 300             # اگه سرور بیشتر از این ثانیه صبر خواست، پخش متوقف می‌شه
LEAVE_DEFAULT_HOURS = 10       # پیش‌فرض «خروج»: گروه/کانال بی‌پیام‌تر از این ساعت
LEAVE_MAX = 100                # سقف تعداد خروج در هر بار اجرا
LEAVE_DELAY_MIN, LEAVE_DELAY_MAX = 1.0, 2.0
TYPING_PV_MAX = 8              # «تایپ پیوی»: روی چند پیوی اخیر (بیشتر = ریسک محدودیت سرور)
BIO_LIMIT = 70                 # حداکثر طول بیو

_FA2EN = str.maketrans("۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩", "01234567890123456789")
DUR_UNITS = {"ثانیه": 1, "ثانيه": 1, "s": 1, "sec": 1, "دقیقه": 60, "دقيقه": 60, "m": 60, "min": 60,
             "ساعت": 3600, "h": 3600, "روز": 86400, "d": 86400, "هفته": 604800, "w": 604800}
BC_SCOPES = {"همه": "all", "پیوی": "pv", "خصوصی": "pv", "گروه": "group", "گروهها": "group"}
LEAVE_SCOPES = {"همه": "all", "گروه": "group", "گروهها": "group", "کانال": "channel",
                "کانالها": "channel", "چنل": "channel", "چنلها": "channel"}
# اولین الگوی ساعت‌مانند (۱۲:۳۰ / 00:00 / ۰۰:۰۰) یا {time} توی متن بیو جای ساعت زنده می‌شینه
TIME_RE = re.compile(r"[0-9۰-۹٠-٩]{1,2}\s*[:：]\s*[0-9۰-۹٠-٩]{2}|\{time\}|\{ساعت\}", re.I)


def build_bio(tpl, t, limit=BIO_LIMIT):
    """متن بیو + ساعت. اگه توی متن جای ساعت (00:00 یا {time}) باشه همون‌جا، وگرنه آخر متن اضافه می‌شه.
    اگه طول از حد بیشتر شد، از خودِ متن کم می‌شه نه از ساعت."""
    tpl = (tpl or "").strip()
    if not tpl:
        return t[:limit]
    m = TIME_RE.search(tpl)
    if m:
        parts = [tpl[:m.start()], tpl[m.end():]]
        over = len(parts[0]) + len(t) + len(parts[1]) - limit
        if over > 0:
            i = 0 if len(parts[0]) >= len(parts[1]) else 1
            parts[i] = parts[i][:max(0, len(parts[i]) - over)]
        return parts[0] + t + parts[1]
    base = tpl[:max(0, limit - len(t) - 1)].rstrip()
    return f"{base} {t}".strip()


def parse_duration(arg):
    """'1 روز متن' ، '30 ثانیه' ، '10 سلام' (بدون واحد = ثانیه) ← (ثانیه، باقی‌متن) یا None.
    متنِ برگشتی دقیقاً همون متن اصلیه (رقم‌ها و قالب‌ها دست نمی‌خورن)."""
    src = (arg or "").strip()
    m = re.match(r"(\d+)\s*", src.translate(_FA2EN))
    if not m:
        return None
    n = int(m.group(1))
    rest = src[m.end():].strip()
    tok, _, after = rest.partition(" ")
    unit = DUR_UNITS.get(norm(tok)) if tok else None
    if unit:
        return n * unit, after.strip()
    return n, rest


def human_dur(sec):
    for name, n in (("روز", 86400), ("ساعت", 3600), ("دقیقه", 60)):
        if sec >= n and sec % n == 0:
            return f"{sec // n} {name}"
    return f"{sec} ثانیه"


def u16(t):
    """طول متن بر حسب UTF-16 (آفست entity ها اینطوری حساب می‌شه)"""
    return len(t.encode("utf-16-le")) // 2


def sub_entities(ents, raw, text):
    """قالب‌های (بولد، نقل‌قول، لینک…) بخشِ «text» که آخر پیام «raw» اومده، با آفست جدید"""
    if not ents or not text:
        return []
    idx = raw.rfind(text)
    if idx < 0:
        return []
    base, n = u16(raw[:idx]), u16(text)
    out = []
    for e in ents:
        off, ln = getattr(e, "offset", None), getattr(e, "length", None)
        if off is None or ln is None or off < base or off >= base + n:
            continue
        e2 = copy.copy(e)
        e2.offset = off - base
        e2.length = min(ln, n - (off - base))
        out.append(e2)
    return out


def ents_to_json(ents):
    out = []
    for e in ents or []:
        try:
            out.append(e.to_dict())
        except Exception:
            pass
    return out


def ents_from_json(lst):
    out = []
    for d in lst or []:
        try:
            cls = getattr(tl_types, d["_"])
            out.append(cls(**{k: v for k, v in d.items() if k != "_"}))
        except Exception:
            pass
    return out


def can_post(ent):
    """آیا عضو عادی اجازه‌ی پیام دادن توی این گروه رو داره؟ (ادمین/مالک همیشه اره)"""
    if ent is None:
        return True
    if getattr(ent, "creator", False) or getattr(ent, "admin_rights", None):
        return True
    br = getattr(ent, "default_banned_rights", None)
    return not (br is not None and getattr(br, "send_messages", False))


def flood_seconds(e):
    if "FloodWait" in type(e).__name__ or "flood" in str(e).lower():
        try:
            return int(getattr(e, "seconds", 0) or 30)
        except Exception:
            return 30
    return 0



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
        self.bc_task = None       # تسک «پخش»
        self.typing_chats = set() # چت‌هایی که توشون «در حال تایپ» نشون داده می‌شه
        self._pv_cache, self._pv_at = [], 0
        self.sd_path = os.path.join(DATA_DIR, f"{uid}_sd.json")   # صف پیام‌های خودتخریب (ماندگار)
        _sd = load_json(self.sd_path, [])
        self.sd = [x for x in _sd if isinstance(x, list) and len(x) == 3] if isinstance(_sd, list) else []

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
        self.tasks.append(asyncio.create_task(self.sd_loop()))
        self.tasks.append(asyncio.create_task(self.presence_loop()))
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
        for t in self.tasks + [x for x in (self.spam_task, self.bc_task) if x]:
            t.cancel()
        try:
            await self.client.disconnect()
        except Exception:
            pass

    async def stop(self):
        for t in self.tasks + [x for x in (self.spam_task, self.bc_task) if x]:
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
            about = build_bio(self.s["bio_text"], t)
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
                hint = "" if TIME_RE.search(arg) else "\n(ساعت خودکار آخر متن اضافه می‌شه؛ برای جای دلخواه 00:00 بنویس)"
                return await self.say(ev, "✅ متن بیو ثبت شد\nنمونه: " + build_bio(arg, fmt_time(s["font"])) + hint, 8)
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

        if ph in ("ذخیره", "کپی", "سیو"):
            r = await ev.get_reply_message()
            if not r and arg:
                r, err = await self.msg_from_link(arg)
                if err:
                    return await self.say(ev, err, 8)
            if not r:
                return await self.say(ev, "❌ روی پیام ریپلای کن یا لینک پیام بده:\n.ذخیره <لینک پیام>")
            return await self.save_msg(ev, r, "کپی" if ph == "کپی" else "ذخیره")

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
            e_ = ents_to_json(self.cmd_ents(ev, v))
            if e_:
                s["reply_ents"][k] = e_
            else:
                s["reply_ents"].pop(k, None)
            self.save()
            return await self.say(ev, f"✅ ثبت شد: «{k}» ← «{v}»")
        if ph == "حذف پاسخ":
            if s["replies"].pop(arg, None) is None:
                return await self.say(ev, "❌ چنین کلمه‌ای نیست")
            s["reply_ents"].pop(arg, None)
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
            s["reply_ents"] = {}
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
                return await self.rel_mode(ev, kind, "banner", arg)
            if ph == f"{nm} تک کلمه":
                return await self.rel_mode(ev, kind, "word", arg)
            if ph == f"حذف {nm}":
                return await self.rel_remove(ev, kind, arg)
            if ph == f"لیست {nm}":
                return await self.rel_list(ev, kind)
            if ph == f"پاکسازی {nm}":
                s[kind] = {"ids": {}, "reply_mode": ""}
                self.save()
                return await self.say(ev, f"✅ همه‌ی تنظیمات {nm} پاک شد")

        if ph == "ترجمه":
            return await self.translate(ev, arg)

        if ph == "راهنما":
            return await self.say(ev, help_text(), 60)
        if ph == "آنلاین":
            v = on_off(arg)
            if v is None:
                return await self.say(ev, "🟢 آنلاین: " + ("روشن ✅" if s["online"] else "خاموش ❌") +
                                      "\n.آنلاین روشن  |  .آنلاین خاموش")
            if UpdateStatusRequest is None:
                return await self.say(ev, "❌ این نسخه‌ی کتابخونه از وضعیت آنلاین پشتیبانی نمی‌کنه")
            s["online"] = v
            self.save()
            if v:
                try:
                    await self.client(UpdateStatusRequest(offline=False))
                except Exception:
                    pass
            return await self.say(ev, "✅ همیشه‌آنلاین " + ("روشن شد" if v else "خاموش شد"))
        if ph == "تایپ":
            toks = norm(arg).split()
            v = on_off(toks[-1]) if toks else None
            if v is None:
                return await self.say(ev, "⌨️ در حال تایپ:\n.تایپ روشن / خاموش ← همین چت\n"
                                          ".تایپ پیوی روشن / خاموش ← " + str(TYPING_PV_MAX) + " پیوی اخیر\n"
                                          "وضعیت پیوی: " + ("روشن ✅" if s["typing_pv"] else "خاموش ❌"), 10)
            if SetTypingRequest is None:
                return await self.say(ev, "❌ این نسخه‌ی کتابخونه از وضعیت تایپ پشتیبانی نمی‌کنه")
            if len(toks) > 1 and toks[0] in ("پیوی", "همه"):
                s["typing_pv"] = v
                self.save()
                return await self.say(ev, "✅ تایپِ پیوی " + ("روشن شد" if v else "خاموش شد"))
            if v:
                self.typing_chats.add(ev.chat_id)
            else:
                self.typing_chats.discard(ev.chat_id)
            return await self.say(ev, "✅ تایپ توی این چت " + ("روشن شد" if v else "خاموش شد"))

        if ph == "خودتخریب":
            return await self.self_destruct(ev, arg)
        if ph == "پایان خودتخریب":
            n = len(self.sd)
            self.sd = []
            self.sd_flush()
            return await self.say(ev, f"✅ {n} پیامِ در انتظار حذف لغو شد (خودِ پیام‌ها می‌مونن)")
        if ph in ("پخش", "تبچی"):
            return await self.broadcast(ev, arg)
        if ph == "پایان پخش":
            if self.bc_task and not self.bc_task.done():
                self.bc_task.cancel()
                return await self.say(ev, "✅ پخش متوقف شد؛ گزارشش به Saved Messages میاد")
            return await self.say(ev, "پخشی در حال اجرا نیست")
        if ph == "خروج":
            return await self.leave_cmd(ev, arg)

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
            s["sec_ents"] = ents_to_json(self.cmd_ents(ev, arg))
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
            if ev.id is None or not getattr(ev, "is_private", False):   # نگهبان فقط توی پیوی، همیشه و همه‌جا
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
        mode_label = {"banner": "بنر", "word": "تک کلمه", "": "خاموش (هیچی ریپلای نمی‌شه)"}[d.get("reply_mode", "")]
        cur = f"\n\nوضعیت فعلی:\n• تعداد {nm}: {len(d['ids'])}\n• حالت ریپلای: {mode_label}"
        return (f"{self.REL_ICON[kind]} {nm}\n"
                f"• روی پیام فرد ریپلای کن و بنویس: .{nm}\n   (یا  .{nm} @یوزرنیم  /  .{nm} آیدی_عددی)\n"
                f"• .{nm} بنر ← از الان به بعد، هر وقت یکی از {nm}‌ها پیام بده، یکی از متن‌های "
                f"{'ENEMY_BANNERS' if kind == 'enemy' else 'FRIEND_BANNERS'} توی فایل رندوم ریپلای می‌شه "
                f"(حداکثر هر {BANNER_CD} ثانیه)\n"
                f"• .{nm} تک کلمه ← از الان به بعد از متن‌های "
                f"{'ENEMY_WORDS' if kind == 'enemy' else 'FRIEND_WORDS'} توی فایل رندوم استفاده می‌شه "
                f"(حداکثر هر {WORD_CD} ثانیه)\n"
                f"• خودِ متن‌ها رو توی فایل self_runner.py پر می‌کنی، نه با دستور\n"
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
        extra = "" if self.s[kind]["reply_mode"] else f"\n⚠️ حالت ریپلای روشن نیست:\n.{nm} بنر  |  .{nm} تک کلمه"
        return await self.say(ev, f"{self.REL_ICON[kind]} {label} شد {nm}{extra}", 8)

    async def rel_mode(self, ev, kind, which, arg):
        """.دشمن بنر  یا  .دشمن تک کلمه  ← هم (اگه ریپلای/آیدی داده باشی) طرف رو به لیست اضافه می‌کنه،
        هم حالت رو عوض می‌کنه. متن‌ها همیشه از فایل میان، نه از خودِ دستور."""
        nm, other = REL[kind], ("friend" if kind == "enemy" else "enemy")
        if (arg or "").strip() in ("پاک", "خاموش", "حذف"):
            self.s[kind]["reply_mode"] = ""
            self.save()
            return await self.say(ev, f"✅ ریپلای خودکار {nm} خاموش شد")
        added_note = ""
        t = await self.resolve_target(ev, arg)
        if t:
            tid, label = t
            if self.me is not None and tid == str(getattr(self.me, "id", "")):
                return await self.say(ev, "❌ خودت رو نمی‌تونی اضافه کنی")
            if tid not in self.s[kind]["ids"]:
                self.s[other]["ids"].pop(tid, None)
                self.s[kind]["ids"][tid] = label
                added_note = f"\n{self.REL_ICON[kind]} {label} هم به لیست {nm} اضافه شد"
        pool = {"banner": ENEMY_BANNERS if kind == "enemy" else FRIEND_BANNERS,
                "word": ENEMY_WORDS if kind == "enemy" else FRIEND_WORDS}[which]
        self.s[kind]["reply_mode"] = which
        self.save()
        label2 = "بنر" if which == "banner" else "تک کلمه"
        warn = "" if pool else f"\n⚠️ لیست {label2} توی فایل خالیه — اول با متن پرش کن، وگرنه چیزی ریپلای نمی‌شه."
        return await self.say(ev, f"✅ حالت {nm} رو گذاشتم روی «{label2}» — از متن‌های خودِ فایل استفاده می‌شه.{added_note}{warn}")

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
            mode = d.get("reply_mode") or ""
            if mode == "banner":
                pool, cd, tag = (ENEMY_BANNERS if kind == "enemy" else FRIEND_BANNERS), BANNER_CD, "b"
            elif mode == "word":
                pool, cd, tag = (ENEMY_WORDS if kind == "enemy" else FRIEND_WORDS), WORD_CD, "w"
            else:
                return False     # حالت انتخاب نشده ← هیچی ریپلای نمی‌شه
            if pool and now - self.cool.get((kind, tag, sid), 0) > cd:
                self.cool[(kind, tag, sid)] = now
                reply = random.choice(pool)
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

    # ---------- ارسال با حفظ قالب (بولد / نقل‌قول / لینک …) ----------
    def cmd_ents(self, ev, text):
        msg = getattr(ev, "message", ev)
        return sub_entities(getattr(msg, "entities", None), ev.raw_text or "", text)

    async def send_fmt(self, chat, text, ents=None):
        self.mark(text)        # هر بار علامت می‌خوره تا سلف پیام خودش رو دستور/حالت‌متن حساب نکنه
        kw = {"formatting_entities": ents} if ents else {}
        try:
            return await self.client.send_message(chat, text, parse_mode=None, **kw)
        except TypeError:
            return await self.client.send_message(chat, text)

    async def reply_fmt(self, ev, text, ents=None):
        self.mark(text)
        kw = {"formatting_entities": ents} if ents else {}
        try:
            return await ev.reply(text, parse_mode=None, **kw)
        except TypeError:
            return await ev.reply(text)

    async def edit_fmt(self, ev, text, ents=None):
        self.mark(text)
        kw = {"formatting_entities": ents} if ents else {}
        try:
            return await ev.edit(text, parse_mode=None, **kw)
        except TypeError:
            return await ev.edit(text)

    # ---------- همیشه آنلاین / همیشه در حال تایپ ----------
    async def send_typing(self, chat):
        if SetTypingRequest is None:
            return
        try:
            peer = await self.client.get_input_entity(chat)
            await self.client(SetTypingRequest(peer, SendMessageTypingAction()))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            fs = flood_seconds(e)
            if fs:
                await asyncio.sleep(min(fs + 1, 60))
            else:
                self.last_error = f"تایپ: {type(e).__name__}"

    async def recent_pv_ids(self):
        if time.time() - self._pv_at > 60:
            self._pv_at = time.time()
            try:
                try:
                    ds = await self.client.get_dialogs(limit=30)
                except TypeError:
                    ds = await self.client.get_dialogs()
                my = getattr(self.me, "id", None)
                self._pv_cache = [d.id for d in ds if getattr(d, "is_user", False)
                                  and not getattr(d.entity, "bot", False)
                                  and getattr(d.entity, "id", None) != my][:TYPING_PV_MAX]
            except asyncio.CancelledError:
                raise
            except Exception:
                pass
        return list(self._pv_cache)

    async def presence_loop(self):
        tick = 0
        while True:
            try:
                if self.s.get("online") and UpdateStatusRequest is not None and tick % 6 == 0:
                    await self.client(UpdateStatusRequest(offline=False))
                chats = set(self.typing_chats)
                if self.s.get("typing_pv"):
                    chats |= set(await self.recent_pv_ids())
                for c in chats:
                    await self.send_typing(c)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.last_error = f"حضور: {type(e).__name__}"
            tick += 1
            await asyncio.sleep(4)

    # ---------- ذخیره از لینک ----------
    async def msg_from_link(self, link):
        """لینک پیام (…/username/123) ← (پیام، خطا)"""
        clean = (link or "").translate({0x200c: None, 0x200e: None, 0x200f: None}).split()[0]
        m = re.search(r"([^/]+)/(\d+)/?(?:\?.*)?$", clean)
        if not m:
            return None, f"❌ لینک درست تشخیص داده نشد: {clean}"
        raw = m.group(1)
        ident = int(raw) if raw.lstrip("-").isdigit() else raw
        try:
            msg = await self.client.get_messages(ident, ids=int(m.group(2)))
        except Exception as e:
            return None, f"❌ پیام از روی لینک پیدا نشد: {str(e)[:80]}"
        if msg is None:
            return None, "❌ پیامی با این لینک پیدا نشد"
        return msg, None

    # ---------- خودتخریب (با مدت دلخواه تا چند روز) ----------
    def sd_flush(self):
        try:
            with open(self.sd_path, "w", encoding="utf-8") as f:
                json.dump(self.sd, f)
        except Exception as e:
            log(self.uid, "selfdestruct save error", repr(e))

    def sd_add(self, chat_id, msg_id, seconds):
        if chat_id is None or msg_id is None or len(self.sd) >= SD_MAX_PENDING:
            return False
        self.sd.append([chat_id, msg_id, time.time() + seconds])
        self.sd_flush()          # همون لحظه ذخیره می‌شه تا با ری‌استارت از دست نره
        return True

    async def sd_loop(self):
        while True:
            try:
                now = time.time()
                due = [x for x in self.sd if x[2] <= now]
                if due:
                    self.sd = [x for x in self.sd if x[2] > now]
                    by = {}
                    for c, m, d in due:
                        by.setdefault(c, []).append((m, d))
                    for c, items in by.items():
                        try:
                            await self.client.delete_messages(c, [m for m, _ in items])
                        except asyncio.CancelledError:
                            raise
                        except Exception as e:
                            log(self.uid, "selfdestruct delete failed", c, repr(e))
                            for m, d in items:            # خطای موقت (شبکه): تا یک ساعت هر دقیقه دوباره
                                if now - d < 3600:
                                    self.sd.append([c, m, now + 60])
                        await asyncio.sleep(0.3)
                    self.sd_flush()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log(self.uid, "sd loop error", repr(e))
            await asyncio.sleep(2)

    async def self_destruct(self, ev, arg):
        usage = ("❌ مثال‌ها:\n.خودتخریب 1 روز متن پیام\n.خودتخریب 30 ثانیه سلام\n"
                 "روی پیام خودت ریپلای + .خودتخریب 2 ساعت\n(واحدها: ثانیه، دقیقه، ساعت، روز، هفته؛ بدون واحد = ثانیه)\n"
                 "لغو همه: .پایان خودتخریب")
        pr = parse_duration(arg)
        if not pr:
            return await self.say(ev, usage, 14)
        secs, text = pr
        if secs < 1 or secs > SD_MAX_SECONDS:
            return await self.say(ev, "❌ مدت باید بین ۱ ثانیه تا ۳۶۵ روز باشه")
        chat = ev.chat_id
        if text:
            ents = self.cmd_ents(ev, text)
            target = ev
            try:
                await self.edit_fmt(ev, text, ents)
            except Exception:
                target = await ev.respond(text)
                try:
                    await ev.delete()
                except Exception:
                    pass
            if not self.sd_add(chat, target.id, secs):
                return await self.say(target, "❌ صف خودتخریب پره", 5)
            return
        r = await ev.get_reply_message()
        if r is None or not getattr(r, "out", False):
            return await self.say(ev, "❌ متن بنویس یا روی پیامِ خودت ریپلای کن")
        if not self.sd_add(chat, r.id, secs):
            return await self.say(ev, "❌ صف خودتخریب پره")
        return await self.say(ev, f"💣 اون پیام {human_dur(secs)} دیگه پاک می‌شه")

    # ---------- پخش / تبچی (پیوی + گروه) ----------
    async def broadcast(self, ev, arg):
        if self.bc_task and not self.bc_task.done():
            return await self.say(ev, "⏳ یه پخش در حال اجراست. برای توقف: .پایان پخش")
        toks = arg.split(None, 1)
        scope, text = "all", arg.strip()
        if toks and norm(toks[0]) in BC_SCOPES:
            scope = BC_SCOPES[norm(toks[0])]
            text = toks[1].strip() if len(toks) > 1 else ""
        reply = await ev.get_reply_message()
        rtext = (getattr(reply, "raw_text", "") or "") if reply is not None else ""
        rmedia = getattr(reply, "media", None) if reply is not None else None
        t_ents = self.cmd_ents(ev, text) if text else []
        r_ents = list(getattr(reply, "entities", None) or []) if reply is not None else []
        if rmedia is not None and getattr(rmedia, "ttl_seconds", None):
            return await self.say(ev, "❌ رسانه‌ی زمان‌دار پخش نمی‌شه")
        if not text and not (rtext or rmedia is not None):
            return await self.say(ev, "❌ مثال:\n.پخش سلام (لینک یا متن)\n.پخش پیوی متن  |  .پخش گروه متن\n"
                                      "یا روی یه پیام ریپلای کن و بنویس: .پخش\nتوقف: .پایان پخش", 14)
        try:
            dialogs = await self.client.get_dialogs()
        except Exception as e:
            return await self.say(ev, f"❌ لیست چت‌ها رو نتونستم بگیرم: {str(e)[:80]}")
        my_id = getattr(self.me, "id", None)
        targets = []
        for d in dialogs:
            ent = getattr(d, "entity", None)
            if ent is not None and getattr(ent, "id", None) == my_id:
                continue
            if getattr(d, "is_user", False):
                if scope in ("all", "pv") and not getattr(ent, "bot", False) and not getattr(ent, "deleted", False):
                    targets.append(d)
            elif getattr(d, "is_group", False):
                if scope in ("all", "group") and can_post(ent):
                    targets.append(d)
        if not targets:
            return await self.say(ev, "❌ مقصدی پیدا نشد")
        planned = min(len(targets), BC_MAX)
        eta = int(planned * (BC_DELAY_MIN + BC_DELAY_MAX) / 2)
        content = text or rtext or "[رسانه]"
        st = {"sent": 0, "failed": 0, "errs": [], "note": "", "total": len(targets)}

        async def send_one(chat):
            if text:
                await self.send_fmt(chat, text, t_ents)
            elif rmedia is not None:
                if rtext:
                    self.mark(rtext)
                try:
                    await self.client.send_file(chat, rmedia, caption=rtext or None,
                                                formatting_entities=r_ents or None, parse_mode=None)
                except TypeError:
                    await self.client.send_file(chat, rmedia, caption=rtext or None)
            else:
                await self.send_fmt(chat, rtext, r_ents)

        async def run():
            for d in targets:
                if st["sent"] >= BC_MAX:
                    st["note"] = f"به سقف {BC_MAX} ارسال رسید؛ برای ادامه دوباره بزن"
                    return
                try:
                    await send_one(d.id)
                    st["sent"] += 1
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    fs = flood_seconds(e)
                    if fs and fs > BC_MAX_FLOOD:
                        st["note"] = f"سرور {fs} ثانیه محدودیت داد؛ پخش متوقف شد"
                        return
                    if fs:
                        await asyncio.sleep(fs + 1)
                        try:
                            await send_one(d.id)
                            st["sent"] += 1
                        except asyncio.CancelledError:
                            raise
                        except Exception as e2:
                            st["failed"] += 1
                            st["errs"].append(f"{getattr(d, 'name', d.id)}: {type(e2).__name__}")
                    else:
                        st["failed"] += 1
                        st["errs"].append(f"{getattr(d, 'name', d.id)}: {type(e).__name__}")
                await asyncio.sleep(random.uniform(BC_DELAY_MIN, BC_DELAY_MAX))

        async def guard():
            try:
                await run()
            except asyncio.CancelledError:
                st["note"] = "با دستور متوقف شد"
            except Exception as e:
                st["note"] = f"خطا: {type(e).__name__}: {str(e)[:80]}"
            rep = (f"📢 گزارش پخش\n━━━━━━━━━━━━━━━\n✅ ارسال‌شده: {st['sent']}\n❌ ناموفق: {st['failed']}\n"
                   f"🎯 کل مقصد: {st['total']}\n")
            if st["note"]:
                rep += f"⚠️ {st['note']}\n"
            if st["errs"]:
                rep += "━━━━━━━━━━━━━━━\nخطاها:\n" + "\n".join(st["errs"][:10])
            try:
                self.mark(rep)
                await self.client.send_message("me", rep)
            except Exception as e:
                log(self.uid, "broadcast report error", repr(e))

        self.bc_task = asyncio.create_task(guard())
        label = {"all": "پیوی + گروه", "pv": "پیوی", "group": "گروه"}[scope]
        return await self.say(ev, f"📢 پخش شروع شد ({label})\n🎯 {planned} مقصد | حدود {eta // 60 + 1} دقیقه\n"
                                  f"گزارش آخرش میاد Saved Messages | توقف: .پایان پخش", 8)

    # ---------- خروج از گروه‌ها و کانال‌ها ----------
    async def leave_dialog(self, d):
        c = self.client
        fn = getattr(c, "leave_chat", None)
        if fn:
            return await fn(d.id)
        fn = getattr(c, "delete_dialog", None)
        if fn:
            return await fn(d.entity)
        from splusthon.tl.functions.channels import LeaveChannelRequest
        return await c(LeaveChannelRequest(d.entity))

    async def leave_cmd(self, ev, arg):
        scope, hours, dry = "all", LEAVE_DEFAULT_HOURS, False
        for tok in arg.translate(_FA2EN).split():
            nt = norm(tok)
            if nt in LEAVE_SCOPES:
                scope = LEAVE_SCOPES[nt]
            elif nt == "بررسی":
                dry = True
            elif tok.isdigit():
                hours = max(1, int(tok))
            else:
                return await self.say(ev, "❌ مثال: .خروج  |  .خروج گروه 24  |  .خروج کانال 48  |  .خروج بررسی\n"
                                          "(عدد = ساعتِ بی‌پیامی؛ پیش‌فرض " + str(LEAVE_DEFAULT_HOURS) + ")", 12)
        try:
            dialogs = await self.client.get_dialogs()
        except Exception as e:
            return await self.say(ev, f"❌ لیست چت‌ها رو نتونستم بگیرم: {str(e)[:80]}")
        await self.say(ev, "🚪 شروع کردم؛ گزارش به Saved Messages میاد" + (" (فقط بررسی)" if dry else ""), 5)
        now = datetime.now(timezone.utc)
        checked = admin_skipped = 0
        picked = []
        for d in dialogs:
            ent = getattr(d, "entity", None)
            is_grp = bool(getattr(d, "is_group", False))
            is_chan = bool(getattr(d, "is_channel", False)) and not is_grp
            if not (is_grp or is_chan):
                continue
            if (scope == "group" and not is_grp) or (scope == "channel" and not is_chan):
                continue
            checked += 1
            if getattr(ent, "creator", False) or getattr(ent, "admin_rights", None):
                admin_skipped += 1          # جایی که مالک/ادمینشی هیچ‌وقت ترک نمی‌شه
                continue
            reason = None
            if is_grp and not can_post(ent):
                reason = "بسته (پیام دادن غیرفعاله)"
            else:
                last = getattr(d, "date", None) or getattr(getattr(d, "message", None), "date", None)
                if last is not None:
                    if last.tzinfo is None:
                        last = last.replace(tzinfo=timezone.utc)
                    hrs = (now - last).total_seconds() / 3600
                    if hrs >= hours:
                        reason = f"{int(hrs)} ساعت بی‌پیام"
            if reason:
                picked.append((d, reason, "گروه" if is_grp else "کانال"))
        left, failed, lines, note = 0, 0, [], ""
        if not dry:
            for d, reason, kind in picked[:LEAVE_MAX]:
                try:
                    await self.leave_dialog(d)
                    left += 1
                    lines.append(f"{kind}: {getattr(d, 'name', d.id)} ({reason})")
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    fs = flood_seconds(e)
                    failed += 1
                    if fs > BC_MAX_FLOOD:
                        note = f"سرور {fs} ثانیه محدودیت داد؛ متوقف شد"
                        break
                    if fs:
                        await asyncio.sleep(fs + 1)
                    log(self.uid, "leave error", getattr(d, "name", d.id), repr(e))
                await asyncio.sleep(random.uniform(LEAVE_DELAY_MIN, LEAVE_DELAY_MAX))
            if len(picked) > LEAVE_MAX:
                note = note or f"سقف {LEAVE_MAX} خروج در هر اجرا؛ برای ادامه دوباره بزن"
        else:
            lines = [f"{kind}: {getattr(d, 'name', d.id)} ({reason})" for d, reason, kind in picked]
        scope_label = {"all": "گروه + کانال", "group": "گروه", "channel": "کانال"}[scope]
        rep = (f"🚪 گزارش خروج ({scope_label}، آستانه {hours} ساعت)" + (" — فقط بررسی" if dry else "") +
               f"\n━━━━━━━━━━━━━━━\n🔎 بررسی‌شده: {checked}\n"
               f"🛡 ادمین/مالک (ردشده): {admin_skipped}\n")
        rep += f"📋 مشمول خروج: {len(picked)}\n" if dry else f"✅ ترک‌شده: {left}\n❌ خطا: {failed}\n"
        if note:
            rep += f"⚠️ {note}\n"
        if lines:
            rep += "━━━━━━━━━━━━━━━\n" + "\n".join(lines[:20])
            if len(lines) > 20:
                rep += f"\n… و {len(lines) - 20} مورد دیگه"
        self.mark(rep)
        await self.client.send_message("me", rep)

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
        ents = self.cmd_ents(ev, text)
        try:
            await ev.delete()
        except Exception:
            pass

        async def run():
            for _ in range(n):
                await self.send_fmt(chat, text, ents)
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
                        await self.reply_fmt(ev, v, ents_from_json(s["reply_ents"].get(k)))
                        return
            if s["sec_on"] and getattr(ev, "is_private", False):
                sid = getattr(ev, "sender_id", chat)
                if now - self.cool.get(("s", sid), 0) > s["sec_time"]:
                    self.cool[("s", sid)] = now
                    await self.reply_fmt(ev, s["sec_text"], ents_from_json(s["sec_ents"]))
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


def notify(uid, text, buttons=None):
    if not NOTIFY_TOKEN:
        return
    try:
        data = {"chat_id": uid, "text": text}
        if buttons:
            data["reply_markup"] = json.dumps({"inline_keyboard": buttons})
        requests.post(f"{NOTIFY_BASE}{NOTIFY_TOKEN}/sendMessage", data=data, timeout=15)
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
                        notify(uid, entry_message(), HELP_BUTTONS if ENTRY_HELP_BUTTON else None)
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
