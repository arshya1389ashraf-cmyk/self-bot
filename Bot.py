# راه‌انداز: هاست فقط "python Bot.py" رو اجرا می‌کنه؛ این فایل هر دو ربات رو بالا میاره.
# ربات فروشگاه باید با اسم shop.py و رانر با اسم self_runner.py کنارش باشن (حروف بزرگ/کوچک مهم نیست).
import os, subprocess, sys, time

HERE = os.path.dirname(os.path.abspath(__file__))
os.chdir(HERE)

def find(name):
    for f in os.listdir(HERE):
        if f.lower() == name.lower():
            return f
    return None

def ensure_packages():
    need = {"requests": "requests", "deep_translator": "deep-translator", "splusthon": "splusthon"}
    for mod, pkg in need.items():
        try:
            __import__(mod)
        except ImportError:
            print("installing", pkg, flush=True)
            subprocess.call([sys.executable, "-m", "pip", "install", "--quiet", pkg])

ensure_packages()

procs = {}
def start(f):
    procs[f] = subprocess.Popen([sys.executable, "-u", f])
    print("started", f, flush=True)

for name in ("shop.py", "self_runner.py"):
    f = find(name)
    if not f:
        print("FILE MISSING:", name, "- files here:", os.listdir(HERE), flush=True)
        continue
    start(f)
    time.sleep(2)

while True:
    for f, p in list(procs.items()):
        if p.poll() is not None:
            print(f, "exited", p.returncode, "- restarting in 5s", flush=True)
            time.sleep(5)
            if f.lower() == "self_runner.py":
                try:
                    os.remove("runner.lock")
                except OSError:
                    pass
            start(f)
    time.sleep(3)
