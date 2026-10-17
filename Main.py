# نقطه‌ی شروع برای هاست‌هایی که فقط یک فایل اجرا می‌کنند: هر دو ربات را بالا می‌آورد
import subprocess, sys, time, os

os.chdir(os.path.dirname(os.path.abspath(__file__)))
FILES = ["bot.py", "self_runner.py"]
procs = {}

def start(f):
    procs[f] = subprocess.Popen([sys.executable, "-u", f])
    print("started", f, flush=True)

for f in FILES:
    start(f)
    time.sleep(2)

while True:
    for f, p in list(procs.items()):
        if p.poll() is not None:
            print(f, "exited", p.returncode, "- restarting in 5s", flush=True)
            time.sleep(5)
            if f == "self_runner.py":
                try:
                    os.remove("runner.lock")
                except OSError:
                    pass
            start(f)
    time.sleep(3)
