import subprocess
import sys
import time
from datetime import datetime

# ===============================
# CONFIG ETL FILES
# ===============================
etl_files = [
    "etl_pkrt.py",
    "etl_pkp.py",
    "etl_pmtb.py",
    "etl_eksim.py",
    "etl_pdb.py",
]

# ===============================
# COLOR ANSI
# ===============================
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
RESET = "\033[0m"

# ===============================
# START
# ===============================
start_all = time.time()

print(f"{BLUE}")
print("=" * 60)
print("🚀 ETL MASTER RUNNER START")
print("🕒", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
print("=" * 60)
print(f"{RESET}")

success = []
failed = []

# ===============================
# LOOP FILES
# ===============================
for no, file in enumerate(etl_files, start=1):

    print(f"{YELLOW}[{no}/{len(etl_files)}] Menjalankan {file}...{RESET}")

    start = time.time()

    result = subprocess.run([sys.executable, file], capture_output=True, text=True)

    duration = round(time.time() - start, 2)

    if result.returncode == 0:
        print(f"{GREEN}✅ SUCCESS - {file} ({duration}s){RESET}")
        success.append(file)

        if result.stdout.strip():
            print(result.stdout)

    else:
        print(f"{RED}❌ FAILED - {file} ({duration}s){RESET}")
        failed.append(file)

        if result.stderr.strip():
            print(result.stderr)

    print("-" * 60)

# ===============================
# SUMMARY
# ===============================
total_time = round(time.time() - start_all, 2)

print(f"\n{BLUE}" + "=" * 60)
print("📊 ETL SUMMARY")
print("=" * 60 + f"{RESET}")

print(f"{GREEN}Berhasil : {len(success)} file{RESET}")
for x in success:
    print(f"   ✅ {x}")

print(f"\n{RED}Gagal    : {len(failed)} file{RESET}")
for x in failed:
    print(f"   ❌ {x}")

print(f"\n⏱️ Total Waktu: {total_time} detik")
print("🏁 Selesai")
print("=" * 60)
