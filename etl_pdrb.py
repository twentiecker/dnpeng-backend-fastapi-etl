import pandas as pd
import psycopg2
from datetime import datetime, timezone
import os
import re

# =========================
# Workaround: beberapa file xlsx (mis. PDRB) punya elemen <fill> yang
# punya atribut tambahan sehingga openpyxl gagal parse
# ("expected <class 'openpyxl.styles.fills.Fill'>").
# Patch Fill.__init__ agar menerima & mengabaikan argumen tak dikenal.
# =========================
import openpyxl.styles.fills as _ob_fills

_orig_fill_init = _ob_fills.Fill.__init__

from dotenv import load_dotenv
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

# Kalau .env ada, load.
# Kalau tidak ada, tidak masalah.
load_dotenv(BASE_DIR / ".env")


def _safe_fill_init(self, *args, **kwargs):
    try:
        _orig_fill_init(self, *args, **kwargs)
    except TypeError:
        pass


_ob_fills.Fill.__init__ = _safe_fill_init


# =========================
# Kode provinsi (dari etl_pma_pmdn.py)
# =========================
PROV_MAP = {
    "Aceh": "11",
    "Sumatera Utara": "12",
    "Sumatera Barat": "13",
    "Riau": "14",
    "Jambi": "15",
    "Sumatera Selatan": "16",
    "Bengkulu": "17",
    "Lampung": "18",
    "Kepulauan Bangka Belitung": "19",
    "Kepulauan Riau": "21",
    "Daerah Khusus Ibukota Jakarta": "31",
    "Jawa Barat": "32",
    "Jawa Tengah": "33",
    "Daerah Istimewa Yogyakarta": "34",
    "Jawa Timur": "35",
    "Banten": "36",
    "Bali": "51",
    "Nusa Tenggara Barat": "52",
    "Nusa Tenggara Timur": "53",
    "Kalimantan Barat": "61",
    "Kalimantan Tengah": "62",
    "Kalimantan Selatan": "63",
    "Kalimantan Timur": "64",
    "Kalimantan Utara": "65",
    "Sulawesi Utara": "71",
    "Sulawesi Tengah": "72",
    "Sulawesi Selatan": "73",
    "Sulawesi Tenggara": "74",
    "Gorontalo": "75",
    "Sulawesi Barat": "76",
    "Maluku": "81",
    "Maluku Utara": "82",
    "Papua Barat": "91",
    "Papua Barat Daya": "92",
    "Papua": "94",
    "Papua Selatan": "95",
    "Papua Tengah": "96",
    "Papua Pegunungan": "97",
    "INDONESIA": "00",
}

PROV_MAP_REV = {k.upper(): v for k, v in PROV_MAP.items()}

# alias nama sheet di file PDRB Provinsi.xlsx
PROV_MAP_REV["DKI JAKARTA"] = "31"
PROV_MAP_REV["DI YOGYAKARTA"] = "34"


# =========================
# Helper: parse periode (sama seperti etl_pdb.py)
# =========================
def parse_periode(p):
    p = str(p).strip()

    tahun = int(p[:4])
    freq = p[4]  # Q atau M
    period = int(p[5:])

    return tahun, freq, period


# =========================
# Helper: ambil kode dari deskripsi komponen
#   - 1, 2, ...                 -> 10000, 20000, ...
#   - 1.a, 3.a, 3.b, ...        -> 10100, 30100, 30200, ...
#   - P D R B                   -> 90000
# =========================
def extract_kode(desc):
    d = str(desc).strip()

    if re.fullmatch(r"[Pp][Dd][Rr][Bb]", d.replace(" ", "")):
        return "90000"

    m = re.match(r"(\d+)\.([a-z])\.", d)
    if m:
        n = int(m.group(1))
        x = ord(m.group(2)) - ord("a") + 1
        return f"{n * 10000 + x * 100:05d}"

    m = re.match(r"(\d+)\.", d)
    if m:
        n = int(m.group(1))
        return f"{n * 10000:05d}"

    return d


# =========================
# Helper: bersihkan teks deskripsi komponen
#   - hapus kode di awal kalimat (1., 1.a., dst)
#   - hapus keterangan di dalam kurung di akhir ((1.a. s/d 1.l.), dst)
# =========================
def clean_deskripsi(desc):
    d = str(desc).strip()
    d = re.sub(r"^\s*\d+(?:\.[a-z]+)?\.?\s*", "", d)
    d = re.sub(r"\s*\([^)]*\)\s*$", "", d)
    return d.strip()


# =========================
# Helper: proses satu blok (ADHB / ADHK) dalam sheet
# =========================
def process_block(df, start, end, jenis, kode_wilayah):
    header = df.iloc[start + 1]
    block = df.iloc[start + 2 : end].copy()

    # hanya ambil baris yang punya komponen & bukan baris judul tabel lain
    block = block[block[0].notna()]
    block = block[~block[0].astype(str).str.contains("Tabel", case=False, na=False)]

    if block.empty:
        return pd.DataFrame()

    period_cols = [c for c in df.columns if c != 0]
    period_labels = {c: str(header[c]).strip() for c in period_cols}

    rows = []
    for _, r in block.iterrows():
        deskripsi = str(r[0]).strip()
        kode = extract_kode(deskripsi)
        deskripsi_bersih = clean_deskripsi(deskripsi)

        for c in period_cols:
            label = period_labels[c]
            # if pd.isna(r[c]) or str(r[c]).strip() == "":
            #     continue

            # nilai = pd.to_numeric(str(r[c]).replace(",", ""), errors="coerce")
            # if pd.isna(nilai):
            #     nilai = 0

            nilai = pd.to_numeric(str(r[c]).replace(",", ""), errors="coerce")

            if pd.isna(nilai):
                nilai = 0

            rows.append((kode, deskripsi_bersih, label, nilai))

    res = pd.DataFrame(rows, columns=["kode", "deskripsi", "periode", "nilai"])
    res["jenis"] = jenis
    res["satuan"] = "Juta Rupiah"
    res["konversi"] = "SUM"
    res["kode_wilayah"] = kode_wilayah
    res["created_at"] = datetime.now(timezone.utc)

    res[["tahun", "freq", "period"]] = res["periode"].apply(
        lambda x: pd.Series(parse_periode(x))
    )

    return res


# =========================
# Helper: proses satu sheet provinsi (ADHB + ADHK)
# =========================
def process_sheet(df, sheet_name, kode_wilayah):
    print(f"Memproses sheet: {sheet_name} (kode_wilayah={kode_wilayah})")

    # cari baris marker ADHB & ADHK (hanya tabel utama, bukan Distribusi)
    markers = []
    for idx in df.index:
        val = str(df.at[idx, 0]).upper()
        if ("ADHB" in val or "ADHK" in val) and "DISTRIBUSI" not in val:
            jenis = "ADHB" if "ADHB" in val else "ADHK"
            markers.append((jenis, idx))

    if not markers:
        print(f"  Tidak ada blok ADHB/ADHK di sheet {sheet_name}, lewati")
        return pd.DataFrame()

    # indeks semua baris judul tabel lain (pembatas blok)
    tabel_rows = [i for i in df.index if "TABEL" in str(df.at[i, 0]).upper()]

    combined = pd.DataFrame()
    for jenis, m_idx in markers:
        # blok berakhir di baris "Tabel" berikutnya
        next_tabel = [t for t in tabel_rows if t > m_idx]
        end = next_tabel[0] if next_tabel else len(df)
        block = process_block(df, m_idx, end, jenis, kode_wilayah)
        combined = pd.concat([combined, block], ignore_index=True)

    return combined


# =========================
# Main ETL
# =========================
if __name__ == "__main__":
    file_path = "data/PDRB Provinsi.xlsx"

    DATABASE_URL = os.getenv(
        "DATABASE_URL", "postgresql://dnpeng:dnpeng7240@localhost:5432/dnpeng_db"
    )

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    try:
        # =========================
        # 1. Truncate table + reset id
        # =========================
        print("Add table PDB: PDRB ADHB dan ADHK...")
        # cur.execute("TRUNCATE TABLE pdrb RESTART IDENTITY;")
        # conn.commit()

        # =========================
        # 2. Proses semua sheet provinsi otomatis
        # =========================
        xls = pd.read_excel(file_path, sheet_name=None, header=None)
        insert_query = """
        INSERT INTO pdb
        (kode, deskripsi, satuan, jenis, konversi, tahun, freq, period, nilai, created_at, kode_wilayah)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """

        total_rows = 0

        for sheet_name, df in xls.items():
            kode_wilayah = PROV_MAP_REV.get(sheet_name.upper())
            if not kode_wilayah:
                print(f"Sheet {sheet_name} tidak ada di PROV_MAP, lewati")
                continue

            df_long = process_sheet(df, sheet_name, kode_wilayah)

            if df_long.empty:
                continue

            for _, row in df_long.iterrows():
                cur.execute(
                    insert_query,
                    (
                        row["kode"],
                        row["deskripsi"],
                        row["satuan"],
                        row["jenis"],
                        row["konversi"],
                        int(row["tahun"]),
                        row["freq"],
                        int(row["period"]),
                        row["nilai"],
                        row["created_at"],
                        row["kode_wilayah"],
                    ),
                )

            total_rows += len(df_long)
            conn.commit()
            print(f"{sheet_name} selesai ({len(df_long)} rows)")
            print(df_long)

        print(f"\nSemua data berhasil dimasukkan ({total_rows} rows)")

    except Exception as e:
        conn.rollback()
        print("ERROR:", e)

    finally:
        cur.close()
        conn.close()
