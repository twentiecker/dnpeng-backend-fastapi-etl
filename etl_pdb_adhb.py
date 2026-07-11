import pandas as pd
import psycopg2
from datetime import datetime, timezone
import os
import re


# =========================
# Workaround: beberapa file xlsx (mis. PDB ADHB) punya elemen <fill> yang
# punya atribut tambahan sehingga openpyxl gagal parse
# ("expected <class 'openpyxl.styles.fills.Fill'>").
# Patch Fill.__init__ agar menerima & mengabaikan argumen tak dikenal.
# =========================
import openpyxl.styles.fills as _ob_fills

_orig_fill_init = _ob_fills.Fill.__init__


def _safe_fill_init(self, *args, **kwargs):
    try:
        _orig_fill_init(self, *args, **kwargs)
    except TypeError:
        pass


_ob_fills.Fill.__init__ = _safe_fill_init


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
# Helper: bersihkan teks deskripsi komponen
#   - hapus kode di awal kalimat (1., 1.a., a.1., dst)
# =========================
def clean_deskripsi(desc):
    d = str(desc).strip()
    d = re.sub(r"^\s*[\d.a-z]+\.?\s*", "", d)
    return d.strip()


# =========================
# Helper: buat kode dari daftar deskripsi secara berurutan
#   - 1, 2, ...                 -> 10000, 20000, ...
#   - 1.a, 3.a, 3.b, ...        -> 10100, 30100, 30200, ...
#   - 1.a.1, 6.a.1, 6.a.2, ...  -> 10110, 60110, 60120, ...
#   - P D R B / Produk Domestik Bruto -> 90000
#   Perhitungan bersifat kontekstual karena sub-komponen
#   (a., a.1., dst) tidak menyertakan angka induknya.
# =========================
def build_codes(deskripsi_list):
    last_n = None
    codes = []
    clean = []

    for raw in deskripsi_list:
        d = str(raw).strip()
        c = clean_deskripsi(d)

        m_subsub = re.match(r"(\d+)\.([a-z])\.(\d+)", d)
        m_sub = re.match(r"(\d+)\.([a-z])", d)
        m_top = re.match(r"(\d+)\.", d)
        m_letter_sub = re.match(r"([a-z])\.(\d+)", d)
        m_letter = re.match(r"([a-z])", d)

        if m_subsub:
            n = int(m_subsub.group(1))
            x = ord(m_subsub.group(2)) - 96
            y = int(m_subsub.group(3))
            code = n * 10000 + x * 100 + y * 10
            last_n = n
        elif m_sub:
            n = int(m_sub.group(1))
            x = ord(m_sub.group(2)) - 96
            code = n * 10000 + x * 100
            last_n = n
        elif m_top:
            n = int(m_top.group(1))
            code = n * 10000
            last_n = n
        elif m_letter_sub:
            x = ord(m_letter_sub.group(1)) - 96
            y = int(m_letter_sub.group(2))
            n = last_n if last_n is not None else 0
            code = n * 10000 + x * 100 + y * 10
        elif m_letter:
            x = ord(m_letter.group(1)) - 96
            n = last_n if last_n is not None else 0
            code = n * 10000 + x * 100
        else:
            code = c

        codes.append(f"{code:05d}")
        clean.append(c)

    return codes, clean


# =========================
# Helper: proses sheet (hanya ADHB, nasional)
# =========================
def process_sheet(df, sheet_name, kode_wilayah="00", jenis="ADHB"):
    print(f"Memproses sheet: {sheet_name} (kode_wilayah={kode_wilayah}, jenis={jenis})")

    # cari baris header "Komponen"
    header_idx = None
    for i in df.index:
        if str(df.at[i, 0]).strip().lower() == "komponen":
            header_idx = i
            break

    if header_idx is None:
        print(f"  Tidak ada baris header 'Komponen' di sheet {sheet_name}, lewati")
        return pd.DataFrame()

    header = df.iloc[header_idx]
    block = df.iloc[header_idx + 1 :].copy()

    # hanya ambil baris yang punya komponen & bukan baris judul tabel lain
    block = block[block[0].notna()]
    block = block[~block[0].astype(str).str.contains("Tabel", case=False, na=False)]

    if block.empty:
        return pd.DataFrame()

    period_cols = [c for c in df.columns if c != 0]
    period_labels = {c: str(header[c]).strip() for c in period_cols}

    deskripsi_list = [str(r[0]).strip() for _, r in block.iterrows()]
    codes, deskripsi_bersih = build_codes(deskripsi_list)

    rows = []
    for idx, (_, r) in enumerate(block.iterrows()):
        kode = codes[idx]
        d_bersih = deskripsi_bersih[idx]

        for c in period_cols:
            label = period_labels[c]
            if pd.isna(r[c]) or str(r[c]).strip() == "":
                continue

            nilai = pd.to_numeric(str(r[c]).replace(",", ""), errors="coerce")
            if pd.isna(nilai):
                nilai = 0

            rows.append((kode, d_bersih, label, nilai))

    res = pd.DataFrame(rows, columns=["kode", "deskripsi", "periode", "nilai"])
    res["jenis"] = jenis
    res["satuan"] = "Miliar Rupiah"
    res["konversi"] = "SUM"
    res["kode_wilayah"] = kode_wilayah
    res["created_at"] = datetime.now(timezone.utc)

    res[["tahun", "freq", "period"]] = res["periode"].apply(
        lambda x: pd.Series(parse_periode(x))
    )

    return res


# =========================
# Main ETL
# =========================
if __name__ == "__main__":
    file_path = "data/PDB ADHB.xlsx"

    # DATABASE_URL = os.getenv(
    #     "DATABASE_URL", "postgresql://dnpeng:dnpeng7240@localhost:5432/dnpeng_db"
    # )

    # conn = psycopg2.connect(DATABASE_URL)
    # cur = conn.cursor()

    try:
        # =========================
        # 1. Truncate table + reset id
        # =========================
        print("Add table PDB: PDB ADHB...")
        # cur.execute("TRUNCATE TABLE pdb RESTART IDENTITY;")
        # conn.commit()

        # =========================
        # 2. Proses sheet (nasional, kode_wilayah=00)
        # =========================
        xls = pd.read_excel(file_path, sheet_name=None, header=None)
        # insert_query = """
        # INSERT INTO pdb
        # (kode, deskripsi, satuan, jenis, konversi, tahun, freq, period, nilai, created_at, kode_wilayah)
        # VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        # """

        total_rows = 0

        for sheet_name, df in xls.items():
            df_long = process_sheet(df, sheet_name, "00", "ADHB")

            if df_long.empty:
                continue

            # for _, row in df_long.iterrows():
            #     cur.execute(
            #         insert_query,
            #         (
            #             row["kode"],
            #             row["deskripsi"],
            #             row["satuan"],
            #             row["jenis"],
            #             row["konversi"],
            #             int(row["tahun"]),
            #             row["freq"],
            #             int(row["period"]),
            #             row["nilai"],
            #             row["created_at"],
            #             row["kode_wilayah"],
            #         ),
            #     )

            total_rows += len(df_long)
            # conn.commit()
            print(f"{sheet_name} selesai ({len(df_long)} rows)")
            print(df_long)

        print(f"\nSemua data berhasil dimasukkan ({total_rows} rows)")

    except Exception as e:
        # conn.rollback()
        print("ERROR:", e)

    # finally:
    #     cur.close()
    #     conn.close()
