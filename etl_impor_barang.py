import pandas as pd
import psycopg2
from datetime import datetime, timezone
import os
import re
from dotenv import load_dotenv
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

# Kalau .env ada, load.
# Kalau tidak ada, tidak masalah.
load_dotenv(BASE_DIR / ".env")

BULAN_MAP = {
    "Jan": 1,
    "Feb": 2,
    "Mar": 3,
    "Apr": 4,
    "Mei": 5,
    "Jun": 6,
    "Jul": 7,
    "Ags": 8,
    "Sep": 9,
    "Okt": 10,
    "Nov": 11,
    "Des": 12,
}


def parse_periode(p):
    if not isinstance(p, str):
        p = str(p)

    p = p.strip()
    bulan = BULAN_MAP.get(p)
    if bulan:
        return "M", bulan

    raise ValueError(f"Tidak bisa parse periode: {p}")


def process_sheet(file_path, sheet_name, deskripsi_prefix):
    print(f"Memproses sheet: {sheet_name} ({deskripsi_prefix})")

    df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)

    tahun_row = df.iloc[0].copy()
    header_row = df.iloc[1].copy()

    header_row = header_row.str.strip()
    header_row.iloc[0] = "Provinsi"
    header_row.iloc[1] = "Kategori BEC"
    header_row.iloc[2] = "Kode BEC"

    df_data = df.iloc[2:].copy()
    df_data.columns = header_row
    df_data = df_data.reset_index(drop=True)

    period_cols = [
        c for c in df_data.columns if c not in ["Provinsi", "Kategori BEC", "Kode BEC"]
    ]

    period_info = []
    current_tahun = None
    for col_idx in range(len(df.columns)):
        t = tahun_row.iloc[col_idx]
        if pd.notna(t):
            current_tahun = int(t)

        h = header_row.iloc[col_idx]
        if h in period_cols:
            period_info.append((h, current_tahun))

    df_long = df_data.melt(
        id_vars=["Provinsi", "Kategori BEC", "Kode BEC"],
        var_name="periode",
        value_vars=period_cols,
        value_name="nilai",
    )

    df_long.rename(columns={"Provinsi": "kode_wilayah"}, inplace=True)

    df_long["kode_wilayah"] = (
        df_long["kode_wilayah"].astype(str).str.extract(r"(\d+)")[0]
    )

    tahun_list = [y for _, y in period_info]
    df_long["tahun"] = tahun_list * len(df_data)

    df_long["nilai"] = df_long["nilai"].astype(str).str.replace(",", "", regex=False)
    df_long["nilai"] = pd.to_numeric(df_long["nilai"], errors="coerce")
    df_long["nilai"] = df_long["nilai"].fillna(0)

    df_long[["freq", "period"]] = df_long["periode"].apply(
        lambda x: pd.Series(parse_periode(x))
    )

    df_long["deskripsi"] = deskripsi_prefix + " " + df_long["Kategori BEC"].astype(str)
    df_long["kode"] = "M" + df_long["Kode BEC"].astype(str)
    df_long["satuan"] = "USD"
    df_long["konversi"] = "SUM"
    df_long["created_at"] = datetime.now(timezone.utc)

    return df_long[
        [
            "kode_wilayah",
            "tahun",
            "freq",
            "period",
            "nilai",
            "kode",
            "deskripsi",
            "satuan",
            "konversi",
            "created_at",
        ]
    ]


if __name__ == "__main__":
    file_path = "data/indikator/regional/8 Impor Barang Konsumsi.xlsx"

    # DATABASE_URL = os.getenv(
    #     "DATABASE_URL", "postgresql://postgres:rayy19@localhost:5432/dnpeng_db"
    # )

    # conn = psycopg2.connect(DATABASE_URL)
    # cur = conn.cursor()

    try:
        print("Add table EKSIM: Impor Barang...")

        sheets = [
            ("bec_bulanan_2025_mar2026s nilai", "Impor Barang"),
        ]

        # insert_query = """
        # INSERT INTO eksim
        # (kode_wilayah, tahun, freq, period, nilai, kode, deskripsi, satuan, konversi, created_at)
        # VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        # """

        total_rows = 0

        for sheet_name, deskripsi_prefix in sheets:
            df_long = process_sheet(file_path, sheet_name, deskripsi_prefix)

            # for _, row in df_long.iterrows():
            #     cur.execute(
            #         insert_query,
            #         (
            #             row["kode_wilayah"],
            #             int(row["tahun"]),
            #             row["freq"],
            #             int(row["period"]),
            #             row["nilai"],
            #             row["kode"],
            #             row["deskripsi"],
            #             row["satuan"],
            #             row["konversi"],
            #             row["created_at"],
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
