import pandas as pd
import psycopg2
from datetime import datetime, timezone
import os
import re

BULAN_MAP = {
    "Jan": 1,
    "Feb": 2,
    "Mar": 3,
    "Apr": 4,
    "April": 4,
    "Mei": 5,
    "Jun": 6,
    "Juni": 6,
    "Jul": 7,
    "Juli": 7,
    "Ags": 8,
    "Agustus": 8,
    "Aug": 8,
    "September": 9,
    "Sep": 9,
    "Oktober": 10,
    "Okt": 10,
    "Oct": 10,
    "November": 11,
    "Nov": 11,
    "Nop": 11,
    "Des": 12,
    "Dec": 12,
    "Desember": 12,
}


def parse_periode(year_val, month_val):
    bulan = BULAN_MAP.get(str(month_val).strip())
    if not bulan:
        raise ValueError(f"Tidak bisa parse bulan: {month_val}")

    tahun = int(year_val) if pd.notna(year_val) else datetime.now().year
    return tahun, "M", bulan


def process_sheet(file_path, sheet_name, deskripsi_prefix):
    print(f"Memproses sheet: {sheet_name} ({deskripsi_prefix})")

    df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)

    df_data = df.iloc[2:].copy().reset_index(drop=True)

    period_cols = []
    tahun_list = []
    bulan_list = []

    for col_idx in range(1, len(df.columns)):
        year_val = df.iloc[0, col_idx]
        month_val = df.iloc[1, col_idx]

        tahun, freq, bulan = parse_periode(year_val, month_val)
        period_cols.append(col_idx)
        tahun_list.append(tahun)
        bulan_list.append(bulan)

    id_vars = [0]
    df_melt = df_data.melt(
        id_vars=id_vars,
        value_vars=period_cols,
        value_name="nilai",
    )

    col_to_tahun = {}
    col_to_bulan = {}
    for pos, col_idx in enumerate(period_cols):
        col_to_tahun[col_idx] = tahun_list[pos]
        col_to_bulan[col_idx] = bulan_list[pos]

    df_melt.rename(columns={0: "kode_wilayah"}, inplace=True)

    df_melt["kode_wilayah"] = df_melt["kode_wilayah"].astype(str).str.extract(r"(\d+)")[0]

    df_melt["nilai"] = df_melt["nilai"].astype(str).str.replace(",", "", regex=False)
    df_melt["nilai"] = pd.to_numeric(df_melt["nilai"], errors="coerce")
    df_melt["nilai"] = df_melt["nilai"].fillna(0)

    df_melt["tahun"] = df_melt["variable"].map(col_to_tahun)
    df_melt["period"] = df_melt["variable"].map(col_to_bulan)
    df_melt["freq"] = "M"

    df_melt["deskripsi"] = deskripsi_prefix
    df_melt["kode"] = "M-" + sheet_name
    df_melt["satuan"] = "ton"
    df_melt["konversi"] = "SUM"
    df_melt["created_at"] = datetime.now(timezone.utc)

    return df_melt[
        ["kode_wilayah", "tahun", "freq", "period", "nilai", "kode", "deskripsi", "satuan", "konversi", "created_at"]
    ]


if __name__ == "__main__":
    file_path = "data/indikator/regional/27 Realisasi Semen s_d 2026Q1(1).xlsx"

    # DATABASE_URL = os.getenv(
    #     "DATABASE_URL", "postgresql://dnpeng:dnpeng7240@localhost:5432/dnpeng_db"
    # )

    # conn = psycopg2.connect(DATABASE_URL)
    # cur = conn.cursor()

    try:
        print("Truncate table transaksi_regional...")
        # cur.execute("TRUNCATE TABLE transaksi_regional RESTART IDENTITY;")
        # conn.commit()

        sheets = [
            ("RPS", "Realisasi Semen"),
        ]

        # insert_query = """
        # INSERT INTO transaksi_regional
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

