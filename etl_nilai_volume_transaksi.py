import pandas as pd
import psycopg2
from datetime import datetime, timezone
import os
import re

BULAN_MAP = {
    "Januari": 1,
    "Februari": 2,
    "Maret": 3,
    "April": 4,
    "Mei": 5,
    "Juni": 6,
    "Juli": 7,
    "Agustus": 8,
    "September": 9,
    "Oktober": 10,
    "November": 11,
    "Desember": 12,
}


def parse_periode(p):
    if isinstance(p, datetime):
        return p.year, "M", p.month

    p = str(p).strip()

    q_match = re.match(r'Q([1-4])', p, re.IGNORECASE)
    if q_match:
        nums = re.findall(r'\d+', p)
        if nums:
            tahun = int(nums[0])
            if tahun < 100:
                tahun = 2000 + tahun
        else:
            tahun = datetime.now().year
        return tahun, "Q", int(q_match.group(1))

    parts = p.split()
    if len(parts) == 2:
        nama_bulan, tahun_part = parts
        bulan = BULAN_MAP.get(nama_bulan)
        if bulan:
            tahun = int(tahun_part)
            if tahun < 100:
                tahun = 2000 + tahun
            return tahun, "M", bulan

    raise ValueError(f"Tidak bisa parse periode: {p}")


def process_sheet(file_path, sheet_name, deskripsi):
    print(f"Memproses sheet: {sheet_name} ({deskripsi})")

    df = pd.read_excel(file_path, sheet_name=sheet_name)

    df_long = df.melt(
        id_vars=["Provinsi"],
        var_name="periode",
        value_vars=[c for c in df.columns if c != "Provinsi"],
        value_name="nilai",
    )

    df_long.rename(columns={"Provinsi": "kode_wilayah"}, inplace=True)

    df_long["kode_wilayah"] = df_long["kode_wilayah"].astype(str).str.extract(r"(\d+)")[0]

    df_long["nilai"] = df_long["nilai"].astype(str).str.replace(",", "", regex=False)
    df_long["nilai"] = pd.to_numeric(df_long["nilai"], errors="coerce")
    df_long["nilai"] = df_long["nilai"].fillna(0)

    df_long[["tahun", "freq", "period"]] = df_long["periode"].apply(
        lambda x: pd.Series(parse_periode(x))
    )

    df_long["kode"] = df_long["freq"] + "-" + sheet_name
    df_long["deskripsi"] = deskripsi

    def get_satuan(d):
        dl = str(d).lower()
        if "volume" in dl:
            return "frekuensi"
        if "nilai" in dl:
            return "juta rupiah"
        return "transaksi"

    df_long["satuan"] = df_long["deskripsi"].apply(get_satuan)
    df_long["konversi"] = "SUM"
    df_long["created_at"] = datetime.now(timezone.utc)

    return df_long[
        ["kode_wilayah", "tahun", "freq", "period", "nilai", "kode", "deskripsi", "satuan", "konversi", "created_at"]
    ]


if __name__ == "__main__":
    file_path = "data/indikator/regional/1-6 Nilai dan volume transaksi kredit debit emoney 2024Q1 - 2026Q1.xlsx"

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
            ("VKK", "Volume Kartu Kredit"),
            ("NKK", "Nilai Kartu Kredit"),
            ("VKD", "Volume Kartu Debit"),
            ("NKD", "Nilai Kartu Debit"),
            ("VUE", "Volume Uang Elektronik"),
            ("NUE", "Nilai Uang Elektronik"),
        ]

        # insert_query = """
        # INSERT INTO transaksi_regional
        # (kode_wilayah, tahun, freq, period, nilai, kode, deskripsi, satuan, konversi, created_at)
        # VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        # """

        total_rows = 0

        for sheet_name, deskripsi in sheets:
            df_long = process_sheet(file_path, sheet_name, deskripsi)

            # for _, row in df_long.iterrows():
                # cur.execute(
                #     insert_query,
                #     (
                #         row["kode_wilayah"],
                #         int(row["tahun"]),
                #         row["freq"],
                #         int(row["period"]),
                #         row["nilai"],
                #         row["kode"],
                #         row["deskripsi"],
                #         row["satuan"],
                #         row["konversi"],
                #         row["created_at"],
                #     ),
                # )

            total_rows += len(df_long)
            # conn.commit()
            print(f"{sheet_name} selesai ({len(df_long)} rows)")
            print(df_long)

        print(f"\nSemua data berhasil dimasukkan ({total_rows} rows)")

    except Exception as e:
        # conn.rollback()
        print("ERROR:", e)

    # finally:
        # cur.close()
        # conn.close()

