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


def parse_periode(p):
    if not isinstance(p, str):
        p = str(p)

    m = re.match(r"Q([1-4])-(\d{4})", p.strip(), re.IGNORECASE)
    if m:
        period = int(m.group(1))
        tahun = int(m.group(2))
        return tahun, "Q", period

    raise ValueError(f"Tidak bisa parse periode: {p}")


def process_sheet(file_path, sheet_name, deskripsi_prefix):
    print(f"Memproses sheet: {sheet_name} ({deskripsi_prefix})")

    df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)

    df_data = df.iloc[1:39].copy().reset_index(drop=True)

    period_cols = []
    tahun_list = []
    quarter_list = []

    for col_idx in range(1, len(df.columns)):
        header_val = df.iloc[0, col_idx]
        if pd.isna(header_val):
            continue

        tahun, freq, quarter = parse_periode(str(header_val))
        period_cols.append(col_idx)
        tahun_list.append(tahun)
        quarter_list.append(quarter)

    col_to_tahun = {}
    col_to_quarter = {}
    for pos, col_idx in enumerate(period_cols):
        col_to_tahun[col_idx] = tahun_list[pos]
        col_to_quarter[col_idx] = quarter_list[pos]

    df_melt = df_data.melt(
        id_vars=[0],
        value_vars=period_cols,
        value_name="nilai",
    )

    df_melt.rename(columns={0: "kode_wilayah"}, inplace=True)

    df_melt["kode_wilayah"] = (
        df_melt["kode_wilayah"].astype(str).str.extract(r"(\d+)")[0]
    )

    df_melt = df_melt[
        df_melt["kode_wilayah"].notna() & df_melt["kode_wilayah"].str.match(r"^\d{2}$")
    ]

    df_melt["nilai"] = df_melt["nilai"].astype(str).str.replace(",", "", regex=False)
    df_melt["nilai"] = pd.to_numeric(df_melt["nilai"], errors="coerce")
    df_melt["nilai"] = df_melt["nilai"].fillna(0)

    df_melt["tahun"] = df_melt["variable"].map(col_to_tahun)
    df_melt["period"] = df_melt["variable"].map(col_to_quarter)
    df_melt["freq"] = "Q"

    df_melt["deskripsi"] = deskripsi_prefix
    df_melt["kode"] = "Q" + sheet_name
    df_melt["satuan"] = "Juta Rupiah"
    df_melt["konversi"] = "SUM"
    df_melt["created_at"] = datetime.now(timezone.utc)

    return df_melt[
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
    file_path = "data/indikator/regional/13-17 belanja apbn.xlsx"

    DATABASE_URL = os.getenv(
        "DATABASE_URL", "postgresql://postgres:rayy19@localhost:5432/dnpeng_db"
    )

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    try:
        print("Add table PKP: Belanja APBN...")

        sheets = [
            ("51-Pegawai", "Belanja Pegawai"),
            ("52-Barang", "Belanja Barang"),
            ("53-Penyusutan", "Penyusutan"),
            ("56-Bansos", "Belanja Bansos"),
            ("4x-Penerimaan", "Penerimaan"),
        ]

        insert_query = """
        INSERT INTO pkp
        (kode_wilayah, tahun, freq, period, nilai, kode, deskripsi, satuan, konversi, created_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """

        total_rows = 0

        for sheet_name, deskripsi_prefix in sheets:
            df_long = process_sheet(file_path, sheet_name, deskripsi_prefix)

            for _, row in df_long.iterrows():
                cur.execute(
                    insert_query,
                    (
                        row["kode_wilayah"],
                        int(row["tahun"]),
                        row["freq"],
                        int(row["period"]),
                        row["nilai"],
                        row["kode"],
                        row["deskripsi"],
                        row["satuan"],
                        row["konversi"],
                        row["created_at"],
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
