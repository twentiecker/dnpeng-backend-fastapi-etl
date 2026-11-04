import pandas as pd
import psycopg2
from datetime import datetime, timezone
import os
from dotenv import load_dotenv
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

# Kalau .env ada, load.
# Kalau tidak ada, tidak masalah.
load_dotenv(BASE_DIR / ".env")


def process_sheet(file_path, sheet_name):
    print(f"Memproses sheet: {sheet_name}")

    df = pd.read_excel(file_path, sheet_name=sheet_name, header=0)

    df.rename(
        columns={
            df.columns[0]: "kode_prov",
            df.columns[1]: "nama_prov",
            df.columns[2]: "kode_kako",
            df.columns[3]: "nama_kako",
        },
        inplace=True,
    )

    df["kode_prov"] = df["kode_prov"].astype(str).str.strip()
    df["nama_prov"] = df["nama_prov"].astype(str).str.strip()
    df["kode_kako"] = df["kode_kako"].astype(str).str.strip().str.zfill(2)
    df["nama_kako"] = df["nama_kako"].astype(str).str.strip()

    df = df[df["kode_prov"].notna() & df["kode_kako"].notna()]
    df = df[df["kode_prov"].str.match(r"^\d+$")]
    df = df[df["kode_kako"].str.match(r"^\d+$")]

    created_at = datetime.now(timezone.utc)
    rows = []

    # 1. NASIONAL
    rows.append(
        {
            "kode_wilayah": "00",
            "nama_wilayah": "INDONESIA",
            "level": "NASIONAL",
            "parent_kode": None,
            "created_at": created_at,
        }
    )

    # 2. PROVINSI (dedupe per kode_prov)
    prov_unik = df.drop_duplicates(subset=["kode_prov"])[["kode_prov", "nama_prov"]]

    for _, r in prov_unik.iterrows():
        rows.append(
            {
                "kode_wilayah": r["kode_prov"],
                "nama_wilayah": r["nama_prov"],
                "level": "PROVINSI",
                "parent_kode": "00",
                "created_at": created_at,
            }
        )

    # 3. KABKOT (kode_prov + kode_kako)
    for _, r in df.iterrows():
        kode_wilayah = r["kode_prov"] + r["kode_kako"]
        rows.append(
            {
                "kode_wilayah": kode_wilayah,
                "nama_wilayah": r["nama_kako"],
                "level": "KABKOT",
                "parent_kode": r["kode_prov"],
                "created_at": created_at,
            }
        )

    return pd.DataFrame(rows)


if __name__ == "__main__":
    file_path = "data/indikator/regional/Tabulasi MFD 2023 sem 1.xlsx"

    DATABASE_URL = os.getenv(
        "DATABASE_URL", "postgresql://dnpeng:dnpeng7240@localhost:5432/dnpeng_db"
    )

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    try:
        print("Truncate table MASTER WILAYAH...")
        cur.execute("TRUNCATE TABLE master_wilayah RESTART IDENTITY CASCADE;")
        conn.commit()

        sheets = [
            ("Kabupaten",),
        ]

        insert_query = """
        INSERT INTO master_wilayah
        (kode_wilayah, nama_wilayah, level, parent_kode, created_at)
        VALUES (%s,%s,%s,%s,%s)
        """

        total_rows = 0

        for sheet_name in sheets:
            df_long = process_sheet(file_path, sheet_name[0])

            for _, row in df_long.iterrows():
                cur.execute(
                    insert_query,
                    (
                        row["kode_wilayah"],
                        row["nama_wilayah"],
                        row["level"],
                        row["parent_kode"],
                        row["created_at"],
                    ),
                )

            total_rows += len(df_long)
            conn.commit()
            print(f"{sheet_name[0]} selesai ({len(df_long)} rows)")
            print(df_long)

        print(f"\nSemua data berhasil dimasukkan ({total_rows} rows)")

    except Exception as e:
        conn.rollback()
        print("ERROR:", e)

    finally:
        cur.close()
        conn.close()
