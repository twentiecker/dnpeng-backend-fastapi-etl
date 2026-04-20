import pandas as pd
import psycopg2
from datetime import datetime, timezone
import os


# =========================
# Helper: parse periode
# =========================
def parse_periode(p):
    p = str(p).strip()

    tahun = int(p[:4])
    freq = p[4]  # Q atau M
    period = int(p[5:])

    return tahun, freq, period


# =========================
# Helper: proses sheet
# =========================
def process_sheet(file_path, sheet_name):
    print(f"Memproses sheet: {sheet_name}")

    df = pd.read_excel(file_path, sheet_name=sheet_name)

    # Wide -> Long
    df_long = df.melt(
        id_vars=["Kode", "Deskripsi", "Satuan", "Konversi"],
        var_name="periode",
        value_name="nilai",
    )

    # Rename kolom
    df_long.rename(
        columns={
            "Kode": "kode",
            "Deskripsi": "deskripsi",
            "Satuan": "satuan",
            "Konversi": "konversi",
        },
        inplace=True,
    )

    # Bersihkan nilai
    df_long["nilai"] = df_long["nilai"].astype(str).str.replace(",", "", regex=False)

    df_long["nilai"] = pd.to_numeric(df_long["nilai"], errors="coerce")
    df_long["nilai"] = df_long["nilai"].fillna(0)

    # Parse periode
    df_long[["tahun", "freq", "period"]] = df_long["periode"].apply(
        lambda x: pd.Series(parse_periode(x))
    )

    df_long["created_at"] = datetime.now(timezone.utc)

    return df_long


# =========================
# Main ETL
# =========================
file_path = "pmtb_final_input.xlsx"

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://dnpeng:dnpeng7240@localhost:5432/dnpeng_db"
)

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

try:
    # =========================
    # 1. Truncate table + reset id
    # =========================
    print("Truncate table pmtb...")
    cur.execute("TRUNCATE TABLE pmtb RESTART IDENTITY;")
    conn.commit()

    # =========================
    # 2. Proses semua sheet otomatis
    # =========================
    all_sheets = ["Bulanan", "Triwulanan"]

    insert_query = """
    INSERT INTO pmtb
    (kode, deskripsi, tahun, freq, period, nilai, created_at, satuan, konversi)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    total_rows = 0

    for sheet in all_sheets:
        df_long = process_sheet(file_path, sheet)

        for _, row in df_long.iterrows():
            cur.execute(
                insert_query,
                (
                    row["kode"],
                    row["deskripsi"],
                    row["tahun"],
                    row["freq"],
                    row["period"],
                    row["nilai"],
                    row["created_at"],
                    row["satuan"],
                    row["konversi"],
                ),
            )

        total_rows += len(df_long)
        conn.commit()
        print(f"{sheet} selesai ({len(df_long)} rows)")

    print(f"\nSemua data berhasil dimasukkan ({total_rows} rows)")

except Exception as e:
    conn.rollback()
    print("ERROR:", e)

finally:
    cur.close()
    conn.close()
