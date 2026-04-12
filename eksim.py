import pandas as pd
import psycopg2
from datetime import datetime, timezone
import os


# =========================
# Helper: parse periode
# =========================
def parse_periode(p):
    p = str(p)

    tahun = int(p[:4])
    freq = p[4]  # Q atau M
    period = int(p[5:])  # 1-4 atau 1-12

    return tahun, freq, period


# =========================
# 1. Baca Excel
# =========================
file_path = "eksim_final.xlsx"

df = pd.read_excel(file_path, sheet_name="Bulanan")

# =========================
# 2. Wide → Long
# =========================
df_long = df.melt(
    id_vars=["Kode", "Deskripsi", "Satuan", "Konversi"],
    var_name="periode",
    value_name="nilai",
)

df_long.rename(
    columns={
        "Kode": "kode",
        "Deskripsi": "deskripsi",
        "Satuan": "satuan",
        "Konversi": "konversi",
    },
    inplace=True,
)

# =========================
# 3. Bersihkan nilai
# =========================
df_long["nilai"] = (
    df_long["nilai"].astype(str).str.replace(",", "")  # hilangkan koma ribuan
)

df_long["nilai"] = pd.to_numeric(df_long["nilai"], errors="coerce")
df_long["nilai"] = df_long["nilai"].fillna(0)

# =========================
# 4. Parse periode
# =========================
df_long[["tahun", "freq", "period"]] = df_long["periode"].apply(
    lambda x: pd.Series(parse_periode(x))
)

df_long["created_at"] = datetime.now(timezone.utc)

print(df_long.head())

# =========================
# 5. Koneksi PostgreSQL
# =========================
DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://dnpeng:dnpeng7240@localhost:5432/dnpeng_db"
)
conn = psycopg2.connect(DATABASE_URL)

cur = conn.cursor()

# =========================
# 6. Insert ke DB
# =========================
insert_query = """
INSERT INTO eksim
(kode, deskripsi, tahun, freq, period, nilai, created_at, satuan, konversi)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (kode, tahun, freq, period)
DO UPDATE SET
    nilai = EXCLUDED.nilai
"""

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

conn.commit()

cur.close()
conn.close()

print("Data berhasil dimasukkan ke PostgreSQL")
