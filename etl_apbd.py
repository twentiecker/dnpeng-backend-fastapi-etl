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

DESKRIPSI_MAP = {
    "Belanja Pegawai": "BP",
    "Belanja Barang dan jasa": "BBJ",
    "Belanja Modal": "BM",
    "Belanja Bantuan Sosial": "BBS",
    "Belanja Tidak Terduga": "BTT",
    "Retribusi daerah": "RD",
    # "Belanja Tak Terduga": "BTT",
}

# Kanonisasi deskripsi agar varian penulisan yang sama tidak muncul
# sebagai dua nama berbeda (mis. "Belanja Tidak Terduga" / "Belanja Tak Terduga")
DESKRIPSI_CANON = {
    "Belanja Tak Terduga": "Belanja Tidak Terduga",
}


def parse_periode(p):
    if not isinstance(p, str):
        p = str(p)

    p = p.strip()
    m = re.search(r"Q([1-4])/(\d{4})", p, re.IGNORECASE)
    if m:
        period = int(m.group(1))
        tahun = int(m.group(2))
        return tahun, "Q", period

    raise ValueError(f"Tidak bisa parse periode: {p}")


def process_sheet(file_path, sheet_name):
    print(f"Memproses sheet: {sheet_name}")

    df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)

    header_periode = df.iloc[0].copy()
    header_deskripsi = df.iloc[1].copy()

    header_periode = header_periode.ffill()

    df_data = df.iloc[2:].copy().reset_index(drop=True)

    period_info = []
    for col_idx in range(4, len(df.columns)):
        periode_name = header_periode.iloc[col_idx]
        deskripsi_name = header_deskripsi.iloc[col_idx]

        if pd.isna(periode_name) or pd.isna(deskripsi_name):
            continue

        try:
            tahun, freq, period = parse_periode(str(periode_name))
            deskripsi = str(deskripsi_name).strip()
            period_info.append(
                {
                    "col_idx": col_idx,
                    "tahun": tahun,
                    "freq": freq,
                    "period": period,
                    "deskripsi": deskripsi,
                }
            )
        except ValueError:
            continue

    id_vars = [1, 3]
    value_vars = [info["col_idx"] for info in period_info]

    df_long = df_data.melt(
        id_vars=id_vars,
        var_name="col_idx",
        value_vars=value_vars,
        value_name="nilai",
    )

    df_long.rename(columns={1: "kode_wilayah", 3: "provinsi"}, inplace=True)

    df_long["kode_wilayah"] = (
        df_long["kode_wilayah"].astype(str).str.extract(r"(\d+)")[0]
    )
    df_long = df_long[df_long["kode_wilayah"].notna()]

    df_long["nilai"] = df_long["nilai"].astype(str).str.replace(",", "", regex=False)
    df_long["nilai"] = pd.to_numeric(df_long["nilai"], errors="coerce")
    df_long["nilai"] = df_long["nilai"].fillna(0)

    col_to_info = {info["col_idx"]: info for info in period_info}
    df_long["tahun"] = df_long["col_idx"].map(lambda x: col_to_info[x]["tahun"])
    df_long["freq"] = df_long["col_idx"].map(lambda x: col_to_info[x]["freq"])
    df_long["period"] = df_long["col_idx"].map(lambda x: col_to_info[x]["period"])
    df_long["deskripsi"] = df_long["col_idx"].map(lambda x: col_to_info[x]["deskripsi"])
    df_long["deskripsi"] = df_long["deskripsi"].str.strip().replace(DESKRIPSI_CANON)

    df_long["kode"] = df_long["freq"] + df_long["deskripsi"].map(DESKRIPSI_MAP)
    df_long["satuan"] = "Rupiah"
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
    file_path = "data/indikator/regional/19-23 Data APBD (Sitralis 2022-2026).xlsx"

    DATABASE_URL = os.getenv(
        "DATABASE_URL", "postgresql://dnpeng:dnpeng7240@localhost:5432/dnpeng_db"
    )

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    try:
        print("Add table PKP: APBD...")

        sheets = [
            ("Rekap APBD Total Triwulanan",),
        ]

        insert_query = """
        INSERT INTO pkp
        (kode_wilayah, tahun, freq, period, nilai, kode, deskripsi, satuan, konversi, created_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """

        total_rows = 0

        for sheet_name in sheets:
            df_long = process_sheet(file_path, sheet_name[0])

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
            print(f"{sheet_name[0]} selesai ({len(df_long)} rows)")
            print(df_long)

        print(f"\nSemua data berhasil dimasukkan ({total_rows} rows)")

    except Exception as e:
        conn.rollback()
        print("ERROR:", e)

    finally:
        cur.close()
        conn.close()
