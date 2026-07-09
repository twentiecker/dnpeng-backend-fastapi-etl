import pandas as pd
import psycopg2
from datetime import datetime, timezone
import os
import re


def parse_periode(p):
    if not isinstance(p, str):
        p = str(p)

    m = re.match(r"(\d{4})\.M\.(\d{2})", p.strip())
    if m:
        tahun = int(m.group(1))
        period = int(m.group(2))
        return tahun, "M", period

    raise ValueError(f"Tidak bisa parse periode: {p}")


def process_sheet(file_path, sheet_name, deskripsi_prefix):
    print(f"Memproses sheet: {sheet_name} ({deskripsi_prefix})")

    df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)

    header_row = df.iloc[0].copy()
    df_data = df.iloc[1:].copy().reset_index(drop=True)

    period_cols = list(range(1, len(df.columns)))

    df_long = df_data.melt(
        id_vars=[0],
        var_name="col_idx",
        value_vars=period_cols,
        value_name="nilai",
    )

    col_to_tahun = {}
    col_to_period = {}

    for col_idx in period_cols:
        tahun, freq, period = parse_periode(header_row.iloc[col_idx])
        col_to_tahun[col_idx] = tahun
        col_to_period[col_idx] = period

    df_long.rename(columns={0: "kode_wilayah"}, inplace=True)

    df_long["kode_wilayah"] = (
        df_long["kode_wilayah"].astype(str).str.extract(r"(\d+)")[0]
    )

    df_long["nilai"] = df_long["nilai"].astype(str).str.replace(",", "", regex=False)
    df_long["nilai"] = pd.to_numeric(df_long["nilai"], errors="coerce")
    df_long["nilai"] = df_long["nilai"].fillna(0)

    df_long["tahun"] = df_long["col_idx"].map(col_to_tahun)
    df_long["period"] = df_long["col_idx"].map(col_to_period)
    df_long["freq"] = "M"

    df_long["deskripsi"] = deskripsi_prefix
    df_long["kode"] = "M" + sheet_name
    df_long["satuan"] = "kunjungan"
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
    file_path = "data/indikator/regional/54 Wisnus tujuan.xlsx"

    DATABASE_URL = os.getenv(
        "DATABASE_URL", "postgresql://dnpeng:dnpeng7240@localhost:5432/dnpeng_db"
    )

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    try:
        print("Add table PKRT: Wisnus Provinsi Tujuan...")

        sheets = [
            ("WPT", "Wisnus Provinsi Tujuan"),
        ]

        insert_query = """
        INSERT INTO pkrt
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
