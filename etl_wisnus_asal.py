import pandas as pd
import psycopg2
from datetime import datetime, timezone
import os
import re


def parse_periode(p):
    if not isinstance(p, str):
        p = str(p)

    m = re.match(r'(\d{4})\.M\.(\d{2})', p.strip())
    if m:
        tahun = int(m.group(1))
        period = int(m.group(2))
        return tahun, "M", period

    raise ValueError(f"Tidak bisa parse periode: {p}")


def process_sheet(file_path, sheet_name, deskripsi):
    print(f"Memproses sheet: {sheet_name} ({deskripsi})")

    df = pd.read_excel(file_path, sheet_name=sheet_name)

    period_cols = [c for c in df.columns if c != "Provinsi"]

    df_long = df.melt(
        id_vars=["Provinsi"],
        var_name="periode",
        value_vars=period_cols,
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
    df_long["satuan"] = "kunjungan"
    df_long["konversi"] = "SUM"
    df_long["created_at"] = datetime.now(timezone.utc)

    return df_long[
        ["kode_wilayah", "tahun", "freq", "period", "nilai", "kode", "deskripsi", "satuan", "konversi", "created_at"]
    ]


if __name__ == "__main__":
    file_path = "regional/7 Wisnus Asal.xlsx"

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
            ("WA", "Wisnus Asal"),
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
