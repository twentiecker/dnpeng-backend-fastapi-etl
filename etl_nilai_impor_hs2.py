import pandas as pd
import psycopg2
from datetime import datetime, timezone
import os


def process_sheet(file_path, sheet_name):
    print(f"Memproses sheet: {sheet_name}")

    df = pd.read_excel(file_path, sheet_name=sheet_name, header=0)

    id_vars = ["ID", "Provinsi", "Jenis", "HS"]
    period_cols = [c for c in df.columns if c not in id_vars]

    df_long = df.melt(
        id_vars=id_vars,
        var_name="periode",
        value_vars=period_cols,
        value_name="nilai",
    )

    df_long.rename(columns={
        "Provinsi": "provinsi",
        "Jenis": "jenis",
        "HS": "kode_hs",
    }, inplace=True)

    df_long["provinsi"] = df_long["provinsi"].astype(str)
    df_long["jenis"] = df_long["jenis"].astype(str)
    df_long["kode_hs"] = df_long["kode_hs"].astype(str).replace("Migas", "27")

    df_long["nilai"] = pd.to_numeric(df_long["nilai"], errors="coerce")
    df_long["nilai"] = df_long["nilai"].fillna(0)

    df_long["periode"] = pd.to_datetime(df_long["periode"], errors="coerce")
    df_long["tahun"] = df_long["periode"].dt.year
    df_long["period"] = df_long["periode"].dt.month

    df_long["freq"] = "M"
    df_long["satuan"] = "USD"
    df_long["konversi"] = "SUM"
    df_long["created_at"] = datetime.now(timezone.utc)

    return df_long[
        ["provinsi", "jenis", "kode_hs", "tahun", "freq", "period", "nilai", "satuan", "konversi", "created_at"]
    ]


if __name__ == "__main__":
    file_path = "regional/hs 2 digit prov/1b. Nilai Impor Provinsi HS 2 Digit 2026Q1_29042026.xlsx"

    # DATABASE_URL = os.getenv(
    #     "DATABASE_URL", "postgresql://dnpeng:dnpeng7240@localhost:5432/dnpeng_db"
    # )

    # conn = psycopg2.connect(DATABASE_URL)
    # cur = conn.cursor()

    try:
        print("Truncate table impor_hs2...")
        # cur.execute("TRUNCATE TABLE impor_hs2 RESTART IDENTITY;")
        # conn.commit()

        sheets = [
            ("Rekap",),
        ]

        # insert_query = """
        # INSERT INTO impor_hs2
        # (provinsi, jenis, kode_hs, tahun, freq, period, nilai, satuan, konversi, created_at)
        # VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        # """

        total_rows = 0

        for sheet_name in sheets:
            df_long = process_sheet(file_path, sheet_name[0])

            # for _, row in df_long.iterrows():
            #     cur.execute(
            #         insert_query,
            #         (
            #             row["provinsi"],
            #             row["jenis"],
            #             row["kode_hs"],
            #             int(row["tahun"]),
            #             row["freq"],
            #             int(row["period"]),
            #             row["nilai"],
            #             row["satuan"],
            #             row["konversi"],
            #             row["created_at"],
            #         ),
            #     )

            total_rows += len(df_long)
            # conn.commit()
            print(f"{sheet_name[0]} selesai ({len(df_long)} rows)")
            print(df_long)

        print(f"\nSemua data berhasil dimasukkan ({total_rows} rows)")

    except Exception as e:
        # conn.rollback()
        print("ERROR:", e)

    # finally:
    #     cur.close()
    #     conn.close()
