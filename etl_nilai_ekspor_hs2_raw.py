import pandas as pd
import psycopg2
from datetime import datetime, timezone
import os


def process_sheet(file_path, sheet_name):
    print(f"Memproses sheet: {sheet_name}")

    df = pd.read_excel(file_path, sheet_name=sheet_name, header=0)

    df_grouped = df.groupby(
        ["ORIG2", "K", "HS2", "YEAR", "MTH"], as_index=False
    )["FOB"].sum()

    df_grouped.rename(columns={
        "ORIG2": "provinsi",
        "K": "jenis",
        "HS2": "kode_hs",
        "YEAR": "tahun",
        "MTH": "period",
        "FOB": "nilai",
    }, inplace=True)

    df_grouped["provinsi"] = df_grouped["provinsi"].astype(str)
    df_grouped["jenis"] = df_grouped["jenis"].astype(str)
    df_grouped["kode_hs"] = df_grouped["kode_hs"].astype(str)

    df_grouped["nilai"] = pd.to_numeric(df_grouped["nilai"], errors="coerce")
    df_grouped["nilai"] = df_grouped["nilai"].fillna(0)

    df_grouped["freq"] = "M"
    df_grouped["satuan"] = "USD"
    df_grouped["konversi"] = "SUM"
    df_grouped["created_at"] = datetime.now(timezone.utc)

    return df_grouped[
        ["provinsi", "jenis", "kode_hs", "tahun", "freq", "period", "nilai", "satuan", "konversi", "created_at"]
    ]


if __name__ == "__main__":
    file_path = "regional/hs 2 digit prov/RAW_Ekspor HS 2 Digit Prov dan KabKota.xlsx"

    # DATABASE_URL = os.getenv(
    #     "DATABASE_URL", "postgresql://dnpeng:dnpeng7240@localhost:5432/dnpeng_db"
    # )

    # conn = psycopg2.connect(DATABASE_URL)
    # cur = conn.cursor()

    try:
        print("Truncate table ekspor_hs2...")
        # cur.execute("TRUNCATE TABLE ekspor_hs2 RESTART IDENTITY;")
        # conn.commit()

        sheets = [
            ("2. NP_Asem_TW1-2026_HS2_Prov_As",),
        ]

        # insert_query = """
        # INSERT INTO ekspor_hs2
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
