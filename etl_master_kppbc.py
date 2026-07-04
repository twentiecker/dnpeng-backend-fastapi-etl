import pandas as pd
import psycopg2
from datetime import datetime, timezone
import os


def process_sheet(file_path, sheet_name):
    print(f"Memproses sheet: {sheet_name}")

    df = pd.read_excel(file_path, sheet_name=sheet_name, header=0)

    df.rename(columns={
        "prov_kppbc": "kode_prov",
        "kppbc": "kode_kppbc",
        "nama_kppbc": "nama_kppbc",
        "kod_kab": "kode_kako",
    }, inplace=True)

    df["kode_prov"] = df["kode_prov"].astype(str).str.strip()
    df["kode_kppbc"] = df["kode_kppbc"].astype(str).str.strip()
    df["nama_kppbc"] = df["nama_kppbc"].astype(str).str.strip()
    df["kode_kako"] = df["kode_kako"].apply(lambda x: str(int(x)) if pd.notna(x) else None)

    df["created_at"] = datetime.now(timezone.utc)

    return df[["kode_prov", "kode_kppbc", "nama_kppbc", "kode_kako", "created_at"]]


if __name__ == "__main__":
    file_path = "regional/RAW_Impor_master_kppbc.xlsx"

    # DATABASE_URL = os.getenv(
    #     "DATABASE_URL", "postgresql://dnpeng:dnpeng7240@localhost:5432/dnpeng_db"
    # )

    # conn = psycopg2.connect(DATABASE_URL)
    # cur = conn.cursor()

    try:
        print("Truncate table master_kppbc...")
        # cur.execute("TRUNCATE TABLE master_kppbc RESTART IDENTITY;")
        # conn.commit()

        sheets = [
            ("master_kppbc",),
        ]

        # insert_query = """
        # INSERT INTO master_kppbc
        # (kode_prov, kode_kppbc, nama_kppbc, kode_kako, created_at)
        # VALUES (%s,%s,%s,%s,%s)
        # """

        total_rows = 0

        for sheet_name in sheets:
            df_long = process_sheet(file_path, sheet_name[0])

            # for _, row in df_long.iterrows():
            #     cur.execute(
            #         insert_query,
            #         (
            #             row["kode_prov"],
            #             row["kode_kppbc"],
            #             row["nama_kppbc"],
            #             row["kode_kako"],
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
