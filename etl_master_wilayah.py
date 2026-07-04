import pandas as pd
import psycopg2
from datetime import datetime, timezone
import os


def process_sheet(file_path, sheet_name):
    print(f"Memproses sheet: {sheet_name}")

    df = pd.read_excel(file_path, sheet_name=sheet_name, header=0)

    df.rename(columns={
        df.columns[0]: "kode_prov",
        df.columns[1]: "nama_prov",
        df.columns[2]: "kode_kako",
        df.columns[3]: "nama_kako",
    }, inplace=True)

    df["kode_prov"] = df["kode_prov"].astype(str).str.strip()
    df["nama_prov"] = df["nama_prov"].astype(str).str.strip()
    df["kode_kako"] = df["kode_kako"].astype(str).str.strip().str.zfill(2)
    df["nama_kako"] = df["nama_kako"].astype(str).str.strip()

    df = df[df["kode_prov"].notna() & df["kode_kako"].notna()]
    df = df[df["kode_prov"].str.match(r"^\d+$")]
    df = df[df["kode_kako"].str.match(r"^\d+$")]

    df["created_at"] = datetime.now(timezone.utc)

    return df[["kode_prov", "nama_prov", "kode_kako", "nama_kako", "created_at"]]


if __name__ == "__main__":
    file_path = "regional/Tabulasi MFD 2023 sem 1.xlsx"

    # DATABASE_URL = os.getenv(
    #     "DATABASE_URL", "postgresql://dnpeng:dnpeng7240@localhost:5432/dnpeng_db"
    # )

    # conn = psycopg2.connect(DATABASE_URL)
    # cur = conn.cursor()

    try:
        print("Truncate table master_wilayah...")
        # cur.execute("TRUNCATE TABLE master_wilayah RESTART IDENTITY;")
        # conn.commit()

        sheets = [
            ("Kabupaten",),
        ]

        # insert_query = """
        # INSERT INTO master_wilayah
        # (kode_prov, nama_prov, kode_kako, nama_kako, created_at)
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
            #             row["nama_prov"],
            #             row["kode_kako"],
            #             row["nama_kako"],
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
