import pandas as pd
import psycopg2
from datetime import datetime, timezone
import os
import re


def parse_periode(p):
    if not isinstance(p, str):
        p = str(p)

    p = p.strip()
    m = re.match(r'(\d{4})\s*Q([1-4])', p, re.IGNORECASE)
    if m:
        tahun = int(m.group(1))
        period = int(m.group(2))
        return tahun, "Q", period

    raise ValueError(f"Tidak bisa parse periode: {p}")


def process_sheet(file_path, sheet_name):
    print(f"Memproses sheet: {sheet_name}")

    df = pd.read_excel(file_path, sheet_name=sheet_name, header=0)

    id_vars = ["KdProv", "Kode Kabkota", "Status Penanaman Modal", "Tahap Konstruksi", "Jenis"]
    period_cols = [c for c in df.columns if c not in id_vars]

    df_long = df.melt(
        id_vars=id_vars,
        var_name="periode",
        value_vars=period_cols,
        value_name="nilai",
    )

    df_long.rename(columns={
        "KdProv": "kode_prov",
        "Kode Kabkota": "kode_kako",
        "Status Penanaman Modal": "status_modal",
        "Tahap Konstruksi": "tahap",
        "Jenis": "deskripsi",
    }, inplace=True)

    df_long["kode_prov"] = df_long["kode_prov"].astype(str).str.extract(r"(\d+)")[0]
    df_long["kode_kako"] = df_long["kode_kako"].astype(str).str.extract(r"(\d+)")[0].str.zfill(2)

    df_long = df_long[df_long["kode_prov"].notna() & df_long["kode_kako"].notna()]

    df_long["nilai"] = pd.to_numeric(df_long["nilai"], errors="coerce")
    df_long["nilai"] = df_long["nilai"].fillna(0)

    df_long[["tahun", "freq", "period"]] = df_long["periode"].apply(
        lambda x: pd.Series(parse_periode(x))
    )

    df_long["kode"] = df_long["freq"] + "-" + df_long["deskripsi"].apply(lambda x: "".join([w[0].upper() for w in str(x).replace("_", " ").replace("/", " ").replace("&", " ").split() if w]))
    df_long["satuan"] = "triliun rupiah"
    df_long["konversi"] = "SUM"
    df_long["created_at"] = datetime.now(timezone.utc)

    return df_long[
        ["kode_prov", "kode_kako", "status_modal", "tahap", "tahun", "freq", "period", "nilai", "kode", "deskripsi", "satuan", "konversi", "created_at"]
    ]


if __name__ == "__main__":
    file_path = "regional/30-44 Modal tetap .xlsx"

    # DATABASE_URL = os.getenv(
    #     "DATABASE_URL", "postgresql://dnpeng:dnpeng7240@localhost:5432/dnpeng_db"
    # )

    # conn = psycopg2.connect(DATABASE_URL)
    # cur = conn.cursor()

    try:
        print("Truncate table modal_tetap...")
        # cur.execute("TRUNCATE TABLE modal_tetap RESTART IDENTITY;")
        # conn.commit()

        sheets = [
            ("Breakdown",),
        ]

        # insert_query = """
        # INSERT INTO modal_tetap
        # (kode_prov, kode_kako, status_modal, tahap, jenis, tahun, freq, period, nilai, kode, deskripsi, satuan, konversi, created_at)
        # VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        # """

        total_rows = 0

        for sheet_name in sheets:
            df_long = process_sheet(file_path, sheet_name[0])

            # for _, row in df_long.iterrows():
            #     cur.execute(
            #         insert_query,
            #         (
            #             row["kode_prov"],
            #             row["kode_kako"],
            #             row["status_modal"],
            #             row["tahap"],
            #             row["jenis"],
            #             int(row["tahun"]),
            #             row["freq"],
            #             int(row["period"]),
            #             float(row["nilai"]),
            #             row["kode"],
            #             row["deskripsi"],
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
