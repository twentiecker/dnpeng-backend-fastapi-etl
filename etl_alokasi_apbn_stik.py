import pandas as pd
import psycopg2
from datetime import datetime, timezone
import os
import re


def parse_periode(p):
    if not isinstance(p, str):
        p = str(p)

    p = p.strip()
    m = re.match(r"(\d{4})Q([1-4])", p, re.IGNORECASE)
    if m:
        tahun = int(m.group(1))
        period = int(m.group(2))
        return tahun, "Q", period

    raise ValueError(f"Tidak bisa parse periode: {p}")


def process_sheet(file_path, sheet_name):
    print(f"Memproses sheet: {sheet_name}")

    df = pd.read_excel(file_path, sheet_name=sheet_name, header=0)

    df_long = df.melt(
        id_vars=["Kode BPS", "Urut APBN"],
        var_name="periode",
        value_name="nilai",
    )

    df_long.rename(
        columns={
            "Kode BPS": "kode_wilayah",
            "Urut APBN": "rank_apbn",
        },
        inplace=True,
    )

    df_long["kode_wilayah"] = (
        df_long["kode_wilayah"].astype(str).str.extract(r"(\d+)")[0]
    )
    df_long = df_long[df_long["kode_wilayah"].notna()]

    df_long["nilai"] = pd.to_numeric(df_long["nilai"], errors="coerce")
    df_long["nilai"] = df_long["nilai"].fillna(0)

    df_long[["tahun", "freq", "period"]] = df_long["periode"].apply(
        lambda x: pd.Series(parse_periode(x))
    )

    df_long["kode"] = df_long["freq"] + "STIK"
    df_long["deskripsi"] = "Alokasi APBN STIK MBG"
    df_long["satuan"] = "Juta Rupiah"
    df_long["konversi"] = "UM"
    df_long["created_at"] = datetime.now(timezone.utc)

    return df_long[
        [
            "kode_wilayah",
            "rank_apbn",
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
    file_path = (
        "data/indikator/regional/18 Alokasi APBN - stik mbg 2025q1 sd 2026q1.xlsx"
    )

    # DATABASE_URL = os.getenv(
    #     "DATABASE_URL", "postgresql://dnpeng:dnpeng7240@localhost:5432/dnpeng_db"
    # )

    # conn = psycopg2.connect(DATABASE_URL)
    # cur = conn.cursor()

    try:
        print("Add table PKP: Alokasi APBN STIK...")

        sheets = [
            ("STIK",),
        ]

        # insert_query = """
        # INSERT INTO pkp
        # (kode_wilayah, rank_apbn, tahun, freq, period, nilai, kode, deskripsi, satuan, konversi, created_at)
        # VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        # """

        total_rows = 0

        for sheet_name in sheets:
            df_long = process_sheet(file_path, sheet_name[0])

            # for _, row in df_long.iterrows():
            #     cur.execute(
            #         insert_query,
            #         (
            #             row["kode_wilayah"],
            #             int(row["rank_apbn"]),
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
