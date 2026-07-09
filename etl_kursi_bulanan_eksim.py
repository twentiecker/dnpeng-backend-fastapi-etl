import pandas as pd
import psycopg2
from datetime import datetime, timezone
import os


def process_sheet(file_path, sheet_name):
    print(f"Memproses sheet: {sheet_name}")

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

    df_long.rename(columns={0: "deskripsi"}, inplace=True)

    df_long["deskripsi"] = df_long["deskripsi"].astype(str).str.strip()
    df_long = df_long[df_long["deskripsi"].notna() & (df_long["deskripsi"] != "nan")]

    df_long["nilai"] = pd.to_numeric(df_long["nilai"], errors="coerce")
    df_long["nilai"] = df_long["nilai"].fillna(0)

    col_to_tahun = {}
    col_to_period = {}
    for col_idx in period_cols:
        h = header_row.iloc[col_idx]
        if isinstance(h, datetime):
            col_to_tahun[col_idx] = h.year
            col_to_period[col_idx] = h.month
        else:
            try:
                h_parsed = pd.to_datetime(h)
                col_to_tahun[col_idx] = h_parsed.year
                col_to_period[col_idx] = h_parsed.month
            except Exception:
                col_to_tahun[col_idx] = None
                col_to_period[col_idx] = None

    df_long["tahun"] = df_long["col_idx"].map(col_to_tahun)
    df_long["period"] = df_long["col_idx"].map(col_to_period)
    df_long["freq"] = "M"

    df_long["kode"] = df_long["freq"] + df_long["deskripsi"].apply(
        lambda x: "".join([w[0].upper() for w in str(x).split() if w])
    )
    df_long["satuan"] = "rupiah"
    df_long["created_at"] = datetime.now(timezone.utc)

    return df_long[
        [
            "tahun",
            "freq",
            "period",
            "nilai",
            "kode",
            "deskripsi",
            "satuan",
            "created_at",
        ]
    ]


if __name__ == "__main__":
    file_path = "data/indikator/regional/Kursi bulanan eksim.xlsx"

    # DATABASE_URL = os.getenv(
    #     "DATABASE_URL", "postgresql://dnpeng:dnpeng7240@localhost:5432/dnpeng_db"
    # )

    # conn = psycopg2.connect(DATABASE_URL)
    # cur = conn.cursor()

    try:
        print("Truncate table kurs...")
        # cur.execute("TRUNCATE TABLE kurs RESTART IDENTITY;")
        # conn.commit()

        sheets = [
            ("Data Dasar SM (US$)",),
        ]

        # insert_query = """
        # INSERT INTO kurs
        # (tahun, freq, period, nilai, kode, deskripsi, satuan, created_at)
        # VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        # """

        total_rows = 0

        for sheet_name in sheets:
            df_long = process_sheet(file_path, sheet_name[0])

            # for _, row in df_long.iterrows():
            #     cur.execute(
            #         insert_query,
            #         (
            #             int(row["tahun"]),
            #             row["freq"],
            #             int(row["period"]),
            #             float(row["nilai"]),
            #             row["kode"],
            #             row["deskripsi"],
            #             row["satuan"],
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
