import pandas as pd
import psycopg2
from datetime import datetime, timezone
import os
import re

PROV_MAP = {
    "Aceh": "11",
    "Sumatera Utara": "12",
    "Sumatera Barat": "13",
    "Riau": "14",
    "Jambi": "15",
    "Sumatera Selatan": "16",
    "Bengkulu": "17",
    "Lampung": "18",
    "Kepulauan Bangka Belitung": "19",
    "Kepulauan Riau": "21",
    "Daerah Khusus Ibukota Jakarta": "31",
    "Jawa Barat": "32",
    "Jawa Tengah": "33",
    "Daerah Istimewa Yogyakarta": "34",
    "Jawa Timur": "35",
    "Banten": "36",
    "Bali": "51",
    "Nusa Tenggara Barat": "52",
    "Nusa Tenggara Timur": "53",
    "Kalimantan Barat": "61",
    "Kalimantan Tengah": "62",
    "Kalimantan Selatan": "63",
    "Kalimantan Timur": "64",
    "Kalimantan Utara": "65",
    "Sulawesi Utara": "71",
    "Sulawesi Tengah": "72",
    "Sulawesi Selatan": "73",
    "Sulawesi Tenggara": "74",
    "Gorontalo": "75",
    "Sulawesi Barat": "76",
    "Maluku": "81",
    "Maluku Utara": "82",
    "Papua Barat": "91",
    "Papua Barat Daya": "92",
    "Papua": "94",
    "Papua Selatan": "95",
    "Papua Tengah": "96",
    "Papua Pegunungan": "97",
    "INDONESIA": "00",
}


def parse_periode(p):
    if not isinstance(p, str):
        p = str(p)

    p = p.strip()
    m = re.match(r"(\d{4})\s*-\s*Q([1-4])", p, re.IGNORECASE)
    if m:
        tahun = int(m.group(1))
        period = int(m.group(2))
        return tahun, "Q", period

    raise ValueError(f"Tidak bisa parse periode: {p}")


def process_sheet(file_path, sheet_name, deskripsi_prefix):
    print(f"Memproses sheet: {sheet_name} ({deskripsi_prefix})")

    df = pd.read_excel(file_path, sheet_name=sheet_name, header=0)

    header_row = df.columns

    period_cols = [c for c in header_row if c != "Provinsi"]

    df_long = df.melt(
        id_vars=["Provinsi"],
        var_name="periode",
        value_vars=period_cols,
        value_name="nilai",
    )

    df_long.rename(columns={"Provinsi": "provinsi"}, inplace=True)

    df_long["nilai"] = df_long["nilai"].astype(str).str.replace(",", "", regex=False)
    df_long["nilai"] = pd.to_numeric(df_long["nilai"], errors="coerce")
    df_long["nilai"] = df_long["nilai"].fillna(0)

    df_long[["tahun", "freq", "period"]] = df_long["periode"].apply(
        lambda x: pd.Series(parse_periode(x))
    )

    df_long["kode_wilayah"] = df_long["provinsi"].map(PROV_MAP)
    df_long = df_long[df_long["kode_wilayah"].notna()]

    df_long["kode"] = "Q" + sheet_name
    df_long["deskripsi"] = deskripsi_prefix
    df_long["satuan"] = "juta rupiah"
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
    file_path = "data/indikator/regional/24-26 Realisasi PMA PDMN BKPM 2010-2026.xlsx"

    DATABASE_URL = os.getenv(
        "DATABASE_URL", "postgresql://dnpeng:dnpeng7240@localhost:5432/dnpeng_db"
    )

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    try:
        print("Add table PMTB: PMA dan PMDN...")

        sheets = [
            ("PMA", "PMA"),
            ("PMDN", "PMDN"),
        ]

        insert_query = """
        INSERT INTO pmtb
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
