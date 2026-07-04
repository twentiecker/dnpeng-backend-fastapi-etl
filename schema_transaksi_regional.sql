CREATE TABLE IF NOT EXISTS transaksi_regional (
    id SERIAL PRIMARY KEY,
    kode_wilayah VARCHAR(10) NOT NULL,
    tahun INT NOT NULL,
    freq VARCHAR(1) NOT NULL,
    period INT NOT NULL,
    nilai BIGINT NOT NULL DEFAULT 0,
    kode VARCHAR(100) NOT NULL,
    deskripsi VARCHAR(50) NOT NULL,
    satuan VARCHAR(50) NOT NULL,
    konversi VARCHAR(50) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (kode_wilayah, tahun, freq, period, deskripsi)
);
