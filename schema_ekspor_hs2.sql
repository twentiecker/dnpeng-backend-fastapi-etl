CREATE TABLE IF NOT EXISTS ekspor_hs2 (
    id SERIAL PRIMARY KEY,
    provinsi VARCHAR(50) NOT NULL,
    jenis VARCHAR(50) NOT NULL,
    kode_hs VARCHAR(20) NOT NULL,
    tahun INT NOT NULL,
    freq VARCHAR(1) NOT NULL,
    period INT NOT NULL,
    nilai BIGINT NOT NULL DEFAULT 0,
    satuan VARCHAR(50) NOT NULL DEFAULT 'rupiah',
    konversi VARCHAR(50) NOT NULL DEFAULT 'SUM',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (provinsi, jenis, kode_hs, tahun, freq, period)
);
