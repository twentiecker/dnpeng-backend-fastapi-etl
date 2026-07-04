CREATE TABLE IF NOT EXISTS kurs (
    id SERIAL PRIMARY KEY,
    tahun INT NOT NULL,
    freq VARCHAR(1) NOT NULL,
    period INT NOT NULL,
    nilai NUMERIC NOT NULL DEFAULT 0,
    kode VARCHAR(50) NOT NULL,
    deskripsi VARCHAR(100) NOT NULL,
    satuan VARCHAR(50) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tahun, freq, period, kode)
);
