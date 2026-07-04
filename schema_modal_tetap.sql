CREATE TABLE IF NOT EXISTS modal_tetap (
    id SERIAL PRIMARY KEY,
    kode_prov VARCHAR(10) NOT NULL,
    kode_kako VARCHAR(10) NOT NULL,
    status_modal VARCHAR(50) NOT NULL,
    tahap VARCHAR(50) NOT NULL,
    tahun INT NOT NULL,
    freq VARCHAR(1) NOT NULL,
    period INT NOT NULL,
    nilai NUMERIC NOT NULL DEFAULT 0,
    kode VARCHAR(50) NOT NULL,
    deskripsi VARCHAR(100) NOT NULL,
    satuan VARCHAR(50) NOT NULL,
    konversi VARCHAR(50) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (kode_prov, kode_kako, status_modal, tahap, tahun, freq, period, kode)
);
