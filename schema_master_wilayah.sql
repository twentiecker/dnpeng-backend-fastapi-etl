CREATE TABLE IF NOT EXISTS master_wilayah (
    id SERIAL PRIMARY KEY,
    kode_prov VARCHAR(10) NOT NULL,
    nama_prov VARCHAR(100) NOT NULL,
    kode_kako VARCHAR(10) NOT NULL,
    nama_kako VARCHAR(100) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (kode_prov, kode_kako)
);
