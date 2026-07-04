CREATE TABLE IF NOT EXISTS master_kppbc (
    id SERIAL PRIMARY KEY,
    kode_prov VARCHAR(10) NOT NULL,
    kode_kppbc VARCHAR(20) NOT NULL,
    nama_kppbc VARCHAR(100) NOT NULL,
    kode_kako VARCHAR(10),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (kode_kppbc, kode_kako)
);
