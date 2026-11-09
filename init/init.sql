CREATE TABLE IF NOT EXISTS processos_juridicos (
    id SERIAL PRIMARY KEY,
    numero_processo VARCHAR(50) UNIQUE NOT NULL,
    cpf_autor VARCHAR(20),
    nome_autor VARCHAR(255),
    valor_causa NUMERIC(15, 2),
    tempo_tramitacao INT,
    tipo_processo VARCHAR(50),
    numero_recursos_anteriores INT,
    risco_recurso VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);