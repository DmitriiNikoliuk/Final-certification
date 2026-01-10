-- Создание базы данных
CREATE DATABASE datacenter_db
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.UTF-8'
    LC_CTYPE = 'en_US.UTF-8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;

-- Подключение к созданной базе данных
\c datacenter_db;

-- Создание таблицы гипервизоров (кластер подразумевается один)
CREATE TABLE hypervisors (
    hv_name VARCHAR(50) PRIMARY KEY,
    cpu INTEGER NOT NULL CHECK (cpu > 0),
    ram INTEGER NOT NULL CHECK (ram > 0),
    free_cpu INTEGER NOT NULL CHECK (free_cpu >= 0 AND free_cpu <= cpu),
    free_ram INTEGER NOT NULL CHECK (free_ram >= 0 AND free_ram <= ram),
    num_vms INTEGER DEFAULT 0 CHECK (num_vms >= 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT check_min_resources CHECK (free_cpu >= cpu * 0.1 AND free_ram >= ram * 0.1)
);

-- Создание таблицы виртуальных машин
CREATE TABLE virtual_machines (
    vm_name VARCHAR(50) PRIMARY KEY,
    vcpu INTEGER NOT NULL CHECK (vcpu BETWEEN 2 AND 24 AND vcpu % 2 = 0),
    vram INTEGER NOT NULL CHECK (vram BETWEEN 4 AND 128),
    vhdd INTEGER NOT NULL CHECK (vhdd BETWEEN 40 AND 4096),
    hv_name VARCHAR(50) NOT NULL,
    creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (hv_name) REFERENCES hypervisors(hv_name) ON DELETE CASCADE
);

-- Таблица конфигурации кластера (кластер подразумевается один)
CREATE TABLE cluster_config (
    config_key VARCHAR(50) PRIMARY KEY,
    config_value VARCHAR(200),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Инициализация конфигурации кластера
INSERT INTO cluster_config (config_key, config_value) VALUES
    ('cluster_name', 'Moscow_Cluster'),
    ('disk_pool', '1000000'),
    ('overcommit_cpu', '3.0'),
    ('overcommit_ram', '1.0'),
    ('max_hypervisors', '24')
ON CONFLICT (config_key) DO NOTHING;

-- Индексы для улучшения производительности
CREATE INDEX idx_vm_hv_name ON virtual_machines(hv_name);
CREATE INDEX idx_vm_creation_date ON virtual_machines(creation_date);