-- TN-MBSE 2025 – FINAL REAL-WORLD SCHEMA (MULTIPLE EMITTERS ALLOWED)

DROP TABLE IF EXISTS FluxConsumptions;
DROP TABLE IF EXISTS FluxEmissions;
DROP TABLE IF EXISTS Functions;
DROP TABLE IF EXISTS Fluxes;
DROP TABLE IF EXISTS Subsystems;

-- 1. Subsystems
CREATE TABLE Subsystems (
    id   INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(200) UNIQUE NOT NULL
);

-- 2. Functions – SAME NAME ALLOWED IN DIFFERENT SUBSYSTEMS
CREATE TABLE Functions (
    id           INT AUTO_INCREMENT PRIMARY KEY,
    fct_tag      VARCHAR(150) NOT NULL,
    subsystem_id INT NOT NULL,
    source_file  VARCHAR(255),
    source_row   INT,
    FOREIGN KEY (subsystem_id) REFERENCES Subsystems(id) ON DELETE CASCADE,
    UNIQUE KEY uq_func_ss (fct_tag, subsystem_id)   -- optional: no duplicate in same SS
);

-- 3. Fluxes – pure data carriers
CREATE TABLE Fluxes (
    id   INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(300) NOT NULL UNIQUE
);

-- 4. EMISSIONS – MULTIPLE EMITTERS ALLOWED (REAL LIFE!)
CREATE TABLE FluxEmissions (
    id              INT AUTO_INCREMENT PRIMARY KEY,
    flux_id         INT NOT NULL,
    emitter_func_id INT NOT NULL,
    source_file     VARCHAR(255),
    source_row      INT,
    FOREIGN KEY (flux_id)         REFERENCES Fluxes(id)         ON DELETE CASCADE,
    FOREIGN KEY (emitter_func_id) REFERENCES Functions(id)      ON DELETE RESTRICT,
    UNIQUE KEY uq_one_per_func (flux_id, emitter_func_id)   -- one function emits it only once
);

-- 5. CONSUMPTIONS – many allowed
CREATE TABLE FluxConsumptions (
    id               INT AUTO_INCREMENT PRIMARY KEY,
    flux_id          INT NOT NULL,
    consumer_func_id INT NOT NULL,
    source_file      VARCHAR(255),
    source_row       INT,
    FOREIGN KEY (flux_id)          REFERENCES Fluxes(id)          ON DELETE CASCADE,
    FOREIGN KEY (consumer_func_id) REFERENCES Functions(id)       ON DELETE CASCADE,
    UNIQUE KEY uq_cons (flux_id, consumer_func_id)
);