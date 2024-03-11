CREATE TABLE IF NOT EXISTS assets (
    asset_id INT,
    asset_name VARCHAR(255),
    asset_symbol VARCHAR(255),
    website VARCHAR(255),
    sector VARCHAR(255),
    PRIMARY KEY (asset_id)
);

CREATE TABLE IF NOT EXISTS info (
    info_id INT,
    asset_id INT,
    date DATE,
    price FLOAT,
    volume INT,
    dividends FLOAT,
    split FLOAT,

    PRIMARY KEY (info_id),
    FOREIGN KEY (asset_id) REFERENCES assets(asset_id)
);