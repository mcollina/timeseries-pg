INSERT INTO datapoints (value, asset) VALUES ($1, $2) RETURNING id, value, asset, timestamp;
