INSERT INTO datapoints (value, asset, timestamp) VALUES ($1, $2, $3) RETURNING id, value, asset, timestamp;
