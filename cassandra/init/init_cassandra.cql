-- Create keyspace if it doesn't exist
CREATE KEYSPACE IF NOT EXISTS interaction_data 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Create table if it doesn't exist
CREATE TABLE IF NOT EXISTS interaction_data.interaction_data (
    interaction_type TEXT,
    timestamp TIMESTAMP,
    user_id DOUBLE,
    item_id DOUBLE,
    PRIMARY KEY (user_id, interaction_type, item_id, timestamp)
);


CREATE TABLE IF NOT EXISTS interaction_data.user_aggregates (
    user_id TEXT PRIMARY KEY,
    total_interactions INT,
    average_interactions DOUBLE,
    timestamp TIMESTAMP
);

CREATE TABLE IF NOT EXISTS interaction_data.item_aggregates (
    item_id TEXT PRIMARY KEY,
    total_interactions INT,
    average_interactions DOUBLE,
    timestamp TIMESTAMP
);
