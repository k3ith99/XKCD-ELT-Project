DO $$ 
BEGIN 
    IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'comics') THEN 
        CREATE DATABASE comics;
    END IF;
END $$;
\c comics;


CREATE TABLE IF NOT EXISTS raw_comics(
  num INT PRIMARY KEY,
  month INT,
  year INT,
  news TEXT,
  safe_title TEXT,
  transcript TEXT,
  alt TEXT,
  img TEXT,
  title TEXT,
  day INT
);

