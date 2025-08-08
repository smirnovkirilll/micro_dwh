CREATE TABLE IF NOT EXISTS curated_list (
    "url" VARCHAR PRIMARY KEY,
    "title" VARCHAR NOT NULL,
    "topic" VARCHAR NOT NULL,
    "type_of_content" VARCHAR NOT NULL,
    "language" VARCHAR NOT NULL
);
