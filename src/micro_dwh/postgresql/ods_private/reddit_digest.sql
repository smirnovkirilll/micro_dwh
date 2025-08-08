CREATE TABLE IF NOT EXISTS reddit_digest (
    "reddit_url" VARCHAR PRIMARY KEY,
    "title" VARCHAR NOT NULL,
    "subreddit" VARCHAR NOT NULL,
    "tag" VARCHAR,
    "author" VARCHAR NOT NULL,
    "external_url" VARCHAR,
    "comments" INTEGER,
    "up_votes" INTEGER,
    "down_votes" INTEGER,
    "created_utc_dttm" DATETIME NOT NULL
);
