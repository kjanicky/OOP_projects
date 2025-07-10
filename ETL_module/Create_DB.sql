--DROP DATABASE IF EXISTS "Online_Posts";

CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT,
    username TEXT,
    email TEXT,
    address_street TEXT,
    address_suite TEXT,
    address_city TEXT,
    address_zipcode TEXT,
    address_geo_lat TEXT,
    address_geo_lng TEXT,
    phone TEXT,
    website TEXT,
    company_name TEXT,
    company_catchPhrase TEXT,
    company_bs TEXT
);

CREATE TABLE posts (
    id INTEGER PRIMARY KEY,
    userId INTEGER,
    title TEXT,
    body TEXT
);

CREATE TABLE comments (
    id INTEGER PRIMARY KEY,
    postId INTEGER,
    name TEXT,
    email TEXT,
    body TEXT
);