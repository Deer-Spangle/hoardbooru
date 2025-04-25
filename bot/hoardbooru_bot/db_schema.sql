create table if not exists cache_entries
(
    post_id               INTEGER not null,
    is_photo              BOOLEAN not null,
    media_id              INTEGER not null,
    access_hash           INTEGER not null,
    file_url              TEXT,
    mime_type             TEXT,
    cache_date            DATE    not null,
    is_thumbnail          BOOLEAN not null,  -- If true, this cache is only for inline results
    sent_as_file          BOOLEAN  -- Whether this was sent as a file, rather than a photo
);

drop index if exists cache_entries_site_code_post_id_uindex;

create unique index if not exists cache_entries_post_id_is_photo_uindex
    on cache_entries (post_id, is_photo);