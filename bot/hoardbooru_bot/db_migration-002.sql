
create unique index if not exists cache_entries_post_id_sent_as_file_uindex
    on cache_entries (post_id, sent_as_file);