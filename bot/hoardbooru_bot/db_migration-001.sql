-- ignore exceptions

alter table cache_entries
    add sent_as_file BOOLEAN;  -- Whether this was sent as an uncompressed document, rather than a photo
);