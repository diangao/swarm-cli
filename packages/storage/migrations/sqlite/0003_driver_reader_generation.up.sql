ALTER TABLE driver_event_cursor ADD COLUMN reader_journal_instance_id TEXT CHECK (
  reader_journal_instance_id IS NULL OR (
    length(reader_journal_instance_id) = 30 AND
    substr(reader_journal_instance_id, 1, 4) = 'cmd_' AND
    substr(reader_journal_instance_id, 5)
      NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'
  )
);

ALTER TABLE local_turns ADD COLUMN delivery_id TEXT CHECK (
  delivery_id IS NULL OR (
    length(delivery_id) = 30 AND substr(delivery_id, 1, 4) = 'dlv_' AND
    substr(delivery_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'
  )
);
ALTER TABLE local_turns ADD COLUMN attempt INTEGER CHECK (
  attempt IS NULL OR (attempt >= 1 AND attempt <= 2147483647)
);
ALTER TABLE local_turns ADD COLUMN invocation_id TEXT CHECK (
  invocation_id IS NULL OR (
    length(invocation_id) = 30 AND substr(invocation_id, 1, 4) = 'cmd_' AND
    substr(invocation_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'
  )
);
ALTER TABLE local_turns ADD COLUMN invocation_generation INTEGER CHECK (
  invocation_generation IS NULL OR (
    invocation_generation >= 1 AND invocation_generation <= 9007199254740991
  )
);
ALTER TABLE local_turns ADD COLUMN permit_id TEXT CHECK (
  permit_id IS NULL OR (
    length(permit_id) = 30 AND substr(permit_id, 1, 4) = 'cmd_' AND
    substr(permit_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'
  )
);
ALTER TABLE local_turns ADD COLUMN runtime_write_id TEXT CHECK (
  runtime_write_id IS NULL OR (
    length(runtime_write_id) = 30 AND substr(runtime_write_id, 1, 4) = 'cmd_' AND
    substr(runtime_write_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'
  )
);
ALTER TABLE local_turns ADD COLUMN visibility_event_id TEXT CHECK (
  visibility_event_id IS NULL OR (
    length(visibility_event_id) = 30 AND substr(visibility_event_id, 1, 4) = 'cmd_' AND
    substr(visibility_event_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'
  )
);
