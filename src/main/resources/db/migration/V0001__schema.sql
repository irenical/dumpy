CREATE TABLE dumpy_stream (
  id SERIAL NOT NULL PRIMARY KEY,
  job_code TEXT NOT NULL,
  stream_code TEXT NOT NULL,
  cursor TEXT,
  CONSTRAINT job_id_name_unique UNIQUE ( job_code, stream_code )
);

CREATE TABLE dumpy_stream_entity (
  id SERIAL NOT NULL PRIMARY KEY,
  stream_id INTEGER NOT NULL,
  entity_id TEXT NOT NULL,
  last_error_stamp TIMESTAMP WITH TIME ZONE,
  last_updated_stamp TIMESTAMP WITH TIME ZONE NOT NULL,
  CONSTRAINT stream_id_fkey FOREIGN KEY ( stream_id ) REFERENCES dumpy_stream ( id ),
  CONSTRAINT stream_id_entity_id_unique UNIQUE ( stream_id, entity_id )
);
