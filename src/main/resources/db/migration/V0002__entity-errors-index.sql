CREATE INDEX IF NOT EXISTS dumpy_stream_errors_index
  ON public.dumpy_stream_entity
  USING btree
  (stream_id, last_error_stamp)
  WHERE last_error_stamp IS NOT NULL;
