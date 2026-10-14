CREATE TABLE IF NOT EXISTS flame_schema_version(singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),version integer NOT NULL);
DO $migration$
DECLARE relation text; old_primary text;
BEGIN
 IF EXISTS(SELECT 1 FROM flame_schema_version WHERE singleton AND version>2) THEN RAISE EXCEPTION 'unsupported newer FlameDB schema version'; END IF;
 IF EXISTS(SELECT 1 FROM flame_schema_version WHERE singleton AND version=2) THEN RETURN; END IF;
 FOREACH relation IN ARRAY ARRAY['flame_metrics','flame_events','flame_scores','flame_tag_values','flame_metric_clock'] LOOP
  IF NOT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid=relation::regclass AND attname='row_id' AND NOT attisdropped) THEN
   EXECUTE format('ALTER TABLE %I ADD COLUMN row_id bigserial',relation);
  END IF;
  SELECT conname INTO old_primary FROM pg_constraint WHERE conrelid=relation::regclass AND contype='p';
  IF old_primary IS NOT NULL THEN EXECUTE format('ALTER TABLE %I DROP CONSTRAINT %I',relation,old_primary); END IF;
  IF relation='flame_events' THEN
   EXECUTE format('ALTER TABLE %I ADD PRIMARY KEY(row_id,ts)',relation);
  ELSE
   EXECUTE format('ALTER TABLE %I ADD PRIMARY KEY(row_id)',relation);
  END IF;
 END LOOP;
 DROP INDEX IF EXISTS flame_scores_order;
 DROP INDEX IF EXISTS flame_scores_exact_order;
 CREATE UNIQUE INDEX flame_metrics_small_identity ON flame_metrics(metric) WHERE octet_length(metric)<=2000;
 CREATE UNIQUE INDEX flame_events_small_identity ON flame_events(metric,ts) WHERE octet_length(metric)<=2000;
 CREATE UNIQUE INDEX flame_scores_small_identity ON flame_scores(metric,board,entity) WHERE octet_length(metric)+octet_length(board)+octet_length(entity)<=2000;
 CREATE UNIQUE INDEX flame_tag_values_small_identity ON flame_tag_values(metric,tag,value) WHERE octet_length(metric)+octet_length(tag)+octet_length(value)<=2000;
 CREATE UNIQUE INDEX flame_metric_clock_small_identity ON flame_metric_clock(metric) WHERE octet_length(metric)<=2000;
 CREATE FUNCTION flame_identity_hash(input text) RETURNS text LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS 'SELECT md5(input)';
 CREATE INDEX flame_metrics_lookup ON flame_metrics(flame_identity_hash(metric));
 CREATE INDEX flame_events_lookup ON flame_events(flame_identity_hash(metric),ts);
 CREATE INDEX flame_scores_lookup ON flame_scores(flame_identity_hash(metric),flame_identity_hash(board),flame_identity_hash(entity));
 CREATE INDEX flame_tag_values_lookup ON flame_tag_values(flame_identity_hash(metric),flame_identity_hash(tag),flame_identity_hash(value));
 CREATE INDEX flame_metric_clock_lookup ON flame_metric_clock(flame_identity_hash(metric));
 CREATE INDEX flame_scores_order ON flame_scores(flame_identity_hash(metric),flame_identity_hash(board),value DESC);
 CREATE INDEX flame_scores_exact_order ON flame_scores(metric,board,value DESC,flame_entity_bytes(entity)) WHERE octet_length(metric)+octet_length(board)+octet_length(entity)<=2000;
 INSERT INTO flame_schema_version(singleton,version) VALUES(true,2) ON CONFLICT(singleton) DO UPDATE SET version=excluded.version;
END
$migration$;

CREATE OR REPLACE FUNCTION flame_large_identity() RETURNS trigger LANGUAGE plpgsql AS $identity$
DECLARE existing_id bigint; needs_lock boolean;
BEGIN
 CASE TG_ARGV[0]
  WHEN 'metric' THEN needs_lock := octet_length(NEW.metric)>2000;
  WHEN 'event' THEN needs_lock := octet_length(NEW.metric)>2000;
  WHEN 'score' THEN needs_lock := octet_length(NEW.metric)+octet_length(NEW.board)+octet_length(NEW.entity)>2000;
  WHEN 'tag' THEN needs_lock := octet_length(NEW.metric)+octet_length(NEW.tag)+octet_length(NEW.value)>2000;
  WHEN 'clock' THEN needs_lock := octet_length(NEW.metric)>2000;
 END CASE;
 IF NOT needs_lock THEN RETURN NEW; END IF;
 -- API writes already hold this lock before executing their statements. The
 -- trigger preserves canonical identity for imports and direct inserts too.
 PERFORM pg_advisory_xact_lock(hashtextextended(NEW.metric,0));
 CASE TG_ARGV[0]
  WHEN 'metric' THEN SELECT row_id INTO existing_id FROM flame_metrics WHERE flame_identity_hash(metric)=flame_identity_hash(NEW.metric) AND metric=NEW.metric;
  WHEN 'event' THEN SELECT row_id INTO existing_id FROM flame_events WHERE flame_identity_hash(metric)=flame_identity_hash(NEW.metric) AND metric=NEW.metric AND ts=NEW.ts;
  WHEN 'score' THEN SELECT row_id INTO existing_id FROM flame_scores WHERE flame_identity_hash(metric)=flame_identity_hash(NEW.metric) AND flame_identity_hash(board)=flame_identity_hash(NEW.board) AND flame_identity_hash(entity)=flame_identity_hash(NEW.entity) AND metric=NEW.metric AND board=NEW.board AND entity=NEW.entity;
  WHEN 'tag' THEN SELECT row_id INTO existing_id FROM flame_tag_values WHERE flame_identity_hash(metric)=flame_identity_hash(NEW.metric) AND flame_identity_hash(tag)=flame_identity_hash(NEW.tag) AND flame_identity_hash(value)=flame_identity_hash(NEW.value) AND metric=NEW.metric AND tag=NEW.tag AND value=NEW.value;
  WHEN 'clock' THEN SELECT row_id INTO existing_id FROM flame_metric_clock WHERE flame_identity_hash(metric)=flame_identity_hash(NEW.metric) AND metric=NEW.metric;
 END CASE;
 IF existing_id IS NOT NULL THEN NEW.row_id := existing_id; END IF;
 RETURN NEW;
END
$identity$;
-- The explicit kind also works on Timescale's inherited chunk triggers.
-- Ordinary keys use their unique text indexes directly. Evaluating WHEN in the
-- executor avoids invoking PL/pgSQL once per ordinary event/tag/score row.
CREATE OR REPLACE TRIGGER flame_large_key BEFORE INSERT ON flame_metrics FOR EACH ROW WHEN (octet_length(NEW.metric)>2000) EXECUTE FUNCTION flame_large_identity('metric');
CREATE OR REPLACE TRIGGER flame_large_key BEFORE INSERT ON flame_events FOR EACH ROW WHEN (octet_length(NEW.metric)>2000) EXECUTE FUNCTION flame_large_identity('event');
CREATE OR REPLACE TRIGGER flame_large_key BEFORE INSERT ON flame_scores FOR EACH ROW WHEN (octet_length(NEW.metric)+octet_length(NEW.board)+octet_length(NEW.entity)>2000) EXECUTE FUNCTION flame_large_identity('score');
CREATE OR REPLACE TRIGGER flame_large_key BEFORE INSERT ON flame_tag_values FOR EACH ROW WHEN (octet_length(NEW.metric)+octet_length(NEW.tag)+octet_length(NEW.value)>2000) EXECUTE FUNCTION flame_large_identity('tag');
CREATE OR REPLACE TRIGGER flame_large_key BEFORE INSERT ON flame_metric_clock FOR EACH ROW WHEN (octet_length(NEW.metric)>2000) EXECUTE FUNCTION flame_large_identity('clock');
