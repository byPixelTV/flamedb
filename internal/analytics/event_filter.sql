-- Fixed-size candidate tokens work for arbitrary metrics and tag keys/values.
-- Tokens never establish equality: reads still check the original metric/tags.
-- This permits hash collisions without changing query results or key limits.
CREATE OR REPLACE FUNCTION flame_event_tag_tokens(metric text,tags jsonb) RETURNS text[]
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $tokens$
 SELECT coalesce(array_agg(md5(jsonb_build_array(metric,t.key,t.value)::text)),ARRAY[]::text[])
 FROM jsonb_each_text(tags) t
$tokens$;
ALTER TABLE flame_events ADD COLUMN IF NOT EXISTS tag_tokens text[]
 GENERATED ALWAYS AS (flame_event_tag_tokens(metric,tags)) STORED;
-- Avoid scanning a growing pending list on every latency-sensitive tag read.
CREATE INDEX IF NOT EXISTS flame_events_tag_candidates ON flame_events
 USING gin(tag_tokens) WITH (fastupdate=off);
