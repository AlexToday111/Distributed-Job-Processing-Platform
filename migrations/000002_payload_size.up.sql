-- JSONB normalization can expand dense arrays and exponent-form numbers.
-- Bound the stored form as well as the HTTP body to keep RPC batches bounded.
ALTER TABLE jobs ADD CONSTRAINT jobs_payload_size CHECK (octet_length(payload::text) <= 1048576);
