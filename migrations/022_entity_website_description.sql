-- Optional public entity metadata. Existing rows remain valid with NULL values.
ALTER TABLE geodata_entity
  ADD COLUMN IF NOT EXISTS website text,
  ADD COLUMN IF NOT EXISTS description text;
