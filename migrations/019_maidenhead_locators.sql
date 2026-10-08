-- Persist derived Maidenhead cells covered by each entity geometry.
ALTER TABLE geodata_entity
    ADD COLUMN IF NOT EXISTS maidenhead_grid_squares_4 text[] NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS maidenhead_locators_6 text[] NOT NULL DEFAULT '{}';

COMMENT ON COLUMN geodata_entity.maidenhead_grid_squares_4 IS
    'Sorted unique four-character Maidenhead grid squares intersected by geom.';
COMMENT ON COLUMN geodata_entity.maidenhead_locators_6 IS
    'Sorted unique six-character Maidenhead locators intersected by geom.';

CREATE OR REPLACE FUNCTION myota_maidenhead_for_point(
    longitude double precision,
    latitude double precision,
    character_count integer
)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
STRICT
AS $$
DECLARE
    safe_longitude double precision := LEAST(longitude, 180.0 - 1e-12);
    safe_latitude double precision := LEAST(latitude, 90.0 - 1e-12);
    longitude_field integer;
    latitude_field integer;
    longitude_square integer;
    latitude_square integer;
    longitude_subsquare integer;
    latitude_subsquare integer;
    longitude_remainder double precision;
    latitude_remainder double precision;
    locator text;
BEGIN
    IF character_count NOT IN (4, 6) THEN
        RAISE EXCEPTION 'Maidenhead precision must be 4 or 6 characters';
    END IF;
    IF safe_longitude < -180 OR safe_longitude > 180
        OR safe_latitude < -90 OR safe_latitude > 90 THEN
        RAISE EXCEPTION 'Maidenhead coordinates must be WGS84 longitude/latitude';
    END IF;

    longitude_field := GREATEST(0, LEAST(17, FLOOR((safe_longitude + 180) / 20)::integer));
    latitude_field := GREATEST(0, LEAST(17, FLOOR((safe_latitude + 90) / 10)::integer));
    locator := CHR(65 + longitude_field) || CHR(65 + latitude_field);
    longitude_remainder := safe_longitude + 180 - longitude_field * 20;
    latitude_remainder := safe_latitude + 90 - latitude_field * 10;
    longitude_square := GREATEST(0, LEAST(9, FLOOR(longitude_remainder / 2)::integer));
    latitude_square := GREATEST(0, LEAST(9, FLOOR(latitude_remainder)::integer));
    locator := locator || longitude_square::text || latitude_square::text;
    IF character_count = 4 THEN
        RETURN locator;
    END IF;

    longitude_remainder := longitude_remainder - longitude_square * 2;
    latitude_remainder := latitude_remainder - latitude_square;
    longitude_subsquare := GREATEST(0, LEAST(23, FLOOR(longitude_remainder * 12)::integer));
    latitude_subsquare := GREATEST(0, LEAST(23, FLOOR(latitude_remainder * 24)::integer));
    RETURN locator || CHR(97 + longitude_subsquare) || CHR(97 + latitude_subsquare);
END;
$$;

CREATE OR REPLACE FUNCTION myota_maidenhead_cells_for_geometry(
    input_geometry geometry,
    character_count integer
)
RETURNS text[]
LANGUAGE plpgsql
STABLE
STRICT
AS $$
DECLARE
    longitude_step double precision;
    latitude_step double precision;
    longitude_count integer;
    latitude_count integer;
    envelope geometry;
    minimum_x double precision;
    maximum_x double precision;
    minimum_y double precision;
    maximum_y double precision;
    first_x integer;
    last_x integer;
    first_y integer;
    last_y integer;
    x_index integer;
    y_index integer;
    left_x double precision;
    right_x double precision;
    bottom_y double precision;
    top_y double precision;
    locators text[] := ARRAY[]::text[];
BEGIN
    IF character_count NOT IN (4, 6) THEN
        RAISE EXCEPTION 'Maidenhead precision must be 4 or 6 characters';
    END IF;
    IF ST_IsEmpty(input_geometry) THEN
        RETURN locators;
    END IF;
    IF ST_SRID(input_geometry) <> 4326 THEN
        RAISE EXCEPTION 'Maidenhead geometry must use SRID 4326';
    END IF;
    IF GeometryType(input_geometry) = 'POINT' THEN
        RETURN ARRAY[myota_maidenhead_for_point(
            ST_X(input_geometry), ST_Y(input_geometry), character_count
        )];
    END IF;

    longitude_step := CASE WHEN character_count = 4 THEN 2.0 ELSE 1.0 / 12.0 END;
    latitude_step := CASE WHEN character_count = 4 THEN 1.0 ELSE 1.0 / 24.0 END;
    longitude_count := ROUND(360.0 / longitude_step)::integer;
    latitude_count := ROUND(180.0 / latitude_step)::integer;
    envelope := ST_Envelope(input_geometry);
    minimum_x := ST_XMin(envelope);
    maximum_x := ST_XMax(envelope);
    minimum_y := ST_YMin(envelope);
    maximum_y := ST_YMax(envelope);

    first_x := FLOOR((minimum_x + 180) / longitude_step)::integer;
    IF ABS((minimum_x + 180) / longitude_step - ROUND((minimum_x + 180) / longitude_step)) < 1e-10 THEN
        first_x := first_x - 1;
    END IF;
    last_x := FLOOR((maximum_x + 180) / longitude_step)::integer;
    first_y := FLOOR((minimum_y + 90) / latitude_step)::integer;
    IF ABS((minimum_y + 90) / latitude_step - ROUND((minimum_y + 90) / latitude_step)) < 1e-10 THEN
        first_y := first_y - 1;
    END IF;
    last_y := FLOOR((maximum_y + 90) / latitude_step)::integer;
    first_x := GREATEST(0, first_x);
    last_x := LEAST(longitude_count - 1, last_x);
    first_y := GREATEST(0, first_y);
    last_y := LEAST(latitude_count - 1, last_y);

    FOR x_index IN first_x..last_x LOOP
        left_x := -180 + x_index * longitude_step;
        right_x := LEAST(180, left_x + longitude_step);
        FOR y_index IN first_y..last_y LOOP
            bottom_y := -90 + y_index * latitude_step;
            top_y := LEAST(90, bottom_y + latitude_step);
            IF ST_Intersects(
                input_geometry,
                ST_MakeEnvelope(left_x, bottom_y, right_x, top_y, 4326)
            ) THEN
                locators := ARRAY_APPEND(
                    locators,
                    myota_maidenhead_for_point(
                        (left_x + right_x) / 2,
                        (bottom_y + top_y) / 2,
                        character_count
                    )
                );
            END IF;
        END LOOP;
    END LOOP;
    RETURN ARRAY(
        SELECT DISTINCT cell
        FROM UNNEST(locators) AS cells(cell)
        ORDER BY cell
    );
END;
$$;

-- Backfill existing catalogue rows before enabling automatic recalculation.
UPDATE geodata_entity
SET maidenhead_grid_squares_4 = myota_maidenhead_cells_for_geometry(geom, 4),
    maidenhead_locators_6 = myota_maidenhead_cells_for_geometry(geom, 6);

CREATE OR REPLACE FUNCTION geodata_entity_set_maidenhead_cells()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.maidenhead_grid_squares_4 := myota_maidenhead_cells_for_geometry(NEW.geom, 4);
    NEW.maidenhead_locators_6 := myota_maidenhead_cells_for_geometry(NEW.geom, 6);
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS geodata_entity_maidenhead_cells ON geodata_entity;
CREATE TRIGGER geodata_entity_maidenhead_cells
BEFORE INSERT OR UPDATE OF geom ON geodata_entity
FOR EACH ROW EXECUTE FUNCTION geodata_entity_set_maidenhead_cells();
