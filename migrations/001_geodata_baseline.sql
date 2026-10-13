--
-- PostgreSQL database dump
--

-- Dumped from database version 16.4 (Debian 16.4-1.pgdg110+2)
-- Dumped by pg_dump version 16.4 (Debian 16.4-1.pgdg110+2)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: tiger; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA IF NOT EXISTS tiger;


--
-- Name: tiger_data; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA IF NOT EXISTS tiger_data;


--
-- Name: topology; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA IF NOT EXISTS topology;


--
-- Name: SCHEMA topology; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON SCHEMA topology IS 'PostGIS Topology schema';


--
-- Name: fuzzystrmatch; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS fuzzystrmatch WITH SCHEMA public;


--
-- Name: EXTENSION fuzzystrmatch; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION fuzzystrmatch IS 'determine similarities and distance between strings';


--
-- Name: pgcrypto; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;


--
-- Name: EXTENSION pgcrypto; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION pgcrypto IS 'cryptographic functions';


--
-- Name: postgis; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA public;


--
-- Name: EXTENSION postgis; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION postgis IS 'PostGIS geometry and geography spatial types and functions';


--
-- Name: postgis_tiger_geocoder; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS postgis_tiger_geocoder WITH SCHEMA tiger;


--
-- Name: EXTENSION postgis_tiger_geocoder; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION postgis_tiger_geocoder IS 'PostGIS tiger geocoder and reverse geocoder';


--
-- Name: postgis_topology; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS postgis_topology WITH SCHEMA topology;


--
-- Name: EXTENSION postgis_topology; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION postgis_topology IS 'PostGIS topology spatial types and functions';


--
-- Name: geodata_bump_revision(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.geodata_bump_revision() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  NEW.revision := OLD.revision + 1;
  RETURN NEW;
END;
$$;


--
-- Name: geodata_entity_set_maidenhead_cells(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.geodata_entity_set_maidenhead_cells() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.maidenhead_grid_squares_4 := myota_maidenhead_cells_for_geometry(NEW.geom, 4);
    NEW.maidenhead_locators_6 := myota_maidenhead_cells_for_geometry(NEW.geom, 6);
    RETURN NEW;
END;
$$;


--
-- Name: geodata_require_row_writer(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.geodata_require_row_writer() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  IF current_setting('myota.geodata_writer',true) IS DISTINCT FROM 'row-v1' THEN
    RAISE EXCEPTION 'Geodata writer is obsolete; deploy the row-authoritative service image'
      USING ERRCODE='55000';
  END IF;
  RETURN CASE WHEN TG_OP='DELETE' THEN OLD ELSE NEW END;
END;
$$;


--
-- Name: myota_maidenhead_cells_for_geometry(public.geometry, integer); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.myota_maidenhead_cells_for_geometry(input_geometry public.geometry, character_count integer) RETURNS text[]
    LANGUAGE plpgsql STABLE STRICT
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


--
-- Name: myota_maidenhead_for_point(double precision, double precision, integer); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.myota_maidenhead_for_point(longitude double precision, latitude double precision, character_count integer) RETURNS text
    LANGUAGE plpgsql IMMUTABLE STRICT
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


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: conflation_candidate; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.conflation_candidate (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    left_entity_id uuid NOT NULL,
    right_entity_id uuid NOT NULL,
    similarity numeric NOT NULL,
    reason jsonb DEFAULT '{}'::jsonb NOT NULL,
    resolution text DEFAULT 'OPEN'::text NOT NULL,
    survivor_entity_id uuid,
    resolution_history jsonb DEFAULT '[]'::jsonb NOT NULL,
    resolved_by uuid,
    resolved_at timestamp with time zone,
    CONSTRAINT conflation_candidate_resolution_check CHECK ((resolution = ANY (ARRAY['OPEN'::text, 'MERGED'::text, 'KEPT_SEPARATE'::text, 'IGNORED'::text])))
);


--
-- Name: consumer_checkpoint; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.consumer_checkpoint (
    consumer text NOT NULL,
    last_event_id uuid,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: consumer_processed_event; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.consumer_processed_event (
    consumer text NOT NULL,
    event_id uuid NOT NULL,
    processed_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: dead_letter_event; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dead_letter_event (
    event_id uuid NOT NULL,
    event_type text NOT NULL,
    payload jsonb NOT NULL,
    attempts integer NOT NULL,
    error text,
    dead_lettered_at timestamp with time zone DEFAULT now() NOT NULL,
    resolved_at timestamp with time zone
);


--
-- Name: entity_review; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.entity_review (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    entity_id uuid NOT NULL,
    proposer_id uuid,
    reviewer_id uuid,
    proposal_note text,
    review_note text,
    decision text,
    proposed_at timestamp with time zone,
    reviewed_at timestamp with time zone,
    CONSTRAINT entity_review_decision_check CHECK ((decision = ANY (ARRAY['APPROVED'::text, 'REJECTED'::text])))
);


--
-- Name: entity_type; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.entity_type (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    programme_id uuid,
    code text NOT NULL,
    label text NOT NULL,
    geometry_kind text NOT NULL,
    config jsonb DEFAULT '{}'::jsonb NOT NULL
);


--
-- Name: geodata_audit_event; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.geodata_audit_event (
    event_id uuid NOT NULL,
    aggregate_type text NOT NULL,
    aggregate_id text NOT NULL,
    event jsonb NOT NULL,
    occurred_at timestamp with time zone NOT NULL
);


--
-- Name: geodata_control_record; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.geodata_control_record (
    kind text NOT NULL,
    id text NOT NULL,
    payload jsonb NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT geodata_control_record_kind_check CHECK ((kind = ANY (ARRAY['schedules'::text, 'conflationCandidates'::text, 'sourceManifests'::text, 'entityDeletionJobs'::text])))
);


--
-- Name: geodata_edit_staging; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.geodata_edit_staging (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    entity_id uuid,
    programme_id uuid,
    proposed_name text,
    geom public.geometry(Geometry,4326) NOT NULL,
    editor_subject text NOT NULL,
    edit_note text,
    state text DEFAULT 'OPEN'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT geodata_edit_staging_state_check CHECK ((state = ANY (ARRAY['OPEN'::text, 'SUBMITTED'::text, 'DISCARDED'::text])))
);


--
-- Name: geodata_entity; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.geodata_entity (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    programme_id uuid,
    entity_type_id uuid,
    name text NOT NULL,
    lifecycle_status text DEFAULT 'CANDIDATE'::text NOT NULL,
    geom public.geometry(Geometry,4326) NOT NULL,
    centroid public.geography(Point,4326),
    public_properties jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    source_state text DEFAULT 'CURRENT'::text NOT NULL,
    source_key text,
    source_hash text,
    jurisdiction text,
    attachments jsonb DEFAULT '[]'::jsonb NOT NULL,
    continent text,
    continent_code text,
    country text,
    country_code text,
    region text,
    region_code text,
    subdivision text,
    subdivision_code text,
    province text,
    province_code text,
    county text,
    county_code text,
    city text,
    locality text,
    geocode_provider text,
    geocode_status text DEFAULT 'NOT_CONFIGURED'::text NOT NULL,
    geocode_lookup_source text,
    geocoded_at timestamp with time zone,
    geocode_payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    municipality text,
    manual_location_fields jsonb DEFAULT '[]'::jsonb NOT NULL,
    manual_location_updated_at timestamp with time zone,
    programme_slug text,
    entity_type_code text,
    revision bigint DEFAULT 1 NOT NULL,
    maidenhead_grid_squares_4 text[] DEFAULT '{}'::text[] NOT NULL,
    maidenhead_locators_6 text[] DEFAULT '{}'::text[] NOT NULL,
    location_work_dispatched_at timestamp with time zone DEFAULT now() NOT NULL,
    website text,
    description text,
    CONSTRAINT geodata_entity_lifecycle_status_check CHECK ((lifecycle_status = ANY (ARRAY['CANDIDATE'::text, 'APPROVED'::text, 'REJECTED'::text, 'RETIRED'::text]))),
    CONSTRAINT geodata_entity_source_state_check CHECK ((source_state = ANY (ARRAY['CURRENT'::text, 'STALE'::text, 'REVIEW_REQUIRED'::text, 'RETIRED'::text])))
);


--
-- Name: COLUMN geodata_entity.programme_id; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.geodata_entity.programme_id IS 'Optional programme assignment. Imports create platform-wide candidates; programme eligibility is assigned separately.';


--
-- Name: COLUMN geodata_entity.maidenhead_grid_squares_4; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.geodata_entity.maidenhead_grid_squares_4 IS 'Sorted unique four-character Maidenhead grid squares intersected by geom.';


--
-- Name: COLUMN geodata_entity.maidenhead_locators_6; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.geodata_entity.maidenhead_locators_6 IS 'Sorted unique six-character Maidenhead locators intersected by geom.';


--
-- Name: geodata_entity_attachment; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.geodata_entity_attachment (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    entity_id uuid NOT NULL,
    name text NOT NULL,
    media_type text NOT NULL,
    size_bytes bigint DEFAULT 0 NOT NULL,
    uri text,
    sha256 text,
    license text,
    caption text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT geodata_entity_attachment_check CHECK (((uri IS NOT NULL) OR (sha256 IS NOT NULL))),
    CONSTRAINT geodata_entity_attachment_size_bytes_check CHECK ((size_bytes >= 0))
);


--
-- Name: geodata_entity_category; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.geodata_entity_category (
    entity_id uuid NOT NULL,
    category_id uuid NOT NULL,
    category_code text NOT NULL,
    is_primary boolean DEFAULT false NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: geodata_import_candidate; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.geodata_import_candidate (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    import_run_id uuid NOT NULL,
    ordinal integer NOT NULL,
    planned_entity_id uuid,
    programme_slug text,
    entity_type_codes jsonb DEFAULT '[]'::jsonb NOT NULL,
    name text NOT NULL,
    geom public.geometry(Geometry,4326) NOT NULL,
    candidate_source jsonb DEFAULT '{}'::jsonb NOT NULL,
    source_ref text,
    source_hash text,
    provenance jsonb DEFAULT '{}'::jsonb NOT NULL,
    entity_payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    validation_status text DEFAULT 'PENDING'::text NOT NULL,
    validation_note text,
    validated_by text,
    validated_at timestamp with time zone,
    target_status text,
    processed_entity_id uuid,
    processed_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    existing_entity_id uuid,
    processing_queue_id uuid,
    CONSTRAINT geodata_import_candidate_target_status_check CHECK ((target_status = ANY (ARRAY['CANDIDATE'::text, 'APPROVED'::text]))),
    CONSTRAINT geodata_import_candidate_validation_status_check CHECK ((validation_status = ANY (ARRAY['PENDING'::text, 'CONFIRMED'::text, 'REJECTED'::text, 'PROCESSED'::text])))
);


--
-- Name: TABLE geodata_import_candidate; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.geodata_import_candidate IS 'Normalized import records awaiting explicit administrator validation and promotion.';


--
-- Name: geodata_import_processing_queue; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.geodata_import_processing_queue (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    import_run_id uuid NOT NULL,
    candidate_ids jsonb NOT NULL,
    target_status text NOT NULL,
    requested_by text NOT NULL,
    status text DEFAULT 'QUEUED'::text NOT NULL,
    result jsonb DEFAULT '{}'::jsonb NOT NULL,
    error text,
    requested_at timestamp with time zone DEFAULT now() NOT NULL,
    started_at timestamp with time zone,
    completed_at timestamp with time zone,
    attempt_count integer DEFAULT 0 NOT NULL,
    heartbeat_at timestamp with time zone,
    lease_until timestamp with time zone,
    job_metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    work_dispatched_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT geodata_import_processing_queue_status_check CHECK ((status = ANY (ARRAY['QUEUED'::text, 'PROCESSING'::text, 'COMPLETED'::text, 'FAILED'::text]))),
    CONSTRAINT geodata_import_processing_queue_target_status_check CHECK ((target_status = ANY (ARRAY['CANDIDATE'::text, 'APPROVED'::text])))
);


--
-- Name: TABLE geodata_import_processing_queue; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.geodata_import_processing_queue IS 'Durable projection of the NATS-backed import promotion queue.';


--
-- Name: geodata_schema_feature; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.geodata_schema_feature (
    name text NOT NULL,
    applied_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: geodata_upload_part; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.geodata_upload_part (
    upload_session_id uuid NOT NULL,
    part_number integer NOT NULL,
    size_bytes bigint NOT NULL,
    sha256 text NOT NULL,
    etag text NOT NULL,
    uploaded_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT geodata_upload_part_part_number_check CHECK (((part_number >= 1) AND (part_number <= 10000))),
    CONSTRAINT geodata_upload_part_size_bytes_check CHECK ((size_bytes > 0))
);


--
-- Name: geodata_upload_session; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.geodata_upload_session (
    id uuid NOT NULL,
    owner_subject text NOT NULL,
    idempotency_key text NOT NULL,
    filename text NOT NULL,
    metadata jsonb NOT NULL,
    bucket text NOT NULL,
    object_key text NOT NULL,
    multipart_upload_id text NOT NULL,
    expected_size bigint NOT NULL,
    expected_sha256 text,
    status text NOT NULL,
    import_run_id uuid,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    completed_at timestamp with time zone,
    CONSTRAINT geodata_upload_session_expected_size_check CHECK ((expected_size > 0)),
    CONSTRAINT geodata_upload_session_status_check CHECK ((status = ANY (ARRAY['UPLOADING'::text, 'COMPLETING'::text, 'COMPLETED'::text, 'ABORTED'::text, 'EXPIRED'::text, 'FAILED'::text])))
);


--
-- Name: TABLE geodata_upload_session; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.geodata_upload_session IS 'Restart-safe browser upload sessions. Object bytes live in S3-compatible storage, never a shared API pod volume.';


--
-- Name: idempotency_record; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.idempotency_record (
    service text NOT NULL,
    key text NOT NULL,
    response jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: import_run; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.import_run (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    adapter_code text NOT NULL,
    source_metadata jsonb NOT NULL,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    completed_at timestamp with time zone,
    stats jsonb DEFAULT '{}'::jsonb NOT NULL,
    status text DEFAULT 'RUNNING'::text NOT NULL,
    programme_id uuid,
    source_key text,
    source_hash text,
    complete_snapshot boolean DEFAULT false NOT NULL,
    disappearance_policy text DEFAULT 'REVIEW_REQUIRED'::text NOT NULL,
    attempt_count integer DEFAULT 0 NOT NULL,
    heartbeat_at timestamp with time zone,
    lease_until timestamp with time zone,
    last_error text,
    processed_at timestamp with time zone,
    processed_by text,
    cancellation_requested_at timestamp with time zone,
    cancellation_requested_by text,
    work_dispatched_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT import_run_disappearance_policy_ck CHECK ((disappearance_policy = ANY (ARRAY['UNCHANGED'::text, 'STALE'::text, 'RETIRED'::text, 'REVIEW_REQUIRED'::text])))
);


--
-- Name: COLUMN import_run.lease_until; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.import_run.lease_until IS 'Execution lease expiry. A queued or processing run can be recovered after this time.';


--
-- Name: COLUMN import_run.cancellation_requested_at; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.import_run.cancellation_requested_at IS 'Time an administrator requested cancellation of upload/preprocessing work.';


--
-- Name: COLUMN import_run.cancellation_requested_by; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.import_run.cancellation_requested_by IS 'Authenticated administrator subject that requested import cancellation.';


--
-- Name: import_schedule; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.import_schedule (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    programme_id uuid,
    adapter_code text NOT NULL,
    source_key text NOT NULL,
    source_metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    interval_seconds integer NOT NULL,
    disappearance_policy text DEFAULT 'REVIEW_REQUIRED'::text NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    last_run_at timestamp with time zone,
    next_run_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    entity_type_code text,
    CONSTRAINT import_schedule_disappearance_policy_check CHECK ((disappearance_policy = ANY (ARRAY['UNCHANGED'::text, 'STALE'::text, 'RETIRED'::text, 'REVIEW_REQUIRED'::text]))),
    CONSTRAINT import_schedule_interval_seconds_check CHECK ((interval_seconds >= 300))
);


--
-- Name: outbox_event; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.outbox_event (
    event_id uuid NOT NULL,
    event_type text NOT NULL,
    producer text NOT NULL,
    aggregate_type text NOT NULL,
    aggregate_id text NOT NULL,
    payload jsonb NOT NULL,
    occurred_at timestamp with time zone NOT NULL,
    available_at timestamp with time zone DEFAULT now() NOT NULL,
    attempts integer DEFAULT 0 NOT NULL,
    published_at timestamp with time zone,
    last_error text,
    correlation_id text,
    CONSTRAINT outbox_event_correlation_id_length_check CHECK (((correlation_id IS NULL) OR (((length(correlation_id) >= 1) AND (length(correlation_id) <= 128)) AND (correlation_id !~ '[^!-~]'::text))))
);


--
-- Name: outbox_redrive_audit; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.outbox_redrive_audit (
    id bigint NOT NULL,
    event_id uuid NOT NULL,
    actor text NOT NULL,
    reason text NOT NULL,
    previous_attempts integer NOT NULL,
    previous_error text,
    requested_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT outbox_redrive_audit_actor_check CHECK (((length(actor) >= 1) AND (length(actor) <= 120))),
    CONSTRAINT outbox_redrive_audit_reason_check CHECK (((length(reason) >= 1) AND (length(reason) <= 500)))
);


--
-- Name: outbox_redrive_audit_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.outbox_redrive_audit_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: outbox_redrive_audit_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.outbox_redrive_audit_id_seq OWNED BY public.outbox_redrive_audit.id;


--
-- Name: qgis_approved_entities; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.qgis_approved_entities AS
 SELECT id,
    programme_id,
    name,
    geom,
    public_properties,
    source_state,
    jurisdiction,
    continent,
    continent_code,
    country,
    country_code,
    region,
    region_code,
    subdivision,
    subdivision_code,
    province,
    province_code,
    county,
    county_code,
    city,
    locality,
    municipality,
    manual_location_fields
   FROM public.geodata_entity
  WHERE (lifecycle_status = 'APPROVED'::text);


--
-- Name: source_reference; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.source_reference (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    entity_id uuid NOT NULL,
    adapter_code text NOT NULL,
    source_uri text,
    source_record_id text,
    license text,
    attribution text,
    retrieved_at timestamp with time zone,
    source_payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    CONSTRAINT source_reference_adapter_code_check CHECK ((adapter_code = ANY (ARRAY['PARKSERVE_US'::text, 'OSM'::text, 'GOVERNMENT_GIS'::text, 'MANUAL'::text])))
);


--
-- Name: qgis_entity_review_queue; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.qgis_entity_review_queue AS
 SELECT e.id,
    e.programme_id,
    e.name,
    e.lifecycle_status,
    e.source_state,
    e.jurisdiction,
    e.geom,
    e.public_properties,
    e.continent,
    e.continent_code,
    e.country,
    e.country_code,
    e.region,
    e.region_code,
    e.subdivision,
    e.subdivision_code,
    e.province,
    e.province_code,
    e.county,
    e.county_code,
    e.city,
    e.locality,
    s.adapter_code,
    s.source_uri,
    s.source_record_id,
    s.license,
    s.attribution,
    s.source_payload,
    e.municipality,
    e.manual_location_fields
   FROM (public.geodata_entity e
     LEFT JOIN public.source_reference s ON ((s.entity_id = e.id)))
  WHERE ((e.lifecycle_status = ANY (ARRAY['CANDIDATE'::text, 'REJECTED'::text])) OR (e.source_state = ANY (ARRAY['STALE'::text, 'REVIEW_REQUIRED'::text])));


--
-- Name: service_state; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.service_state (
    service text NOT NULL,
    state jsonb NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: TABLE service_state; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.service_state IS 'Legacy geodata snapshot archive. New geodata runtime never reads or writes this table. Stop old writers before migration.';


--
-- Name: source_snapshot_manifest; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.source_snapshot_manifest (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    import_run_id uuid NOT NULL,
    source_key text NOT NULL,
    adapter_code text NOT NULL,
    source_hash text NOT NULL,
    record_count integer DEFAULT 0 NOT NULL,
    source_metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    retrieved_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: source_snapshot_record; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.source_snapshot_record (
    manifest_id uuid NOT NULL,
    source_record_id text NOT NULL,
    record_hash text NOT NULL
);


--
-- Name: outbox_redrive_audit id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.outbox_redrive_audit ALTER COLUMN id SET DEFAULT nextval('public.outbox_redrive_audit_id_seq'::regclass);


--
-- Name: conflation_candidate conflation_candidate_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.conflation_candidate
    ADD CONSTRAINT conflation_candidate_pkey PRIMARY KEY (id);


--
-- Name: consumer_checkpoint consumer_checkpoint_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.consumer_checkpoint
    ADD CONSTRAINT consumer_checkpoint_pkey PRIMARY KEY (consumer);


--
-- Name: consumer_processed_event consumer_processed_event_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.consumer_processed_event
    ADD CONSTRAINT consumer_processed_event_pkey PRIMARY KEY (consumer, event_id);


--
-- Name: dead_letter_event dead_letter_event_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dead_letter_event
    ADD CONSTRAINT dead_letter_event_pkey PRIMARY KEY (event_id);


--
-- Name: entity_review entity_review_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.entity_review
    ADD CONSTRAINT entity_review_pkey PRIMARY KEY (id);


--
-- Name: entity_type entity_type_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.entity_type
    ADD CONSTRAINT entity_type_pkey PRIMARY KEY (id);


--
-- Name: entity_type entity_type_programme_id_code_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.entity_type
    ADD CONSTRAINT entity_type_programme_id_code_key UNIQUE (programme_id, code);


--
-- Name: geodata_audit_event geodata_audit_event_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.geodata_audit_event
    ADD CONSTRAINT geodata_audit_event_pkey PRIMARY KEY (event_id);


--
-- Name: geodata_control_record geodata_control_record_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.geodata_control_record
    ADD CONSTRAINT geodata_control_record_pkey PRIMARY KEY (kind, id);


--
-- Name: geodata_edit_staging geodata_edit_staging_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.geodata_edit_staging
    ADD CONSTRAINT geodata_edit_staging_pkey PRIMARY KEY (id);


--
-- Name: geodata_entity_attachment geodata_entity_attachment_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.geodata_entity_attachment
    ADD CONSTRAINT geodata_entity_attachment_pkey PRIMARY KEY (id);


--
-- Name: geodata_entity_category geodata_entity_category_entity_id_category_code_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.geodata_entity_category
    ADD CONSTRAINT geodata_entity_category_entity_id_category_code_key UNIQUE (entity_id, category_code);


--
-- Name: geodata_entity_category geodata_entity_category_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.geodata_entity_category
    ADD CONSTRAINT geodata_entity_category_pkey PRIMARY KEY (entity_id, category_id);


--
-- Name: geodata_entity geodata_entity_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.geodata_entity
    ADD CONSTRAINT geodata_entity_pkey PRIMARY KEY (id);


--
-- Name: geodata_import_candidate geodata_import_candidate_import_run_id_ordinal_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.geodata_import_candidate
    ADD CONSTRAINT geodata_import_candidate_import_run_id_ordinal_key UNIQUE (import_run_id, ordinal);


--
-- Name: geodata_import_candidate geodata_import_candidate_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.geodata_import_candidate
    ADD CONSTRAINT geodata_import_candidate_pkey PRIMARY KEY (id);


--
-- Name: geodata_import_processing_queue geodata_import_processing_queue_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.geodata_import_processing_queue
    ADD CONSTRAINT geodata_import_processing_queue_pkey PRIMARY KEY (id);


--
-- Name: geodata_schema_feature geodata_schema_feature_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.geodata_schema_feature
    ADD CONSTRAINT geodata_schema_feature_pkey PRIMARY KEY (name);


--
-- Name: geodata_upload_part geodata_upload_part_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.geodata_upload_part
    ADD CONSTRAINT geodata_upload_part_pkey PRIMARY KEY (upload_session_id, part_number);


--
-- Name: geodata_upload_session geodata_upload_session_object_key_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.geodata_upload_session
    ADD CONSTRAINT geodata_upload_session_object_key_key UNIQUE (object_key);


--
-- Name: geodata_upload_session geodata_upload_session_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.geodata_upload_session
    ADD CONSTRAINT geodata_upload_session_pkey PRIMARY KEY (id);


--
-- Name: idempotency_record idempotency_record_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.idempotency_record
    ADD CONSTRAINT idempotency_record_pkey PRIMARY KEY (service, key);


--
-- Name: import_run import_run_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.import_run
    ADD CONSTRAINT import_run_pkey PRIMARY KEY (id);


--
-- Name: import_schedule import_schedule_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.import_schedule
    ADD CONSTRAINT import_schedule_pkey PRIMARY KEY (id);


--
-- Name: outbox_event outbox_event_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.outbox_event
    ADD CONSTRAINT outbox_event_pkey PRIMARY KEY (event_id);


--
-- Name: outbox_redrive_audit outbox_redrive_audit_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.outbox_redrive_audit
    ADD CONSTRAINT outbox_redrive_audit_pkey PRIMARY KEY (id);


--
-- Name: service_state service_state_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.service_state
    ADD CONSTRAINT service_state_pkey PRIMARY KEY (service);


--
-- Name: source_reference source_reference_adapter_code_source_record_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.source_reference
    ADD CONSTRAINT source_reference_adapter_code_source_record_id_key UNIQUE (adapter_code, source_record_id);


--
-- Name: source_reference source_reference_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.source_reference
    ADD CONSTRAINT source_reference_pkey PRIMARY KEY (id);


--
-- Name: source_snapshot_manifest source_snapshot_manifest_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.source_snapshot_manifest
    ADD CONSTRAINT source_snapshot_manifest_pkey PRIMARY KEY (id);


--
-- Name: source_snapshot_record source_snapshot_record_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.source_snapshot_record
    ADD CONSTRAINT source_snapshot_record_pkey PRIMARY KEY (manifest_id, source_record_id);


--
-- Name: entity_type_global_code_uidx; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX entity_type_global_code_uidx ON public.entity_type USING btree (code) WHERE (programme_id IS NULL);


--
-- Name: geodata_audit_aggregate_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX geodata_audit_aggregate_idx ON public.geodata_audit_event USING btree (aggregate_type, aggregate_id, occurred_at, event_id);


--
-- Name: geodata_candidate_review_page_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX geodata_candidate_review_page_idx ON public.geodata_import_candidate USING btree (import_run_id, validation_status, ordinal, id);


--
-- Name: geodata_category_lookup_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX geodata_category_lookup_idx ON public.geodata_entity_category USING btree (lower(category_code), entity_id);


--
-- Name: geodata_control_status_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX geodata_control_status_idx ON public.geodata_control_record USING btree (kind, ((payload ->> 'status'::text)));


--
-- Name: geodata_edit_staging_geom_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX geodata_edit_staging_geom_idx ON public.geodata_edit_staging USING gist (geom);


--
-- Name: geodata_entity_catalogue_sort_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX geodata_entity_catalogue_sort_idx ON public.geodata_entity USING btree (lower(name), id);


--
-- Name: geodata_entity_category_code_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX geodata_entity_category_code_idx ON public.geodata_entity_category USING btree (category_code, entity_id);


--
-- Name: geodata_entity_category_primary_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX geodata_entity_category_primary_idx ON public.geodata_entity_category USING btree (entity_id) WHERE is_primary;


--
-- Name: geodata_entity_geog_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX geodata_entity_geog_idx ON public.geodata_entity USING gist (((geom)::public.geography));


--
-- Name: geodata_entity_geom_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX geodata_entity_geom_idx ON public.geodata_entity USING gist (geom);


--
-- Name: geodata_entity_lifecycle_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX geodata_entity_lifecycle_idx ON public.geodata_entity USING btree (lifecycle_status, lower(name), id);


--
-- Name: geodata_entity_location_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX geodata_entity_location_idx ON public.geodata_entity USING btree (continent_code, country_code, region_code, province_code, county_code, city);


--
-- Name: geodata_entity_manual_location_fields_gin_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX geodata_entity_manual_location_fields_gin_idx ON public.geodata_entity USING gin (manual_location_fields);


--
-- Name: geodata_entity_programme_slug_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX geodata_entity_programme_slug_idx ON public.geodata_entity USING btree (programme_slug, lifecycle_status);


--
-- Name: geodata_entity_source_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX geodata_entity_source_idx ON public.geodata_entity USING btree (programme_id, source_key, source_state);


--
-- Name: geodata_entity_source_ref_lookup_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX geodata_entity_source_ref_lookup_idx ON public.geodata_entity USING btree (programme_slug, ((public_properties ->> 'sourceRef'::text))) WHERE (public_properties ? 'sourceRef'::text);


--
-- Name: INDEX geodata_entity_source_ref_lookup_idx; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON INDEX public.geodata_entity_source_ref_lookup_idx IS 'Supports source-reference conflation without scanning the full entity catalogue.';


--
-- Name: geodata_entity_status_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX geodata_entity_status_idx ON public.geodata_entity USING btree (programme_id, lifecycle_status);


--
-- Name: geodata_entity_type_code_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX geodata_entity_type_code_idx ON public.geodata_entity USING btree (entity_type_code);


--
-- Name: geodata_import_candidate_geom_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX geodata_import_candidate_geom_idx ON public.geodata_import_candidate USING gist (geom);


--
-- Name: geodata_import_candidate_run_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX geodata_import_candidate_run_idx ON public.geodata_import_candidate USING btree (import_run_id, ordinal);


--
-- Name: geodata_import_candidate_validation_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX geodata_import_candidate_validation_idx ON public.geodata_import_candidate USING btree (validation_status, import_run_id);


--
-- Name: geodata_import_history_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX geodata_import_history_idx ON public.import_run USING btree (started_at DESC, id);


--
-- Name: geodata_import_processing_queue_status_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX geodata_import_processing_queue_status_idx ON public.geodata_import_processing_queue USING btree (status, requested_at);


--
-- Name: geodata_import_queue_recovery_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX geodata_import_queue_recovery_idx ON public.geodata_import_processing_queue USING btree (status, lease_until, requested_at);


--
-- Name: geodata_location_city_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX geodata_location_city_idx ON public.geodata_entity USING btree (lower(COALESCE(NULLIF((public_properties ->> 'city'::text), ''::text), ((public_properties -> 'location'::text) ->> 'city'::text), ''::text)));


--
-- Name: geodata_location_continent_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX geodata_location_continent_idx ON public.geodata_entity USING btree (lower(COALESCE(NULLIF((public_properties ->> 'continent'::text), ''::text), ((public_properties -> 'location'::text) ->> 'continent'::text), ''::text)));


--
-- Name: geodata_location_country_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX geodata_location_country_idx ON public.geodata_entity USING btree (lower(COALESCE(NULLIF((public_properties ->> 'country'::text), ''::text), ((public_properties -> 'location'::text) ->> 'country'::text), ''::text)));


--
-- Name: geodata_location_municipality_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX geodata_location_municipality_idx ON public.geodata_entity USING btree (lower(COALESCE(NULLIF((public_properties ->> 'municipality'::text), ''::text), ((public_properties -> 'location'::text) ->> 'municipality'::text), ''::text)));


--
-- Name: geodata_location_province_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX geodata_location_province_idx ON public.geodata_entity USING btree (lower(COALESCE(NULLIF((public_properties ->> 'province'::text), ''::text), ((public_properties -> 'location'::text) ->> 'province'::text), ''::text)));


--
-- Name: geodata_location_region_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX geodata_location_region_idx ON public.geodata_entity USING btree (lower(COALESCE(NULLIF((public_properties ->> 'region'::text), ''::text), ((public_properties -> 'location'::text) ->> 'region'::text), ''::text)));


--
-- Name: geodata_location_work_recovery_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX geodata_location_work_recovery_idx ON public.geodata_entity USING btree (location_work_dispatched_at, id) WHERE ((public_properties ->> 'locationEnrichmentStatus'::text) = 'QUEUED'::text);


--
-- Name: geodata_upload_session_expiry_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX geodata_upload_session_expiry_idx ON public.geodata_upload_session USING btree (expires_at) WHERE (status = ANY (ARRAY['UPLOADING'::text, 'COMPLETING'::text]));


--
-- Name: geodata_upload_session_idempotency_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX geodata_upload_session_idempotency_idx ON public.geodata_upload_session USING btree (owner_subject, idempotency_key);


--
-- Name: geodata_upload_session_owner_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX geodata_upload_session_owner_idx ON public.geodata_upload_session USING btree (owner_subject, created_at DESC);


--
-- Name: import_queue_work_recovery_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX import_queue_work_recovery_idx ON public.geodata_import_processing_queue USING btree (status, work_dispatched_at, lease_until) WHERE (status = ANY (ARRAY['QUEUED'::text, 'PROCESSING'::text]));


--
-- Name: import_run_cancellation_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX import_run_cancellation_idx ON public.import_run USING btree (status, cancellation_requested_at) WHERE (status = ANY (ARRAY['CANCELLING'::text, 'CANCELLED'::text]));


--
-- Name: import_run_cancelled_retention_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX import_run_cancelled_retention_idx ON public.import_run USING btree (GREATEST(started_at, COALESCE(heartbeat_at, '-infinity'::timestamp with time zone), COALESCE(completed_at, '-infinity'::timestamp with time zone)), id) WHERE (status = ANY (ARRAY['CANCELLING'::text, 'CANCELLED'::text]));


--
-- Name: import_run_processed_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX import_run_processed_idx ON public.import_run USING btree (processed_at);


--
-- Name: import_run_recovery_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX import_run_recovery_idx ON public.import_run USING btree (status, lease_until, started_at);


--
-- Name: import_run_retention_due_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX import_run_retention_due_idx ON public.import_run USING btree (processed_at, id) WHERE ((status = 'PROCESSED'::text) AND (processed_at IS NOT NULL));


--
-- Name: import_run_stale_retention_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX import_run_stale_retention_idx ON public.import_run USING btree (GREATEST(started_at, COALESCE(heartbeat_at, '-infinity'::timestamp with time zone), COALESCE(completed_at, '-infinity'::timestamp with time zone)), id) WHERE (status = ANY (ARRAY['UPLOAD_PENDING'::text, 'QUEUED'::text, 'PROCESSING'::text, 'PREPROCESSED'::text, 'PREPROCESSED_WITH_ERRORS'::text, 'COMPLETED'::text, 'COMPLETED_WITH_ERRORS'::text, 'FAILED'::text]));


--
-- Name: import_run_work_recovery_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX import_run_work_recovery_idx ON public.import_run USING btree (status, work_dispatched_at, lease_until) WHERE (status = ANY (ARRAY['QUEUED'::text, 'PROCESSING'::text]));


--
-- Name: import_schedule_due_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX import_schedule_due_idx ON public.import_schedule USING btree (enabled, next_run_at);


--
-- Name: outbox_dead_letter_unresolved_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX outbox_dead_letter_unresolved_idx ON public.dead_letter_event USING btree (dead_lettered_at) WHERE (resolved_at IS NULL);


--
-- Name: outbox_pending_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX outbox_pending_idx ON public.outbox_event USING btree (available_at, occurred_at) WHERE (published_at IS NULL);


--
-- Name: outbox_redrive_audit_event_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX outbox_redrive_audit_event_idx ON public.outbox_redrive_audit USING btree (event_id, requested_at DESC);


--
-- Name: source_snapshot_manifest_lookup_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX source_snapshot_manifest_lookup_idx ON public.source_snapshot_manifest USING btree (source_key, created_at DESC);


--
-- Name: geodata_entity geodata_entity_maidenhead_cells; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER geodata_entity_maidenhead_cells BEFORE INSERT OR UPDATE OF geom ON public.geodata_entity FOR EACH ROW EXECUTE FUNCTION public.geodata_entity_set_maidenhead_cells();


--
-- Name: geodata_entity geodata_entity_revision; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER geodata_entity_revision BEFORE UPDATE ON public.geodata_entity FOR EACH ROW EXECUTE FUNCTION public.geodata_bump_revision();


--
-- Name: geodata_audit_event geodata_writer_fence; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER geodata_writer_fence BEFORE INSERT OR DELETE OR UPDATE ON public.geodata_audit_event FOR EACH ROW EXECUTE FUNCTION public.geodata_require_row_writer();


--
-- Name: geodata_control_record geodata_writer_fence; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER geodata_writer_fence BEFORE INSERT OR DELETE OR UPDATE ON public.geodata_control_record FOR EACH ROW EXECUTE FUNCTION public.geodata_require_row_writer();


--
-- Name: geodata_entity geodata_writer_fence; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER geodata_writer_fence BEFORE INSERT OR DELETE OR UPDATE ON public.geodata_entity FOR EACH ROW EXECUTE FUNCTION public.geodata_require_row_writer();


--
-- Name: geodata_import_candidate geodata_writer_fence; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER geodata_writer_fence BEFORE INSERT OR DELETE OR UPDATE ON public.geodata_import_candidate FOR EACH ROW EXECUTE FUNCTION public.geodata_require_row_writer();


--
-- Name: geodata_import_processing_queue geodata_writer_fence; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER geodata_writer_fence BEFORE INSERT OR DELETE OR UPDATE ON public.geodata_import_processing_queue FOR EACH ROW EXECUTE FUNCTION public.geodata_require_row_writer();


--
-- Name: import_run geodata_writer_fence; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER geodata_writer_fence BEFORE INSERT OR DELETE OR UPDATE ON public.import_run FOR EACH ROW EXECUTE FUNCTION public.geodata_require_row_writer();


--
-- Name: conflation_candidate conflation_candidate_left_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.conflation_candidate
    ADD CONSTRAINT conflation_candidate_left_entity_id_fkey FOREIGN KEY (left_entity_id) REFERENCES public.geodata_entity(id);


--
-- Name: conflation_candidate conflation_candidate_right_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.conflation_candidate
    ADD CONSTRAINT conflation_candidate_right_entity_id_fkey FOREIGN KEY (right_entity_id) REFERENCES public.geodata_entity(id);


--
-- Name: conflation_candidate conflation_candidate_survivor_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.conflation_candidate
    ADD CONSTRAINT conflation_candidate_survivor_entity_id_fkey FOREIGN KEY (survivor_entity_id) REFERENCES public.geodata_entity(id);


--
-- Name: entity_review entity_review_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.entity_review
    ADD CONSTRAINT entity_review_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.geodata_entity(id);


--
-- Name: geodata_edit_staging geodata_edit_staging_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.geodata_edit_staging
    ADD CONSTRAINT geodata_edit_staging_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.geodata_entity(id);


--
-- Name: geodata_entity_attachment geodata_entity_attachment_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.geodata_entity_attachment
    ADD CONSTRAINT geodata_entity_attachment_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.geodata_entity(id) ON DELETE CASCADE;


--
-- Name: geodata_entity_category geodata_entity_category_category_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.geodata_entity_category
    ADD CONSTRAINT geodata_entity_category_category_id_fkey FOREIGN KEY (category_id) REFERENCES public.entity_type(id) ON DELETE RESTRICT;


--
-- Name: geodata_entity_category geodata_entity_category_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.geodata_entity_category
    ADD CONSTRAINT geodata_entity_category_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.geodata_entity(id) ON DELETE CASCADE;


--
-- Name: geodata_entity geodata_entity_entity_type_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.geodata_entity
    ADD CONSTRAINT geodata_entity_entity_type_id_fkey FOREIGN KEY (entity_type_id) REFERENCES public.entity_type(id);


--
-- Name: geodata_import_candidate geodata_import_candidate_import_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.geodata_import_candidate
    ADD CONSTRAINT geodata_import_candidate_import_run_id_fkey FOREIGN KEY (import_run_id) REFERENCES public.import_run(id) ON DELETE CASCADE;


--
-- Name: geodata_import_processing_queue geodata_import_processing_queue_import_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.geodata_import_processing_queue
    ADD CONSTRAINT geodata_import_processing_queue_import_run_id_fkey FOREIGN KEY (import_run_id) REFERENCES public.import_run(id) ON DELETE CASCADE;


--
-- Name: geodata_upload_part geodata_upload_part_upload_session_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.geodata_upload_part
    ADD CONSTRAINT geodata_upload_part_upload_session_id_fkey FOREIGN KEY (upload_session_id) REFERENCES public.geodata_upload_session(id) ON DELETE CASCADE;


--
-- Name: geodata_upload_session geodata_upload_session_import_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.geodata_upload_session
    ADD CONSTRAINT geodata_upload_session_import_run_id_fkey FOREIGN KEY (import_run_id) REFERENCES public.import_run(id) ON DELETE SET NULL;


--
-- Name: source_reference source_reference_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.source_reference
    ADD CONSTRAINT source_reference_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.geodata_entity(id);


--
-- Name: source_snapshot_manifest source_snapshot_manifest_import_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.source_snapshot_manifest
    ADD CONSTRAINT source_snapshot_manifest_import_run_id_fkey FOREIGN KEY (import_run_id) REFERENCES public.import_run(id);


--
-- Name: source_snapshot_record source_snapshot_record_manifest_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.source_snapshot_record
    ADD CONSTRAINT source_snapshot_record_manifest_id_fkey FOREIGN KEY (manifest_id) REFERENCES public.source_snapshot_manifest(id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--


-- Record that this immutable schema baseline has been installed or adopted.
CREATE TABLE public.myota_schema_migration (
  migration_id text PRIMARY KEY,
  checksum text,
  application_mode text NOT NULL CHECK (application_mode IN ('applied', 'adopted')),
  applied_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO public.myota_schema_migration (migration_id, application_mode)
VALUES ('baseline-v1', 'applied');

-- Enable startup after installing the relational-authority schema.
INSERT INTO public.geodata_schema_feature (name)
VALUES ('row_authority_v1')
ON CONFLICT (name) DO NOTHING;

-- Create the least-privilege roles and QGIS grants from the authoritative
-- Geodata migration source. Roles are cluster-level; grants are database-local.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'myota_geo_qgis_readonly') THEN
    CREATE ROLE myota_geo_qgis_readonly NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'myota_geo_qgis_editor') THEN
    CREATE ROLE myota_geo_qgis_editor NOLOGIN;
  END IF;
END
$$;
GRANT USAGE ON SCHEMA public TO myota_geo_qgis_readonly, myota_geo_qgis_editor;
GRANT SELECT ON public.qgis_entity_review_queue, public.qgis_approved_entities TO myota_geo_qgis_readonly;
GRANT SELECT ON public.qgis_entity_review_queue, public.qgis_approved_entities TO myota_geo_qgis_editor;
GRANT SELECT, INSERT, UPDATE ON public.geodata_edit_staging TO myota_geo_qgis_editor;
REVOKE DELETE ON public.geodata_entity FROM myota_geo_qgis_editor;
