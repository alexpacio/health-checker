-- Create the ENUM type first
CREATE TYPE healthcheck_error_type AS ENUM (
    'StatusCodeError',
    'ContentMatchError',
    'TimeoutError',
    'ConnectionError',
    'UnexpectedError'
);

-- Create the healthcheck_settings table
CREATE TABLE healthcheck_settings (
    id bigserial PRIMARY KEY, -- target id
    url text NOT NULL,
    regex_match text,
    expected_status_code integer,
    check_interval integer, -- in seconds
    timeout_ms integer,
    headers jsonb,
    active boolean NOT NULL DEFAULT true,
    created_at timestamp with time zone DEFAULT NOW(),
    updated_at timestamp with time zone DEFAULT NOW()
);

-- Create indexes on healthcheck_settings
CREATE INDEX idx_healthcheck_settings_active ON healthcheck_settings (active);

-- Create the partitioned healthcheck_results table
CREATE TABLE healthcheck_results (
    id bigserial,
    target_id integer NOT NULL,
    check_time timestamp with time zone NOT NULL,
    response_time real,
    status_code integer,
    success boolean NOT NULL,
    error_type healthcheck_error_type,
    error_message text,
    content_match_success boolean,
    PRIMARY KEY (id, check_time),
    CONSTRAINT fk_healthcheck_settings FOREIGN KEY (target_id) REFERENCES healthcheck_settings(id)
) PARTITION BY RANGE (check_time);

-- Create indexes for common queries
CREATE INDEX idx_healthcheck_results_target_id ON healthcheck_results (target_id);
CREATE INDEX idx_healthcheck_results_check_time ON healthcheck_results (check_time);
CREATE INDEX idx_healthcheck_results_combined ON healthcheck_results (target_id, check_time);
CREATE INDEX idx_healthcheck_results_success ON healthcheck_results (success);
CREATE INDEX idx_healthcheck_results_status_code ON healthcheck_results (status_code);
CREATE INDEX idx_healthcheck_results_error_type ON healthcheck_results (error_type);
CREATE INDEX idx_healthcheck_results_response_time ON healthcheck_results (response_time);
CREATE INDEX idx_healthcheck_results_content_match_success ON healthcheck_results (content_match_success);