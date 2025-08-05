CREATE DATABASE IF NOT EXISTS PHANTOM_SEC_POC;
CREATE SCHEMA IF NOT EXISTS PHANTOM_SEC_POC.SI;

-- Ensure we're using the correct database and schema
USE DATABASE PHANTOM_SEC_POC;
USE SCHEMA SI;

-- Create a stage for loading JSON files
CREATE OR REPLACE STAGE GONG_DATA_STAGE
    FILE_FORMAT = (TYPE = 'JSON');

/*
===================================================================================
FILE UPLOAD - DYNAMIC PATH HANDLING
===================================================================================

Since Snowflake's PUT command requires absolute paths and doesn't support relative
paths, you need to adjust the paths based on your local repository location.

The data files are located at: ../../data/ (relative to this script)

Using SnowSQL variables (RECOMMENDED):
------------------------------------------------
PUT file://data/GONG_USERS.json @GONG_DATA_STAGE/users/;
PUT file://data/GONG_DATA.json @GONG_DATA_STAGE/transcripts/;

===================================================================================
*/

-- Upload files to the stage
-- IMPORTANT: Replace {REPO_PATH} with the absolute path to your vanta-si-aisql repository
-- The data files are located in the /data subdirectory of the repository
PUT file://data/GONG_USERS.json @GONG_DATA_STAGE/users/;
PUT file://data/GONG_DATA.json @GONG_DATA_STAGE/transcripts/;

-- List files in stage to verify upload
LIST @GONG_DATA_STAGE;

-- Create a temporary table for GONG_USERS data
CREATE OR REPLACE TEMPORARY TABLE TEMP_GONG_USERS_JSON (
    json_data VARIANT
);

-- Load JSON data into temporary table
COPY INTO TEMP_GONG_USERS_JSON
FROM @GONG_DATA_STAGE/users/GONG_USERS.json
FILE_FORMAT = (TYPE = 'JSON');

-- Insert flattened data into GONG_USERS
INSERT INTO GONG_USERS
SELECT 
    u.value:id::VARCHAR(20) AS ID,
    u.value:emailAddress::VARCHAR(255) AS EMAIL_ADDRESS,
    u.value:created::TIMESTAMP_TZ AS CREATED,
    u.value:active::BOOLEAN AS ACTIVE,
    u.value:emailAliases::ARRAY AS EMAIL_ALIASES,
    u.value:trustedEmailAddress::VARCHAR(255) AS TRUSTED_EMAIL_ADDRESS,
    u.value:firstName::VARCHAR(100) AS FIRST_NAME,
    u.value:lastName::VARCHAR(100) AS LAST_NAME,
    u.value:title::VARCHAR(255) AS TITLE,
    u.value:phoneNumber::VARCHAR(50) AS PHONE_NUMBER,
    u.value:extension::VARCHAR(10) AS EXTENSION,
    u.value:personalMeetingUrls::ARRAY AS PERSONAL_MEETING_URLS,
    u.value:settings::OBJECT AS SETTINGS,
    u.value:managerId::VARCHAR(20) AS MANAGER_ID,
    u.value:meetingConsentPageUrl::VARCHAR(500) AS MEETING_CONSENT_PAGE_URL,
    u.value:spokenLanguages::ARRAY AS SPOKEN_LANGUAGES
FROM TEMP_GONG_USERS_JSON t,
LATERAL FLATTEN(input => t.json_data:users) u;

-- Create a temporary table for GONG_TRANSCRIPTS data
CREATE OR REPLACE TEMPORARY TABLE TEMP_GONG_TRANSCRIPTS_JSON (
    json_data VARIANT
);

-- Load JSON data into temporary table
COPY INTO TEMP_GONG_TRANSCRIPTS_JSON
FROM @GONG_DATA_STAGE/transcripts/GONG_DATA.json
FILE_FORMAT = (TYPE = 'JSON');

-- Insert flattened data into GONG_TRANSCRIPTS
INSERT INTO GONG_TRANSCRIPTS (CALL_ID, TRANSCRIPT, CREATED_AT, UPDATED_AT)
SELECT 
    t.value:callId::VARCHAR(20) AS CALL_ID,
    t.value:transcript::ARRAY AS TRANSCRIPT,
    CURRENT_TIMESTAMP() AS CREATED_AT,
    CURRENT_TIMESTAMP() AS UPDATED_AT
FROM TEMP_GONG_TRANSCRIPTS_JSON f,
LATERAL FLATTEN(input => f.json_data:callTranscripts) t;

-- Verify data was loaded
SELECT * FROM GONG_USERS;
SELECT COUNT(*) AS user_count FROM GONG_USERS;

SELECT * FROM GONG_TRANSCRIPTS;
SELECT COUNT(*) AS transcript_count FROM GONG_TRANSCRIPTS;