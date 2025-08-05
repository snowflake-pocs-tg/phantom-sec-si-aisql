/*
===================================================================================
CORTEX SEARCH SERVICE FOR GONG TRANSCRIPTS
===================================================================================

This script creates a Snowflake Cortex Search Service to enable semantic search
on Gong call transcripts. Users can search for conversations using natural language
queries and filter by various attributes.

PREREQUISITES:
1. GONG_CLEAN_TRANSCRIPTS table must be created and populated
2. User must have SNOWFLAKE.CORTEX_USER database role
3. Change tracking must be enabled on the source table

===================================================================================
*/

-- Ensure we're in the correct database and schema
USE DATABASE PHANTOM_SEC_POC;
USE SCHEMA SI;

-- Step 1: Enable change tracking on the source table (required for Cortex Search)
ALTER TABLE GONG_CLEAN_TRANSCRIPTS SET CHANGE_TRACKING = TRUE;

-- Step 2: Create a dedicated warehouse for the search service (recommended)
-- Using XSMALL as transcripts are relatively small dataset
CREATE WAREHOUSE IF NOT EXISTS GONG_SEARCH_WH 
    WITH WAREHOUSE_SIZE = 'XSMALL' 
    AUTO_SUSPEND = 60 
    AUTO_RESUME = TRUE;

-- Step 3: Create the Cortex Search Service
CREATE OR REPLACE CORTEX SEARCH SERVICE GONG_TRANSCRIPT_SEARCH
    ON CHRONOLOGICAL_TRANSCRIPT  -- The text column to search
    ATTRIBUTES 
        CALL_ID,
        PARTICIPANTS,
        CUSTOMER_EMAILS,
        TOPICS_DISCUSSED,
        COMPANY_DOMAINS,
        DURATION_MINUTES,
        SPEAKER_COUNT
    WAREHOUSE = DEVELOPMENT_WH
    TARGET_LAG = '1 hour'  -- Refresh index every hour
    INITIALIZE = ON_CREATE  -- Build index immediately
    COMMENT = 'Semantic search for Gong call transcripts'
    AS (
        SELECT 
            CALL_ID,
            CHRONOLOGICAL_TRANSCRIPT,
            PARTICIPANTS,
            CUSTOMER_EMAILS,
            TOPICS_DISCUSSED,
            COMPANY_DOMAINS,
            DURATION_MINUTES,
            SPEAKER_COUNT,
            CALL_START_SECONDS,
            CALL_END_SECONDS
        FROM GONG_CLEAN_TRANSCRIPTS
    );

-- Step 4: Verify the search service was created
SHOW CORTEX SEARCH SERVICES LIKE 'GONG_TRANSCRIPT_SEARCH';

-- Step 5: Example search queries using SEARCH_PREVIEW function

-- Search for conversations about pricing
SELECT PARSE_JSON(
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
      'PHANTOM_SEC_POC.SI.GONG_TRANSCRIPT_SEARCH',
      '{
        "query": "discussions about pricing or budget",
        "columns": ["CALL_ID", "PARTICIPANTS", "TOPICS_DISCUSSED", "CHRONOLOGICAL_TRANSCRIPT"],
        "limit": 10
      }'
  )
)['results'] as results;

-- Search for company in the query text itself
SELECT PARSE_JSON(
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
      'PHANTOM_SEC_POC.SI.GONG_TRANSCRIPT_SEARCH',
      '{
        "query": "techguard security compliance requirements",
        "columns": ["CALL_ID", "CUSTOMER_EMAILS", "COMPANY_DOMAINS", "DURATION_MINUTES"],
        "limit": 5
      }'
  )
)['results'] as results;


-- Search for executive-level conversations
SELECT PARSE_JSON(
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
      'PHANTOM_SEC_POC.SI.GONG_TRANSCRIPT_SEARCH',
      '{
        "query": "chief information security officer discussing compliance",
        "columns": ["CALL_ID", "PARTICIPANTS", "DURATION_MINUTES", "TOPICS_DISCUSSED"],
        "limit": 10
      }'
  )
)['results'] as results;

-- Search for competitor mentions
SELECT PARSE_JSON(
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
      'PHANTOM_SEC_POC.SI.GONG_TRANSCRIPT_SEARCH',
      '{
        "query": "Secureframe OR Drata OR competitor",
        "columns": ["CALL_ID", "PARTICIPANTS", "CHRONOLOGICAL_TRANSCRIPT"],
        "limit": 5
      }'
  )
)['results'] as results;