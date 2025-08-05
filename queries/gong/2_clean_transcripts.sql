/*
===================================================================================
GONG TRANSCRIPT FORMATTER - Creates Human-Readable Call Transcripts
===================================================================================

PURPOSE:
This query transforms Gong call transcript data stored in JSON/VARIANT format into 
readable, chronologically-ordered conversations with speaker names and timestamps.

DATA STRUCTURE:
The raw transcript data in GONG_TRANSCRIPTS table contains a VARIANT column with:
- An array of speaker segments
- Each segment contains:
  - speakerId: Links to GONG_USERS table
  - topic: The conversation phase (e.g., "Small Talk", "Pricing", etc.)
  - sentences: Array of sentence objects with start/end timestamps and text

QUERY LOGIC:
1. LATERAL FLATTEN extracts nested JSON data from the VARIANT column
2. Groups sentences by speaker segment (preserving natural conversation flow)
3. Joins with GONG_USERS to get speaker names
4. Formats output with timestamps and proper spacing

OUTPUT FORMAT:
[MM:SS] Speaker Name: All their sentences from that segment concatenated together.

[MM:SS] Next Speaker: Their response sentences grouped together.

EXAMPLE OUTPUT:
[00:28] Rachel Kim: Last Black Friday we hit 50,000 transactions per hour. It's intense. 
We're a 400-person company but our customer data is growing exponentially.

[00:52] Lisa Chen: PCI compliance must be a major focus then. What's your current approach?

ADDITIONAL FIELDS:
- PARTICIPANTS: List of all people on the call with their titles
- ALL_PARTICIPANT_EMAILS: All email addresses for meeting follow-ups
- CUSTOMER_EMAILS: Just the customer/prospect emails (excludes @phantomsec.com)
- TOPICS_DISCUSSED: Comma-separated list of conversation topics
- COMPANY_DOMAINS: Email domains of all participants (shows which companies were on the call)

===================================================================================
*/

--  Group sentences by speaker segments from JSON structure

CREATE OR REPLACE TABLE GONG_CLEAN_TRANSCRIPTS AS (

    WITH speaker_segments AS (
        -- First, get each speaker segment with its sentences grouped
        SELECT DISTINCT
            gt.CALL_ID,
            f.INDEX as segment_index,
            f.value:speakerId::VARCHAR AS speaker_id,
            f.value:topic::VARCHAR AS topic,
            MIN(s.value:start::NUMBER) AS segment_start_ms,
            MAX(s.value:end::NUMBER) AS segment_end_ms,
            LISTAGG(s.value:text::VARCHAR, ' ') WITHIN GROUP (ORDER BY s.value:start::NUMBER) AS segment_text
        FROM GONG_TRANSCRIPTS gt,
        LATERAL FLATTEN(INPUT => gt.TRANSCRIPT) f,
        LATERAL FLATTEN(INPUT => f.value:sentences) s
        GROUP BY gt.CALL_ID, f.INDEX, f.value:speakerId::VARCHAR, f.value:topic::VARCHAR
    ),
    unique_users AS (
        -- Get unique user info (in case of duplicates)
        SELECT DISTINCT
            ID,
            FIRST_VALUE(FIRST_NAME) OVER (PARTITION BY ID ORDER BY CREATED DESC) AS FIRST_NAME,
            FIRST_VALUE(LAST_NAME) OVER (PARTITION BY ID ORDER BY CREATED DESC) AS LAST_NAME,
            FIRST_VALUE(EMAIL_ADDRESS) OVER (PARTITION BY ID ORDER BY CREATED DESC) AS EMAIL_ADDRESS,
            FIRST_VALUE(TITLE) OVER (PARTITION BY ID ORDER BY CREATED DESC) AS TITLE
        FROM GONG_USERS
    )
    SELECT 
        ss.CALL_ID,
        
        -- Transcript with grouped sentences per speaker segment
        LISTAGG(
            '[' || LPAD(FLOOR(ss.segment_start_ms/1000/60), 2, '0') || ':' || 
            LPAD(MOD(FLOOR(ss.segment_start_ms/1000), 60), 2, '0') || '] ' ||
            COALESCE(uu.FIRST_NAME || ' ' || uu.LAST_NAME, 'Unknown') || ': ' || 
            ss.segment_text,
            '\n\n'
        ) WITHIN GROUP (ORDER BY ss.segment_start_ms) AS CHRONOLOGICAL_TRANSCRIPT,
        
        -- Call metrics
        MIN(ss.segment_start_ms) / 1000 AS CALL_START_SECONDS,
        MAX(ss.segment_end_ms) / 1000 AS CALL_END_SECONDS,
        ROUND((MAX(ss.segment_end_ms) - MIN(ss.segment_start_ms)) / 1000 / 60, 2) AS DURATION_MINUTES,
        COUNT(DISTINCT ss.speaker_id) AS SPEAKER_COUNT,
        SUM(ARRAY_SIZE(SPLIT(ss.segment_text, '. '))) AS APPROX_SENTENCE_COUNT,
        
        -- Participant list with names and titles
        LISTAGG(DISTINCT 
            COALESCE(uu.FIRST_NAME || ' ' || uu.LAST_NAME, 'Unknown') || 
            CASE WHEN uu.TITLE IS NOT NULL THEN ' (' || uu.TITLE || ')' ELSE '' END,
            ', '
        ) AS PARTICIPANTS,
        
        -- Email list for follow-up
        LISTAGG(DISTINCT uu.EMAIL_ADDRESS, '; ') AS ALL_PARTICIPANT_EMAILS,
        
        -- Customer emails only (non-phantomsec.com)
        LISTAGG(DISTINCT 
            CASE WHEN uu.EMAIL_ADDRESS NOT LIKE '%@phantomsec.com%' 
            THEN uu.EMAIL_ADDRESS END, '; '
        ) AS CUSTOMER_EMAILS,
        
        -- Topics discussed
        LISTAGG(DISTINCT ss.topic, ', ') AS TOPICS_DISCUSSED,
        
        -- Company domains involved
        LISTAGG(DISTINCT 
            SPLIT_PART(uu.EMAIL_ADDRESS, '@', 2), ', '
        ) AS COMPANY_DOMAINS

    FROM speaker_segments ss
    LEFT JOIN unique_users uu ON ss.speaker_id = uu.ID
    GROUP BY ss.CALL_ID

); 

SELECT * FROM CLEAN_TRANSCRIPTS;