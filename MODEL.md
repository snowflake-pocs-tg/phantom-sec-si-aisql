# Transcript Data Modeling Options

## Overview
The transcript data is stored as a VARIANT column containing an array of conversation segments. Each segment includes:
- `speakerId`: The ID of the person speaking
- `topic`: The conversation topic/phase
- `sentences`: Array of sentence objects with `start`, `end`, and `text` fields

## Option 1: Concatenated Full Transcript by Call
Creates one record per call with all sentences concatenated in chronological order.

```sql
SELECT 
    gt.CALL_ID,
    LISTAGG(
        s.value:text::VARCHAR, 
        ' '
    ) WITHIN GROUP (ORDER BY ts.INDEX, s.INDEX) AS FULL_TRANSCRIPT,
    MIN(s.value:start::NUMBER) AS CALL_START_MS,
    MAX(s.value:end::NUMBER) AS CALL_END_MS,
    COUNT(DISTINCT ts.value:speakerId::VARCHAR) AS SPEAKER_COUNT,
    ARRAY_AGG(DISTINCT ts.value:topic::VARCHAR) AS TOPICS_DISCUSSED
FROM GONG_TRANSCRIPTS gt,
LATERAL FLATTEN(input => gt.TRANSCRIPT) ts,
LATERAL FLATTEN(input => ts.value:sentences) s
GROUP BY gt.CALL_ID;
```

### Benefits:
- Single row per call for easy full-text search
- Complete conversation flow preserved
- Useful for NLP/sentiment analysis on entire conversations
- Simple to query and analyze call-level metrics

## Option 2: Sentence-Level Detail Table
Creates one record per sentence with full context.

```sql
SELECT 
    gt.CALL_ID,
    ts.INDEX AS SEGMENT_INDEX,
    ts.value:speakerId::VARCHAR(20) AS SPEAKER_ID,
    ts.value:topic::VARCHAR(255) AS TOPIC,
    s.INDEX AS SENTENCE_INDEX,
    s.value:start::NUMBER AS START_TIME_MS,
    s.value:end::NUMBER AS END_TIME_MS,
    s.value:text::VARCHAR AS SENTENCE_TEXT,
    (s.value:end::NUMBER - s.value:start::NUMBER) AS DURATION_MS,
    gu.FIRST_NAME || ' ' || gu.LAST_NAME AS SPEAKER_NAME,
    gu.EMAIL_ADDRESS AS SPEAKER_EMAIL,
    gu.TITLE AS SPEAKER_TITLE,
    gu.ACTIVE AS SPEAKER_ACTIVE
FROM GONG_TRANSCRIPTS gt,
LATERAL FLATTEN(input => gt.TRANSCRIPT) ts,
LATERAL FLATTEN(input => ts.value:sentences) s
LEFT JOIN GONG_USERS gu ON ts.value:speakerId::VARCHAR = gu.ID;
```

### Benefits:
- Granular analysis at the sentence level
- Easy to filter by speaker, topic, or time range
- Supports detailed conversation flow analysis
- Can calculate speaking time per person
- Preserves all original data relationships

## Option 3: Speaker-Topic Summary Table
Creates aggregated view by speaker and topic within each call.

```sql
SELECT 
    gt.CALL_ID,
    ts.value:speakerId::VARCHAR(20) AS SPEAKER_ID,
    ts.value:topic::VARCHAR(255) AS TOPIC,
    COUNT(s.value) AS SENTENCE_COUNT,
    LISTAGG(
        s.value:text::VARCHAR, 
        ' '
    ) WITHIN GROUP (ORDER BY s.INDEX) AS TOPIC_TRANSCRIPT,
    MIN(s.value:start::NUMBER) AS TOPIC_START_MS,
    MAX(s.value:end::NUMBER) AS TOPIC_END_MS,
    SUM(s.value:end::NUMBER - s.value:start::NUMBER) AS TOTAL_SPEAKING_TIME_MS,
    gu.FIRST_NAME || ' ' || gu.LAST_NAME AS SPEAKER_NAME,
    gu.EMAIL_ADDRESS AS SPEAKER_EMAIL,
    gu.TITLE AS SPEAKER_TITLE,
    CASE 
        WHEN gu.TITLE LIKE '%Sales%' OR gu.TITLE LIKE '%Account Executive%' THEN 'Sales'
        WHEN gu.TITLE LIKE '%Chief%' OR gu.TITLE LIKE '%Director%' OR gu.TITLE LIKE '%VP%' THEN 'Customer'
        ELSE 'Other'
    END AS SPEAKER_ROLE
FROM GONG_TRANSCRIPTS gt,
LATERAL FLATTEN(input => gt.TRANSCRIPT) ts,
LATERAL FLATTEN(input => ts.value:sentences) s
LEFT JOIN GONG_USERS gu ON ts.value:speakerId::VARCHAR = gu.ID
GROUP BY 
    gt.CALL_ID,
    ts.value:speakerId::VARCHAR(20),
    ts.value:topic::VARCHAR(255),
    gu.FIRST_NAME,
    gu.LAST_NAME,
    gu.EMAIL_ADDRESS,
    gu.TITLE;
```

### Benefits:
- Perfect for analyzing conversation dynamics by topic
- Easy to see how much each person spoke about each topic
- Useful for sales coaching (time spent on different topics)
- Can identify conversation patterns
- Maintains topic boundaries for context

## Recommended Approach

For most use cases, I recommend creating materialized views for all three options:

1. **Option 1** as `MV_CALL_TRANSCRIPTS` - For full-text search and call summaries
2. **Option 2** as `MV_TRANSCRIPT_SENTENCES` - For detailed analysis and ML features
3. **Option 3** as `MV_SPEAKER_TOPICS` - For conversation analytics and coaching

This gives you flexibility to query at different granularities without repeatedly flattening the VARIANT column.

## Performance Considerations

- Create clustering keys on CALL_ID for all tables
- Consider adding secondary clustering on START_TIME_MS for Option 2
- Add search optimization on text columns if using Snowflake Enterprise Edition
- Monitor query patterns and add additional indexes as needed