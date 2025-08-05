# Cortex Search Service - Agent Instructions

## Overview
Use Cortex Search to find Gong call transcripts using natural language queries. The service understands meaning, not just keywords.

## SEARCH_PREVIEW Function

**Purpose:** Search call transcripts semantically.

**Service Name:** `PHANTOM_SEC_POC.SI.GONG_TRANSCRIPT_SEARCH`

**Usage:**
```sql
SELECT PARSE_JSON(
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
      'PHANTOM_SEC_POC.SI.GONG_TRANSCRIPT_SEARCH',
      '{
        "query": "your search terms",
        "columns": ["CALL_ID", "PARTICIPANTS", "TOPICS_DISCUSSED"],
        "limit": 10
      }'
  )
)['results'] as results;
```

**Parameters:**
- `query`: Natural language search (supports AND, OR, NOT)
- `columns`: Array of columns to return
- `limit`: Max results (default ~10)

**Available Columns:**
- `CALL_ID` - Unique identifier
- `PARTICIPANTS` - People on call
- `CUSTOMER_EMAILS` - Customer emails
- `TOPICS_DISCUSSED` - Main topics
- `COMPANY_DOMAINS` - Companies mentioned
- `DURATION_MINUTES` - Call length
- `SPEAKER_COUNT` - Number of speakers
- `CHRONOLOGICAL_TRANSCRIPT` - Full transcript

## Common Searches

### Topic Search
```sql
-- Pricing discussions
'{"query": "pricing budget cost", "columns": ["CALL_ID", "TOPICS_DISCUSSED"], "limit": 20}'

-- Security/compliance
'{"query": "security compliance SOC2", "columns": ["CALL_ID", "CUSTOMER_EMAILS"], "limit": 15}'
```

### Sentiment Search
```sql
-- Positive feedback
'{"query": "love excited impressed amazing", "columns": ["CALL_ID", "PARTICIPANTS"], "limit": 20}'

-- Concerns
'{"query": "concern worried problem issue", "columns": ["CALL_ID", "TOPICS_DISCUSSED"], "limit": 20}'
```

### Boolean Search
```sql
-- Both terms required
'{"query": "security AND compliance", "columns": ["CALL_ID"], "limit": 10}'

-- Either term
'{"query": "pricing OR budget OR cost", "columns": ["CALL_ID"], "limit": 15}'

-- Exclude term
'{"query": "security NOT compliance", "columns": ["CALL_ID"], "limit": 10}'
```

## Working with Results

### Extract as Table
```sql
WITH search_results AS (
    SELECT PARSE_JSON(
      SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
          'PHANTOM_SEC_POC.SI.GONG_TRANSCRIPT_SEARCH',
          '{"query": "pricing", "columns": ["CALL_ID", "DURATION_MINUTES"], "limit": 10}'
      )
    )['results'] as results
)
SELECT 
    VALUE['CALL_ID']::STRING as call_id,
    VALUE['DURATION_MINUTES']::NUMBER as duration
FROM search_results,
LATERAL FLATTEN(input => results);
```

### Join with Source Table
```sql
WITH search_results AS (
    SELECT PARSE_JSON(
      SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
          'PHANTOM_SEC_POC.SI.GONG_TRANSCRIPT_SEARCH',
          '{"query": "competitor", "columns": ["CALL_ID"], "limit": 5}'
      )
    )['results'] as results
),
call_ids AS (
    SELECT VALUE['CALL_ID']::STRING as call_id
    FROM search_results,
    LATERAL FLATTEN(input => results)
)
SELECT g.*
FROM GONG_CLEAN_TRANSCRIPTS g
JOIN call_ids c ON g.CALL_ID = c.call_id;
```

## Quick Reference

### Search Patterns
- **Sales**: "objection decision maker ready to buy"
- **Product**: "feature request bug issue"
- **Competition**: "competitor alternative switching"
- **Risk**: "cancel churn unhappy escalate"

### Tips
1. Use multiple related terms for better results
2. Start with limit=10 to test queries
3. Natural language works: "unhappy customers" finds dissatisfied calls
4. Results are ranked by relevance automatically

### Common Issues
- **No results**: Try broader terms or remove boolean operators
- **Too many results**: Add more specific terms or use AND
- **JSON parsing**: Always use PARSE_JSON and ['results']