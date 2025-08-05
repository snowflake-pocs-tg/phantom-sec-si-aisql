/*
===============================================================================
AI SUMMARIZE AND COMPARE WORKFLOW
===============================================================================
Purpose: Demonstrate a two-step process to summarize transcripts and then
compare the summaries to the original text using AI functions.

This workflow shows how to:
1. Create AI summaries of call transcripts
2. Measure how well the summaries capture the original content
3. Identify which summaries may need human review

Documentation:
- AI_SUMMARIZE_AGG: https://docs.snowflake.com/en/sql-reference/functions/ai_summarize_agg
- AI_SIMILARITY: https://docs.snowflake.com/en/sql-reference/functions/ai_similarity
===============================================================================
*/

/*
===============================================================================
STEP 1: CREATE AI SUMMARIES
===============================================================================
This query generates AI summaries for each call transcript.

Key Points:
- AI_SUMMARIZE_AGG creates a concise summary of the text
- Replace {text_field} with the actual column name (e.g., CHRONOLOGICAL_TRANSCRIPT)
- Consider adding LIMIT for testing before processing all records
- Summaries typically reduce text by 80-90%

Note: This appears to be a template - the {text_field} placeholder needs to be
replaced with the actual column name.
===============================================================================
*/
SELECT 
    CALL_ID as record_id,
    CHRONOLOGICAL_TRANSCRIPT as original_text,
    AI_SUMMARIZE_AGG(CHRONOLOGICAL_TRANSCRIPT) as call_summary,
    CURRENT_TIMESTAMP() as summarized_at
FROM PHANTOM_SEC_POC.SI.GONG_CLEAN_TRANSCRIPTS
GROUP BY 1,2
LIMIT 10;  -- Add limit for testing

/*
===============================================================================
STEP 2: COMPARE SUMMARIES TO ORIGINALS
===============================================================================
This query computes similarity between original transcripts and their summaries
to assess summary quality.

Key Points:
- AI_SIMILARITY returns a score between -1 and 1 (higher = more similar)
- Categories help identify summaries that may have lost important information
- Low similarity might indicate either poor summary or highly condensed content
- Replace {text_field1} and {text_field2} with actual column names

Note: This query assumes you have a table with both original_text and 
call_summary columns, likely created from Step 1.
===============================================================================
*/
SELECT 
    CALL_ID as record_id,
    LEFT(TRANSCRIPT, 200) as text1_preview,      -- Preview of original
    LEFT(CALL_SUMMARY, 200) as text2_preview,       -- Preview of summary
    AI_SIMILARITY(TRANSCRIPT, CALL_SUMMARY) as similarity_score,
    CASE 
        WHEN AI_SIMILARITY(TRANSCRIPT, CALL_SUMMARY) >= 0.8 THEN 'High Similarity'
        WHEN AI_SIMILARITY(TRANSCRIPT,CALL_SUMMARY) >= 0.5 THEN 'Medium Similarity'
        WHEN AI_SIMILARITY(TRANSCRIPT, CALL_SUMMARY) >= 0.2 THEN 'Low Similarity'
        ELSE 'Very Low Similarity'
    END as similarity_category,
    CURRENT_TIMESTAMP() as compared_at
FROM PHANTOM_SEC_POC.SI.CALL_SUMMARIES_WITH_PREVIEW  -- This table needs to exist
ORDER BY similarity_score DESC;

/*
===============================================================================
INSIGHTS AND BEST PRACTICES
===============================================================================

1. SIMILARITY SCORE INTERPRETATION:
   - 0.8+: Excellent - Summary captures key concepts very well
   - 0.6-0.8: Good - Most important information retained
   - 0.4-0.6: Fair - Some key information may be missing
   - <0.4: Review needed - Summary may have missed critical content

2. FACTORS AFFECTING SIMILARITY:
   - Compression rate: Higher compression = lower similarity (expected)
   - Content type: Technical content may score lower than conversational
   - Length: Very long transcripts may have lower similarity scores

3. USE CASES:
   - Quality assurance for automated summaries
   - Identifying transcripts that need manual summarization
   - Tuning summary parameters for optimal results
   - Compliance verification that key information is retained

4. OPTIMIZATION TIPS:
   - Test with small batches before processing all data
   - Monitor costs as AI functions consume compute credits
   - Consider storing results for reuse rather than recomputing
===============================================================================
*/