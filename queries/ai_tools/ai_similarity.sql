/*
===============================================================================
AI SIMILARITY TOOL
===============================================================================

/*
===============================================================================
SIMILARITY AND SHOW - Returns similarity scores while saving to table
===============================================================================
This procedure computes similarity AND returns the results immediately.
Following the same pattern as CLASSIFY_AND_SHOW and SUMMARIZE_AND_SHOW.
*/

CREATE OR REPLACE PROCEDURE SIMILARITY_AND_SHOW(
    source_table STRING,
    id_field STRING,
    text_field1 STRING,
    text_field2 STRING,
    output_table STRING,
    record_limit INTEGER DEFAULT 10
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'similarity_and_show'
AS
$$
def similarity_and_show(session, source_table, id_field, text_field1, text_field2, output_table, record_limit):
    try:
        # Build the query with limit handling
        if record_limit:
            source_clause = f"(SELECT * FROM {source_table} LIMIT {record_limit})"
        else:
            source_clause = source_table
            
        # Build the full query
        query = f"""
        CREATE OR REPLACE TABLE {output_table} AS
        SELECT 
            {id_field} as record_id,
            LEFT({text_field1}, 1000) as text1_preview,
            LEFT({text_field2}, 1000) as text2_preview,
            AI_SIMILARITY({text_field1}, {text_field2}) as similarity_score,
            CASE 
                WHEN AI_SIMILARITY({text_field1}, {text_field2}) >= 0.8 THEN 'High Similarity'
                WHEN AI_SIMILARITY({text_field1}, {text_field2}) >= 0.5 THEN 'Medium Similarity'
                WHEN AI_SIMILARITY({text_field1}, {text_field2}) >= 0.2 THEN 'Low Similarity'
                ELSE 'Very Low Similarity'
            END as similarity_category,
            CURRENT_TIMESTAMP() as compared_at
        FROM {source_clause}
        """
        
        # Execute to create the table
        session.sql(query).collect()
        
        # Create a DataFrame from the results
        df = session.table(output_table).select(
            "RECORD_ID", 
            "TEXT1_PREVIEW", 
            "TEXT2_PREVIEW", 
            "SIMILARITY_SCORE", 
            "SIMILARITY_CATEGORY"
        ).order_by("SIMILARITY_SCORE", ascending=False).to_pandas()
        
        # Convert to formatted string output
        output_lines = []
        output_lines.append(f"Results saved to: {output_table}")
        output_lines.append(f"Total records: {len(df)}")
        output_lines.append("-" * 140)
        
        # Add column headers
        output_lines.append(f"{'RECORD_ID':<15} {'TEXT1':<40} {'TEXT2':<40} {'SCORE':<8} {'CATEGORY':<20}")
        output_lines.append("-" * 140)
        
        # Add data rows
        for _, row in df.iterrows():
            record_id = str(row['RECORD_ID'])[:15]
            text1 = str(row['TEXT1_PREVIEW'])[:37] + "..." if len(str(row['TEXT1_PREVIEW'])) > 40 else str(row['TEXT1_PREVIEW'])
            text2 = str(row['TEXT2_PREVIEW'])[:37] + "..." if len(str(row['TEXT2_PREVIEW'])) > 40 else str(row['TEXT2_PREVIEW'])
            score = f"{row['SIMILARITY_SCORE']:.3f}"
            category = row['SIMILARITY_CATEGORY']
            output_lines.append(f"{record_id:<15} {text1:<40} {text2:<40} {score:<8} {category:<20}")
        
        # Add summary statistics
        output_lines.append("-" * 140)
        output_lines.append(f"\nSummary Statistics:")
        output_lines.append(f"Average Similarity: {df['SIMILARITY_SCORE'].mean():.3f}")
        output_lines.append(f"Highest Similarity: {df['SIMILARITY_SCORE'].max():.3f}")
        output_lines.append(f"Lowest Similarity: {df['SIMILARITY_SCORE'].min():.3f}")
        
        # Category breakdown
        category_counts = df['SIMILARITY_CATEGORY'].value_counts()
        output_lines.append(f"\nCategory Breakdown:")
        for category, count in category_counts.items():
            output_lines.append(f"  {category}: {count} ({count/len(df)*100:.1f}%)")
        
        return "\n".join(output_lines)
        
    except Exception as e:
        return f"Error: {str(e)}"
$$;

/*
===============================================================================
EXAMPLE USAGE - SIMILARITY AND SHOW
===============================================================================
*/

-- Example 1: Compare transcript to summary and show results immediately
CALL SIMILARITY_AND_SHOW(
    'PHANTOM_SEC_POC.SI.CALL_SUMMARIES',
    'call_id',
    'transcript',
    'call_summary',
    'PHANTOM_SEC_POC.SI.TRANSCRIPT_SUMMARY_SIMILARITY',
    5  -- Just process 5 records for testing
);

-- Example 2: Compare two different text fields in Gong transcripts
-- This could be used to compare different parts of the same conversation
CALL SIMILARITY_AND_SHOW(
    'PHANTOM_SEC_POC.SI.GONG_CLEAN_TRANSCRIPTS',
    'CALL_ID',
    'CHRONOLOGICAL_TRANSCRIPT',
    'CHRONOLOGICAL_TRANSCRIPT',  -- Compare to itself as a test
    'PHANTOM_SEC_POC.SI.SELF_SIMILARITY_TEST',
    3
);