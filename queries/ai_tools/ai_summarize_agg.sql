/*
===============================================================================
AI SUMMARIZE AGGREGATION TOOL
===============================================================================

/*
===============================================================================
SUMMARIZE AND SHOW - Returns summarized data while saving to table
===============================================================================
This procedure summarizes text AND returns the results immediately.
Following the same pattern as CLASSIFY_AND_SHOW for consistency.
*/

CREATE OR REPLACE PROCEDURE SUMMARIZE_AND_SHOW(
    source_table STRING,
    id_field STRING,
    text_field STRING,
    output_table STRING,
    record_limit INTEGER DEFAULT 10
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'summarize_and_show'
AS
$$
def summarize_and_show(session, source_table, id_field, text_field, output_table, record_limit):
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
            {id_field} as call_id,
            ANY_VALUE({text_field}) as transcript,
            AI_SUMMARIZE_AGG({text_field}) as call_summary,
            CURRENT_TIMESTAMP() as summarized_at
        FROM {source_clause}
        GROUP BY {id_field}
        """
        
        # Execute to create the table
        session.sql(query).collect()
        
        # Create a DataFrame from the results (use uppercase column names)
        df = session.table(output_table).select("CALL_ID", "TRANSCRIPT", "CALL_SUMMARY").to_pandas()
        
        # Convert to formatted string output (like a table view)
        output_lines = []
        output_lines.append(f"Results saved to: {output_table}")
        output_lines.append(f"Total records: {len(df)}")
        output_lines.append("-" * 120)
        
        # Add column headers
        output_lines.append(f"{'CALL_ID':<20} {'TRANSCRIPT':<50} {'CALL_SUMMARY':<50}")
        output_lines.append("-" * 120)
        
        # Add data rows
        for _, row in df.iterrows():
            call_id = str(row['CALL_ID'])[:20]
            # Create preview of transcript for display only
            transcript_preview = str(row['TRANSCRIPT'])[:47] + "..." if len(str(row['TRANSCRIPT'])) > 50 else str(row['TRANSCRIPT'])
            summary = str(row['CALL_SUMMARY'])[:47] + "..." if len(str(row['CALL_SUMMARY'])) > 50 else str(row['CALL_SUMMARY'])
            output_lines.append(f"{call_id:<20} {transcript_preview:<50} {summary:<50}")
        
        return "\n".join(output_lines)
        
    except Exception as e:
        return f"Error: {str(e)}"
$$;

/*
===============================================================================
EXAMPLE USAGE - SUMMARIZE AND SHOW
===============================================================================
*/

-- Example: Summarize Gong transcripts and show results immediately
CALL SUMMARIZE_AND_SHOW(
    'PHANTOM_SEC_POC.SI.GONG_CLEAN_TRANSCRIPTS',
    'CALL_ID',
    'CHRONOLOGICAL_TRANSCRIPT',
    'PHANTOM_SEC_POC.SI.CALL_SUMMARIES_WITH_PREVIEW',
    10  -- Just process 5 records for testing
);

SELECT *
FROM PHANTOM_SEC_POC.SI.CALL_SUMMARIES_WITH_PREVIEW;