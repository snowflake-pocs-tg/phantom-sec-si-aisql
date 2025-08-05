/*
===============================================================================
AI CLASSIFY WITH RESULTS - Returns classified data while saving to table
===============================================================================
This procedure classifies text AND returns the results immediately.
*/

CREATE OR REPLACE PROCEDURE CLASSIFY_AND_SHOW(
    source_table STRING,
    id_field STRING,
    text_field STRING,
    categories STRING,    -- Comma-separated categories
    output_table STRING,
    record_limit INTEGER DEFAULT 10
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'classify_and_show'
AS
$$
import json

def classify_and_show(session, source_table, id_field, text_field, categories, output_table, record_limit):
    try:
        # Convert categories to array format using proper SQL array syntax
        categories_list = [f"'{cat.strip()}'" for cat in categories.split(',')]
        categories_array = f"ARRAY_CONSTRUCT({', '.join(categories_list)})"
        
        # Build the full query
        query = f"""
        CREATE OR REPLACE TABLE {output_table} AS
        SELECT 
            {id_field} as record_id,
            LEFT({text_field}, 200) as original_text,
            AI_CLASSIFY({text_field}, {categories_array}) as classification,
            CURRENT_TIMESTAMP() as classified_at
        FROM {source_table}
        """
        
        if record_limit:
            query += f" LIMIT {record_limit}"
        
        # Execute to create the table
        session.sql(query).collect()
        
        # Query and return the results directly
        results_query = f"""
        SELECT 
            record_id,
            original_text,
            classification
        FROM {output_table}
        ORDER BY classified_at DESC
        """
        
        # Create a DataFrame from the results (use uppercase column names)
        df = session.table(output_table).select("RECORD_ID", "ORIGINAL_TEXT", "CLASSIFICATION").to_pandas()
        
        # Convert to formatted string output (like a table view)
        output_lines = []
        output_lines.append(f"Results saved to: {output_table}")
        output_lines.append(f"Total records: {len(df)}")
        output_lines.append("-" * 100)
        
        # Add column headers
        output_lines.append(f"{'RECORD_ID':<20} {'ORIGINAL_TEXT':<50} {'CLASSIFICATION':<30}")
        output_lines.append("-" * 100)
        
        # Add data rows
        for _, row in df.iterrows():
            record_id = str(row['RECORD_ID'])[:20]
            text = str(row['ORIGINAL_TEXT'])[:47] + "..." if len(str(row['ORIGINAL_TEXT'])) > 50 else str(row['ORIGINAL_TEXT'])
            classification = str(row['CLASSIFICATION'])[:30]
            output_lines.append(f"{record_id:<20} {text:<50} {classification:<30}")
        
        return "\n".join(output_lines)
        
    except Exception as e:
        return f"Error: {str(e)}"
$$;
===============================================================================
USAGE EXAMPLES
===============================================================================
*/

-- Example 1: Classify and show results with Python version (recommended)
CALL CLASSIFY_AND_SHOW(
    'PHANTOM_SEC_POC.SI.GONG_CLEAN_TRANSCRIPTS',
    'CALL_ID',
    'CHRONOLOGICAL_TRANSCRIPT',
    'Secureframe,Drata,Sprinto,TrustCloud,No_Competitor_Mentioned',
    'PHANTOM_SEC_POC.SI.COMPETITOR_CLASSIFICATIONS',
    10
);

SELECT *
FROM PHANTOM_SEC_POC.SI.COMPETITOR_CLASSIFICATIONS;