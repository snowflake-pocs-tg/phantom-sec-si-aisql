/*
===============================================================================
AI FILTER TOOL
===============================================================================

PURPOSE:
This stored procedure uses Snowflake's AI_FILTER function to classify text
into boolean values based on natural language predicates. Perfect for filtering
records based on semantic criteria, identifying specific conditions in text,
or creating dynamic data subsets based on meaning rather than keywords.

FUNCTION OVERVIEW:
AI_FILTER is a function that:
- Classifies text inputs into TRUE/FALSE based on a predicate
- Uses natural language understanding for flexible filtering
- Supports complex conditions expressed in plain English
- Can be used in WHERE clauses for semantic filtering

Documentation: https://docs.snowflake.com/en/sql-reference/functions/ai_filter

===============================================================================
*/

/*
===============================================================================
FILTER AND SHOW - Returns filtered results while saving to table
===============================================================================
This procedure applies AI filtering AND returns the results immediately.
Following the same pattern as CLASSIFY_AND_SHOW and SUMMARIZE_AND_SHOW.
*/

CREATE OR REPLACE PROCEDURE FILTER_AND_SHOW(
    source_table STRING,
    id_field STRING,
    text_field STRING,
    filter_predicate STRING,
    output_table STRING,
    record_limit INTEGER DEFAULT 10
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'filter_and_show'
AS
$$
def filter_and_show(session, source_table, id_field, text_field, filter_predicate, output_table, record_limit):
    try:
        # Build the query with limit handling
        if record_limit:
            source_clause = f"(SELECT * FROM {source_table} LIMIT {record_limit})"
        else:
            source_clause = source_table
        
        # Escape single quotes in the predicate
        escaped_predicate = filter_predicate.replace("'", "''")
            
        # Build the full query
        # AI_FILTER requires CONCAT to combine predicate with text
        query = f"""
        CREATE OR REPLACE TABLE {output_table} AS
        SELECT 
            {id_field} as record_id,
            LEFT({text_field}, 500) as text_preview,
            AI_FILTER(CONCAT('{escaped_predicate} ', {text_field})) as meets_criteria,
            '{escaped_predicate}' as filter_used,
            CURRENT_TIMESTAMP() as filtered_at
        FROM {source_clause}
        """
        
        # Execute to create the table
        session.sql(query).collect()
        
        # Create a DataFrame from the results
        df = session.table(output_table).select(
            "RECORD_ID", 
            "TEXT_PREVIEW", 
            "MEETS_CRITERIA",
            "FILTER_USED"
        ).to_pandas()
        
        # Calculate statistics
        total_records = len(df)
        matching_records = len(df[df['MEETS_CRITERIA'] == True])
        match_percentage = (matching_records / total_records * 100) if total_records > 0 else 0
        
        # Convert to formatted string output
        output_lines = []
        output_lines.append(f"Results saved to: {output_table}")
        output_lines.append(f"Filter: {filter_predicate}")
        output_lines.append(f"Total records: {total_records}")
        output_lines.append(f"Matching records: {matching_records} ({match_percentage:.1f}%)")
        output_lines.append("-" * 100)
        
        # Add column headers
        output_lines.append(f"{'RECORD_ID':<20} {'MEETS_CRITERIA':<15} {'TEXT_PREVIEW':<65}")
        output_lines.append("-" * 100)
        
        # Show matching records first
        output_lines.append("\n=== MATCHING RECORDS ===")
        matching_df = df[df['MEETS_CRITERIA'] == True]
        for _, row in matching_df.iterrows():
            record_id = str(row['RECORD_ID'])[:20]
            meets = "TRUE"
            text = str(row['TEXT_PREVIEW'])[:62] + "..." if len(str(row['TEXT_PREVIEW'])) > 65 else str(row['TEXT_PREVIEW'])
            output_lines.append(f"{record_id:<20} {meets:<15} {text:<65}")
        
        if matching_records == 0:
            output_lines.append("No records match the filter criteria.")
        
        # Optionally show non-matching records
        if total_records <= 20:  # Only show all if small dataset
            non_matching = total_records - matching_records
            if non_matching > 0:
                output_lines.append("\n=== NON-MATCHING RECORDS ===")
                non_matching_df = df[df['MEETS_CRITERIA'] == False]
                for _, row in non_matching_df.iterrows():
                    record_id = str(row['RECORD_ID'])[:20]
                    meets = "FALSE"
                    text = str(row['TEXT_PREVIEW'])[:62] + "..." if len(str(row['TEXT_PREVIEW'])) > 65 else str(row['TEXT_PREVIEW'])
                    output_lines.append(f"{record_id:<20} {meets:<15} {text:<65}")
        
        return "\n".join(output_lines)
        
    except Exception as e:
        return f"Error: {str(e)}"
$$;

/*
===============================================================================
MULTI COLUMN FILTER AND SHOW - Filter based on multiple columns
===============================================================================
This procedure demonstrates the PROMPT pattern for filtering across multiple
columns as shown in Snowflake documentation.
*/

CREATE OR REPLACE PROCEDURE MULTI_COLUMN_FILTER_AND_SHOW(
    source_table STRING,
    id_field STRING,
    column_list STRING,        -- Comma-separated list of columns to use
    filter_template STRING,    -- Template with {0}, {1}, etc. placeholders
    output_table STRING,
    record_limit INTEGER DEFAULT 10
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'multi_column_filter_and_show'
AS
$$
def multi_column_filter_and_show(session, source_table, id_field, column_list, filter_template, output_table, record_limit):
    try:
        # Parse columns
        columns = [col.strip() for col in column_list.split(',')]
        
        # Build the query with limit handling
        if record_limit:
            source_clause = f"(SELECT * FROM {source_table} LIMIT {record_limit})"
        else:
            source_clause = source_table
        
        # Escape the filter template
        escaped_template = filter_template.replace("'", "''")
        
        # Build PROMPT arguments
        prompt_args = ', '.join(columns)
        
        # Build the full query using PROMPT function
        query = f"""
        CREATE OR REPLACE TABLE {output_table} AS
        SELECT 
            {id_field} as record_id,
            {', '.join([f"LEFT({col}, 200) as {col}_preview" for col in columns])},
            AI_FILTER(PROMPT('{escaped_template}', {prompt_args})) as meets_criteria,
            '{escaped_template}' as filter_template_used,
            CURRENT_TIMESTAMP() as filtered_at
        FROM {source_clause}
        """
        
        # Execute to create the table
        session.sql(query).collect()
        
        # Get column preview names
        preview_columns = [f"{col}_preview" for col in columns]
        
        # Create a DataFrame from the results
        df = session.table(output_table).select(
            "RECORD_ID",
            *[col.upper() for col in preview_columns],
            "MEETS_CRITERIA",
            "FILTER_TEMPLATE_USED"
        ).to_pandas()
        
        # Calculate statistics
        total_records = len(df)
        matching_records = len(df[df['MEETS_CRITERIA'] == True])
        match_percentage = (matching_records / total_records * 100) if total_records > 0 else 0
        
        # Convert to formatted string output
        output_lines = []
        output_lines.append(f"Results saved to: {output_table}")
        output_lines.append(f"Filter Template: {filter_template}")
        output_lines.append(f"Columns Used: {column_list}")
        output_lines.append(f"Total records: {total_records}")
        output_lines.append(f"Matching records: {matching_records} ({match_percentage:.1f}%)")
        output_lines.append("-" * 140)
        
        # Show matching records
        output_lines.append("\n=== MATCHING RECORDS ===")
        matching_df = df[df['MEETS_CRITERIA'] == True]
        
        if len(matching_df) > 0:
            # Create dynamic header based on columns
            header = f"{'RECORD_ID':<15} "
            for col in columns:
                header += f"{col[:20]:<25} "
            header += "RESULT"
            output_lines.append(header)
            output_lines.append("-" * 140)
            
            for _, row in matching_df.head(10).iterrows():
                line = f"{str(row['RECORD_ID'])[:15]:<15} "
                for col in preview_columns:
                    col_upper = col.upper()
                    val = str(row[col_upper])[:22] + "..." if len(str(row[col_upper])) > 25 else str(row[col_upper])
                    line += f"{val:<25} "
                line += "TRUE"
                output_lines.append(line)
        else:
            output_lines.append("No records match the filter criteria.")
        
        return "\n".join(output_lines)
        
    except Exception as e:
        return f"Error: {str(e)}"
$$;

/*
===============================================================================
EXAMPLE USAGE
===============================================================================
*/

-- Example 1: Single filter - Find pricing discussions
CALL FILTER_AND_SHOW(
    'PHANTOM_SEC_POC.SI.GONG_CLEAN_TRANSCRIPTS',
    'CALL_ID',
    'CHRONOLOGICAL_TRANSCRIPT',
    'Is this conversation discussing pricing, budget, or costs?',
    'PHANTOM_SEC_POC.SI.PRICING_DISCUSSIONS',
    10
);

-- Example 3: Customer sentiment filtering
CALL FILTER_AND_SHOW(
    'PHANTOM_SEC_POC.SI.GONG_CLEAN_TRANSCRIPTS',
    'CALL_ID',
    'CHRONOLOGICAL_TRANSCRIPT',
    'Does the customer express dissatisfaction, complaints, or concerns?',
    'PHANTOM_SEC_POC.SI.UNHAPPY_CUSTOMERS',
    20
);

-- Example 4: Competitive intelligence
CALL FILTER_AND_SHOW(
    'PHANTOM_SEC_POC.SI.GONG_CLEAN_TRANSCRIPTS',
    'CALL_ID',
    'CHRONOLOGICAL_TRANSCRIPT',
    'Does this conversation mention Secureframe, Drata, Sprinto, or any other competitors?',
    'PHANTOM_SEC_POC.SI.COMPETITOR_MENTIONS',
    15
);

-- Example 5: Multi-column filtering using PROMPT pattern
-- This example assumes you have a table with multiple relevant columns
CALL MULTI_COLUMN_FILTER_AND_SHOW(
    'PHANTOM_SEC_POC.SI.GONG_CLEAN_TRANSCRIPTS',
    'CALL_ID',
    'PARTICIPANTS, TOPICS_DISCUSSED, COMPANY_DOMAINS',  -- Multiple columns
    '{0} includes executives AND {1} mentions security compliance AND {2} contains .com domain',  -- Template
    'PHANTOM_SEC_POC.SI.EXECUTIVE_SECURITY_CALLS',
    10
);

-- Example 6: Complex multi-column relationship filtering
-- Find calls where participant role matches the topic discussed
CALL MULTI_COLUMN_FILTER_AND_SHOW(
    'PHANTOM_SEC_POC.SI.GONG_ENRICHED_CALLS',  -- Assumes enriched table exists
    'CALL_ID',
    'PARTICIPANT_ROLE, PRIMARY_TOPIC',
    'The {0} is discussing topics relevant to their role which is {1}',
    'PHANTOM_SEC_POC.SI.ROLE_RELEVANT_DISCUSSIONS',
    20
);

/*
===============================================================================
COMMON FILTER PREDICATES
===============================================================================

Sales & Pipeline:
- "Is the customer ready to buy or showing buying signals?"
- "Does this call have a clear next step or follow-up planned?"
- "Is there a decision maker or executive involved in this conversation?"

Customer Sentiment:
- "Does the customer express enthusiasm or excitement?"
- "Are there concerns or objections being raised?"
- "Is the customer comparing us to competitors?"

Technical & Product:
- "Are there technical questions or requirements discussed?"
- "Does the customer mention specific features or integrations?"
- "Is there discussion about implementation or deployment?"

Risk & Churn:
- "Does the customer mention cancellation or not renewing?"
- "Are there unresolved issues or blockers mentioned?"
- "Is the customer frustrated with support or product issues?"

===============================================================================
*/