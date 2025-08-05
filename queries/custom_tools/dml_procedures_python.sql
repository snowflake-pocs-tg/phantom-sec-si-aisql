/*
===============================================================================
DML PROCEDURES - PYTHON VERSION FOR SNOWFLAKE INTELLIGENCE
===============================================================================

PURPOSE:
Python-based stored procedures that can handle query results and return
formatted data for Snowflake Intelligence tools. These procedures can
process and return actual query results as formatted strings.

OVERVIEW:
- QUERY_DATA_PY: Query data and return formatted results
- EXECUTE_DML_PY: Execute DML and return results/status
- INSERT_DATA_PY: Insert data with validation and feedback

===============================================================================
*/

/*
===============================================================================
QUERY DATA - PYTHON VERSION
===============================================================================
Queries data and returns formatted results as a string.
*/

CREATE OR REPLACE PROCEDURE QUERY_DATA_PY(
    table_name STRING,
    columns_list STRING DEFAULT NULL,
    where_clause STRING DEFAULT NULL,
    order_by_clause STRING DEFAULT NULL,
    record_limit INTEGER DEFAULT 100,
    output_format STRING DEFAULT 'table'  -- 'table', 'json', 'csv'
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'query_data'
AS
$$
import json

def query_data(session, table_name, columns_list, where_clause, order_by_clause, record_limit, output_format):
    try:
        # Build the query
        if columns_list:
            query = f"SELECT {columns_list} FROM {table_name}"
        else:
            query = f"SELECT * FROM {table_name}"
        
        # Add WHERE clause
        if where_clause:
            query += f" WHERE {where_clause}"
        
        # Add ORDER BY clause
        if order_by_clause:
            query += f" ORDER BY {order_by_clause}"
        
        # Add LIMIT
        if record_limit and record_limit > 0:
            query += f" LIMIT {record_limit}"
        
        # Execute query
        result = session.sql(query)
        df = result.collect()
        
        # Get column names
        columns = result.schema.names
        
        # Format based on output type
        if output_format == 'json':
            # Convert to JSON format
            rows = []
            for row in df:
                row_dict = {columns[i]: str(row[i]) for i in range(len(columns))}
                rows.append(row_dict)
            return json.dumps({"query": query, "row_count": len(df), "data": rows}, indent=2)
        
        elif output_format == 'csv':
            # Convert to CSV format
            csv_lines = [','.join(columns)]
            for row in df:
                csv_lines.append(','.join([str(val) for val in row]))
            return '\n'.join(csv_lines)
        
        else:  # Default to table format
            # Create a formatted table
            output = f"Query: {query}\n"
            output += f"Rows returned: {len(df)}\n\n"
            
            if len(df) == 0:
                return output + "No data found."
            
            # Calculate column widths
            col_widths = []
            for i, col in enumerate(columns):
                max_width = len(col)
                for row in df[:50]:  # Check first 50 rows for width
                    val_len = len(str(row[i]))
                    if val_len > max_width:
                        max_width = val_len
                col_widths.append(min(max_width, 50))  # Cap at 50 chars
            
            # Create header
            header = '| '
            separator = '|-'
            for i, col in enumerate(columns):
                header += col.ljust(col_widths[i]) + ' | '
                separator += '-' * col_widths[i] + '-|-'
            
            output += header + '\n'
            output += separator + '\n'
            
            # Add data rows
            for row in df:
                row_str = '| '
                for i, val in enumerate(row):
                    val_str = str(val)[:col_widths[i]]
                    row_str += val_str.ljust(col_widths[i]) + ' | '
                output += row_str + '\n'
            
            return output
            
    except Exception as e:
        return f"Error executing query: {str(e)}\nQuery attempted: {query if 'query' in locals() else 'Query not built'}"
$$;

/*
===============================================================================
EXECUTE DML - PYTHON VERSION
===============================================================================
Executes any DML statement and returns appropriate results.
*/

CREATE OR REPLACE PROCEDURE EXECUTE_DML_PY(
    dml_statement STRING,
    output_format STRING DEFAULT 'table'  -- 'table', 'json', 'summary'
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'execute_dml'
AS
$$
import json

def execute_dml(session, dml_statement, output_format):
    try:
        # Validate input
        if not dml_statement or not dml_statement.strip():
            return "Error: DML statement cannot be empty"
        
        # Get statement type
        statement_type = dml_statement.strip().split()[0].upper()
        
        # Validate it's a DML statement
        if statement_type not in ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'MERGE']:
            return f"Error: Only DML statements are allowed. Got: {statement_type}"
        
        # Execute the statement
        result = session.sql(dml_statement)
        
        if statement_type == 'SELECT':
            # For SELECT, collect and format results
            df = result.collect()
            columns = result.schema.names
            
            if output_format == 'json':
                rows = []
                for row in df:
                    row_dict = {columns[i]: str(row[i]) for i in range(len(columns))}
                    rows.append(row_dict)
                return json.dumps({
                    "statement": dml_statement,
                    "type": "SELECT",
                    "row_count": len(df),
                    "data": rows
                }, indent=2)
            
            elif output_format == 'summary':
                return f"SELECT executed successfully. Retrieved {len(df)} rows."
            
            else:  # table format
                output = f"Statement: {dml_statement}\n"
                output += f"Rows returned: {len(df)}\n\n"
                
                if len(df) == 0:
                    return output + "No data found."
                
                # Format as table (similar to QUERY_DATA_PY)
                col_widths = []
                for i, col in enumerate(columns):
                    max_width = len(col)
                    for row in df[:50]:
                        val_len = len(str(row[i]))
                        if val_len > max_width:
                            max_width = val_len
                    col_widths.append(min(max_width, 50))
                
                header = '| '
                separator = '|-'
                for i, col in enumerate(columns):
                    header += col.ljust(col_widths[i]) + ' | '
                    separator += '-' * col_widths[i] + '-|-'
                
                output += header + '\n'
                output += separator + '\n'
                
                for row in df[:100]:  # Limit display to 100 rows
                    row_str = '| '
                    for i, val in enumerate(row):
                        val_str = str(val)[:col_widths[i]]
                        row_str += val_str.ljust(col_widths[i]) + ' | '
                    output += row_str + '\n'
                
                if len(df) > 100:
                    output += f"\n... ({len(df) - 100} more rows)"
                
                return output
        else:
            # For other DML operations, get affected rows
            result.collect()  # Execute the statement
            return f"{statement_type} executed successfully. Use QUERY_HISTORY to check affected rows."
            
    except Exception as e:
        return f"Error executing DML: {str(e)}"
$$;

/*
===============================================================================
INSERT DATA - PYTHON VERSION
===============================================================================
Inserts data with better error handling and validation.
*/

CREATE OR REPLACE PROCEDURE INSERT_DATA_PY(
    table_name STRING,
    data_json STRING  -- JSON string with column-value pairs
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'insert_data'
AS
$$
import json

def insert_data(session, table_name, data_json):
    try:
        # Parse JSON data
        data = json.loads(data_json)
        
        if not isinstance(data, dict) or len(data) == 0:
            return "Error: data_json must be a non-empty JSON object"
        
        # Build INSERT statement
        columns = list(data.keys())
        values = []
        
        for val in data.values():
            if val is None or str(val).upper() == 'NULL':
                values.append('NULL')
            elif isinstance(val, bool):
                values.append('TRUE' if val else 'FALSE')
            elif isinstance(val, (int, float)):
                values.append(str(val))
            else:
                # Escape single quotes in strings
                escaped_val = str(val).replace("'", "''")
                values.append(f"'{escaped_val}'")
        
        columns_str = ', '.join(columns)
        values_str = ', '.join(values)
        
        insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_str})"
        
        # Execute INSERT
        result = session.sql(insert_sql)
        result.collect()
        
        return f"Successfully inserted 1 row into {table_name}\nColumns: {columns_str}"
        
    except json.JSONDecodeError as e:
        return f"Error: Invalid JSON format - {str(e)}"
    except Exception as e:
        return f"Error inserting data: {str(e)}"
$$;

/*
===============================================================================
EXAMPLE USAGE
===============================================================================
*/

-- Example 1: Query with formatted table output
CALL QUERY_DATA_PY(
    'PHANTOM_SEC_POC.SI.PRICING_CRITERIA_SUMMARY',
    'meets_criteria, total',
    NULL,
    'total DESC',
    10,
    'table'
);

-- Example 2: Query with JSON output
CALL QUERY_DATA_PY(
    'PHANTOM_SEC_POC.SI.PRICING_DISCUSSIONS',
    'record_id, meets_criteria, filter_used',
    'meets_criteria = TRUE',
    NULL,
    5,
    'json'
);

-- Example 3: Execute DML with results
CALL EXECUTE_DML_PY(
    'SELECT meets_criteria, COUNT(*) as count FROM PHANTOM_SEC_POC.SI.PRICING_DISCUSSIONS GROUP BY meets_criteria',
    'table'
);

-- Example 4: Insert data using JSON
CALL INSERT_DATA_PY(
    'MY_DB.PUBLIC.TEST_TABLE',
    '{"id": 1, "name": "Test User", "status": "active", "score": 95.5}'
);

/*
===============================================================================
BENEFITS OF PYTHON VERSION
===============================================================================

1. ACTUAL RESULTS: Returns formatted query results directly
2. FLEXIBLE OUTPUT: Support for table, JSON, and CSV formats
3. ERROR HANDLING: Better exception handling with detailed messages
4. DATA PROCESSING: Can manipulate and format data before returning
5. NO EXTRA STEPS: No need for RESULT_SCAN - get results immediately

These procedures are ideal for Snowflake Intelligence tools where you need
programmatic access to query results with formatted output.
===============================================================================
*/