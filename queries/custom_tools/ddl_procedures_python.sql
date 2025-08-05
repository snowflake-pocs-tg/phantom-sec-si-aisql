/*
===============================================================================
DDL PROCEDURES - PYTHON VERSION FOR SNOWFLAKE INTELLIGENCE
===============================================================================

PURPOSE:
Python-based stored procedures for Data Definition Language (DDL) operations.
These procedures manage database structure including creating, altering, and
dropping database objects like databases, schemas, tables, and views.

OVERVIEW:
- EXECUTE_DDL_PY: Execute any DDL statement with result formatting
- CREATE_DATABASE_PY: Create a new database
- CREATE_SCHEMA_PY: Create a new schema in a database
- CREATE_TABLE_PY: Create a table with column definitions
- SHOW_OBJECTS_PY: List database objects with filtering

===============================================================================
*/

/*
===============================================================================
EXECUTE DDL - PYTHON VERSION
===============================================================================
Executes any DDL statement and returns formatted results.
*/

CREATE OR REPLACE PROCEDURE EXECUTE_DDL_PY(
    ddl_statement STRING,
    show_results BOOLEAN DEFAULT TRUE
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'execute_ddl'
AS
$$
def execute_ddl(session, ddl_statement, show_results):
    try:
        # Validate input
        if not ddl_statement or not ddl_statement.strip():
            return "Error: DDL statement cannot be empty"
        
        # Get statement type
        statement_type = ddl_statement.strip().split()[0].upper()
        
        # Common DDL keywords
        ddl_keywords = ['CREATE', 'ALTER', 'DROP', 'TRUNCATE', 'GRANT', 'REVOKE', 'SHOW', 'DESCRIBE', 'DESC']
        
        if statement_type not in ddl_keywords:
            return f"Error: Expected DDL statement, got: {statement_type}"
        
        # Execute the DDL
        result = session.sql(ddl_statement)
        
        # Handle SHOW and DESCRIBE commands differently
        if statement_type in ['SHOW', 'DESCRIBE', 'DESC'] and show_results:
            df = result.collect()
            columns = result.schema.names
            
            output = f"Statement: {ddl_statement}\n"
            output += f"Results: {len(df)} rows\n\n"
            
            if len(df) == 0:
                return output + "No objects found."
            
            # Format as table
            col_widths = []
            for i, col in enumerate(columns):
                max_width = len(col)
                for row in df[:50]:
                    val_len = len(str(row[i]) if row[i] is not None else 'NULL')
                    if val_len > max_width:
                        max_width = val_len
                col_widths.append(min(max_width, 40))
            
            # Header
            header = '| '
            separator = '|-'
            for i, col in enumerate(columns):
                header += col[:col_widths[i]].ljust(col_widths[i]) + ' | '
                separator += '-' * col_widths[i] + '-|-'
            
            output += header + '\n'
            output += separator + '\n'
            
            # Data rows
            for row in df:
                row_str = '| '
                for i, val in enumerate(row):
                    val_str = str(val) if val is not None else 'NULL'
                    val_str = val_str[:col_widths[i]]
                    row_str += val_str.ljust(col_widths[i]) + ' | '
                output += row_str + '\n'
            
            return output
        else:
            # For other DDL, just execute and return success
            result.collect()
            return f"DDL executed successfully: {ddl_statement}"
            
    except Exception as e:
        return f"Error executing DDL: {str(e)}"
$$;

/*
===============================================================================
CREATE DATABASE - PYTHON VERSION
===============================================================================
Creates a new database with optional parameters.
*/

CREATE OR REPLACE PROCEDURE CREATE_DATABASE_PY(
    database_name STRING,
    comment STRING DEFAULT NULL,
    transient BOOLEAN DEFAULT FALSE
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'create_database'
AS
$$
def create_database(session, database_name, comment, transient):
    try:
        # Build CREATE DATABASE statement
        ddl = f"CREATE {'TRANSIENT ' if transient else ''}DATABASE IF NOT EXISTS {database_name}"
        
        if comment:
            # Escape single quotes in comment
            escaped_comment = comment.replace("'", "''")
            ddl += f" COMMENT = '{escaped_comment}'"
        
        # Execute DDL
        session.sql(ddl).collect()
        
        # Get database info
        info_query = f"SHOW DATABASES LIKE '{database_name}'"
        info_result = session.sql(info_query).collect()
        
        output = f"Database '{database_name}' created successfully!\n\n"
        output += f"Type: {'TRANSIENT' if transient else 'PERMANENT'}\n"
        
        if comment:
            output += f"Comment: {comment}\n"
        
        if info_result:
            db_info = info_result[0]
            output += f"Owner: {db_info['owner']}\n"
            output += f"Created: {db_info['created_on']}\n"
        
        return output
        
    except Exception as e:
        return f"Error creating database: {str(e)}"
$$;

/*
===============================================================================
CREATE SCHEMA - PYTHON VERSION
===============================================================================
Creates a new schema within a database.
*/

CREATE OR REPLACE PROCEDURE CREATE_SCHEMA_PY(
    database_name STRING,
    schema_name STRING,
    comment STRING DEFAULT NULL,
    managed_access BOOLEAN DEFAULT FALSE
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'create_schema'
AS
$$
def create_schema(session, database_name, schema_name, comment, managed_access):
    try:
        # Build CREATE SCHEMA statement
        full_schema_name = f"{database_name}.{schema_name}"
        ddl = f"CREATE SCHEMA IF NOT EXISTS {full_schema_name}"
        
        if managed_access:
            ddl += " WITH MANAGED ACCESS"
        
        if comment:
            escaped_comment = comment.replace("'", "''")
            ddl += f" COMMENT = '{escaped_comment}'"
        
        # Execute DDL
        session.sql(ddl).collect()
        
        output = f"Schema '{full_schema_name}' created successfully!\n\n"
        output += f"Database: {database_name}\n"
        output += f"Schema: {schema_name}\n"
        output += f"Managed Access: {'Yes' if managed_access else 'No'}\n"
        
        if comment:
            output += f"Comment: {comment}\n"
        
        return output
        
    except Exception as e:
        return f"Error creating schema: {str(e)}"
$$;

/*
===============================================================================
CREATE TABLE - PYTHON VERSION
===============================================================================
Creates a new table with column definitions.
*/

CREATE OR REPLACE PROCEDURE CREATE_TABLE_PY(
    table_name STRING,              -- Fully qualified table name
    columns_json STRING,            -- JSON array of column definitions
    table_type STRING DEFAULT 'PERMANENT',  -- PERMANENT, TRANSIENT, TEMPORARY
    comment STRING DEFAULT NULL
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python', 'json')
HANDLER = 'create_table'
AS
$$
import json

def create_table(session, table_name, columns_json, table_type, comment):
    try:
        # Parse column definitions
        columns = json.loads(columns_json)
        
        if not isinstance(columns, list) or len(columns) == 0:
            return "Error: columns_json must be a non-empty JSON array"
        
        # Build column definitions
        col_defs = []
        for col in columns:
            if not isinstance(col, dict) or 'name' not in col or 'type' not in col:
                return "Error: Each column must have 'name' and 'type' properties"
            
            col_def = f"{col['name']} {col['type']}"
            
            # Add optional column properties
            if col.get('nullable') == False:
                col_def += " NOT NULL"
            if 'default' in col:
                col_def += f" DEFAULT {col['default']}"
            if 'comment' in col:
                escaped = col['comment'].replace("'", "''")
                col_def += f" COMMENT '{escaped}'"
            
            col_defs.append(col_def)
        
        # Build CREATE TABLE statement
        table_type_clause = ""
        if table_type.upper() == 'TRANSIENT':
            table_type_clause = "TRANSIENT "
        elif table_type.upper() == 'TEMPORARY':
            table_type_clause = "TEMPORARY "
        
        ddl = f"CREATE {table_type_clause}TABLE IF NOT EXISTS {table_name} (\n"
        ddl += ",\n".join(f"    {col_def}" for col_def in col_defs)
        ddl += "\n)"
        
        if comment:
            escaped_comment = comment.replace("'", "''")
            ddl += f" COMMENT = '{escaped_comment}'"
        
        # Execute DDL
        session.sql(ddl).collect()
        
        # Build output
        output = f"Table '{table_name}' created successfully!\n\n"
        output += f"Type: {table_type.upper()}\n"
        output += f"Columns ({len(columns)}):\n"
        
        for col in columns:
            output += f"  • {col['name']}: {col['type']}"
            if col.get('nullable') == False:
                output += " NOT NULL"
            if 'default' in col:
                output += f" DEFAULT {col['default']}"
            output += "\n"
        
        if comment:
            output += f"\nTable Comment: {comment}\n"
        
        return output
        
    except json.JSONDecodeError as e:
        return f"Error: Invalid JSON format - {str(e)}"
    except Exception as e:
        return f"Error creating table: {str(e)}"
$$;


/*
===============================================================================
SHOW OBJECTS - PYTHON VERSION
===============================================================================
Lists database objects with filtering and formatting.
*/

CREATE OR REPLACE PROCEDURE SHOW_OBJECTS_PY(
    object_type STRING,         -- DATABASES, SCHEMAS, TABLES, VIEWS, etc.
    in_database STRING DEFAULT NULL,
    in_schema STRING DEFAULT NULL,
    like_pattern STRING DEFAULT NULL,
    limit_rows INTEGER DEFAULT 100
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'show_objects'
AS
$$
def show_objects(session, object_type, in_database, in_schema, like_pattern, limit_rows):
    try:
        # Build SHOW statement
        object_type_upper = object_type.upper()
        
        # Handle different object types
        if object_type_upper == 'DATABASES':
            query = "SHOW DATABASES"
        elif object_type_upper == 'SCHEMAS':
            query = "SHOW SCHEMAS"
            if in_database:
                query += f" IN DATABASE {in_database}"
        elif object_type_upper in ['TABLES', 'VIEWS', 'STAGES', 'FILE FORMATS', 'SEQUENCES', 
                                   'PIPES', 'STREAMS', 'TASKS', 'FUNCTIONS', 'PROCEDURES']:
            query = f"SHOW {object_type_upper}"
            if in_schema:
                query += f" IN SCHEMA {in_schema}"
            elif in_database:
                query += f" IN DATABASE {in_database}"
        else:
            return f"Error: Unsupported object type '{object_type}'"
        
        # Add LIKE pattern if specified
        if like_pattern:
            query += f" LIKE '{like_pattern}'"
        
        # Execute query
        result = session.sql(query)
        df = result.collect()
        
        # Apply limit
        if limit_rows and limit_rows > 0:
            df = df[:limit_rows]
        
        # Format output
        output = f"Showing {object_type_upper}"
        if in_database:
            output += f" in database {in_database}"
        if in_schema:
            output += f" in schema {in_schema}"
        if like_pattern:
            output += f" matching '{like_pattern}'"
        output += f"\nFound {len(df)} objects\n\n"
        
        if len(df) == 0:
            return output + "No objects found."
        
        # Get relevant columns based on object type
        if object_type_upper == 'DATABASES':
            show_cols = ['name', 'owner', 'created_on', 'comment']
        elif object_type_upper == 'SCHEMAS':
            show_cols = ['name', 'database_name', 'owner', 'created_on']
        elif object_type_upper == 'TABLES':
            show_cols = ['name', 'database_name', 'schema_name', 'kind', 'rows', 'bytes']
        else:
            show_cols = ['name', 'database_name', 'schema_name', 'owner', 'created_on']
        
        # Filter to available columns
        columns = result.schema.names
        available_cols = [col for col in show_cols if col in columns]
        
        # Create formatted output
        for i, row in enumerate(df):
            output += f"\n{i+1}. "
            row_dict = row.as_dict()
            
            # Show name prominently
            if 'name' in row_dict:
                output += f"{row_dict['name']}"
                
                # Add full qualified name for tables/views
                if object_type_upper in ['TABLES', 'VIEWS'] and all(k in row_dict for k in ['database_name', 'schema_name']):
                    output += f" ({row_dict['database_name']}.{row_dict['schema_name']}.{row_dict['name']})"
            
            output += "\n"
            
            # Show other attributes
            for col in available_cols:
                if col != 'name' and col in row_dict and row_dict[col]:
                    if col == 'bytes' and row_dict[col]:
                        # Format bytes nicely
                        bytes_val = int(row_dict[col])
                        if bytes_val > 1024**3:
                            size_str = f"{bytes_val / 1024**3:.2f} GB"
                        elif bytes_val > 1024**2:
                            size_str = f"{bytes_val / 1024**2:.2f} MB"
                        elif bytes_val > 1024:
                            size_str = f"{bytes_val / 1024:.2f} KB"
                        else:
                            size_str = f"{bytes_val} bytes"
                        output += f"   {col}: {size_str}\n"
                    else:
                        output += f"   {col}: {row_dict[col]}\n"
        
        if len(df) == limit_rows:
            output += f"\n(Results limited to {limit_rows} rows)"
        
        return output
        
    except Exception as e:
        return f"Error showing {object_type}: {str(e)}"
$$;

/*
===============================================================================
EXAMPLE USAGE
===============================================================================
*/

-- Example 1: Execute custom DDL
CALL EXECUTE_DDL_PY(
    'CREATE OR REPLACE VIEW my_view AS SELECT * FROM my_table WHERE status = ''active''',
    TRUE
);

-- Example 2: Create a database
CALL CREATE_DATABASE_PY(
    'ANALYTICS_DB',
    'Database for analytics workloads',
    FALSE  -- not transient
);

-- Example 3: Create a schema
CALL CREATE_SCHEMA_PY(
    'ANALYTICS_DB',
    'REPORTS',
    'Schema for business reports',
    TRUE  -- managed access
);

-- Example 4: Create a table with columns
CALL CREATE_TABLE_PY(
    'ANALYTICS_DB.REPORTS.SALES_SUMMARY',
    '[
        {"name": "id", "type": "NUMBER IDENTITY(1,1) PRIMARY KEY"},
        {"name": "product_name", "type": "VARCHAR(200)", "nullable": false},
        {"name": "sale_date", "type": "DATE", "nullable": false},
        {"name": "quantity", "type": "NUMBER", "default": "0"},
        {"name": "revenue", "type": "NUMBER(10,2)"},
        {"name": "created_at", "type": "TIMESTAMP_LTZ", "default": "CURRENT_TIMESTAMP()"}
    ]',
    'PERMANENT',
    'Sales summary table for reporting'
);

-- Example 5: Show tables in a schema
CALL SHOW_OBJECTS_PY(
    'TABLES',
    NULL,
    'ANALYTICS_DB.REPORTS',
    'SALES%',  -- tables starting with SALES
    50
);

-- Example 6: Show all databases
CALL SHOW_OBJECTS_PY(
    'DATABASES',
    NULL,
    NULL,
    NULL,
    20
);

/*
===============================================================================
BENEFITS OF PYTHON DDL PROCEDURES
===============================================================================

1. FORMATTED OUTPUT: Returns readable, formatted results for SHOW commands
2. VALIDATION: Better input validation and error messages
3. FLEXIBLE PARAMETERS: Support for optional parameters and defaults
4. JSON SUPPORT: Easy column definitions using JSON for CREATE TABLE

These procedures provide a robust interface for DDL operations in
Snowflake Intelligence tools.
===============================================================================
*/