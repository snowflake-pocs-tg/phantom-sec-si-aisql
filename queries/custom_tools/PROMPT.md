# Snowflake Tools - Agent Instructions

## Overview
This guide provides instructions for using Snowflake DML (Data Manipulation Language) and DDL (Data Definition Language) stored procedures. These Python-based procedures return formatted results directly, eliminating the need for RESULT_SCAN.

## DML Tools (Data Manipulation)

### 1. QUERY_DATA_PY

**Purpose:** Query data from tables with formatted output.

**When to use:**
- Retrieving data from any table
- Filtering records with WHERE clauses
- Sorting and limiting results
- Exporting query results to different formats

**Usage:**
```sql
CALL QUERY_DATA_PY(
    table_name,        -- Required: 'DB.SCHEMA.TABLE'
    columns_list,      -- Optional: 'col1, col2' or NULL for all
    where_clause,      -- Optional: 'status = ''active''' (no WHERE keyword)
    order_by_clause,   -- Optional: 'created_at DESC' (no ORDER BY keyword)
    record_limit,      -- Optional: Default 100
    output_format      -- Optional: 'table', 'json', 'csv' (default: 'table')
);
```

**Examples:**
```sql
-- Get all data from a table
CALL QUERY_DATA_PY('MY_DB.PUBLIC.CUSTOMERS', NULL, NULL, NULL, 10, 'table');

-- Filter and sort data
CALL QUERY_DATA_PY(
    'MY_DB.PUBLIC.ORDERS',
    'order_id, customer_name, total',
    'order_date >= ''2024-01-01''',
    'total DESC',
    50,
    'table'
);

-- Export to JSON format
CALL QUERY_DATA_PY('MY_DB.PUBLIC.PRODUCTS', NULL, NULL, NULL, 100, 'json');
```

**Output formats:**
- `table`: Formatted ASCII table with columns aligned
- `json`: JSON object with query metadata and data array
- `csv`: Comma-separated values for easy export

### 2. EXECUTE_DML_PY

**Purpose:** Execute any DML statement (SELECT, INSERT, UPDATE, DELETE, MERGE).

**When to use:**
- Running complex queries
- Performing data modifications
- Executing dynamic SQL
- Getting formatted SELECT results

**Usage:**
```sql
CALL EXECUTE_DML_PY(
    dml_statement,     -- Required: Any valid DML SQL
    output_format      -- Optional: 'table', 'json', 'summary' (default: 'table')
);
```

**Examples:**
```sql
-- Complex SELECT with joins
CALL EXECUTE_DML_PY(
    'SELECT c.name, COUNT(o.id) as order_count 
     FROM customers c 
     LEFT JOIN orders o ON c.id = o.customer_id 
     GROUP BY c.name',
    'table'
);

-- UPDATE statement
CALL EXECUTE_DML_PY(
    'UPDATE products SET price = price * 1.1 WHERE category = ''electronics''',
    'summary'
);
```

**Important notes:**
- SELECT queries return formatted results immediately
- Other DML operations return execution status
- Use single quotes inside strings by doubling them: `''value''`

### 3. INSERT_DATA_PY

**Purpose:** Insert data using JSON format for easy integration.

**When to use:**
- Adding single records to tables
- Programmatic data insertion
- Working with JSON data sources

**Usage:**
```sql
CALL INSERT_DATA_PY(
    table_name,        -- Required: 'DB.SCHEMA.TABLE'
    data_json          -- Required: JSON object with column-value pairs
);
```

**Examples:**
```sql
-- Insert a customer record
CALL INSERT_DATA_PY(
    'MY_DB.PUBLIC.CUSTOMERS',
    '{
        "customer_id": "CUST_001",
        "name": "John Doe",
        "email": "john@example.com",
        "status": "active",
        "credit_limit": 5000.00
    }'
);

-- Insert with NULL values
CALL INSERT_DATA_PY(
    'MY_DB.PUBLIC.PRODUCTS',
    '{
        "product_name": "New Product",
        "category": "electronics",
        "description": null,
        "price": 99.99
    }'
);
```

## DDL Tools (Database Structure)

### 1. EXECUTE_DDL_PY

**Purpose:** Execute any DDL statement with formatted results for SHOW/DESCRIBE commands.

**When to use:**
- Creating, altering, or managing any database object
- Running SHOW commands with formatted output
- Describing table structures

**Usage:**
```sql
CALL EXECUTE_DDL_PY(
    ddl_statement,     -- Required: Any valid DDL SQL
    show_results       -- Optional: TRUE to format SHOW results (default: TRUE)
);
```

**Examples:**
```sql
-- Create a view
CALL EXECUTE_DDL_PY(
    'CREATE OR REPLACE VIEW active_customers AS 
     SELECT * FROM customers WHERE status = ''active'''
);

-- Show tables with formatting
CALL EXECUTE_DDL_PY('SHOW TABLES IN SCHEMA MY_DB.PUBLIC');

-- Describe a table
CALL EXECUTE_DDL_PY('DESCRIBE TABLE MY_DB.PUBLIC.CUSTOMERS');
```

### 2. CREATE_DATABASE_PY

**Purpose:** Create a new database with options.

**When to use:**
- Setting up new databases
- Creating transient databases for temporary work
- Organizing data into separate databases

**Usage:**
```sql
CALL CREATE_DATABASE_PY(
    database_name,     -- Required: Database name
    comment,           -- Optional: Description
    transient          -- Optional: TRUE for transient (default: FALSE)
);
```

**Examples:**
```sql
-- Create permanent database
CALL CREATE_DATABASE_PY(
    'ANALYTICS_DB',
    'Database for analytics and reporting',
    FALSE
);

-- Create transient database
CALL CREATE_DATABASE_PY(
    'TEMP_WORK_DB',
    'Temporary workspace',
    TRUE
);
```

### 3. CREATE_SCHEMA_PY

**Purpose:** Create schemas within databases.

**When to use:**
- Organizing tables into logical groups
- Setting up multi-tenant architectures
- Creating managed access schemas

**Usage:**
```sql
CALL CREATE_SCHEMA_PY(
    database_name,     -- Required: Parent database
    schema_name,       -- Required: Schema name
    comment,           -- Optional: Description
    managed_access     -- Optional: TRUE for managed access (default: FALSE)
);
```

**Examples:**
```sql
-- Create standard schema
CALL CREATE_SCHEMA_PY(
    'ANALYTICS_DB',
    'SALES',
    'Sales data and reports',
    FALSE
);

-- Create managed access schema
CALL CREATE_SCHEMA_PY(
    'SECURE_DB',
    'SENSITIVE_DATA',
    'Restricted access schema',
    TRUE
);
```

### 4. CREATE_TABLE_PY

**Purpose:** Create tables with column definitions using JSON.

**When to use:**
- Creating new tables
- Setting up table structures programmatically
- Working with dynamic table definitions

**Usage:**
```sql
CALL CREATE_TABLE_PY(
    table_name,        -- Required: 'DB.SCHEMA.TABLE'
    columns_json,      -- Required: JSON array of column definitions
    table_type,        -- Optional: 'PERMANENT', 'TRANSIENT', 'TEMPORARY' (default: 'PERMANENT')
    comment            -- Optional: Table description
);
```

**Column definition format:**
```json
{
    "name": "column_name",           -- Required
    "type": "VARCHAR(100)",          -- Required
    "nullable": false,               -- Optional (default: true)
    "default": "CURRENT_TIMESTAMP()", -- Optional
    "comment": "Column description"   -- Optional
}
```

**Examples:**
```sql
-- Create a customers table
CALL CREATE_TABLE_PY(
    'MY_DB.PUBLIC.CUSTOMERS',
    '[
        {"name": "id", "type": "NUMBER IDENTITY(1,1) PRIMARY KEY"},
        {"name": "customer_code", "type": "VARCHAR(20)", "nullable": false},
        {"name": "name", "type": "VARCHAR(200)", "nullable": false},
        {"name": "email", "type": "VARCHAR(100)"},
        {"name": "status", "type": "VARCHAR(20)", "default": "''active''"},
        {"name": "created_at", "type": "TIMESTAMP_LTZ", "default": "CURRENT_TIMESTAMP()"},
        {"name": "metadata", "type": "VARIANT", "comment": "Additional customer data"}
    ]',
    'PERMANENT',
    'Main customers table'
);

-- Create temporary work table
CALL CREATE_TABLE_PY(
    'MY_DB.WORK.TEMP_RESULTS',
    '[
        {"name": "id", "type": "NUMBER"},
        {"name": "result", "type": "VARCHAR(1000)"},
        {"name": "score", "type": "FLOAT"}
    ]',
    'TEMPORARY',
    'Temporary results storage'
);
```

### 5. SHOW_OBJECTS_PY

**Purpose:** List database objects with filtering and formatting.

**When to use:**
- Discovering available databases, schemas, or tables
- Finding objects matching patterns
- Getting object metadata

**Usage:**
```sql
CALL SHOW_OBJECTS_PY(
    object_type,       -- Required: 'DATABASES', 'SCHEMAS', 'TABLES', 'VIEWS', etc.
    in_database,       -- Optional: Filter by database
    in_schema,         -- Optional: Filter by schema
    like_pattern,      -- Optional: SQL LIKE pattern
    limit_rows         -- Optional: Maximum results (default: 100)
);
```

**Examples:**
```sql
-- Show all databases
CALL SHOW_OBJECTS_PY('DATABASES', NULL, NULL, NULL, 20);

-- Show tables in a specific schema
CALL SHOW_OBJECTS_PY(
    'TABLES',
    NULL,
    'MY_DB.PUBLIC',
    NULL,
    50
);

-- Find tables matching pattern
CALL SHOW_OBJECTS_PY(
    'TABLES',
    'ANALYTICS_DB',
    NULL,
    'SALES%',  -- Tables starting with SALES
    100
);

-- Show all views in a database
CALL SHOW_OBJECTS_PY(
    'VIEWS',
    'REPORTING_DB',
    NULL,
    NULL,
    50
);
```

**Object types supported:**
- DATABASES
- SCHEMAS
- TABLES
- VIEWS
- STAGES
- FILE FORMATS
- SEQUENCES
- PIPES
- STREAMS
- TASKS
- FUNCTIONS
- PROCEDURES

## Best Practices

### 1. Fully Qualified Names
Always use fully qualified table names:
```sql
-- Good
CALL QUERY_DATA_PY('MY_DB.PUBLIC.CUSTOMERS', ...);

-- Bad
CALL QUERY_DATA_PY('CUSTOMERS', ...);  -- May fail or use wrong table
```

### 2. Quote Handling
Double single quotes inside string values:
```sql
-- Correct
CALL EXECUTE_DML_PY('SELECT * FROM products WHERE name = ''John''s Product''');

-- Also correct for JSON
CALL INSERT_DATA_PY('MY_TABLE', '{"name": "John''s Product"}');
```

### 3. NULL Handling
Use NULL keyword or null in JSON:
```sql
-- In SQL strings
where_clause = 'description IS NULL'

-- In JSON
'{"description": null}'
```

### 4. Output Format Selection
Choose the right format for your needs:
- `table`: Human-readable display
- `json`: Program integration
- `csv`: Data export
- `summary`: Quick status check

### 5. Limit Usage
Always use limits for exploration:
```sql
-- Good: Explore with small limit first
CALL QUERY_DATA_PY('BIG_TABLE', NULL, NULL, NULL, 10, 'table');

-- Then increase if needed
CALL QUERY_DATA_PY('BIG_TABLE', NULL, NULL, NULL, 1000, 'csv');
```

### 6. Error Handling
All procedures return error messages as strings:
```sql
-- Check for "Error:" prefix in results
-- Successful: "Table created successfully!"
-- Failed: "Error: Table already exists"
```

## Common Patterns

### Data Exploration
```sql
-- 1. Find available schemas
CALL SHOW_OBJECTS_PY('SCHEMAS', 'MY_DB', NULL, NULL, 50);

-- 2. List tables in schema
CALL SHOW_OBJECTS_PY('TABLES', NULL, 'MY_DB.PUBLIC', NULL, 100);

-- 3. Check table structure
CALL EXECUTE_DDL_PY('DESCRIBE TABLE MY_DB.PUBLIC.CUSTOMERS');

-- 4. Preview data
CALL QUERY_DATA_PY('MY_DB.PUBLIC.CUSTOMERS', NULL, NULL, NULL, 5, 'table');
```

### Data Analysis
```sql
-- Get summary statistics
CALL EXECUTE_DML_PY(
    'SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT customer_id) as unique_customers,
        AVG(order_total) as avg_order_value,
        MAX(order_date) as latest_order
     FROM orders',
    'table'
);
```

### Table Creation Workflow
```sql
-- 1. Create database if needed
CALL CREATE_DATABASE_PY('NEW_PROJECT_DB', 'Project database', FALSE);

-- 2. Create schema
CALL CREATE_SCHEMA_PY('NEW_PROJECT_DB', 'STAGING', 'Staging area', FALSE);

-- 3. Create table
CALL CREATE_TABLE_PY(
    'NEW_PROJECT_DB.STAGING.RAW_DATA',
    '[
        {"name": "id", "type": "NUMBER"},
        {"name": "data", "type": "VARIANT"},
        {"name": "loaded_at", "type": "TIMESTAMP_LTZ", "default": "CURRENT_TIMESTAMP()"}
    ]',
    'TRANSIENT',
    'Raw data staging table'
);
```

## Tips for Agents

1. **Start Small**: Use limits and preview data before large operations
2. **Check First**: Use SHOW_OBJECTS_PY to verify object existence
3. **Format Choice**: Use 'json' format for programmatic processing
4. **Safety**: These procedures don't include DROP operations by design
5. **Performance**: Create appropriate table types (TRANSIENT for temporary data)
6. **Documentation**: Always add comments to databases, schemas, and tables

Remember: All procedures return results as formatted strings - no need for RESULT_SCAN!