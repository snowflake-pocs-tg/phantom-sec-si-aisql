# Phantom Sec  
Snowflake Intelligence & AISQL Tools

A comprehensive collection of Snowflake SQL tools and procedures for AI-powered data analysis, focusing on sales call transcripts, competitive intelligence, and Snowflake Intelligence platform integration.

## Overview

This repository contains SQL tools and procedures organized into several categories:
- **AI Query Tools**: Examples and patterns for using Snowflake Cortex AI functions
- **Python Stored Procedures**: Enhanced DDL/DML operations with formatted output
- **Gong Integration**: Tools for loading and analyzing sales call transcripts
- **Snowflake Intelligence**: Infrastructure setup for AI workloads

## Tools and Components

### AI Query Examples (`queries/ai_queries/`)

#### 1. AI Classification (`ai_classify.sql`)
- Demonstrates text classification using Snowflake's AI_CLASSIFY function
- Categories include:
  - Competitor identification (Secureframe, Drata, Sprinto, TrustCloud)
  - Product differentiators (AI automation, integrations, monitoring)
  - Feature classification (AI Agent, questionnaire automation, 24/7 support)
- Use case: Competitive intelligence from sales transcripts

#### 2. AI Filtering (`ai_filter.sql`)
- Shows AI_FILTER function for natural language SQL predicates
- Patterns include:
  - Single predicate filtering with CONCAT
  - Multiple predicates using PROMPT function
  - Multi-column filtering with template placeholders
- Includes performance optimization examples

#### 3. AI Summarization & Comparison (`ai_summarize_compare.sql`)
- Two-step workflow for text summarization and quality assessment
- Creates AI summaries of call transcripts
- Measures summary quality using similarity scoring
- Categorizes similarity: High/Medium/Low/Very Low

### AI Tool Procedures (`queries/ai_tools/`)

#### 1. CLASSIFY_AND_SHOW (`ai_classify.sql`)
Python stored procedure for text classification with immediate results display.
```sql
CALL CLASSIFY_AND_SHOW(
  'table_name',
  'text_column',
  'category1,category2,category3',
  'output_table',
  10
);
```

#### 2. FILTER_AND_SHOW (`ai_filter.sql`)
AI-powered filtering with boolean result analysis and statistics.
```sql
CALL FILTER_AND_SHOW(
  'table_name',
  'text_column',
  'filter_predicate',
  'output_table',
  10
);
```

#### 3. MULTI_COLUMN_FILTER_AND_SHOW (`ai_filter.sql`)
Multi-column filtering using PROMPT templates with match analysis.
```sql
CALL MULTI_COLUMN_FILTER_AND_SHOW(
  'table_name',
  'col1,col2,col3',
  'filter_predicate',
  'output_table',
  10
);
```

#### 4. SIMILARITY_AND_SHOW (`ai_similarity.sql`)
Computes and categorizes similarity scores between text fields.
```sql
CALL SIMILARITY_AND_SHOW(
  'table_name',
  'text_column1',
  'text_column2',
  'output_table',
  10
);
```

#### 5. SUMMARIZE_AND_SHOW (`ai_summarize_agg.sql`)
Text summarization with formatted output display.
```sql
CALL SUMMARIZE_AND_SHOW(
  'table_name',
  'id_column',
  'text_column',
  'output_table',
  10
);
```

### Custom Database Tools (`queries/custom_tools/`)

#### DDL Operations (`ddl_procedures_python.sql`)

- **EXECUTE_DDL_PY**: Execute any DDL statement with formatted results
- **CREATE_DATABASE_PY**: Create databases with transient options
- **CREATE_SCHEMA_PY**: Create schemas with managed access control
- **CREATE_TABLE_PY**: Create tables using JSON column definitions
- **SHOW_OBJECTS_PY**: List database objects with filtering

Example:
```sql
CALL CREATE_TABLE_PY(
  'my_table',
  '[{"name": "id", "type": "NUMBER"}, {"name": "data", "type": "VARCHAR"}]',
  'Transient table for temporary data'
);
```

#### DML Operations (`dml_procedures_python.sql`)

- **QUERY_DATA_PY**: Query data with table/JSON/CSV output formats
- **EXECUTE_DML_PY**: Execute DML statements with immediate results
- **INSERT_DATA_PY**: Insert data using JSON input format

Example:
```sql
CALL QUERY_DATA_PY(
  'SELECT * FROM my_table',
  'TABLE',
  10
);
```

### Gong Integration (`queries/gong/`)

#### 1. Setup (`1_setup.sql`)
- Creates PHANTOM_SEC_POC database and SI schema
- Sets up stage for JSON file loading
- Loads GONG_USERS and GONG_TRANSCRIPTS from JSON files

#### 2. Clean Transcripts (`2_clean_transcripts.sql`)
Creates GONG_CLEAN_TRANSCRIPTS table with:
- Chronological transcript formatting with timestamps
- Speaker identification and role information
- Call metrics (duration, speaker count, sentence count)
- Participant lists with titles and emails
- Topic categorization and company domains

#### 3. Cortex Search Service (`3_cortex_search.sql`)
Creates GONG_TRANSCRIPT_SEARCH service for semantic search:
- Natural language search on call transcripts
- Searchable attributes: participants, topics, domains
- Example queries for pricing, competitors, executives

### Snowflake Intelligence Setup (`queries/snowflake_intelligence/`)

#### Complete Infrastructure (`1_setup.sql`)
Sets up the complete Snowflake Intelligence platform:
- **Role**: SNOWFLAKE_INTELLIGENCE_ADMIN_RL with full permissions
- **Warehouse**: SNOWFLAKE_INTELLIGENCE_WH for AI workloads
- **Database**: SNOWFLAKE_INTELLIGENCE with organized schemas
  - AGENTS: For AI agent configurations
  - INTEGRATIONS: For external system connections
  - TOOLS: For custom tools and utilities
- Complete permission structure and ownership transfers

## Data Files

### Sample Data (`data/`)
- **GONG_DATA.json**: Sample Gong call transcript data
- **GONG_USERS.json**: Sample user information for transcript attribution

## Documentation

- **MODEL.md**: Data model documentation
- **RESEARCH.md**: Research notes and findings
- **PROMPT.md** files: Prompt engineering documentation for specific tools

## Getting Started

1. **Set up Snowflake Intelligence Infrastructure**:
   ```sql
   -- Run the complete setup
   USE ROLE ACCOUNTADMIN;
   -- Execute queries/snowflake_intelligence/1_setup.sql
   ```

2. **Load Gong Data** (if using Gong integration):
   ```sql
   -- Execute queries/gong/1_setup.sql
   -- Execute queries/gong/2_clean_transcripts.sql
   -- Execute queries/gong/3_cortex_search.sql
   ```

3. **Use AI Tools**:
   ```sql
   -- Example: Classify text
   CALL CLASSIFY_AND_SHOW(
     'GONG_CLEAN_TRANSCRIPTS',
     'FULL_TRANSCRIPT',
     'Secureframe,Drata,Sprinto,TrustCloud',
     'COMPETITOR_ANALYSIS',
     100
   );
   ```

## Requirements

- Snowflake account with Cortex AI functions enabled
- ACCOUNTADMIN role for initial setup
- Python stored procedures support
- Cortex Search Service access (for semantic search features)

## License

See LICENSE file for details.
