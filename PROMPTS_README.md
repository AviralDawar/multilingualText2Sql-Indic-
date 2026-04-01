% Requires:
% \usepackage{listings}
% \lstset{
%   basicstyle=\ttfamily\small,
%   breaklines=true,
%   breakatwhitespace=false,
%   columns=fullflexible,
%   keepspaces=true
% }

% ---- DSQG-Syn Skeleton Generation Prompt Box ----
\begin{tcolorbox}[
    title=DSQG-Syn Skeleton Generation Prompt,
    colback=white,
    colframe=gray!75!black,
    fonttitle=\bfseries,
    enhanced,
    breakable
]
Please generate \{num\_skeletons\} SQL templates based on the given question and schema. Ensure that a mix of SQL clauses are included, such as SELECT, FROM, JOIN, WHERE, GROUP BY, ORDER BY, and HAVING.

\vspace{1em}

\#\#\# Instruction:

1. Use `col\_\#` for column names.
2. Use `table\_\#` for table names.
3. Use `value\_\#` for constant values.
4. Follow the difficulty guidance in \{difficulty\_instructions\}.

\vspace{1em}

\#\#\# Prompt Body:

\begin{lstlisting}
Please generate {num_skeletons} SQL templates based on the given question and schema. Ensure that a mix of SQL
clauses are included, such as SELECT, FROM, JOIN, WHERE, GROUP BY, ORDER BY, and HAVING.
Use placeholders for specific table and column names as follows:

1. Use col_# for column names.
2. Use table_# for table names.
3. Use value_# for constant values.

{difficulty_instructions}

Example:
Input:
{"question": "Show me the redshift of spectroscopic object with subclass of STARFORMING"}
Schema:
CREATE TABLE specobj (
specobjid number Example Values[(Decimal('299489952322840576'),), ...],
subclass text Example Values[(None,), ('BROADLINE',), ('STARFORMING',)],
z number Example Values[(7.01124,), (0.00415325,), (0.00415325,)],
. . . . . .
primary key (specobjid)
)
Output:
{
  "templates": [
    {"template": "SELECT col_0 FROM table_1 WHERE col_0 = value_0"},
    {"template": "SELECT col_0 FROM table_1 WHERE col_1 > value_0"},
    ...
  ]
}
The "templates" list must contain exactly {num_skeletons} items.

Now, apply the same transformation to the question below. Do not let specific table names,
column names, or constant values (like "description", "name", "GALAXY", or "BROADLINE")
appear in the template.

Input:
{"question": "{question.question_text}"}
Schema:
{schema_str}
Output in JSON format:
{
  "templates": [
    {"template": "..."},
    ...
  ]
}
The "templates" list must contain exactly {num_skeletons} items.
\end{lstlisting}
\end{tcolorbox}
% \captionof{figure}{DSQG-Syn Skeleton Generation Prompt.}
\label{fig:dsqg_syn_skeleton_generation}


% ---- DSQG-Syn SQL Generation Prompt Box ----
\begin{tcolorbox}[
    title=DSQG-Syn SQL Generation Prompt,
    colback=white,
    colframe=gray!75!black,
    fonttitle=\bfseries,
    enhanced,
    breakable
]
You are an expert in a specific domain and a PostgreSQL SQL expert.

\vspace{1em}

\#\#\# Instruction:

You are provided with:

1. An SQL query template.
2. A question that the query needs to answer.
3. The schema of the relevant database.
4. Optional sample values from the database columns.

You must:

1. Use only the provided schema.
2. Use only foreign-key-valid join predicates.
3. Use only provided sample values for literal filters.
4. Respect type safety for numeric and non-numeric columns.
5. Output JSON only.

\vspace{1em}

\#\#\# Prompt Body:

\begin{lstlisting}
You are an expert in a specific domain and a PostgreSQL SQL expert. You are provided with:
1. An SQL query template.
2. A question that the query needs to answer.
3. The schema of the relevant database.{sample_values_str}

Your task is to:
1. Strictly use the information from the provided schema to complete PostgreSQL queries.
   Ensure that all necessary table names, column names, and clauses (such as FROM and JOIN)
   come from the schema only.
2. **CRITICAL JOIN RULE**: Use ONLY foreign-key-valid join predicates. A JOIN condition must
   exactly match one of the allowed FK relationships listed below (direction can be reversed).
   Do NOT join semantically unrelated IDs (e.g., STATE_ID = STATION_ID) just because data types match.
3. **CRITICAL**: When using literal values in WHERE, HAVING, IN, or other filter clauses,
   you MUST use ONLY the sample values provided above. Do NOT make up or hallucinate values.
   This ensures the generated queries will return actual results when executed against the database.
4. **CRITICAL TYPE RULE**: Use numeric operators/aggregates only on numeric columns.
   - AVG/SUM require numeric columns.
   - Numeric comparisons (>, >=, <, <=) require numeric/date columns.
   - For text columns, use equality/inequality, IN, LIKE, IS NULL, COUNT, GROUP BY.
   - Do not cast text columns to numeric unless values are guaranteed numeric in schema context.
5. Avoid introducing any table names, column names, or other elements that are not explicitly
   defined in the schema.
6. Generate {num_sqls} PostgreSQL SQL queries that are directly related to the given question
   and fit the SQL query template.
7. Use PostgreSQL-compatible syntax only.
8. Keep the output in JSON format.

{difficulty_instructions}

Allowed FK relationships for JOINs:
{fk_constraints_str}

Numeric columns (safe for AVG/SUM and numeric comparisons):
{numeric_cols_str}

Non-numeric columns (do NOT use AVG/SUM or numeric comparisons):
{non_numeric_cols_str}

Example:
Input:
SQL Query Template:
SELECT col_1, col_2 FROM table_1 JOIN table_0 WHERE col_3 = value_0;
Question:
What are the names and descriptions of the different types of photos associated
with objects in the astrophysical classifications from the specobj table?
Database Schema:
CREATE TABLE photo_type (
    value number,
    name text,
    description text,
    primary key (value)
);

CREATE TABLE specobj (
    specobjid number,
    bestobjid number,
    survey text,
    class text,
    subclass text,
    primary key (specobjid),
    foreign key (bestobjid) references photoobj(objid)
);

Sample Values Available:
Table: specobj
  - class: ['GALAXY', 'STAR', 'QSO']
  - subclass: ['BROADLINE', 'STARFORMING', 'STARBURST']
  - survey: ['boss', 'sdss', 'eboss']

Output:
{
  "queries": [
    "SELECT p.name, p.description FROM photo_type p JOIN specobj s ON p.value = s.bestobjid WHERE s.class = 'STAR';",
    "SELECT p.name, p.description FROM photo_type p JOIN specobj s ON p.value = s.bestobjid WHERE s.subclass = 'BROADLINE';",
    "SELECT p.name, p.description FROM photo_type p JOIN specobj s ON p.value = s.bestobjid WHERE s.class = 'GALAXY';"
  ]
}
Note: The WHERE clause values ('STAR', 'BROADLINE', 'GALAXY') are taken from the Sample Values provided.

Now, it's your turn.
Input:
SQL Query Template:
{skeleton.template}
Question:
{question.question_text}
Database Schema:
{schema_str}
Output in JSON format:
{
  "queries": [
    "..."
  ]
}
\end{lstlisting}
\end{tcolorbox}
% \captionof{figure}{DSQG-Syn SQL Generation Prompt.}
\label{fig:dsqg_syn_sql_generation}


% ---- DSQG-Syn NLQ Synthesis Prompt Box ----
\begin{tcolorbox}[
    title=DSQG-Syn NLQ Synthesis Prompt,
    colback=white,
    colframe=gray!75!black,
    fonttitle=\bfseries,
    enhanced,
    breakable
]
You are an expert Data Scientist specializing in Text-to-SQL dataset curation. Your goal is to transform a SQL query into a high-fidelity Natural Language Question (NLQ).

\vspace{1em}

\#\#\# Instruction:

1. Do not leak internal SQL logic.
2. Make the NLQ sound natural.
3. Preserve the functional intent.
4. Output JSON only.

\vspace{1em}

\#\#\# Prompt Body:

\begin{lstlisting}
You are an expert Data Scientist specializing in Text-to-SQL dataset curation. Your goal is to transform a SQL query into a high-fidelity Natural Language Question (NLQ).

### NATURALNESS GUIDELINES:
1. **Selection Conciseness:** You may not list every single column from the `SELECT` clause if a collective term (e.g., "details," "information," "profile") is more natural.
2. **Implicit Filters:** Integrate filter criteria naturally as adjectives or qualifiers (e.g., "rural schools") rather than literal mappings (e.g., "schools where the location is 'Rural'").
3. **Intent-based CTEs:** For queries using CTEs or complex subqueries, describe the *functional intent* (e.g., "For the most recently recorded data...") rather than the *execution logic* (e.g., "Find the maximum year and then...").
4. **Varied Phrasing:** Use a mix of questions, commands ("List all..."), and requests ("Show the...") to maintain variety.
5. **No Logic Leakage:** Ensure the question does not explicitly "leak" the internal SQL structure (like JOIN conditions or specific table aliases). Use domain terminology.

### EXAMPLES:

#### Example 1 (Easy: Single Table, Simple Filter)
Input SQL: "SELECT STATION_NAME, TYPE_OF_WATER_BODY FROM DIM_STATION WHERE STATE_ID = 'ST_001' AND TYPE_OF_WATER_BODY = 'LAKE'"
Output JSON: { "question": "What are the names and water body types of all stations located near lakes in the first state?" }

#### Example 2 (Medium: Join, Aggregation, Group By)
Input SQL: "SELECT T1.STATE_NAME, AVG(T3.MAX_TEMPERATURE_C) FROM DIM_STATE AS T1 JOIN DIM_STATION AS T2 ON T1.STATE_ID = T2.STATE_ID JOIN FACT_THERMAL AS T3 ON T2.STATION_ID = T3.STATION_ID GROUP BY T1.STATE_NAME"
Output JSON: { "question": "Show the average maximum temperature for each state based on available thermal station data." }

#### Example 3 (Hard: CTE, Multiple Joins, Specific Filter)
Input SQL: "WITH top_districts AS (SELECT district_id FROM fact_census WHERE population > 1000000) SELECT d.district_name, s.school_name, s.total_students FROM top_districts td JOIN dim_district d ON td.district_id = d.district_id JOIN dim_school s ON d.district_id = s.district_id WHERE s.school_type = 'Secondary'"
Output JSON: { "question": "For districts with a population over one million, list the names of secondary schools along with their total student counts." }

### TASK:
Input SQL: "{sql}"
Output JSON:
{
  "question": "<your natural language question>"
}
\end{lstlisting}
\end{tcolorbox}
% \captionof{figure}{DSQG-Syn NLQ Synthesis Prompt.}
\label{fig:dsqg_syn_nlq_synthesis}


% ---- Translation System Prompt Box ----
\begin{tcolorbox}[
    title=Translation System Prompt,
    colback=white,
    colframe=gray!75!black,
    fonttitle=\bfseries,
    enhanced,
    breakable
]
You are a professional translator.

\vspace{1em}

\#\#\# Instruction:

Translate the user query into \{target\_language\}. Return only the \{target\_language\} translation with no explanations.
\end{tcolorbox}
% \captionof{figure}{Translation System Prompt.}
\label{fig:translation_system}


% ---- Translation User Prompt Box ----
\begin{tcolorbox}[
    title=Translation User Prompt,
    colback=white,
    colframe=gray!75!black,
    fonttitle=\bfseries,
    enhanced,
    breakable
]
\#\#\# User Query:

\{question\_text\}
\end{tcolorbox}
% \captionof{figure}{Translation User Prompt.}
\label{fig:translation_user}


% ---- Hinglish Translation System Prompt Box ----
\begin{tcolorbox}[
    title=Hinglish Translation System Prompt,
    colback=white,
    colframe=gray!75!black,
    fonttitle=\bfseries,
    enhanced,
    breakable
]
You are a professional translator for Text-to-SQL data generation.

\vspace{1em}

\#\#\# Instruction:

Return only the translated text with no explanations.
\end{tcolorbox}
% \captionof{figure}{Hinglish Translation System Prompt.}
\label{fig:hinglish_translation_system}


% ---- Hinglish Translation User Prompt Box ----
\begin{tcolorbox}[
    title=Hinglish Translation User Prompt,
    colback=white,
    colframe=gray!75!black,
    fonttitle=\bfseries,
    enhanced,
    breakable
]
Translate this English Text-to-SQL prompt into natural Hinglish using Roman script. Keep all table names, column names, and SQL-specific values in their original English. Only translate the natural language intent and the conversational structure. Keep it technical but fluid.

\vspace{1em}

\#\#\# Text:

\{question\_text\}
\end{tcolorbox}
% \captionof{figure}{Hinglish Translation User Prompt.}
\label{fig:hinglish_translation_user}


% ---- Schema Architect Prompt Box ----
\begin{tcolorbox}[
    title=Schema Architect Prompt,
    colback=white,
    colframe=gray!75!black,
    fonttitle=\bfseries,
    enhanced,
    breakable
]
Role: Senior Database Architect.

\vspace{1em}

\#\#\# Task:

Analyze these CSV columns with their 0-based indices:

\{indexed\_columns\}

\vspace{1em}

\#\#\# Requirements:

CRITICAL PRIORITY: Domain Isolation.

Instead of one massive fact table, you must divide the data into 4--10 distinct THEMATIC TABLES based on the categories or domains of the data.

1. Each table should represent a single cohesive domain.
2. No table should have more than 12--15 columns.
3. Every table must have a primary key.
4. Link tables via foreign keys.
5. Ensure the total columns across all tables range between 40--80.

Naming conventions:

1. Use `UPPERCASE_WITH_UNDERSCORES` for all table and column names.
2. Dimension tables must start with `DIM_`.
3. Fact tables must start with `FACT_`.
4. Column names should be descriptive with underscores.

Important: Do not include indexing recommendations.
\end{tcolorbox}
% \captionof{figure}{Schema Architect Prompt.}
\label{fig:schema_architect}


% ---- Schema Auditor Prompt Box ----
\begin{tcolorbox}[
    title=Schema Auditor Prompt,
    colback=white,
    colframe=gray!75!black,
    fonttitle=\bfseries,
    enhanced,
    breakable
]
Role: Database Normalization \& Domain Auditor.

\vspace{1em}

\#\#\# Input Schema:

\{draft\_schema\}

\vspace{1em}

\#\#\# Audit Task:

1. Width Check: Does any single table contain more than 15 columns?
2. Cohesion Check: Are there columns in a table that do not belong to its theme?
3. 3NF Violation Check: Are there transitive dependencies?
4. Complexity Check: Will answering benchmark questions require joining at least 3 tables?

List specific clumping errors and normalization failures for the architect.
\end{tcolorbox}
% \captionof{figure}{Schema Auditor Prompt.}
\label{fig:schema_auditor}


% ---- Schema Refiner Prompt Box ----
\begin{tcolorbox}[
    title=Schema Refiner Prompt,
    colback=white,
    colframe=gray!75!black,
    fonttitle=\bfseries,
    enhanced,
    breakable
]
Role: Lead Architect.

\vspace{1em}

\#\#\# Inputs:

Original Draft: \{draft\_schema\}

Auditor Feedback: \{audit\_feedback\}

Original CSV columns (with indices): \{indexed\_columns\}

\vspace{1em}

\#\#\# Task:

Resolve auditor warnings by aggressively splitting wide tables into thematic sub-domains.

1. If a table is too wide, split it logically.
2. Ensure every table has a clear join path to others.
3. Clean column names for the schema.

\vspace{1em}

\#\#\# Required Output Structure:

1. `## DIMENSION TABLES`
2. Table definitions
3. `## FACT TABLES`
4. Table definitions
5. `## COLUMN MAPPING`

The column mapping must use exact original column names and exact source indices.
\end{tcolorbox}
% \captionof{figure}{Schema Refiner Prompt.}
\label{fig:schema_refiner}


% ---- One-Shot System Prompt Box ----
\begin{tcolorbox}[
    title=One-Shot System Prompt,
    colback=white,
    colframe=gray!75!black,
    fonttitle=\bfseries,
    enhanced,
    breakable
]
You are a professional database administrator and SQL expert.

\vspace{1em}

\#\#\# Instruction:

Your task is to translate a natural language question into a syntactically correct PostgreSQL query based on the provided database schema.

\vspace{1em}

\#\#\# Language and Translation Rules:

1. The input question may be in English or an Indic language.
2. You must understand the question intent and generate SQL over the English database schema.
3. If the question contains entity names in an Indic language, implicitly translate or transliterate them to match the exact English string literals found in the database schema or sample data.

\vspace{1em}

\#\#\# PostgreSQL Rules:

1. Do not use double quotes for identifiers unless strictly required.
2. Always use single quotes for string literals.
3. For case-insensitive matching, always use `ILIKE`.
4. Cast data types explicitly if needed using `::`.
5. Output only the final SQL query.
6. Do not wrap the answer in Markdown unless explicitly requested by the task prompt.
\end{tcolorbox}
% \captionof{figure}{One-Shot System Prompt.}
\label{fig:oneshot_system}


% ---- One-Shot User Prompt Box ----
\begin{tcolorbox}[
    title=One-Shot User Prompt,
    colback=white,
    colframe=gray!75!black,
    fonttitle=\bfseries,
    enhanced,
    breakable
]
\#\#\# Database Schema:

\{ddl\}

\vspace{1em}

\#\#\# Sample Data:

\{samples\}

\vspace{1em}

\#\#\# One-Shot Learning Example:

\{one\_shot\}

\vspace{1em}

\#\#\# Task:

Question: \{question\}

Evidence / External Knowledge: \{evidence\}

\vspace{1em}

Output only the valid PostgreSQL query ending with a semicolon. Do not include markdown formatting.
\end{tcolorbox}
% \captionof{figure}{One-Shot User Prompt.}
\label{fig:oneshot_user}
