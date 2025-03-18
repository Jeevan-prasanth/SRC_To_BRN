'''
import pyodbc

# Database connection details
server = 'localhost'
database = 'AdventureWorks2022'
username = 'jeevan'
password = 'sql'

# Establish connection
connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
connection = pyodbc.connect(connection_string)

# Query to get column details
table_name = 'person'
query = f"""
WITH KeyConstraints AS (
                    SELECT 
                        ic.object_id,
                        ic.column_id,
                        STRING_AGG(kc.type_desc, ', ') AS key_constraint
                    FROM sys.index_columns ic
                    JOIN sys.key_constraints kc 
                        ON ic.object_id = kc.parent_object_id 
                        AND ic.index_id = kc.unique_index_id
                    GROUP BY ic.object_id, ic.column_id
                )
                SELECT 
                    c.column_id,
                    CAST(c.name AS NVARCHAR(MAX)) AS column_name,   -- ✅ Enclose in brackets
                    CAST(
                        CASE 
                            WHEN t.name IN ('namestyle', 'name') THEN 'nvarchar'
                            ELSE t.name
                        END 
                        AS NVARCHAR(MAX)) AS source_data_type,  -- ✅ Enclose in brackets
                    c.max_length AS length, 
                    c.precision AS precision, 
                    c.scale AS scale, 
                    c.is_nullable AS nullable, 
                    kc.key_constraint
                FROM sys.columns c
                JOIN sys.types t ON c.user_type_id = t.user_type_id
                LEFT JOIN KeyConstraints kc 
                    ON c.object_id = kc.object_id AND c.column_id = kc.column_id
                WHERE OBJECT_NAME(c.object_id) = '{table_name}'
                ORDER BY c.column_id;
"""

# Execute query
cursor = connection.cursor()
cursor.execute(query)

# Fetch and display results
columns = cursor.fetchall()
print(columns)
for column in columns:
    print(f"Column ID: {column.COLUMN_ID}, Column Name: {column.COLUMN_NAME}, Data Type: {column.DATA_TYPE}, "
          f"Length: {column.CHARACTER_MAXIMUM_LENGTH}, Precision: {column.NUMERIC_PRECISION}, "
          f"Scale: {column.NUMERIC_SCALE}, Nullable: {column.IS_NULLABLE}, Key Constraint: {column.KEY_CONSTRAINT}")

# Close connection
cursor.close()
connection.close()


import pyodbc

# Database connection details
server = 'localhost'
database = 'AdventureWorks2022'
username = 'jeevan'
password = 'sql'

# Establish connection
connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
connection = pyodbc.connect(connection_string)

# Query to get column details
table_name = 'person'
query = f"""
SELECT distinct
    c.ORDINAL_POSITION AS COLUMN_ID,
    c.COLUMN_NAME,
    c.DATA_TYPE,
    c.CHARACTER_MAXIMUM_LENGTH,
    c.NUMERIC_PRECISION,
    c.NUMERIC_SCALE,
    c.IS_NULLABLE,
    CASE WHEN kcu.COLUMN_NAME IS NOT NULL THEN 'PRIMARY KEY' ELSE '' END AS KEY_CONSTRAINT
FROM 
    INFORMATION_SCHEMA.COLUMNS c
LEFT JOIN 
    INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
ON 
    c.TABLE_NAME = kcu.TABLE_NAME AND c.COLUMN_NAME = kcu.COLUMN_NAME
WHERE 
    c.TABLE_NAME = '{table_name}'
ORDER BY 
    c.ORDINAL_POSITION
"""

# Execute query
cursor = connection.cursor()
cursor.execute(query)

# Fetch and display results
columns = cursor.fetchall()
for column in columns:
    print(f"Column ID: {column.COLUMN_ID}, Column Name: {column.COLUMN_NAME}, Data Type: {column.DATA_TYPE}, "
          f"Length: {column.CHARACTER_MAXIMUM_LENGTH}, Precision: {column.NUMERIC_PRECISION}, "
          f"Scale: {column.NUMERIC_SCALE}, Nullable: {column.IS_NULLABLE}, Key Constraint: {column.KEY_CONSTRAINT}")

# Close connection
cursor.close()
connection.close()



from sqlalchemy import create_engine, inspect, MetaData, Table
from sqlalchemy.sql import select
from sqlalchemy import create_engine, text

# Database connection details
server = 'localhost'
database = 'AdventureWorks2022'
username = 'jeevan'
password = 'sql'

# Establish connection
connection_string = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server'
engine = create_engine(connection_string)

# Inspect the database
inspector = inspect(engine)

# Table name
table_name = 'address'

# Get column details
columns = []
for column in inspector.get_columns(table_name,schema='person'):
    key_constraint = 'PRIMARY KEY' if column.get('primary_key') else ''
    columns.append({
        'COLUMN_ID': column.get('ordinal_position'),
        'COLUMN_NAME': column.get('name'),
        'DATA_TYPE': str(column.get('type')),
        'CHARACTER_MAXIMUM_LENGTH': column.get('length'),
        'NUMERIC_PRECISION': column.get('precision'),
        'NUMERIC_SCALE': column.get('scale'),
        'IS_NULLABLE': 'YES' if column.get('nullable') else 'NO',
        'KEY_CONSTRAINT': key_constraint
    })

# Print column details
for column in columns:
    print(f"Column ID: {column['COLUMN_ID']}, Column Name: {column['COLUMN_NAME']}, Data Type: {column['DATA_TYPE']}, "
          f"Length: {column['CHARACTER_MAXIMUM_LENGTH']}, Precision: {column['NUMERIC_PRECISION']}, "
          f"Scale: {column['NUMERIC_SCALE']}, Nullable: {column['IS_NULLABLE']}, Key Constraint: {column['KEY_CONSTRAINT']}")



# Database connection details
import pandas as pd
from sqlalchemy import create_engine,text


server = 'localhost'
database = 'AdventureWorks2022'
username = 'jeevan'
password = 'sql'

# Establish connection
connection_string = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server'
engine = create_engine(connection_string)

# Table name
table_name = 'address'

# Query to execute
query = f"""
SELECT distinct
    c.ORDINAL_POSITION AS COLUMN_ID,
    c.COLUMN_NAME,
    c.DATA_TYPE,
    c.CHARACTER_MAXIMUM_LENGTH,
    c.NUMERIC_PRECISION,
    c.NUMERIC_SCALE,
    c.IS_NULLABLE,
    CASE WHEN kcu.COLUMN_NAME IS NOT NULL THEN 'PRIMARY KEY' ELSE '' END AS KEY_CONSTRAINT
FROM 
    INFORMATION_SCHEMA.COLUMNS c
LEFT JOIN 
    INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
ON 
    c.TABLE_NAME = kcu.TABLE_NAME AND c.COLUMN_NAME = kcu.COLUMN_NAME
WHERE 
    c.TABLE_NAME = '{table_name}'
ORDER BY 
    c.ORDINAL_POSITION
"""

# Execute query
with engine.connect() as connection:
    result = pd.read_sql(query, connection)
    print(result)

# Print column details
for column in columns:
    print(f"Column ID: {column.COLUMN_ID}, Column Name: {column.COLUMN_NAME}, Data Type: {column.DATA_TYPE}, "
          f"Length: {column.CHARACTER_MAXIMUM_LENGTH}, Precision: {column.NUMERIC_PRECISION}, "
          f"Scale: {column.NUMERIC_SCALE}, Nullable: {column.IS_NULLABLE}, Key Constraint: {column.KEY_CONSTRAINT}")'''


import pandas as pd

df=pd.read_xml(r"D:\data\books.xml")
print(df)