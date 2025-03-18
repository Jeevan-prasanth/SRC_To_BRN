import os
import sys
import yaml
import pandas as pd
import numpy as np
import requests
import time
from sqlalchemy import create_engine, inspect, text
from SrctoStg.connections import DBConnectionManager
from SrctoStg.logs import LoggerManager
from sqlalchemy.sql.sqltypes import NullType

class DatabaseETL:
    """Handles data extraction from various sources and loads it into the staging database."""

    def __init__(self):
        """Initialize ETL process, load config, and establish connections."""
        self.db_manager = DBConnectionManager()
        self.engine_source = self.db_manager.new_db_connection("SqlServer")
        self.engine_staging = self.db_manager.new_db_connection("staging")
        self.engine_srcconfig = self.db_manager.new_db_connection("source-config")
        self.logger = LoggerManager().logger
    
    def copy_single_record_from_source(self, record):
        """Determines the source type and processes the record accordingly."""
        try:
                        
            if record.sourcetype =='Flatfile':
                return self._copy_single_record_flat_file(record)
            elif record.sourcetype in ["API", "KEKA", "Hubspot", "Salesforce"]:
                return self._copy_single_record_api(record)
            else:
                return self._copy_single_record_db(record)
            return True
        except Exception as e:
            self.logger.error(f"❌ Error processing record {record.sourceid}: {str(e)}")
            return 0
        
    def extract_and_store_schema(self, source_type, source_schema, source_table, target_table):
        """Extracts schema from various sources and stores it in source_lookup."""
        self.logger.info(f"🔹 Extracting schema from {source_type} source: {source_schema}.{source_table}")

        # ✅ Fetch metadata dynamically
        metadata = self._get_source_metadata(source_type, source_schema, source_table)
        
        if metadata.empty:
            self.logger.warning(f"⚠️ No metadata found for {source_schema}.{source_table}")
            return

        # ✅ Format DataFrame to match `source_lookup` schema
        metadata["source_type"] = source_type
        metadata["source_schema"] = source_schema
        metadata["source_table"] = source_table
        metadata["target_table"] = target_table
        metadata["nullable"] = metadata["nullable"].astype(bool)


        # ✅ Store metadata in `source_lookup`
        with self.engine_srcconfig.connect() as conn:
            metadata.to_sql("source_lookup", conn, if_exists="append", index=False,schema="ods")

        self.logger.info(f"✅ Metadata for {source_schema}.{source_table} stored in source_lookup successfully!")
        self.call_sp()


    def _get_source_metadata(self, source_type, source_schema, source_table):
        """Fetches metadata for databases, CSVs, Excel, and APIs."""
        query = None

        if source_type == "SQL Server":
            query = f"""
                SELECT distinct
                    c.ORDINAL_POSITION AS column_id,
                    c.COLUMN_NAME as column_name,
                    c.DATA_TYPE as source_data_type,
                    c.CHARACTER_MAXIMUM_LENGTH as length,
                    c.NUMERIC_PRECISION as precisions,
                    c.NUMERIC_SCALE as scale,
                    c.IS_NULLABLE as nullable,
                    CASE WHEN kcu.COLUMN_NAME IS NOT NULL THEN 'PRIMARY KEY' ELSE '' END AS key_constraint
                FROM 
                    INFORMATION_SCHEMA.COLUMNS c
                LEFT JOIN 
                    INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                ON 
                    c.TABLE_NAME = kcu.TABLE_NAME AND c.COLUMN_NAME = kcu.COLUMN_NAME
                WHERE 
                    c.TABLE_NAME = '{source_table}'
                ORDER BY 
                    c.ORDINAL_POSITION;

            """

        elif source_type in ["PostgreSQL", "MySQL"]:
            query = f"""
                SELECT distinct
                    cols.ordinal_position AS column_id,
                    cols.column_name as column_name,
                    cols.data_type AS source_data_type,
                    cols.character_maximum_length AS length,
                    cols.numeric_precision AS precisions,
                    cols.numeric_scale AS scale,
                    CASE
                        WHEN cols.is_nullable = 'YES' THEN TRUE
                        ELSE FALSE
                    END AS nullable ,
                    CASE 
                        WHEN kcu.constraint_name IS NOT NULL THEN 'PRIMARY KEY'
                        ELSE ''
                    END AS key_constraint
                FROM 
                    information_schema.columns AS cols
                LEFT JOIN 
                    information_schema.key_column_usage AS kcu
                    ON cols.column_name = kcu.column_name
                    AND cols.table_name = kcu.table_name
                    AND cols.table_schema = kcu.table_schema
                LEFT JOIN 
                    information_schema.table_constraints AS tc
                    ON kcu.constraint_name = tc.constraint_name
                    AND tc.constraint_type = 'PRIMARY KEY'
                WHERE 
                    cols.table_schema = '{source_schema}' 
                    AND cols.table_name = '{source_table}'
                ORDER BY 
                    cols.ordinal_position;

            """

        elif source_type == "CSV":
            df = pd.read_csv(source_table, nrows=5)  # Read first few rows to infer schema
            metadata = pd.DataFrame({
                "column_id": range(1, len(df.columns) + 1),
                "column_name": df.columns,
                "source_data_type": df.dtypes.astype(str),
                "length": None,
                "precision": None,
                "scale": None,
                "nullable": True,
                "key_constraint": None
            })
            return metadata

        elif source_type == "Excel":
            df = pd.read_excel(source_table, nrows=5)
            metadata = pd.DataFrame({
                "column_id": range(1, len(df.columns) + 1),
                "column_name": df.columns,
                "source_data_type": df.dtypes.astype(str),
                "length": None,
                "precision": None,
                "scale": None,
                "nullable": True,
                "key_constraint": None
            })
            return metadata

        elif source_type == "API":
            response = self._fetch_api_sample(source_table)  # Implement API request logic
            metadata = pd.json_normalize(response).dtypes.reset_index()
            metadata.columns = ["column_name", "source_data_type"]
            metadata["column_id"] = range(1, len(metadata) + 1)
            metadata["length"] = None
            metadata["precision"] = None
            metadata["scale"] = None
            metadata["nullable"] = True
            metadata["key_constraint"] = None
            return metadata

        # Execute query for databases
        if query:
            with self.engine_source.connect() as conn:
                return pd.read_sql(query, conn)

        self.logger.warning(f"⚠️ No metadata extraction logic for source type: {source_type}")
        return pd.DataFrame()
    
    def call_sp(self):
        with self.engine_srcconfig.begin() as conn:
            conn.execute(text("CALL ods.usp_srctomain_lookup();"))
            self.logger.info("✅ Stored procedure ods.usp_SrcToMain_lookup executed successfully!")
    
    def create_tables_from_lookup(self, target_schema):
        """Creates target tables based on transformed metadata from main_lookup."""
        self.logger.info("🔹 Generating CREATE TABLE statements from main_lookup...")

        query = """
            SELECT column_id, column_name, target_table, 
                target_data_type, length, precisions, scale, nullable, 
                key_constraint, default_value
            FROM ods.main_lookup
            ORDER BY target_table, column_id;
        """

        with self.engine_srcconfig.connect() as conn:
            df = pd.read_sql(query, conn)

        # Group by target_table to create tables
        tables = df.groupby("target_table")

        for table, group in tables:
            columns_def = []
            primary_keys = []  # List to store primary key columns

            for _, row in group.iterrows():
                col_def = f'"{row["column_name"]}" {row["target_data_type"]}'

                # ✅ Apply length only for VARCHAR, CHAR, TEXT-like types
                if row["target_data_type"].upper() in ["VARCHAR", "CHAR"] and pd.notna(row["length"]):
                    col_def += f"({int(row['length'])})"

                # ✅ Apply precision & scale only for DECIMAL, NUMERIC
                elif row["target_data_type"].upper() in ["DECIMAL", "NUMERIC"] and pd.notna(row["precisions"]) and pd.notna(row["scale"]):
                    col_def += f"({int(row['precisions'])}, {int(row['scale'])})"

                # ❌ DO NOT apply length for INTEGER, TIMESTAMP, UUID, GEOGRAPHY, etc.

                # ✅ Handle NULL/NOT NULL
                if row["nullable"] == False:
                    col_def += " NOT NULL"

                # ✅ Handle default values
                if pd.notna(row["default_value"]):
                    col_def += f" DEFAULT {row['default_value']}"

                # ✅ Handle constraints
                if row["key_constraint"] == "PRIMARY KEY":
                    primary_keys.append(f'"{row["column_name"]}"')  # Add column name to primary key list
                elif row["key_constraint"] == "UNIQUE":
                    col_def += " UNIQUE"

                columns_def.append(col_def)

            # ✅ Build CREATE TABLE statement with schema "stg"
            sql_stmt = f'CREATE TABLE IF NOT EXISTS {target_schema}."{table}" (\n    ' + ",\n    ".join(columns_def)

            # ✅ Add primary keys at the end
            if primary_keys:
                pk_constraint = ",\n    " + f"PRIMARY KEY ({', '.join(primary_keys)})"
                sql_stmt += pk_constraint

            sql_stmt += "\n);"

            # self.logger.info(f"📝 Executing SQL:\n{sql_stmt}\n")

            # ✅ Execute the statement
            with self.engine_staging.connect() as conn:
                conn.execute(text(sql_stmt))
                conn.commit()

        self.logger.info("✅ Table creation process completed!")


    def _copy_single_record_db(self, record):
        """Extracts data from a source database and inserts it into the staging database."""
        self.extract_and_store_schema(record.sourcetype,record.sourceschema,record.sourceobject,record.targetobject)
        self.create_tables_from_lookup(record.targetschemaname)
        self.logger.info(f"🔹 Processing DB record: {record.sourceid}")

        try:
            with self.engine_source.connect() as conn_source, self.engine_staging.connect() as conn_staging:
                # ✅ **Modify query to cast unsupported data types dynamically**
                query = self.modify_sqlalchemy_query(conn_source, record.sourceschema, record.sourceobject)
                df = pd.read_sql_query(text(query), conn_source)
                # ✅ **Convert Data Types Dynamically**
                df = self.convert_data_types(df, target_db="PostgreSQL")


                # ✅ **Optimize Column Name Formatting**
                #df.columns = df.columns.str.replace(" ", "_").str.lower()

                # ✅ **Optimized Batch Insert into Staging**
                df.to_sql(
                    record.targetobject,
                    self.engine_staging,
                    if_exists="append",
                    index=False,
                    schema=record.targetschemaname,
                    method="multi",  # ✅ Faster bulk inserts
                )

                self.logger.info(f"✅ Copied {len(df)} records to staging")
                return len(df)

        except Exception as e:
            self.logger.error(f"❌ DB extraction error: {str(e)}")
            return 0

    def modify_sqlalchemy_query(self, conn_source, schema_name, table_name):
        """Fetch column data types and modify query to cast unsupported types."""
        query = f"""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}'
        """

        result = conn_source.execute(text(query))
        columns_info = {row[0]: row[1] for row in result.fetchall()}

        unsupported_types = {"geography", "geometry", "hierarchyid", "xml", "uniqueidentifier"}
        cast_columns = []

        for col_name, col_type in columns_info.items():
            if col_type in unsupported_types:
                # ✅ Convert unsupported types to NVARCHAR(MAX)
                cast_columns.append(f"CAST({col_name} AS NVARCHAR(MAX)) AS {col_name}")
            else:
                cast_columns.append(col_name)
        final_query = f"SELECT {', '.join(cast_columns)} FROM {schema_name}.{table_name}"
        #self.logger.info(f"🔍 Final modified query: {final_query}")

        return final_query
    
    def convert_data_types(self, df, target_db):
        """Dynamically converts database types for compatibility across different DBs."""
        for col in df.columns:
            if target_db == "PostgreSQL":
                if df[col].dtype == "object":
                    try:
                        sample_value = df[col].dropna().iloc[0]  # Get first non-null value
                        if "T" in str(sample_value) and "+" in str(sample_value):  # Detect `DATETIMEOFFSET`
                            df[col] = pd.to_datetime(df[col], errors="coerce")  # ✅ Safely convert
                    except IndexError:
                        pass  # Ignore empty columns

                # ✅ Convert BOOLEAN → INTEGER for PostgreSQL
                if df[col].dtype == "bool":
                    df[col] = df[col].astype(int)

            elif target_db == "SQL Server":
                # ✅ Convert TIMESTAMP → VARCHAR for SQL Server compatibility
                if df[col].dtype == "datetime64[ns]":
                    df[col] = df[col].astype(str)

                # ✅ Convert BOOLEAN → BIT (1/0)
                if df[col].dtype == "bool":
                    df[col] = df[col].astype(int)

        return df
    def _copy_single_record_flat_file(self, record):
        """Extracts schema, stores metadata, creates a table, and inserts data for flat files."""
        self.logger.info(f"🔹 Processing flat file: {record.sourceobject}")

        try:
            file_extension = record.sourceobject.split('.')[-1].lower()
            file_path = os.path.join(record.connectionstr, record.sourceobject)

            # ✅ Read the entire file first (to avoid duplicate I/O)
            if file_extension == 'csv':
                df = pd.read_csv(file_path)
            elif file_extension == 'tsv':
                df = pd.read_csv(file_path, sep='\t')
            elif file_extension in ['xls', 'xlsx']:
                df = pd.read_excel(file_path)
            elif file_extension == 'parquet':
                df = pd.read_parquet(file_path, engine='pyarrow')
            elif file_extension == 'json':
                df = pd.read_json(file_path, lines=True)
            else:
                self.logger.error(f"❌ Unsupported file format: {file_extension}")
                return 0

            # ✅ Extract schema from first few rows (without re-reading the file)
            schema_df = df.head(5)

            # ✅ Format metadata for `source_lookup`
            metadata = pd.DataFrame({
                "column_id": range(1, len(schema_df.columns) + 1),
                "column_name": schema_df.columns,
                "source_data_type": schema_df.dtypes.astype(str),
                "length": None,
                "precisions": None,
                "scale": None,
                "nullable": True,
                "key_constraint": None
            })

            metadata["source_type"] = record.sourcetype
            metadata["source_schema"] = "FlatFiles"
            metadata["source_table"] = record.sourceobject
            metadata["target_table"] = record.targetobject

            # ✅ Store metadata in `source_lookup`
            with self.engine_srcconfig.connect() as conn:
                metadata.to_sql("source_lookup", conn, if_exists="append", index=False, schema="ods")

            self.logger.info(f"✅ Metadata for {record.sourceobject} stored in source_lookup!")

            # ✅ Call stored procedure to process metadata
            self.call_sp()

            # ✅ Create table using metadata from `main_lookup`
            self.create_tables_from_lookup(record.targetschemaname)

            # ✅ Insert full data into staging
            df.to_sql(
                record.targetobject,
                self.engine_staging,
                if_exists="append",
                index=False,
                schema=record.targetschemaname
            )

            self.logger.info(f"✅ Copied {len(df)} records to staging")
            return len(df)

        except FileNotFoundError:
            self.logger.error(f"❌ File not found: {file_path}")
        except pd.errors.EmptyDataError:
            self.logger.error(f"❌ Empty file: {file_path}")
        except Exception as e:
            self.logger.error(f"❌ Flat file processing error: {str(e)}")

        return 0
    def _copy_single_record_api(self, record):
        self.logger.info(f"🔹 Processing API record: {record.sourceobject}")
        try:
            response = requests.get(record.apiurl, headers={'Authorization': f'Bearer {record.apiaccesstoken}'})
            response.raise_for_status()

            df = pd.json_normalize(response.json())

            # ✅ Extract schema from first few rows
            schema_df = df.head(5)

            # ✅ Format metadata for `source_lookup`
            metadata = pd.DataFrame({
                "column_id": range(1, len(schema_df.columns) + 1),
                "column_name": schema_df.columns,
                "source_data_type": schema_df.dtypes.astype(str),
                "length": None,
                "precisions": None,
                "scale": None,
                "nullable": True,
                "key_constraint": None
            })

            metadata["source_type"] = record.sourcetype
            metadata["source_schema"] = "API"
            metadata["source_table"] = record.sourceobject
            metadata["target_table"] = record.targetobject

            # ✅ Store metadata in `source_lookup`
            with self.engine_srcconfig.connect() as conn:
                metadata.to_sql("source_lookup", conn, if_exists="append", index=False, schema="ods")

            self.logger.info(f"✅ Metadata for {record.sourceobject} stored in source_lookup!")

            # ✅ Call stored procedure to process metadata
            self.call_sp()

            # ✅ Create table using metadata from `main_lookup`
            self.create_tables_from_lookup(record.targetschemaname)

            # ✅ Insert full data into staging
            df.to_sql(
                record.targetobject,
                self.engine_staging,
                if_exists="append",
                index=False,
                schema=record.targetschemaname
            )

            self.logger.info(f"✅ Copied {len(df)} records to staging")
            return len(df)

        except requests.exceptions.RequestException as e:
            self.logger.error(f"❌ API request error: {str(e)}")
        except Exception as e:
            self.logger.error(f"❌ API processing error: {str(e)}")


