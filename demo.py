
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
        self.engine_source = self.db_manager.new_db_connection("source")
        self.engine_staging = self.db_manager.new_db_connection("staging")
        self.engine_srcconfig = self.db_manager.new_db_connection("source-config")
        self.logger = LoggerManager().logger
        self.data_type_mapping = {
            "int": "integer",
            "bigint": "bigint",
            "smallint": "smallint",
            "tinyint": "smallint",
            "bit": "boolean",
            "decimal": "numeric",
            "numeric": "numeric",
            "float": "double precision",
            "real": "real",
            "money": "numeric(19,4)",
            "smallmoney": "numeric(10,4)",
            "char": "char",
            "varchar": "varchar",
            "nvarchar": "varchar",
            "text": "text",
            "ntext": "text",
            "datetime": "timestamp",
            "datetime2": "timestamp",
            "smalldatetime": "timestamp",
            "date": "date",
            "time": "time",
            "datetimeoffset": "timestamptz",
            "binary": "bytea",
            "varbinary": "bytea",
            "image": "bytea",
            "xml": "text",
            "uniqueidentifier": "uuid",
            "geography": "text",
            "geometry": "text"
        }


    def extract_and_store_schema(self, table_name, source_db, source_schema, target_table):
        """Extracts schema from the source DB and stores it in a lookup table."""
        query = f"""
            WITH KeyConstraints AS (
                SELECT 
                    ic.object_id,
                    ic.column_id,
                    STRING_AGG(kc.type_desc, ', ') AS key_constraint -- Merge multiple constraints into one
                FROM sys.index_columns ic
                JOIN sys.key_constraints kc 
                    ON ic.object_id = kc.parent_object_id 
                    AND ic.index_id = kc.unique_index_id
                GROUP BY ic.object_id, ic.column_id
            )

            SELECT 
                c.name AS column_name, 
                t.name AS user_data_type, 
                c.max_length AS length, 
                c.precision AS precision, 
                c.scale AS scale, 
                c.is_nullable AS nullable, 
                c.column_id,
                kc.key_constraint
            FROM sys.columns c
            JOIN sys.types t ON c.user_type_id = t.user_type_id
            LEFT JOIN KeyConstraints kc 
                ON c.object_id = kc.object_id AND c.column_id = kc.column_id
            WHERE OBJECT_NAME(c.object_id) = '{table_name}'
            ORDER BY c.column_id;

        """
        
        with self.engine_source.connect() as conn:
            df = pd.read_sql(query, conn)
        
        # Add table name and source_db info
        df["source_tablename"] = table_name
        df["target_tablename"] = target_table


        df["source_db"] = source_db
        
        # Store in lookup table
        df.to_sql("schema_lookup", self.engine_staging, if_exists="replace", index=False)


    def create_tables_from_lookup(self, targetschema):
        """Creates tables dynamically from schema_lookup using data_type_mapping."""
        
        query = """
            SELECT l.source_tablename, l.target_tablename, l.column_name, 
                l.user_data_type, l.length, l.precision, l.scale, 
                l.nullable, l.key_constraint
            FROM schema_lookup l
            ORDER BY l.source_tablename, l.column_id;
        """

        with self.engine_staging.connect() as conn:
            df = pd.read_sql(query, conn)

        # ✅ Fix: Group by `target_tablename` instead of `table_name`
        tables = df.groupby("target_tablename")  

        for table_name, table_df in tables:
            columns = []
            primary_keys = []
            unique_keys = []

            for _, row in table_df.iterrows():
                # Map user-defined data type to the target DB type
                target_data_type = self.data_type_mapping.get(row["user_data_type"].lower(), row["user_data_type"])
                col_def = f"{row['column_name']} {target_data_type}"

                # Handle variable-length data types
                if target_data_type in ["char", "varchar"]:
                    col_def += f"({row['length']})"
                elif target_data_type == "numeric" and row["precision"] and row["scale"]:
                    col_def += f"({row['precision']}, {row['scale']})"

                # NULL constraints
                col_def += " NOT NULL" if row["nullable"] == "NO" else " NULL"

                # Handle constraints
                if row["key_constraint"] == "PRIMARY_KEY_CONSTRAINT":
                    primary_keys.append(row["column_name"])
                elif row["key_constraint"] == "UNIQUE_CONSTRAINT":
                    unique_keys.append(row["column_name"])

                columns.append(col_def)

            # PRIMARY KEY constraint
            if primary_keys:
                columns.append(f"PRIMARY KEY ({', '.join(primary_keys)})")

            # UNIQUE constraints
            for uq in unique_keys:
                columns.append(f"UNIQUE ({uq})")

            # ✅ Fix: Use `target_tablename` for table creation
            create_table_sql = f"CREATE TABLE {targetschema}.{table_name} (\n  " + ",\n  ".join(columns) + "\n);"
            print(f"Creating table {table_name}...")

            # Execute SQL
            try:
                with self.engine_staging.connect() as conn:
                    conn.execute(text(create_table_sql))
                    time.sleep(5)
                    conn.commit()
            except Exception as e:
                print(f"Error creating table {table_name}: {str(e)}")
                continue
            self.logger.info(f"✅ Table '{table_name}' created successfully!")



    def copy_single_record_from_source(self, record):
        """Determines the source type and processes the record accordingly."""
        try:
            self.extract_and_store_schema(record.sourceobject, record.sourcetype,record.sourceschema,record.targetobject)
            self.create_tables_from_lookup(record.targetschemaname)
            if record.sourcetype in ["Excel", "CSV"]:
                return self._copy_single_record_flat_file(record)
            elif record.sourcetype in ["API", "KEKA", "Hubspot", "Salesforce"]:
                return self._copy_single_record_api(record)
            else:
                return self._copy_single_record_db(record)
        except Exception as e:
            self.logger.error(f"❌ Error processing record {record.sourceid}: {str(e)}")
            return 0

    def _copy_single_record_db(self, record):
        """Extracts data from a source database and inserts it into the staging database."""
        self.logger.info(f"🔹 Processing DB record: {record.sourceid}")

        try:
            with self.engine_source.connect() as conn_source, self.engine_staging.connect() as conn_staging:
                # ✅ **Modify query to cast unsupported data types dynamically**
                query = self.modify_sqlalchemy_query(conn_source, record.sourceschema, record.sourceobject)
                df = pd.read_sql_query(text(query), conn_source)

                # ✅ **Convert Data Types Dynamically**
                df = self.convert_data_types(df, target_db="PostgreSQL")

                # ✅ **Optimize Column Name Formatting**
                df.columns = df.columns.str.replace(" ", "_").str.lower()

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


    def _copy_single_record_flat_file(self, record):
        """Extracts data from a flat file (CSV/Excel/TSV) and inserts it into the staging database."""
        self.logger.info(f"Processing flat file: {record.sourceobject}")

        try:
            file_extension = record.sourceobject.split('.')[-1]
            file_path = record.connectionstr + record.sourceobject

            if file_extension == 'csv':
                df = pd.read_csv(file_path)
            elif file_extension == 'tsv':
                df = pd.read_csv(file_path, sep='\t')
            elif file_extension in ['xls', 'xlsx']:
                df = pd.read_excel(file_path)
            else:
                self.logger.error(f"❌ Unsupported file format: {file_extension}")
                return 0

            df.to_sql(
                record.targetobject,
                self.engine_staging,
                if_exists="append",
                index=False,
                schema=record.targetschemaname,
            )
            return len(df)
        except Exception as e:
            self.logger.error(f"❌ Flat file processing error: {str(e)}")
            return 0

    

    def _copy_single_record_api(self, record):
        """Extracts data from an API and inserts it into the staging database."""
        self.logger.info(f"Processing API data: {record.apiurl}")

        try:
            response = requests.get(record.apiurl, headers={'Authorization': f'Bearer {record.apiaccesstoken}'})
            response.raise_for_status()
            data = response.json()

            if isinstance(data, list):  # API returns a list of objects
                df = pd.DataFrame(data)
            elif isinstance(data, dict):  # API returns a single object
                df = pd.DataFrame([data])
            else:
                raise ValueError("Unexpected API response format")

            df.to_sql(
                record.targetobject,
                self.engine_staging,
                if_exists="append",
                index=False,
                schema=record.targetschemaname,
            )
            return len(df)
        except requests.exceptions.RequestException as e:
            self.logger.error(f"❌ API request failed: {str(e)}")
            return 0
        except Exception as e:
            self.logger.error(f"❌ API processing error: {str(e)}")
            return 0

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

    def close_connections(self):
        """Closes all database connections properly."""
        self.db_manager.close_all_connections()
        self.logger.info("✅ All database connections closed.")

'''
if __name__ == "__main__":
    db_etl = DatabaseETL()
    # Example usage: db_etl.copy_single_record_from_source(record)
'''
