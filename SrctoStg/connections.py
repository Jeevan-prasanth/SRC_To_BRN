import sys
import os
import yaml
import pyodbc
import oracledb
import psycopg2
import clickhouse_driver
from urllib.parse import quote_plus
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

oracledb.version = "8.3.0"
sys.modules["cx_Oracle"] = oracledb  # Alias for cx_Oracle compatibility

class DBConnectionManager:
    """Manages database connections with connection pooling, retry logic, and dynamic database support."""

    def __init__(self, config_path=None, pool_size=5, max_overflow=10, max_retries=3):
        """Initialize the DB connection manager with pooling and retry logic."""
        config_path = config_path or os.path.join(os.getcwd(), "config.example.yml")
        
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found: {config_path}")

        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)

        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.max_retries = max_retries
        self.sqlalchemy_engines = {}  # Stores SQLAlchemy engines

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((pyodbc.Error, psycopg2.DatabaseError, clickhouse_driver.errors.Error)),
        reraise=True
    )
    def new_db_connection(self, location="", use_sqlalchemy=True):
        """
        Establishes a database connection dynamically based on the config file.

        Parameters:
            location (str): Database configuration section from YAML (e.g., 'staging', 'source').
            use_sqlalchemy (bool): Whether to return an SQLAlchemy engine (True) or a direct connection (False).

        Returns:
            Connection object (SQLAlchemy Engine, PyODBC, Psycopg2, Clickhouse).
        """
        try:
            if location not in self.config:
                raise KeyError(f"Location '{location}' not found in config.")

            config_section = self.config[location]
            if "connection" not in config_section:
                raise KeyError(f"Missing 'connection' section for '{location}' in config.")

            conn_details = config_section["connection"]
            dialect = config_section.get("dialect", "").lower()
            driver = config_section.get("driver", "")

            return self._get_sqlalchemy_engine(dialect, driver, conn_details)

        except Exception as e:
            raise RuntimeError(f"Database connection error: {str(e)}") from e

    def _get_sqlalchemy_engine(self, dialect, driver, conn_details):
        """Handles all database connections using SQLAlchemy (where possible)."""
        conn_details["password"] = quote_plus(conn_details["password"])

        if dialect.startswith("mssql+pyodbc"):
            driver = driver or "ODBC Driver 17 for SQL Server"
            connection_string = (
                f"DRIVER={{{driver}}};"
                f"SERVER={conn_details['host']},{conn_details['port']};"
                f"DATABASE={conn_details['database']};"
                f"UID={conn_details['username']};"
                f"PWD={conn_details['password']};"
                "TrustServerCertificate=yes;"
            )
            sqlalchemy_conn_str = f"mssql+pyodbc:///?odbc_connect={quote_plus(connection_string)}"

        else:
            sqlalchemy_conn_str = f"{dialect}://{conn_details['username']}:{conn_details['password']}@{conn_details['host']}:{conn_details['port']}/{conn_details['database']}"

        engine = create_engine(
            sqlalchemy_conn_str,
            poolclass=QueuePool,
            pool_size=self.pool_size,
            max_overflow=self.max_overflow
        )

        self.sqlalchemy_engines[dialect] = engine
        return engine

    def close_all_connections(self):
        """Closes all database connections properly."""
        for location, engine in self.sqlalchemy_engines.items():
            engine.dispose()
            print(f"Closed SQLAlchemy connection for {location}")