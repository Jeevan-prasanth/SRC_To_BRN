import sys
import time
import concurrent.futures
import pandas as pd
from SrctoStg.db import DatabaseETL
from SrctoStg.onesource import OneSource
from SrctoStg.connections import DBConnectionManager
from SrctoStg import ArgumentParser
from SrctoStg.logs import LoggerManager
#from notify import config

class ETLRunner:
    def __init__(self, args):
        self.args = args
        self.logger = LoggerManager().logger
        self.db=DatabaseETL()
    '''
    def ensure_databases_online(self, dbs, user_agent):
        conn = DBConnectionManager().new_db_connection("source", use_sqlalchemy=False, database="master")
        with conn.cursor() as cursor:
            for sourcename in dbs:
                cursor.execute(f"SELECT name, state_desc FROM sys.databases WHERE name = {repr(sourcename)}")
                result = cursor.fetchone()
                status = result[1]
                if status != "ONLINE":
                    self.logger.info("%s is %s", sourcename, status)
                    config.send_email(
                        f"ERROR in {config.ENV}: Failed to connect to DB",
                        body_text=f"""Hi all,
An error has occurred in the ETL process in {config.ENV}.
Failed to connect to {sourcename}
User Agent: {user_agent}
{sourcename} is {status}""",
                    )
                    sys.exit(1)
'''

    def toggle_db_restore_schedule(self, dbs, enable):
        conn = DBConnectionManager().new_db_connection("source", use_sqlalchemy=False, database="master")
        with conn.cursor() as cursor:
            for sourcename in dbs:
                cursor.execute(f"EXEC msdb.dbo.sp_update_job @job_name='LSRestore_PCSPTDB_{sourcename}', @enabled = {enable}")
                conn.commit()
        self.logger.info("Disabled recovery schedule for %s", ", ".join(dbs))

    def run(self):
        records = OneSource().control_entries(
            'SRCtoStg',
            self.args.sources and self.args.sources.split(self.args.delimiter),
            self.args.groups and self.args.groups.split(self.args.delimiter),
            self.args.exclude_sources and self.args.exclude_sources.split(self.args.delimiter),
            self.args.exclude_groups and self.args.exclude_groups.split(self.args.delimiter),
            self.args.object_type and self.args.object_type.split(self.args.delimiter),
            self.args.calling_sequence and self.args.calling_sequence.split(self.args.delimiter),
            self.args.loadfrequency,
            self.args.failed,
        )

        if records :
            self.logger.info("%d entries in records", len(records))
        else:
            self.logger.info("No entries in records")
            sys.exit(0)

        if self.args.list_sources:
            for record in records:
                self.logger.info(f"{record.dataflowflag:<20} - {record.sourceid}")
            sys.exit(0)

        time_start = time.perf_counter()
        if self.args.parallel:
            with concurrent.futures.ProcessPoolExecutor(max_workers=min(len(records), 8)) as executor:
                for record in records:
                    executor.submit(self.db.copy_single_record_from_source, record)
            self.logger.info("Finished running source to staging in parallel")
        else:
            for record in records:
                self.db.copy_single_record_from_source(record)
            self.logger.info("Finished running source to staging in series")

        time_end = time.perf_counter()
        self.logger.info("Total time taken %.2f seconds", (time_end - time_start))

if __name__ == "__main__":
    arg_parser = ArgumentParser()
    cli_args = arg_parser.parse_args()
    etl_runner = ETLRunner(cli_args)
    etl_runner.run()
