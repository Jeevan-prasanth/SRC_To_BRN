import sys
import time
import concurrent.futures
from SrctoStg.db import DatabaseETL
from SrctoStg.onesource import OneSource
from SrctoStg.connections import DBConnectionManager
from SrctoStg import ArgumentParser
from SrctoStg.logs import LoggerManager

class ETLRunner:
    def __init__(self, args):
        self.args = args
        self.logger = LoggerManager().logger
        self.db = None

    def toggle_db_restore_schedule(self, dbs, enable):
        """Toggle the restore schedule for the given databases."""
        conn = DBConnectionManager().new_db_connection("source", use_sqlalchemy=False, database="master")
        
        with conn.cursor() as cursor:
            for sourcename in dbs:
                cursor.execute(
                    f"EXEC msdb.dbo.sp_update_job @job_name='LSRestore_PCSPTDB_{sourcename}', @enabled = {enable}"
                )
                conn.commit()
        
        self.logger.info("🔄 Disabled recovery schedule for: %s", ", ".join(dbs))

    def run(self):
        """Main ETL execution logic."""
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

        if not records:
            self.logger.info("⚠️ No entries found in records.")
            sys.exit(0)

        self.logger.info("📋 %d entries found in records", len(records))

        if self.args.list_sources:
            for record in records:
                self.logger.info(f"{record.dataflowflag:<20} - {record.sourceid}")
            sys.exit(0)

        time_start = time.perf_counter()

        def process_record(record):
            """Process a single record from source to staging."""
            try:
                db = DatabaseETL(record.sourcetype)  # Initialize inside the function
                db.copy_single_record_from_source(record)
                self.logger.info(f"✅ Successfully processed record: {record.sourceobject}")
            except Exception as e:
                self.logger.error(f"❌ Error processing record {record.sourceobject}: {e}")

        if self.args.parallel:
            # Parallel execution using threads
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(records), 8)) as executor:
                try:
                    executor.map(process_record, records)
                    self.logger.info("✅ Finished running source to staging in parallel.")
                except Exception as e:
                    self.logger.error(f"❌ Parallel processing error: {e}")
        else:
            # Serial execution with optimized DB initialization
            previous_sourcetype = None
            for record in records:
                if record.sourcetype != previous_sourcetype:
                    self.db = DatabaseETL(record.sourcetype)  # Reinitialize only when sourcetype changes
                    previous_sourcetype = record.sourcetype  # Update tracker
                
                self.db.copy_single_record_from_source(record)  # Process record
            
            self.logger.info("✅ Finished running source to staging in series.")

        time_end = time.perf_counter()
        self.logger.info("⏱️ Total time taken: %.2f seconds", (time_end - time_start))

if __name__ == "__main__":
    arg_parser = ArgumentParser()
    cli_args = arg_parser.parse_args()
    
    etl_runner = ETLRunner(cli_args)
    etl_runner.run()
