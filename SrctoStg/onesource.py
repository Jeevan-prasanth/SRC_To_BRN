import time
import functools
from collections import namedtuple
from sqlalchemy import text
from SrctoStg.connections import DBConnectionManager
from SrctoStg import ArgumentParser
from SrctoStg.logs import LoggerManager


class OneSource:
    def __init__(self):
        self.cli_args = ArgumentParser().parse_args()
        self.Record = None
        self.etl_batch_id = ArgumentParser().etl_batch_id
    
    def control_entries(self, dataflowflag=None, sources=None, groups=None, exclude_sources=None, exclude_groups=None, object_type=None, calling_sequence=None, load_frequency=None, failed_only=False):
        engine = DBConnectionManager().new_db_connection('source-config')
        query = self._control_table_query(sources, groups, exclude_sources, exclude_groups, object_type, calling_sequence, load_frequency, failed_only)
        params = {
            'dataflowflag': dataflowflag,
            'sources': tuple(sources) if sources else None,
            'groups': tuple(groups) if groups else None,
            'exclude_sources': tuple(exclude_sources) if exclude_sources else None,
            'exclude_groups': tuple(exclude_groups) if exclude_groups else None,
            'object_type': tuple(object_type) if object_type else None,
            'calling_sequence': tuple(calling_sequence) if calling_sequence else None,
            'load_frequency': load_frequency,
        }
        with engine.connect() as conn:
            result = conn.execute(text(query), params)
            rows = result.fetchall()
            self.Record = namedtuple('Record', result.keys())
            return [self.Record(*row) for row in rows]
    
    def _control_table_query(self, sources=None, groups=None, exclude_sources=None, exclude_groups=None, object_type=None, calling_sequence=None, load_frequency=None, failed_only=False):
        query = '''
            SELECT header.SourceId,
            detail.SourceName,
            detail.SourceType,
            detail.SourceObject,
            detail.sourceschema,
            header.DepSource,
            detail.LoadType,
            detail.LoadFrequency,
            header.ConnectionStr,
            header.ObjectType,
            header.SourceDelimiter,
            detail.SourceQuery,
            header.APIUrl,
            header.APIMethod,
            header.APIAccessToken,
            header.APIQueryParameters,
            header.APIRequestBody,
            header.ADLSContainerName,
            header.DLDirStructure,
            detail.EtlLastRunDate,
            detail.TargetObject,
            header.SourceCallingSeq,
            detail.DataflowFlag,
            detail.LatestBatchId,
            detail.TargetSchemaName,
            detail.TargetProcedureName,
            detail.IntervalDays
            FROM ods.ControlHeader header
            JOIN ods.ControlDetail detail ON header.SourceId = detail.SourceId
            WHERE detail.DataflowFlag = :dataflowflag AND detail.IsReadyForExecution = 1 AND detail.Isapplicable = 1'''
        if sources:
            query += "\n\tAND detail.SourceId IN :sources"
        if load_frequency:
            query += "\n\tAND detail.LoadFrequency = :load_frequency"

        if exclude_sources:
            query += "\n\tAND detail.SourceId NOT IN :exclude_sources"
        if groups:
            query += "\n\tAND header.SourceId IN (SELECT DISTINCT SourceId FROM ods.sourcegroupingdtl WHERE schemaname='dwh' AND SourceGroup IN :groups)"
        if exclude_groups:
            query += "\n\tAND header.SourceId NOT IN (SELECT DISTINCT SourceId FROM ods.sourcegroupingdtl WHERE schemaname='dwh' AND SourceGroup IN :exclude_groups)"
        if object_type:
            query += "\n\tAND header.ObjectType IN :object_type"
        if calling_sequence:
            query += "\n\tAND header.SourceCallingSeq IN :calling_sequence"
        if failed_only:
            query += "\n\tAND detail.FlowStatus = 'Failed'"
        query += '\n\tORDER BY header.Id'
        return query

    def audit_start(self, sourceid, targetobject, dataflowflag, source_count, user_agent, etl_batch_id):
        engine = DBConnectionManager().new_db_connection('source-config')
        query = "CALL ods.usp_etlpreprocess(:sourceid, :targetobject, :dataflowflag, :source_count, :user_agent, :etl_batch_id, NULL);"
        params = {
            'sourceid': sourceid,
            'targetobject': targetobject,
            'dataflowflag': dataflowflag,
            'source_count': source_count,
            'user_agent': user_agent,
            'etl_batch_id': etl_batch_id,
        }
        with engine.begin() as conn:
            result = conn.execute(text(query), params)
            latestbatchid, = result.fetchone()
        return latestbatchid
    
    def audit_end(self, sourceid, targetobject, dataflowflag, latestbatchid, source_count, insert_count, update_count):
        engine = DBConnectionManager().new_db_connection('source-config')
        query = "CALL ods.usp_etlpostprocess(:sourceid, :targetobject, :dataflowflag, :latestbatchid, :source_count, :insert_count, :update_count);"
        params = {
            'sourceid': sourceid,
            'targetobject': targetobject,
            'dataflowflag': dataflowflag,
            'latestbatchid': latestbatchid,
            'source_count': source_count,
            'insert_count': insert_count,
            'update_count': update_count,
        }
        with engine.begin() as conn:
            conn.execute(text(query), params)

    def audit_error(self, sourceid, targetobject, dataflowflag, latestbatchid, task, package, error_id, error_desc, error_line):
        engine = DBConnectionManager().new_db_connection('source-config')
        query = "CALL ods.usp_etlerrorinsert(:sourceid, :targetobject, :dataflowflag, :latestbatchid, :task, :package, :error_id, :error_desc, :error_line);"
        params = {
            'sourceid': sourceid,
            'targetobject': targetobject,
            'dataflowflag': dataflowflag,
            'latestbatchid': latestbatchid,
            'task': task,
            'package': package,
            'error_id': error_id,
            'error_desc': error_desc,
            'error_line': error_line,
        }
        with engine.begin() as conn:
            conn.execute(text(query), params)
