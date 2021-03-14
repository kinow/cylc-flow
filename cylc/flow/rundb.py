# THIS FILE IS PART OF THE CYLC SUITE ENGINE.
# Copyright (C) NIWA & British Crown (Met Office) & Contributors.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""Provide data access object for the suite runtime database."""

import os
import re
import traceback
from collections import defaultdict
from contextlib import suppress
from typing import Dict, List, Union

from sqlalchemy import (
    cast, Column, create_engine, func, INTEGER, NUMERIC, REAL, Table, TEXT,
    BOOLEAN, MetaData)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine.base import Connection
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import and_, select
from sqlalchemy.sql.dml import ValuesBase
from sqlalchemy.sql.expression import Select

import cylc.flow.flags
from cylc.flow import LOG


meta = MetaData()


# --- tables

broadcast_events = Table(
    'broadcast_events', meta,
    Column('time', TEXT),
    Column('change', TEXT),
    Column('point', TEXT),
    Column('namespace', TEXT),
    Column('key', TEXT),
    Column('value', TEXT)
)

broadcast_states = Table(
    'broadcast_states', meta,
    Column('point', TEXT, primary_key=True),
    Column('namespace', TEXT, primary_key=True),
    Column('key', TEXT, primary_key=True),
    Column('value', TEXT)
)

inheritance = Table(
    'inheritance', meta,
    Column('namespace', TEXT, primary_key=True),
    Column('inheritance', TEXT)
)

suite_params = Table(
    'suite_params', meta,
    Column('key', TEXT, primary_key=True),
    Column('value', TEXT)
)

suite_template_vars = Table(
    'suite_template_vars', meta,
    Column('key', TEXT, primary_key=True),
    Column('value', TEXT)
)

task_action_timers = Table(
    'task_action_timers', meta,
    Column('cycle', TEXT, primary_key=True),
    Column('name', TEXT, primary_key=True),
    Column('ctx_key', TEXT, primary_key=True),
    Column('ctx', TEXT),
    Column('delays', TEXT),
    Column('num', INTEGER),
    Column('delay', TEXT),
    Column('timeout', TEXT)
)

task_jobs = Table(
    'task_jobs', meta,
    Column('cycle', TEXT, primary_key=True),
    Column('name', TEXT, primary_key=True),
    Column('submit_num', INTEGER, primary_key=True),
    Column('is_manual_submit', BOOLEAN),
    Column('try_num', INTEGER),
    Column('time_submit', TEXT),
    Column('time_submit_exit', TEXT),
    Column('submit_status', INTEGER),
    Column('time_run', TEXT),
    Column('time_run_exit', TEXT),
    Column('run_signal', TEXT),
    Column('run_status', INTEGER),
    Column('platform_name', TEXT),
    Column('job_runner_name', TEXT),
    Column('job_id', TEXT)
)

task_events = Table(
    'task_events', meta,
    Column('name', TEXT),
    Column('cycle', TEXT),
    Column('time', TEXT),
    Column('submit_num', INTEGER),
    Column('event', TEXT),
    Column('message', TEXT)
)

task_late_flags = Table(
    'task_late_flags', meta,
    Column('cycle', TEXT, primary_key=True),
    Column('name', TEXT, primary_key=True),
    Column('value', INTEGER)
)

task_outputs = Table(
    'task_outputs', meta,
    Column('cycle', TEXT, primary_key=True),
    Column('name', TEXT, primary_key=True),
    Column('outputs', TEXT)
)

task_pool = Table(
    'task_pool', meta,
    Column('cycle', TEXT, primary_key=True),
    Column('name', TEXT, primary_key=True),
    Column('flow_label', TEXT, primary_key=True),
    Column('status', TEXT),
    Column('is_held', BOOLEAN)
)

task_prerequisites = Table(
    'task_prerequisites', meta,
    Column('cycle', TEXT, primary_key=True),
    Column('name', TEXT, primary_key=True),
    Column('prereq_name', TEXT, primary_key=True),
    Column('prereq_cycle', TEXT, primary_key=True),
    Column('prereq_output', TEXT, primary_key=True),
    Column('satisfied', TEXT)
)

xtriggers = Table(
    'xtriggers', meta,
    Column('signature', TEXT, primary_key=True),
    Column('results', TEXT)
)

task_states = Table(
    'task_states', meta,
    Column('name', TEXT, primary_key=True),
    Column('cycle', TEXT, primary_key=True),
    Column('flow_label', TEXT, primary_key=True),
    Column('time_created', TEXT),
    Column('time_updated', TEXT),
    Column('submit_num', INTEGER),
    Column('status', TEXT)
)

task_timeout_timers = Table(
    'task_timeout_timers', meta,
    Column('cycle', TEXT, primary_key=True),
    Column('name', TEXT, primary_key=True),
    Column('timeout', REAL)
)

abs_outputs = Table(
    'abs_outputs', meta,
    Column('cycle', TEXT, primary_key=True),
    Column('name', TEXT, primary_key=True),
    Column('output', TEXT)
)

# ---


class CylcSuiteDAO:
    """Data access object for the suite runtime database."""

    DB_FILE_BASE_NAME = "db"
    MAX_TRIES = 100
    # Can't restart workflow if <= this version
    RESTART_INCOMPAT_VERSION = "8.0a2"

    def __init__(self, file_name: str, is_public=False, timeout=0.2):
        """Initialise database access object.

        FIXME: we must receive a connection URL, not a SQLite DB name
        conn_url (str) - DB connection URL, e.g. sqlite:///tmp/file.db
        is_public (bool) - If True, allow retries, etc

        Args:
            file_name (str): Path to the database file.
            is_public (bool): If True, allow retries, etc.

        """
        self.conn_url = "sqlite://" if file_name == '' \
            else f"sqlite:///{file_name}"
        # self.conn_url = "postgresql://cylc:cylc@localhost/cylc-five"
        self.engine = create_engine(
            self.conn_url,
            # connect_args={
            #     'connect_timeout': timeout
            # },
            echo=False
        )
        if self.is_sqlite() and file_name != '':
            # create if file does not exist
            self.db_file_name = self._get_db_file_name()  # type: Union[str, None]
            os.makedirs(os.path.dirname(self.db_file_name), exist_ok=True)
        else:
            self.db_file_name = None
        self.is_public = is_public
        self.conn = None  # type: Union[Connection, None]
        self.n_tries = 0

        if not self.is_public:
            self.create_tables()

        self.to_delete = defaultdict(list)  # type: Dict[Table, List]
        self.to_insert = defaultdict(list)  # type: Dict[Table, List]
        self.to_update = defaultdict(list)  # type: Dict[Table, List]

    def _get_db_file_name(self) -> str:
        return re.sub("sqlite.*:///", "", self.conn_url)

    def add_delete_item(self, table: Table, where_args: dict = None):
        """Queue a DELETE item for a given table.

        where_args should be a dict, update will only apply to rows matching
        all these items.

        """
        s = table.delete()
        if where_args:
            for left, right in where_args.items():
                if left in table.c:
                    s = s.where(table.c[left] == right)
        self.to_delete[table.name].append(s)

    def add_insert_item(self, table: Table, args: dict):
        """Queue an INSERT args for a given table.

        If args is a list, its length will be adjusted to be the same as the
        number of columns. If args is a dict, will return a list with the same
        length as the number of columns, the elements of which are determined
        by matching the column names with the keys in the dict.

        Empty elements are padded with None.

        """
        s = insert(table)
        self.to_insert[table.name].append([s, args])

    def add_update_item(self, table: Table, set_args: dict,
                        where_args=None):
        """Queue an UPDATE item for a given table.

        set_args should be a dict, with column keys and values to be set.
        where_args should be a dict, update will only apply to rows matching
        all these items.

        """
        s = table.update()
        if where_args:
            for left, right in where_args.items():
                if left in table.c:
                    s = s.where(table.c[left] == right)
        self.to_update[table.name].append([s, set_args])

    # TODO: make it a context manager
    def close(self):
        """Explicitly close the connection."""
        if self.conn is not None:
            with suppress(Exception):
                self.conn.close()
            self.conn = None

    def connect(self) -> Connection:
        """Connect to the database.
        Returns:
            Connection: a SQLAlchemy connection object
        """
        if self.conn is None or self.conn.closed:
            self.conn = self.engine.connect()
        return self.conn

    def create_tables(self):
        """Create tables."""
        meta.create_all(self.engine)

    def execute_queued_items(self):
        """Execute queued items for each table."""
        if not self.to_insert and not self.to_delete and not self.to_update:
            return
        else:
            with self.connect() as conn:
                with conn.begin() as trans:
                    try:
                        for table_name in meta.tables:
                            if table_name in self.to_delete:
                                for stmt in self.to_delete[table_name]:
                                    self._execute_stmt(stmt, [], conn)
                            if table_name in self.to_insert:
                                # TODO: old code computed executemany for inserts
                                for stmt, args in self.to_insert.get(table_name):
                                    # TODO: sqlite-specific "insert or replace"!
                                    if self.is_sqlite():
                                        stmt = stmt.prefix_with("OR REPLACE",
                                                                dialect="sqlite")
                                    else:
                                        # postgres upsert
                                        if stmt.table.primary_key:
                                            stmt = stmt.on_conflict_do_update(
                                                constraint=f'{table_name}_pkey',
                                                set_=args
                                            )
                                    self._execute_stmt(stmt, args, conn)
                            if table_name in self.to_update:
                                for stmt, args in self.to_update[table_name]:
                                    self._execute_stmt(stmt, args, conn)
                        trans.commit()
                    except SQLAlchemyError:
                        if not self.is_public:
                            raise
                        self.n_tries += 1
                        if self.is_sqlite():
                            LOG.warning(
                                "%(file)s: write attempt (%(attempt)d) did not "
                                "complete\n" % {"file": self.db_file_name,
                                                "attempt": self.n_tries})
                        else:
                            LOG.warning(
                                "write attempt (%(attempt)d) did not "
                                "complete\n" % {"attempt": self.n_tries})
                        with suppress(SQLAlchemyError):
                            trans.rollback()
                    else:
                        # Clear the queues
                        self.to_delete.clear()
                        self.to_insert.clear()
                        self.to_update.clear()
                        # Report public database retry recovery if necessary
                        if self.n_tries:
                            if self.is_sqlite():
                                LOG.warning(
                                    "%(file)s: recovered after (%(attempt)d)"
                                    "attempt(s)\n" % {"file": self.db_file_name,
                                                      "attempt": self.n_tries})
                            else:
                                LOG.warning(
                                    "recovered after (%(attempt)d)"
                                    "attempt(s)\n" % {"attempt": self.n_tries})
                        self.n_tries = 0

    def _execute_stmt(self, stmt: ValuesBase, stmt_args_list: List[Dict],
                      conn: Connection):
        """Helper for "self.execute_queued_items".

        Execute a statement. If this is the public database, return True on
        success and False on failure. If this is the private database, return
        True on success, and raise on failure.
        """
        try:
            conn.execute(stmt, stmt_args_list)
        except SQLAlchemyError:
            if not self.is_public:
                raise
            if cylc.flow.flags.debug:
                traceback.print_exc()
            if self.is_sqlite():
                err_log = (
                              "cannot execute database statement:\n"
                              "file=%(file)s:\nstmt=%(stmt)s"
                          ) % {"file": self.db_file_name, "stmt": stmt}
            else:
                err_log = "cannot execute database statement:\n" \
                          "stmt=%(stmt)s" % {"stmt": stmt}
            for i, stmt_args in enumerate(stmt_args_list):
                err_log += ("\nstmt_args[%(i)d]=%(stmt_args)s" % {
                    "i": i, "stmt_args": stmt_args})
            LOG.warning(err_log)
            raise

    def pre_select_broadcast_states(self, order=None):
        """Query statement and args formation for select_broadcast_states."""
        s = select([
            broadcast_states.c.point,
            broadcast_states.c.namespace,
            broadcast_states.c.key,
            broadcast_states.c.value
        ])
        if order == "ASC":
            s = s.order_by(
                broadcast_states.c.point.asc(),
                broadcast_states.c.namespace.asc(),
                broadcast_states.c.key.asc()
            )
        with self.connect() as conn:
            return conn.execute(s).fetchall()

    def select_broadcast_states(self, callback, sort=None):
        """Select from broadcast_states.

        Invoke callback(row_idx, row) on each row, where each row contains:
            [point, namespace, key, value]
        """
        broadcast_states_result = self.pre_select_broadcast_states(order=sort)
        for row_idx, row in enumerate(broadcast_states_result):
            callback(row_idx, list(row))

    def select_suite_params(self, callback):
        """Select from suite_params.

        Invoke callback(row_idx, row) on each row, where each row contains:
            [key, value]

        E.g. a row might be ['UTC mode', '1']
        """
        s = select([
            suite_params.c.key,
            suite_params.c.value
        ])
        with self.connect() as conn:
            for row_idx, row in enumerate(conn.execute(s).fetchall()):
                callback(row_idx, list(row))

    def select_suite_params_restart_count(self):
        """Return number of restarts in suite_params table."""
        s = select([
            suite_params.c.value
        ]).where(suite_params.c.key == 'n_restart')
        with self.connect() as conn:
            result = conn.execute(s).fetchone()
            return int(result[0]) if result else 0

    def select_suite_template_vars(self, callback):
        """Select from suite_template_vars.

        Invoke callback(row_idx, row) on each row, where each row contains:
            [key,value]
        """
        s = select([
            suite_template_vars.c.key,
            suite_template_vars.c.value
        ])
        with self.connect() as conn:
            for row_idx, row in enumerate(conn.execute(s).fetchall()):
                callback(row_idx, list(row))

    def select_task_action_timers(self, callback):
        """Select from task_action_timers for restart.

        Invoke callback(row_idx, row) on each row.
        """
        s = select([column for column in task_action_timers.c])
        with self.connect() as conn:
            for row_idx, row in enumerate(conn.execute(s).fetchall()):
                callback(row_idx, list(row))

    def select_task_job(self, cycle, name, submit_num=None):
        """Select items from task_jobs by (cycle, name, submit_num).

        :return: a dict for mapping keys to the column values
        :rtype: dict
        """
        columns = [
            task_jobs.c.is_manual_submit,
            task_jobs.c.try_num,
            task_jobs.c.time_submit,
            task_jobs.c.time_submit_exit,
            task_jobs.c.submit_status,
            task_jobs.c.time_run,
            task_jobs.c.time_run_exit,
            task_jobs.c.run_signal,
            task_jobs.c.run_status,
            task_jobs.c.platform_name,
            task_jobs.c.job_runner_name,
            task_jobs.c.job_id
        ]
        s = select(columns=columns)
        if submit_num in [None, "NN"]:
            s = s.where(
                and_(
                    task_jobs.c.cycle == cycle,
                    task_jobs.c.name == name
                )
            ).order_by(
                task_jobs.c.submit_num
            ).limit(1)
        else:
            s = s.where(
                and_(
                    task_jobs.c.cycle == cycle,
                    task_jobs.c.name == name,
                    task_jobs.c.submit_num == submit_num
                )
            )
        with suppress(Exception):
            with self.connect() as conn:
                for row in conn.execute(s).fetchall():
                    ret = {}
                    for key, value in zip([c.name for c in columns], row):
                        ret[key] = value
                    return ret
        return None

    def select_jobs_for_restart(self, callback):
        """Select from task_pool+task_states+task_jobs for restart.

        Invoke callback(row_idx, row) on each row, where each row contains:
            [cycle, name, status, submit_num, time_submit, time_run,
             time_run_exit, job_runner_name, job_id, platform_name]
        """
        s = Select(
            from_obj=task_jobs,
            columns=[
                task_pool.c.cycle,
                task_pool.c.name,
                task_pool.c.status,
                task_states.c.submit_num,
                task_jobs.c.time_submit,
                task_jobs.c.time_run,
                task_jobs.c.time_run_exit,
                task_jobs.c.job_runner_name,
                task_jobs.c.job_id,
                task_jobs.c.platform_name
            ]
        )

        s.append_from(
            task_jobs.join(
                task_pool,
                onclause=and_(
                    task_jobs.c.cycle == task_pool.c.cycle,
                    task_jobs.c.name == task_pool.c.name
                )
            ).join(
                task_states,
                onclause=and_(
                    task_jobs.c.cycle == task_states.c.cycle,
                    task_jobs.c.name == task_states.c.name,
                    task_jobs.c.submit_num == task_states.c.submit_num
                )
            )
        )
        with self.connect() as conn:
            for row_idx, row in enumerate(conn.execute(s).fetchall()):
                callback(row_idx, list(row))

    def select_task_job_run_times(self, callback):
        """Select run times of succeeded task jobs grouped by task names.

        Invoke callback(row_idx, row) on each row, where each row contains:
            [name, run_times_str]

        where run_times_str is a string containing comma separated list of
        integer run times. This method is used to re-populate elapsed run times
        of each task on restart.
        """
        s = select([
            task_jobs.c.name,
            func.group_concat(
                cast(task_jobs.c.time_run_exit, NUMERIC) -
                cast(task_jobs.c.time_run, NUMERIC)
            )
        ]).select_from(task_jobs)\
            .where(task_jobs.c.run_status == 0)\
            .group_by(task_jobs.c.name)\
            .order_by(task_jobs.c.time_run_exit)
        with self.connect() as conn:
            for row_idx, row in enumerate(conn.execute(s).fetchall()):
                callback(row_idx, list(row))

    def select_task_job_platforms(self):
        """Return the set of platform names from task_jobs table."""
        s = select([
            task_jobs.c.platform_name.distinct()
        ])
        with self.connect() as conn:
            return set(i[0] for i in conn.execute(s).fetchall())

    def select_submit_nums(self, name, point):
        """Select submit_num and flow_label from task_states table.

        Fetch submit number and flow label for spawning task name.point.
        Return:
        {
            flow_label: submit_num,
            ...,
        }

        Args:
            name: task name
            point: task cycle point (str)
        """
        # Ignore bandit false positive: B608: hardcoded_sql_expressions
        # Not an injection, simply putting the table name in the SQL query
        # expression as a string constant local to this module.
        ret = {}
        s = select([
            task_states.c.flow_label,
            task_states.c.submit_num
        ]).where(and_(
            task_states.c.name == name,
            task_states.c.cycle == point
        ))
        with self.connect() as conn:
            for flow_label, submit_num in conn.execute(s).fetchall():
                ret[flow_label] = submit_num
        return ret

    def select_xtriggers_for_restart(self, callback):
        s = select([
            xtriggers.c.signature,
            xtriggers.c.results
        ])
        with self.connect() as conn:
            for row_idx, row in enumerate(conn.execute(s).fetchall()):
                callback(row_idx, list(row))

    def select_abs_outputs_for_restart(self, callback):
        s = select([
            abs_outputs.c.cycle,
            abs_outputs.c.name,
            abs_outputs.c.output
        ])
        with self.connect() as conn:
            for row_idx, row in enumerate(conn.execute(s).fetchall()):
                callback(row_idx, list(row))

    def select_task_pool(self, callback):
        """Select from task_pool.

        Invoke callback(row_idx, row) on each row, where each row contains:
            [cycle, name, status]
        """
        s = select([
            task_pool.c.cycle,
            task_pool.c.name,
            task_pool.c.status,
            task_pool.c.is_held
        ])
        with self.connect() as conn:
            for row_idx, row in enumerate(conn.execute(s).fetchall()):
                callback(row_idx, list(row))

    def select_task_pool_for_restart(self, callback):
        """Select from task_pool+task_states+task_jobs for restart.

        Invoke callback(row_idx, row) on each row, where each row contains:
            [cycle, name, is_late, status, is_held, submit_num,
             try_num, platform_name, time_submit, time_run, timeout, outputs]
        """
        s = Select(
            from_obj=task_pool,
            columns=[
                task_pool.c.cycle,
                task_pool.c.name,
                task_pool.c.flow_label,
                task_late_flags.c.value,
                task_pool.c.status,
                task_pool.c.is_held,
                task_states.c.submit_num,
                task_jobs.c.try_num,
                task_jobs.c.platform_name,
                task_jobs.c.time_submit,
                task_jobs.c.time_run,
                task_timeout_timers.c.timeout,
                task_outputs.c.outputs
            ])

        s.append_from(
            task_pool.join(
                task_states,
                onclause=and_(
                    task_pool.c.cycle == task_states.c.cycle,
                    task_pool.c.name == task_states.c.name,
                    task_pool.c.flow_label == task_states.c.flow_label
                )
            ).join(
                task_late_flags,
                onclause=and_(
                    task_pool.c.cycle == task_late_flags.c.cycle,
                    task_pool.c.name == task_late_flags.c.name
                ),
                isouter=True
            ).join(
                task_jobs,
                onclause=and_(
                    task_pool.c.cycle == task_jobs.c.cycle,
                    task_pool.c.name == task_jobs.c.name,
                    task_states.c.submit_num == task_jobs.c.submit_num
                ),
                isouter=True
            ).join(
                task_timeout_timers,
                onclause=and_(
                    task_pool.c.cycle == task_timeout_timers.c.cycle,
                    task_pool.c.name == task_timeout_timers.c.name
                ),
                isouter=True
            ).join(
                task_outputs,
                onclause=and_(
                    task_pool.c.cycle == task_outputs.c.cycle,
                    task_pool.c.name == task_outputs.c.name
                ),
                isouter=True
            )
        )
        with self.connect() as conn:
            for row_idx, row in enumerate(conn.execute(s).fetchall()):
                callback(row_idx, list(row))

    def select_task_prerequisites(self, cycle, name):
        """Return prerequisites of a task of the given name & cycle point."""
        s = select([
            task_prerequisites.c.prereq_name,
            task_prerequisites.c.prereq_cycle,
            task_prerequisites.c.prereq_output,
            task_prerequisites.c.satisfied
        ]).where(
            and_(task_prerequisites.c.cycle == cycle,
                 task_prerequisites.c.name == name)
        )
        with self.connect() as conn:
            return conn.execute(s).fetchall()

    def select_task_times(self):
        """Select submit/start/stop times to compute job timings.

        To make data interpretation easier, choose the most recent succeeded
        task to sample timings from.
        """
        s = select([
            task_jobs.c.name,
            task_jobs.c.cycle,
            task_jobs.c.platform_name,
            task_jobs.c.job_runner_name,
            task_jobs.c.time_submit,
            task_jobs.c.time_run,
            task_jobs.c.time_run_exit
        ]).where(task_jobs.c.run_status == 0)
        columns = (
            'name', 'cycle', 'platform_name', 'job_runner_name',
            'time_submit', 'time_run', 'time_run_exit'
        )
        with self.connect() as conn:
            return columns, [r for r in conn.execute(s).fetchall()]

    def is_sqlite(self) -> bool:
        return self.engine.dialect.name == 'sqlite'

    def vacuum(self):
        """Vacuum to the database if the DB currently used supports it."""
        if self.is_sqlite():
            return self.connect().execute("VACUUM")

    def remove_columns(self, table, to_drop):
        t = meta.tables[table]  # type: Table

        # get list of columns to keep
        columns = t.columns  # type: List[Column]
        new_cols = [
            c.name
            for c in columns
            if c.name not in to_drop
        ]

        # copy table
        with self.connect() as conn:
            with conn.begin() as trans:
                # copy table
                conn.execute(
                    rf'''
                        CREATE TABLE {table}_new AS
                        SELECT {', '.join(new_cols)}
                        FROM {table}
                    '''
                )

                # remove original
                conn.execute(
                    rf'''
                        DROP TABLE {table}
                    '''
                )

                # copy table
                conn.execute(
                    rf'''
                        CREATE TABLE {table} AS
                        SELECT {', '.join(new_cols)}
                        FROM {table}_new
                    '''
                )
                # done
                trans.commit()


__all__ = [
    "CylcSuiteDAO",
    "abs_outputs",
    "broadcast_events",
    "broadcast_states",
    "inheritance",
    "suite_params",
    "suite_template_vars",
    "task_jobs",
    "task_events",
    "task_action_timers",
    "task_late_flags",
    "task_outputs",
    "task_pool",
    "task_prerequisites",
    "task_states",
    "task_timeout_timers",
    "xtriggers"
]
