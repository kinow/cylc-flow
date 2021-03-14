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

import sqlite3
import unittest

from unittest import mock
from tempfile import NamedTemporaryFile

from cylc.flow.rundb import *


GLOBAL_CONFIG = """
[platforms]
    [[desktop[0-9]{2}|laptop[0-9]{2}]]
        # hosts = platform name (default)
        # Note: "desktop01" and "desktop02" are both valid and distinct
        # platforms
    [[sugar]]
        hosts = localhost
        job runner = slurm
    [[hpc]]
        hosts = hpcl1, hpcl2
        retrieve job logs = True
        job runner = pbs
    [[hpcl1-bg]]
        hosts = hpcl1
        retrieve job logs = True
        job runner = background
    [[hpcl2-bg]]
        hosts = hpcl2
        retrieve job logs = True
        job runner = background
"""


class TestRunDb(unittest.TestCase):

    def setUp(self):
        self.mocked_connection = mock.Mock()
        self.mocked_connection_cmgr = mock.Mock()
        self.mocked_connection_cmgr.__enter__ = mock.Mock(return_value=(
            self.mocked_connection))
        self.mocked_connection_cmgr.__exit__ = mock.Mock(return_value=None)
        self.dao = CylcSuiteDAO('')
        self.dao.connect = mock.Mock()
        self.dao.connect.return_value = self.mocked_connection_cmgr

    get_select_task_job = [
        ["cycle", "name", "NN"],
        ["cycle", "name", None],
        ["cycle", "name", "02"],
    ]

    def test_select_task_job(self):
        """Test the rundb CylcSuiteDAO select_task_job method"""
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
        expected_values = [[2 for _ in columns]]

        mocked_execute = mock.Mock()
        mocked_execute.fetchall.return_value = expected_values
        self.mocked_connection.execute.return_value = mocked_execute

        # parameterized test
        for cycle, name, submit_num in self.get_select_task_job:
            values = self.dao.select_task_job(cycle, name, submit_num)

            for column in columns:
                self.assertEqual(2, values[column.name])

    def test_select_task_job_sqlite_error(self):
        """Test that when the rundb CylcSuiteDAO select_task_job method raises
        a SQLite exception, the method returns None"""

        self.mocked_connection.execute.side_effect = sqlite3.DatabaseError

        r = self.dao.select_task_job("it'll", "raise", "an error!")
        self.assertIsNone(r)


def test_remove_columns():
    """Test workaround for dropping columns in sqlite3."""
    with NamedTemporaryFile() as nf:
        dao = CylcSuiteDAO(nf.name, is_public=False)
        dao.remove_columns('broadcast_states', ['namespace', 'value'])
        with dao.connect() as conn:
            data = conn.execute('SELECT * from broadcast_states').keys()
            assert data == ['point', 'key']


if __name__ == '__main__':
    unittest.main()
