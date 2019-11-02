#!/usr/bin/env python3
# THIS FILE IS PART OF THE CYLC SUITE ENGINE.
# Copyright (C) 2008-2019 NIWA & British Crown (Met Office) & Contributors.
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
"""cylc [control] jobs-poll JOB-LOG-ROOT [JOB-LOG-DIR ...]

(This command is for internal use. Users should use "cylc poll".) Read job
status files to obtain the statuses of the jobs. If necessary, Invoke the
relevant batch system commands to ask the batch systems for more statuses.

"""
from cylc.flow.option_parsers import CylcOptionParser as COP
from cylc.flow.remote import remrun
from cylc.flow.terminal import cli_function


def get_option_parser():
    parser = COP(
        __doc__,
        argdoc=[
            ("JOB-LOG-ROOT", "The log/job sub-directory for the suite"),
            ("[JOB-LOG-DIR ...]", "A point/name/submit_num sub-directory"),
        ],
    )

    return parser


@cli_function(get_option_parser)
def main(parser, options, job_log_root, *job_log_dirs):
    """CLI main."""
    if not remrun():
        from cylc.flow.batch_sys_manager import BatchSysManager

        BatchSysManager().jobs_poll(job_log_root, job_log_dirs)


if __name__ == "__main__":
    main()
