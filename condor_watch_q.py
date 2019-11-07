#!/usr/bin/env python

# Copyright 2019 HTCondor Team, Computer Sciences Department,
# University of Wisconsin-Madison, WI.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function

import argparse
import collections
import getpass
import itertools
import sys
import os
import re
import textwrap
import time
import enum
import datetime

import htcondor
import classad

PROJECTION = ["ClusterId", "Owner", "UserLog"]


def parse_args():
    parser = argparse.ArgumentParser(
        prog="condor_watch_q",
        description=textwrap.dedent(
            """
            Track the status of HTCondor jobs without repeatedly querying the 
            Schedd.
            
            If no users, cluster ids, or event logs are passed, condor_watch_q will 
            default to tracking all of the current user's jobs.
            """
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "-debug", action="store_true", help="Turn on HTCondor debug printing."
    )

    # select which jobs to track
    parser.add_argument("-users", nargs="+", help="Which users to track.")
    parser.add_argument("-clusters", nargs="+", help="Which cluster IDs to track.")
    parser.add_argument("-files", nargs="+", help="Which event logs to track.")

    # select when (if) to exit
    parser.add_argument("-exit", nargs=2, action="append")

    args = parser.parse_args()

    if all((_ is None for _ in (args.users, args.clusters, args.files))):
        args.users = [getpass.getuser()]

    if args.exit is None:
        args.exit = []

    return args


def cli():
    args = parse_args()

    if args.debug:
        htcondor.enable_debug()

    cluster_ids, event_logs = find_job_event_logs(args.users, args.clusters, args.files)
    if cluster_ids is not None and len(cluster_ids) == 0:
        print("No jobs found")
        sys.exit(0)

    state = JobStateTracker(event_logs)

    exit_checks = []
    for grouper, checker in args.exit:
        disp = "{} {}".format(grouper, checker)
        exit_grouper = {"all": all, "any": any, "none": lambda _: not any(_)}[
            grouper.lower()
        ]
        exit_check = {
            "active": lambda s: s in ACTIVE_STATES,
            "done": lambda s: s is JobStatus.COMPLETED,
            "held": lambda s: s is JobStatus.HELD,
        }[checker.lower()]
        exit_checks.append((exit_grouper, exit_check, disp))

    try:
        msg = None
        while True:
            reading = "Reading new events..."
            print(reading, end="")
            sys.stdout.flush()
            state.process_events()
            print("\r" + (len(reading) * " ") + "\r" + "\033[1A")
            sys.stdout.flush()

            if msg is not None:
                prev_lines = list(msg.splitlines())
                prev_len_lines = [len(line) for line in prev_lines]

                move = "\033[{}A\r".format(len(prev_len_lines))
                clear = "\n".join(" " * len(line) for line in prev_lines) + "\n"
                sys.stdout.write(move + clear + move)

            now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            msg = state.table_by_event_log().splitlines()
            msg += ["", "Updated at {}".format(now)]
            msg = "\n".join(msg)

            print(msg)

            for grouper, checker, disp in exit_checks:
                if grouper((checker(s) for _, s in state.job_states())):
                    print('Exiting because of condition "{}" at {}'.format(disp, now))
                    sys.exit(0)

            time.sleep(2)
    except KeyboardInterrupt:
        sys.exit(0)


def find_job_event_logs(users, cluster_ids, files):
    if users is None:
        users = []
    if cluster_ids is None:
        cluster_ids = []
    if files is None:
        files = []

    constraint = " || ".join(
        itertools.chain(
            ("Owner == {}".format(classad.quote(u)) for u in users),
            ("ClusterId == {}".format(cid) for cid in cluster_ids),
        )
    )
    if constraint != "":
        ads = query(constraint=constraint, projection=PROJECTION)
    else:
        ads = []

    cluster_ids = set()
    event_logs = set()
    for ad in ads:
        cluster_id = ad["ClusterId"]
        cluster_ids.add(cluster_id)
        try:
            event_logs.add(os.path.abspath(ad["UserLog"]))
        except KeyError:
            print(
                "Warning: cluster {} does not have a job event log".format(cluster_id)
            )

    for file in files:
        event_logs.add(os.path.abspath(file))

    return cluster_ids, event_logs


def query(constraint, projection=None):
    schedd = htcondor.Schedd()
    ads = schedd.query(constraint, projection)
    return ads


TOTAL = "TOTAL"
ACTIVE_JOBS = "ACTIVE_JOBS"
EVENT_LOG = "EVENT_LOG"


class JobStateTracker:
    def __init__(self, event_log_paths):
        self.event_readers = {
            p: htcondor.JobEventLog(p).events(0) for p in event_log_paths
        }
        self.state = collections.defaultdict(lambda: collections.defaultdict(dict))

    def job_states(self):
        for event_log, clusters in self.state.items():
            for cluster_id, procs in clusters.items():
                for proc_id, job_status in procs.items():
                    yield ((cluster_id, proc_id), job_status)

    def process_events(self):
        for event_log_path, events in self.event_readers.items():
            for event in events:
                new_status = JOB_EVENT_STATUS_TRANSITIONS.get(event.type, None)
                if new_status is not None:
                    self.state[event_log_path][event.cluster][event.proc] = new_status

    def table_by_event_log(self):
        headers = [EVENT_LOG] + list(JobStatus.ordered()) + [TOTAL, ACTIVE_JOBS]
        rows = []
        event_log_names = normalize_paths(self.state.keys())
        for event_log_path, state in sorted(self.state.items()):
            # todo: total should be derived from somewhere else, for late materialization
            d = {js: 0 for js in JobStatus}
            live_job_ids = []
            for cluster_id, proc_statuses in state.items():
                for proc_status in proc_statuses.values():
                    d[proc_status] += 1

                live_proc_ids = [
                    p
                    for p, status in proc_statuses.items()
                    if status not in (JobStatus.COMPLETED, JobStatus.REMOVED)
                ]

                if len(live_proc_ids) > 0:
                    if len(live_proc_ids) == 1:
                        x = "{}.{}".format(cluster_id, live_proc_ids[0])
                    else:
                        x = "{}.{}-{}".format(
                            cluster_id, min(live_proc_ids), max(live_proc_ids)
                        )
                    live_job_ids.append(x)

            d[TOTAL] = sum(d.values())
            d[EVENT_LOG] = event_log_names[event_log_path]
            d[ACTIVE_JOBS] = ", ".join(live_job_ids)
            rows.append(d)

        dont_include = set()
        for h in headers:
            if all((row[h] == 0 for row in rows)):
                dont_include.add(h)
        dont_include -= ALWAYS_INCLUDE
        headers = [h for h in headers if h not in dont_include]
        for d in dont_include:
            for row in rows:
                row.pop(d)

        return table(headers=headers, rows=rows, alignment=TABLE_ALIGNMENT)


def normalize_paths(paths):
    d = {}
    for path in paths:
        abs = os.path.abspath(path)
        home_dir = os.path.expanduser("~")
        cwd = os.getcwd()
        if not abs.startswith(home_dir):
            d[path] = abs
            continue

        relative_to_user_home = os.path.relpath(path, home_dir)
        if cwd == home_dir:
            d[path] = os.path.join("~", relative_to_user_home)
            continue

        relative_to_cwd = os.path.relpath(path, cwd)
        normalized = (
            os.path.join(".", relative_to_cwd)
            if not relative_to_cwd.startswith("../")
            else os.path.join("~", relative_to_user_home)
        )
        d[path] = normalized

    return d


class JobStatus(enum.Enum):
    REMOVED = "REMOVED"
    HELD = "HELD"
    IDLE = "IDLE"
    RUNNING = "RUN"
    COMPLETED = "DONE"
    TRANSFERRING_OUTPUT = "TRANSFERRING_OUTPUT"
    SUSPENDED = "SUSPENDED"

    def __str__(self):
        return self.value

    @classmethod
    def ordered(cls):
        return (
            cls.REMOVED,
            cls.HELD,
            cls.SUSPENDED,
            cls.IDLE,
            cls.RUNNING,
            cls.TRANSFERRING_OUTPUT,
            cls.COMPLETED,
        )


ACTIVE_STATES = {
    JobStatus.IDLE,
    JobStatus.RUNNING,
    JobStatus.TRANSFERRING_OUTPUT,
    JobStatus.HELD,
    JobStatus.SUSPENDED,
}

ALWAYS_INCLUDE = {
    JobStatus.IDLE,
    JobStatus.RUNNING,
    JobStatus.COMPLETED,
    EVENT_LOG,
    TOTAL,
}

TABLE_ALIGNMENT = {EVENT_LOG: "ljust", TOTAL: "rjust", ACTIVE_JOBS: "ljust"}
for k in JobStatus:
    TABLE_ALIGNMENT[k] = "rjust"


JOB_EVENT_STATUS_TRANSITIONS = {
    htcondor.JobEventType.SUBMIT: JobStatus.IDLE,
    htcondor.JobEventType.JOB_EVICTED: JobStatus.IDLE,
    htcondor.JobEventType.JOB_UNSUSPENDED: JobStatus.IDLE,
    htcondor.JobEventType.JOB_RELEASED: JobStatus.IDLE,
    htcondor.JobEventType.SHADOW_EXCEPTION: JobStatus.IDLE,
    htcondor.JobEventType.JOB_RECONNECT_FAILED: JobStatus.IDLE,
    htcondor.JobEventType.JOB_TERMINATED: JobStatus.COMPLETED,
    htcondor.JobEventType.EXECUTE: JobStatus.RUNNING,
    htcondor.JobEventType.JOB_HELD: JobStatus.HELD,
    htcondor.JobEventType.JOB_SUSPENDED: JobStatus.SUSPENDED,
    htcondor.JobEventType.JOB_ABORTED: JobStatus.REMOVED,
}


def table(headers, rows, fill="", header_fmt=None, row_fmt=None, alignment=None):
    if header_fmt is None:
        header_fmt = lambda _: _
    if row_fmt is None:
        row_fmt = lambda _: _
    if alignment is None:
        alignment = {}

    headers = tuple(headers)
    lengths = [len(str(h)) for h in headers]

    align_methods = [alignment.get(h, "center") for h in headers]

    processed_rows = []
    for row in rows:
        processed_rows.append([str(row.get(key, fill)) for key in headers])

    for row in processed_rows:
        lengths = [max(curr, len(entry)) for curr, entry in zip(lengths, row)]

    header = header_fmt(
        "  ".join(
            getattr(str(h), a)(l) for h, l, a in zip(headers, lengths, align_methods)
        ).rstrip()
    )

    lines = [
        row_fmt(
            "  ".join(getattr(f, a)(l) for f, l, a in zip(row, lengths, align_methods))
        )
        for row in processed_rows
    ]

    output = "\n".join([header] + lines)

    return output


if __name__ == "__main__":
    import os
    import random

    htcondor.enable_debug()

    schedd = htcondor.Schedd()

    for x in range(1, 6):
        log = os.path.join(os.getcwd(), "{}.log".format(x))

        if x == 2:
            log = os.path.join(
                os.getcwd(), "deeply", "nested", "path", "to", "{}.log".format(x)
            )

        try:
            os.makedirs(os.path.dirname(log))
        except OSError:
            pass

        sub = htcondor.Submit(
            dict(
                executable="/bin/sleep",
                arguments="1",
                hold=False,
                log=log,
                transfer_input_files="nope" if x == 4 else "",
            )
        )
        with schedd.transaction() as txn:
            sub.queue(txn, random.randint(1, 5))
        with schedd.transaction() as txn:
            sub.queue(txn, random.randint(1, 5))

    os.system("condor_q")
    print()

    os.chdir(os.path.join(os.getcwd(), "deeply", "nested"))
    # os.chdir(os.path.expanduser("~"))
    print("Running from", os.getcwd())
    print("-" * 40)
    print()

    cli()
