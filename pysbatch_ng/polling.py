#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 02-05-2024 23:40:24

import re
import os
import sys
import time
import toml
import shlex
import logging
import argparse
import subprocess
from enum import Enum
from pathlib import Path
from typing import Union, Dict, Any


from . import config
from .utils import wexec
from . import constants as cs


class SStates(str, Enum):
    BOOT_FAIL = "BOOT_FAIL"
    CANCELLED = "CANCELLED"
    COMPLETED = "COMPLETED"
    CONFIGURING = "CONFIGURING"
    COMPLETING = "COMPLETING"
    DEADLINE = "DEADLINE"
    FAILED = "FAILED"
    NODE_FAIL = "NODE_FAIL"
    OUT_OF_MEMORY = "OUT_OF_MEMORY"
    PENDING = "PENDING"
    PREEMPTED = "PREEMPTED"
    RUNNING = "RUNNING"
    RESV_DEL_HOLD = "RESV_DEL_HOLD"
    REQUEUE_FED = "REQUEUE_FED"
    REQUEUE_HOLD = "REQUEUE_HOLD"
    REQUEUED = "REQUEUED"
    RESIZING = "RESIZING"
    REVOKED = "REVOKED"
    SIGNALING = "SIGNALING"
    SPECIAL_EXIT = "SPECIAL_EXIT"
    STAGE_OUT = "STAGE_OUT"
    STOPPED = "STOPPED"
    SUSPENDED = "SUSPENDED"
    TIMEOUT = "TIMEOUT"
    UNKNOWN_STATE = "UNKNOWN_STATE"


all_states = [
    SStates.BOOT_FAIL,
    SStates.CANCELLED,
    SStates.COMPLETED,
    SStates.CONFIGURING,
    SStates.COMPLETING,
    SStates.DEADLINE,
    SStates.FAILED,
    SStates.NODE_FAIL,
    SStates.OUT_OF_MEMORY,
    SStates.PENDING,
    SStates.PREEMPTED,
    SStates.RUNNING,
    SStates.RESV_DEL_HOLD,
    SStates.REQUEUE_FED,
    SStates.REQUEUE_HOLD,
    SStates.REQUEUED,
    SStates.RESIZING,
    SStates.REVOKED,
    SStates.SIGNALING,
    SStates.SPECIAL_EXIT,
    SStates.STAGE_OUT,
    SStates.STOPPED,
    SStates.SUSPENDED,
    SStates.TIMEOUT,
]


states_str = [
    "BOOT_FAIL",
    "CANCELLED",
    "COMPLETED",
    "CONFIGURING",
    "COMPLETING",
    "DEADLINE",
    "FAILED",
    "NODE_FAIL",
    "OUT_OF_MEMORY",
    "PENDING",
    "PREEMPTED",
    "RUNNING",
    "RESV_DEL_HOLD",
    "REQUEUE_FED",
    "REQUEUE_HOLD",
    "REQUEUED",
    "RESIZING",
    "REVOKED",
    "SIGNALING",
    "SPECIAL_EXIT",
    "STAGE_OUT",
    "STOPPED",
    "SUSPENDED",
    "TIMEOUT",
]


failure_states = [
    SStates.BOOT_FAIL,
    SStates.DEADLINE,
    SStates.NODE_FAIL,
    SStates.OUT_OF_MEMORY,
    SStates.STOPPED,
    SStates.FAILED,
    SStates.CANCELLED,
]


states_to_end = [
    SStates.COMPLETED,
    SStates.TIMEOUT,
]


def perform_check(jobid: int, logger: logging.Logger) -> SStates:
    cmd = f"{cs.execs.sacct} -j {jobid} -n -p -o jobid,state"
    bout = wexec(cmd, logger.getChild("sacct"))
    for line in bout.splitlines():
        if re.match(r"^\d+\|[a-zA-Z]+\|", line):
            return SStates(line.split("|")[1])
    return SStates.UNKNOWN_STATE


def loop(jobid: int, every: int, logger: logging.Logger, times_criteria: int) -> bool:
    state = SStates.PENDING
    last_state = SStates.PENDING
    last_state_times: int = 0

    logger.info("Started main loop")

    try:
        while True:
            time.sleep(every)
            logger.info("Checking job")
            try:
                state = perform_check(jobid, logger.getChild("task_check"))
            except Exception as e:
                logger.critical("Check failed due to exception:")
                logger.exception(e)
                raise
            logger.info(f"Job state: {str(state)}")

            if state in states_to_end:
                logger.info(f"Reached end state: {str(state)}. Exiting loop")
                return True
            elif state in failure_states:
                logger.error(f"Something went wrong with slurm job. State: {str(state)} Exiting...")
                return False
            elif state == SStates.UNKNOWN_STATE:
                logger.error(f"Unknown slurm job state: {str(state)} Exiting...")
                return False
            elif state == SStates.PENDING:
                logger.info("Pending...")
            elif state == SStates.RUNNING:
                last_state = state
                last_state_times = 0
                logger.info("RUNNING")
            else:  # state != SStates.RUNNING:
                if state == last_state:
                    last_state_times += 1
                    if last_state_times > times_criteria:
                        logger.error(f"State {state} was too long (>{times_criteria} times). Exiting...")
                        return False
                    else: logger.info(f"State {state} still for {times_criteria} times")
                else:
                    last_state = state
                    last_state_times = 0
                    logger.warning(f"Strange state {state} encountered")

    except Exception as e:
        logger.critical("Uncaught exception")
        logger.exception(e)
        return False


def main() -> int:
    parser = argparse.ArgumentParser(prog="spolld")  # , formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--debug", action="store_true", help="Debug. Default: False")
    # parser.add_argument("--nofail", action="store_true", help="Attempts to not fail if fail occurs. Default: False")
    parser.add_argument('--log', choices=['screen', 'file', 'both', 'off'], help="Whether to log to screen, to file or both or off logging. Default: file")
    parser.add_argument("--jobid", action="store", type=int, help="Slurm job ID")
    parser.add_argument("--tag", action="store", type=int, help="Project tag (arbitrary int number) (optional)")
    parser.add_argument("--every", action="store", type=int, help="Perform poll every N-minutes. Default: 5")
    parser.add_argument("--tc", action="store", type=int, help="Criteria to wait a normal state (times of chech). Default: 288 (a day)")
    parser.add_argument("--cmd", action="store", type=str, help="CMD to run after")
    parser.add_argument("--file", action="store", type=str, help="Read configuration from TOML configuration file (cli arguments owerwrite file ones)")
    parser.add_argument("--logfolder", action="store", type=str, help="Folder whether to store logs. Default: cwd/log/slurm")
    parser.add_argument("--cwd", action="store", type=str, help="Current working directory. Default: cwd")
    args = parser.parse_args()

    fileconf = False
    if args.file:
        with Path(args.file).resolve().open('r') as fp:
            conf: Dict[str, Any] = toml.load(fp)
            if not config.spoll_check_conf(conf, logging.Logger("checkconf")):
                print("Invalid configuration. Aborting...")
                return 1
            sconf: Dict[str, Any] = conf[cs.fields.spoll]
        fileconf = True

    if args.cwd is None and fileconf and cs.fields.cwd in sconf: os.chdir(Path(sconf[cs.fields.cwd]).resolve())
    if args.cwd: os.chdir(Path(args.cwd).resolve())

    cwd: Path = Path.cwd()

    jobid: int
    if args.jobid: jobid = args.jobid
    elif fileconf and cs.fields.jobid in sconf: jobid = sconf[cs.fields.jobid]
    else: raise RuntimeError("No jobid was specified")

    ptag: Union[int, None] = None
    if args.tag: ptag = args.tag
    elif fileconf and cs.fields.ptag in sconf: ptag = sconf[cs.fields.ptag]

    logfile_name = str(jobid) + "_" + str(round(time.time())) + "_poll.log"
    if ptag: logfile_name = str(ptag) + "_" + logfile_name
    logfolder: Path
    if args.logfolder: logfolder =  cwd / args.logfolder
    elif fileconf and cs.fields.logfolder in sconf: logfolder = Path(sconf[cs.fields.logfolder]).resolve()
    else: logfolder = cwd / cs.folders.log
    if not logfolder.exists(): logfolder.mkdir(parents=True, exist_ok=True)
    logfile: Path = logfolder / logfile_name

    logger = logging.getLogger("spolld")
    loglevel: int = logging.INFO
    if args.debug: loglevel = logging.DEBUG
    elif fileconf and cs.fields.debug in sconf: loglevel = logging.DEBUG
    logger.setLevel(loglevel)

    formatter: logging.Formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s')
    logto: str
    if args.log: logto = args.log
    elif fileconf and cs.fields.logto in sconf: logto = sconf[cs.fields.logto]
    else: logto = 'file'

    if logto == 'file' or logto == 'both':
        FileHandler = logging.FileHandler(logfile)
        FileHandler.setFormatter(formatter)
        FileHandler.setLevel(logging.DEBUG)
        logger.addHandler(FileHandler)
    if logto == 'screen' or logto == 'both':
        soutHandler = logging.StreamHandler(stream=sys.stdout)
        soutHandler.setLevel(logging.DEBUG)
        soutHandler.setFormatter(formatter)
        logger.addHandler(soutHandler)
        serrHandler = logging.StreamHandler(stream=sys.stderr)
        serrHandler.setFormatter(formatter)
        serrHandler.setLevel(logging.WARNING)
        logger.addHandler(serrHandler)

    if logto == 'off': logger.propagate = False

    cmd: Union[str, None] = None
    if args.cmd: cmd = args.cmd
    elif fileconf and cs.fields.cmd in sconf: cmd = sconf[cs.fields.cmd]

    every: int
    if args.every: every = args.every
    elif fileconf and cs.fields.every in sconf: every = sconf[cs.fields.every]

    times_criteria: int
    if args.tc: times_criteria = args.tc
    elif fileconf and cs.fields.times_criteria in sconf: times_criteria = sconf[cs.fields.times_criteria]

    # if fileconf and cs.fields.sbatch in conf:
    #     if cs.fields.execs in conf[cs.fields.sbatch]:
    #         config.cexecs(conf[cs.fields.sbatch], logger.getChild("execs_configuration"))  # already done in check_conf

    logger.debug(f"sacct executable: {cs.execs.sacct}")

    lockfile: Path
    if ptag: lockfile = cwd / f"{ptag}.lock"
    else: lockfile = cwd / cs.files.lock

    if lockfile.exists():
        logger.error(f"Lockfile exists: {lockfile.as_posix()}")
        raise Exception(f"Lockfile exists: {lockfile.as_posix()}")
    lockfile.touch()
    logger.debug("Created lockfile")
    logger.debug("Starting loop")

    fl = loop(jobid, every * 60, logger, times_criteria)
    logger.debug("Loop end, deleting lock")

    if fl:
        logger.debug("Loop ok, checking fo cmd")
        if (cwd / "NORESTART").exists(): logger.info("NORESTART found, not launching cmd")
        else:
            if cmd:
                logger.info(f"Launching: {cmd}")
                cmds = shlex.split(cmd)
                subprocess.Popen(cmds, start_new_session=True)
                logger.info("Succesfully launched command.")
            else: logger.debug("No cmd was specified")
        lockfile.unlink()
    else: logger.debug("Loop not ok, not checking for cmd")


    logger.info("Exiting.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
