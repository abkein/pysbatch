#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (c) 2023-2024 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 26-10-2024 09:32:44

import os
import re
import sys
import time
import shlex
import inspect
import getpass
import argparse
import subprocess
from enum import StrEnum
from pathlib import Path
from typing import Any, Type, Self

import toml
from marshmallow import Schema, fields, post_load, validate

from .platform import Platform, PlatformSchema
from .utils import shell, FieldPath, log2type, log2list, logger, parse_sacct_output, configure_logger
from .dumbdata import SStates, states_to_end, failure_states, SlurmJobInfo


class PollerSchema(Schema):
    execs = fields.Nested(PlatformSchema, missing=None)
    jobid = fields.Integer(allow_none=True, missing=None)
    debug = fields.Boolean(default=True, missing=True)
    logto = fields.String(default='file', missing='file', validate=validate.OneOf(log2list))

    tag = fields.Integer(allow_none=True, missing=None)
    every = fields.Integer(missing=5)
    times_criteria = fields.Integer(missing=288)
    cmd = fields.String(allow_none=True, missing=None)
    lockfilename = fields.String(allow_none=True, default="auto", missing=None)

    logfolder = fields.String(allow_none=True, missing=None, load_only=True)
    logfolder_p = FieldPath(default=Path.cwd(), attribute='logfolder', data_key='logfolder', dump_only=True)
    cwd = FieldPath(missing=Path.cwd())

    @post_load
    def make_spoll(self, data, **kwargs):
        return Poller(**data)


class Actions(StrEnum):
    start = "start"
    detach = "detach"
    genconf = "genconf"


class Poller:
    jobid: int | None = None
    tag: int | None = None
    every: int = 5
    times_criteria: int = 288
    cmd: str | None = None

    __platform: Platform
    __cwd: Path
    __lockfile: Path
    __ok: bool = False
    __allow: bool = False
    __current_state: SStates = SStates.PENDING
    __job: SlurmJobInfo

    __action: Actions | None = None

    def __init__(
        self,
        jobid: int | None = None,
        cmd: str | None = None,
        debug: bool = True,
        logto: log2type = 'both',
        tag: int | None = None,
        every: int = 5,
        times_criteria: int = 288,
        logfolder: str | None = None,
        lockfilename: str | None = None,
        cwd: Path | None = None,
        platform: Platform | None = None
    ):
        self.jobid = jobid
        self.cmd = cmd
        self.tag = tag
        self.every = every
        self.times_criteria = times_criteria
        self.__cwd = cwd.resolve() if cwd is not None else Path.cwd()
        self.__platform = platform if platform is not None else Platform()

        if not self.__cwd.exists():
            raise FileNotFoundError("Current working directory does not exists")
        os.chdir(self.__cwd)

        if lockfilename is None or lockfilename == "auto":
            self.__lockfile = self.__cwd / (f"{self.tag}.lock" if self.tag is not None else "directory.lock")
        else:
            self.__lockfile = self.__cwd / lockfilename

        if logfolder is None:
            self.logfolder = self.__cwd
        else:
            logfolder_path = Path(logfolder)
            if logfolder_path.is_absolute():
                self.logfolder = logfolder_path
            else:
                self.logfolder = self.__cwd / logfolder

        if not self.logfolder.exists():
            self.logfolder.mkdir(parents=True, exist_ok=True)

        logfile_name = f"{self.jobid}_poll.log"
        if self.tag is not None:
            logfile_name = f"{self.tag}_{logfile_name}"
        logfile = self.logfolder / logfile_name
        configure_logger(logto, logfile, debug)

    def check(self) -> bool:
        if self.jobid is None:
            logger.error("No jobid specified")
            return False

        if self.__lockfile.exists():
            logger.error(f"Lockfile exists: {self.__lockfile.as_posix()}")
            return False

        return True

    def detach_start(self):
        d = PollerSchema().dump(self)
        if not isinstance(d, dict):
            raise ValueError(f"Unable to dump {self.__class__.__name__} to data (dev bug:{__file__}:{inspect.currentframe().f_code.co_name})")  # type: ignore

        try:
            pid = os.fork()
            if pid == 0:
                with self:
                    self.start_loop()
            else:
                logger.info("Forked succesfully. Exiting...")
        except OSError as e:
            logger.critical("Something went wrong while trying to fork...")
            logger.exception(e)
            raise
        except RuntimeError as e:
            logger.critical("Something went wrong while trying to fork...")
            logger.exception(e)
            raise
        except SystemError as e:
            logger.critical("Something went wrong while trying to fork...")
            logger.exception(e)
            raise
        # cf = f"{self.jobid}_poll_conf.toml"
        # if self.tag is not None:
        #     cf = f"{self.tag}" + cf
        # wfile = self.logfolder / cf

        # with wfile.open('w') as fp:
        #     toml.dump(d, fp)

        # cmd = f"{self.execs.spolld} --file={wfile.as_posix()}"
        # cmds = shlex.split(cmd)
        # subprocess.Popen(cmds, start_new_session=True, env=os.environ.copy())

    def __enter__(self) -> Self:
        if not self.check():
            logger.critical("Self-check is not passed at enter, falling down")
            raise RuntimeError("Self-check is not passed at enter, falling down")
        self.__lockfile.touch()
        logger.debug("Created lockfile")
        self.__allow = True
        return self

    def __exit__(self, exc_type: Type[Exception], exc_value: Exception, exc_traceback) -> None:
        self.__allow = False
        if not self.__ok:
            logger.debug("Loop was not ok, not deleting lock, not launching cmd")
            return

        logger.debug("Loop ok, deleting lock")
        self.__lockfile.unlink(missing_ok=True)
        if self.state in failure_states:
            logger.warning("Job seems to be failed. Not launching cmd")
            return
        if self.state == SStates.COMPLETED:
            logger.warning("Job seems to be completed. Not launching cmd")
            return
        logger.debug("Checking for cmd")
        if (self.__cwd / "NORESTART").exists():
            logger.info("NORESTART found, not launching cmd")
        else:
            if self.cmd is not None:
                logger.info(f"Launching: {self.cmd}")
                cmds = shlex.split(self.cmd)
                proc = subprocess.run(cmds, capture_output=True, check=True, env=os.environ.copy())
                if proc.returncode == 0: logger.info("Succesfully launched command.")
                else:
                    logger.warning(f"Command returned non-zero exit code: {proc.returncode}.")
                    logger.warning("Command stdout:")
                    logger.warning(proc.stdout.decode().strip())
                    logger.warning("Command stderr:")
                    logger.warning(proc.stderr.decode().strip())
            else:
                logger.info("No cmd was specified")

    def get_slurm_job_info(self, job_id: int) -> SlurmJobInfo:
        assert job_id > 0
        try:
            cmds =  [
                f"{self.__platform.execs.sacct}",
                "--format=JobID%-15,JobName%-20,Partition%-15,User%-20,Account%-20,NNodes%-10,State%-30,ExitCode%-15",
               f"--jobs={job_id}",
                "--noheader"
            ]
            bout, berr = shell.exec(cmds)
            job_infos = parse_sacct_output(bout)
            return job_infos[0]
        except subprocess.CalledProcessError as e:
            logger.error(f"An error occurred while retrieving job info: {e}")
            logger.exception(e)
            raise

    def perform_check(self) -> None:
        cmds = [f"{self.__platform.execs.sacct}", f"-j {self.jobid}", "-n", "-p", "-o", "jobid,state"]
        bout, berr = shell.exec(cmds)
        for line in bout.splitlines():
            if re.match(r"^\d+\|[a-zA-Z]+\|$", line):
                self.state = SStates(line.split("|")[1])
                return
        self.state = SStates.UNKNOWN_STATE

    def start_loop(self) -> bool:
        if self.__allow: return self.__loop()
        else:
            logger.error("Did you entered context?")
            return False

    @property
    def state(self) -> SStates: return self.__current_state

    @state.setter
    def state(self, state: SStates) -> None: self.__current_state = state

    def inform_user(self, message: str) -> None:
        try:
            try: user = getpass.getuser()
            except OSError as e:
                logger.error("Unable to get user name, due to following error:")
                logger.exception(e)
                raise
            bout, berr = shell.exec(['who'])
            ttys = [line.split()[1] for line in bout.splitlines() if line.startswith(user)]
            ttys = [f"/dev/{tty}" for tty in ttys]
            for tty in ttys:
                with open(tty, 'w') as term: term.write(f"\n{message}\n")
        except Exception as e: logger.error("Unable to notify user")

    def __loop(self) -> bool:
        last_state = self.state
        last_state_times: int = 0

        logger.info("Started main loop")

        try:
            while True:
                time.sleep(self.every)
                logger.info("Checking job")
                try: self.perform_check()
                except Exception as e:
                    logger.critical("Check failed due to exception:")
                    logger.exception(e)
                    self.inform_user(f"spoll (PID: {os.getpid()}, jobid: {self.jobid}) check failed due to exception, cwd: {self.__cwd.as_posix()}")
                    raise
                logger.info(f"Job state: {str(self.state)}")

                if self.state in states_to_end:
                    logger.info(f"Reached end state: {str(self.state)}. Exiting loop")
                    self.__ok = True
                    self.inform_user(f"spoll (PID: {os.getpid()}, jobid: {self.jobid}) reached end state, cwd: {self.__cwd.as_posix()}")
                    return True
                elif self.state in failure_states:
                    logger.error(f"Something went wrong with slurm job. State: {str(self.state)} Exiting...")
                    self.__ok = True
                    self.inform_user(f"spoll (PID: {os.getpid()}, jobid: {self.jobid}) Something went wrong with slurm job. State: {str(self.state)}. cwd: {self.__cwd.as_posix()}")
                    return False
                # elif self.state == SStates.UNKNOWN_STATE:
                #     logger.error(f"Unknown slurm job state. Exiting...")
                #     self.ok()
                #     return False
                elif self.state == SStates.PENDING:
                    logger.info("Pending...")
                elif self.state == SStates.RUNNING:
                    last_state = self.state
                    last_state_times = 0
                    logger.info("RUNNING")
                else:  # state != SStates.RUNNING:
                    if self.state == last_state:
                        last_state_times += 1
                        if last_state_times > self.times_criteria:
                            logger.error(f"State {self.state} was too long (>{self.times_criteria} times). Exiting...")
                            self.inform_user(f"spoll (PID: {os.getpid()}, jobid: {self.jobid}) State {self.state} was too long, cwd: {self.__cwd.as_posix()}")
                            self.__ok = True
                            return False
                        else: logger.info(f"State {self.state} still for {self.times_criteria} times")
                    else:
                        last_state = self.state
                        last_state_times = 0
                        logger.warning(f"Strange state {self.state} encountered")

        except Exception as e:
            logger.critical("Uncaught exception")
            logger.exception(e)
            return False

    def set_action(self, action: Actions) -> None: self.__action = action

    def decide(self) -> bool:
        match self.__action:
            case Actions.start:   self.start_loop()
            case Actions.detach:  self.detach_start()
            case Actions.genconf: self.genconf()
            case _: return False
        return True

    @classmethod
    def from_schema(cls, data: dict[str, Any]):
        spoll = PollerSchema().load(data)
        if not isinstance(spoll, Poller): raise ValueError(f"Unable to load {cls.__name__} from data (dev bug:{__file__}:{inspect.currentframe().f_code.co_name})")  # type: ignore
        return spoll

    def dump_schema(self) -> dict[str, Any]:
        data = PollerSchema().dump(self)
        if not isinstance(data, dict): raise ValueError(f"Unable to dump {self.__class__.__name__} to data (dev bug:{__file__}:{inspect.currentframe().f_code.co_name})")  # type: ignore
        return data

    @classmethod
    def from_args(cls) -> "Poller":
        parser = argparse.ArgumentParser(prog="spolld")
        parser.add_argument('action', choices=list(Actions), type=str, help=
                            """Action to perform:
                            start: start
                            detach: start in the background
                            genconf: Generate sample configuration file and exit (Default file: ./spoll_sample_conf.toml), see also --file option"
                            """)
        parser.add_argument("--debug", action="store_true", help="Debug. Default: False")
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

        conf: dict[str, Any] = {}
        if args.file:
            with Path(args.file).resolve().open('r') as fp: conf = toml.load(fp)

        obj_dict: dict[str, Any] = {}

        if args.cwd   is not None: obj_dict["cwd"]            = args.cwd
        if args.jobid is not None: obj_dict["jobid"]          = args.jobid
        if args.tag   is not None: obj_dict["tag"]            = args.tag
        if args.log   is not None: obj_dict["logto"]          = args.log
        if args.cmd   is not None: obj_dict["cmd"]            = args.cmd
        if args.every is not None: obj_dict["every"]          = args.every
        if args.tc    is not None: obj_dict["times_criteria"] = args.tc

        conf.update(**obj_dict)

        poller = cls.from_schema(conf)
        poller.set_action(Actions(args.action))
        return poller

    def genconf(self) -> None: print(toml.dumps(self.dump_schema()))


def main() -> int:
    Poller.from_args().decide()
    return 0


if __name__ == "__main__": sys.exit(main())
