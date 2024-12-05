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
import argparse
import subprocess
from pathlib import Path
from typing import Any, Type

import toml
from marshmallow import Schema, fields, post_load, validate

from .execs import Execs, ExecsSchema, CMD, CMDSchema
from .utils import wexec, FieldPath, log2type, log2list, log
from .dumbdata import SStates, states_to_end, failure_states, SlurmJobInfo


class PollerSchema(Schema):
    execs = fields.Nested(ExecsSchema, missing=Execs())
    jobid = fields.Integer(allow_none=True, missing=None)
    debug = fields.Boolean(default=True, missing=True)
    logto = fields.String(default='file', missing='file', validate=validate.OneOf(log2list))

    tag = fields.Integer(allow_none=True, missing=None)
    every = fields.Integer(missing=5)
    times_criteria = fields.Integer(missing=288)
    cmd = fields.Nested(CMDSchema, allow_none=True, missing=None, read_only=True)
    lockfilename = fields.String(allow_none=True, default="auto", missing=None)

    logfolder = fields.String(allow_none=True, missing=None, load_only=True)
    logfolder_p = FieldPath(default=Path.cwd(), attribute='logfolder', data_key='logfolder', dump_only=True)
    cwd = FieldPath(missing=Path.cwd())

    @post_load
    def make_spoll(self, data, **kwargs):
        return Poller(**data)

class Poller:
    execs: Execs = Execs()
    debug: bool = True
    jobid: int | None = None
    tag: int | None = None
    every: int = 5
    times_criteria: int = 288
    cmd: CMD | None = None
    cwd: Path
    logto: log2type

    __lockfile: Path
    logfolder: Path
    __ok: bool = False
    __allow: bool = False
    __current_state: SStates = SStates.PENDING
    __job: SlurmJobInfo

    def check(self, strict: bool) -> bool:
        logger = log.get_logger()
        if not self.cwd.exists():
            logger.error("Current working directory does not exists")
            return False

        if not self.logfolder.exists():
            self.logfolder.mkdir(parents=True, exist_ok=True)

        if not self.execs.check(strict):
            logger.error("Some executables were not found")
            return False

        if self.cmd is None:
            logger.error("CMD is None")
            return False

        if not self.cmd.check():
            logger.error("CMD misconfigured")
            return False

        if strict:
            if self.jobid is None:
                logger.error("Job ID wasn't specified")

        return True

    def __init__(
        self,
        jobid: int | None = None,
        cmd: CMD | None = None,
        debug: bool = True,
        logto: log2type = 'file',
        tag: int | None = None,
        every: int = 5,
        times_criteria: int = 288,
        logfolder: str | None = None,
        lockfilename: str | None = None,
        cwd: Path = Path.cwd(),
        execs: Execs = Execs()
    ):
        self.jobid = jobid
        self.cmd = cmd
        self.debug = debug
        self.tag = tag
        self.every = every
        self.times_criteria = times_criteria
        self.cwd = cwd.resolve()
        self.execs = execs

        # os.chdir(self.cwd)

        if lockfilename is None or lockfilename == "auto":
            self.__lockfile = self.cwd / (f"{self.tag}.lock" if self.tag is not None else "directory.lock")
        else:
            self.__lockfile = self.cwd / lockfilename

        if logfolder is None:
            self.logfolder = self.cwd
        else:
            self.logfolder = self.cwd / logfolder

        logfile = self.logfolder / self.logfile_name
        log.configure(logto, logfile, True)

    @classmethod
    def from_schema(cls, data: dict[str, Any], immidiate_check: bool = False, strict: bool = False):
        schema = PollerSchema()
        spoll = schema.load(data)
        if not isinstance(spoll, Poller):
            raise ValueError("")
        if immidiate_check:
            if not spoll.check(strict):
                raise RuntimeError("")
        return spoll

    @classmethod
    def set_args(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--debug", action="store_true", help="Debug. Default: False")
        parser.add_argument('--log', choices=['screen', 'file', 'both', 'off'], help="Whether to log to screen, to file or both or off logging. Default: file")
        parser.add_argument("--jobid", action="store", type=int, help="Slurm job ID")
        parser.add_argument("--tag", action="store", type=int, help="Project tag (arbitrary int number) (optional)")
        parser.add_argument("--every", action="store", type=int, help="Perform poll every N-minutes. Default: 5")
        parser.add_argument("--tc", action="store", type=int, help="Criteria to wait a normal state (times of chech). Default: 288 (a day)")
        parser.add_argument("--preload", action="store", type=str, help="CMD to run after")
        parser.add_argument("--executable", action="store", type=str, help="CMD to run after")
        parser.add_argument("--args", action="store", type=str, help="CMD to run after")
        parser.add_argument("--file", action="store", type=str, help="Read configuration from TOML configuration file (cli arguments owerwrite file ones)")
        parser.add_argument("--logfolder", action="store", type=str, help="Folder whether to store logs. Default: cwd/log/slurm")
        parser.add_argument("--cwd", action="store", type=str, help="Current working directory. Default: cwd")

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> "Poller":
        conf: dict[str, Any] = {}
        if args.file:
            with Path(args.file).resolve().open('r') as fp:
                conf = toml.load(fp)

        obj_dict: dict[str, Any] = {}

        if args.cwd is not None: obj_dict["cwd"] = args.cwd

        if args.jobid is not None: obj_dict["jobid"] = args.jobid

        if args.tag is not None: obj_dict["tag"] = args.tag

        if args.log is not None: obj_dict["logto"] = args.log

        if args.executable is not None:
            cmd = CMD(
                preload=args.preload if args.preload is not None else "",
                executable=args.executable,
                args = args.args if args.args is not None else ""
            )
            obj_dict["cmd"] = CMDSchema().dump(cmd)

        if args.every  is not None: obj_dict["every"] = args.every

        if args.tc is not None: obj_dict["times_criteria"] = args.tc

        conf.update(**obj_dict)

        return cls.from_schema(conf)

    def detach_start(self):
        logger = log.get_logger()
        if not self.check(True):
            logger.error("Check not passed")
            return 2

        schema = PollerSchema()
        d = schema.dump(self)
        if not isinstance(d, dict):
            logger.critical("d is not dict")
            return 2

        cf = f"{self.jobid}_poll_conf.toml"
        if self.tag is not None:
            cf = f"{self.tag}" + cf
        wfile = self.logfolder / cf


        with wfile.open('w') as fp:
            toml.dump(d, fp)

        cmd = f"{self.execs.spolld} --file={wfile.as_posix()}"
        cmds = shlex.split(cmd)
        subprocess.Popen(cmds, start_new_session=True)

    @classmethod
    def genconf(cls, write: bool = False, wfolder: Path | None = None):
        logger = log.get_logger()
        p = Poller()
        schema = PollerSchema()
        d = schema.dump(p)
        if not isinstance(d, dict):
            logger.critical("d is not dict")
            raise RuntimeError("A bug")
        if write:
            wfolder = Path.cwd() if wfolder is None else wfolder
            wfile = wfolder / "Sample_poll_configuration.toml"
            with wfile.open('w') as fp:
                toml.dump(d, fp)
            logger.info(f"Sample confguration was written to {wfile.as_posix()}")
        return p

    @property
    def logfile_name(self) -> str:
        logfile_name = f"{self.jobid}_poll.log"
        if self.tag is not None:
            logfile_name = f"{self.tag}_{logfile_name}"
        return logfile_name

    def __enter__(self):
        logger = log.get_logger()
        if not self.check(True):
            raise RuntimeError("Invalud conf")
        if self.__lockfile.exists():
            logger.error(f"Lockfile exists: {self.__lockfile.as_posix()}")
            raise RuntimeError(f"Lockfile exists: {self.__lockfile.as_posix()}")
        self.__lockfile.touch()
        logger.debug("Created lockfile")
        self.__allow = True
        return self

    def __exit__(self, exc_type: Type[Exception], exc_value: Exception, exc_traceback) -> None:
        logger = log.get_logger()
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
        if (self.cwd / "NORESTART").exists():
            logger.info("NORESTART found, not launching cmd")
        else:
            if self.cmd is not None:
                logger.info(f"Launching: {self.cmd}")
                cmds = shlex.split(self.cmd.gen_line())
                subprocess.Popen(cmds, start_new_session=True)
                logger.info("Succesfully launched command.")
            else:
                logger.info("No cmd was specified")

    def parse_sacct_output(self, output: str) -> list[SlurmJobInfo]:
        job_infos = []
        lines = output.strip().split('\n')
        for line in lines:
            parts = line.split()

            if len(parts) >= 8:
                job_id = parts[0]
                job_name = parts[1]
                partition = parts[2]
                user = parts[3]
                account = parts[4]

                try:
                    n_nodes = int(parts[5])
                    state_str = ' '.join(parts[6:-1]).split(" ")[0]
                except ValueError:
                    n_nodes = 0
                    state_str = ' '.join(parts[5:-1])

                state_enum = SStates.from_string(state_str)

                # Split the ExitCode into exit code and signal
                exit_code_str = parts[-1]
                exit_code, signal = map(int, exit_code_str.split(':'))

                job_info = SlurmJobInfo(
                    job_id=job_id,
                    job_name=job_name,
                    partition=partition,
                    user=user,
                    account=account,
                    n_nodes=n_nodes,
                    state=state_enum,
                    exit_code=exit_code,
                    signal=signal
                )
                job_infos.append(job_info)
        return job_infos

    def get_slurm_job_info(self, job_id: int) -> SlurmJobInfo:
        assert job_id > 0
        logger = log.get_logger()
        try:
            cmd =  f"sacct --format=JobID%-15,JobName%-20,Partition%-15,User%-20,Account%-20,NNodes%-10,State%-30,ExitCode%-15 --jobs={job_id} --noheader"
            bout, berr = wexec(cmd)
            job_infos = self.parse_sacct_output(bout)
            return job_infos[0]

        except subprocess.CalledProcessError as e:
            logger.error(f"An error occurred while retrieving job info: {e}")
            logger.exception(e)
            raise

    def perform_check(self) -> None:
        cmd = f"{self.execs.sacct} -j {self.jobid} -n -p -o jobid,state"
        bout, berr = wexec(cmd)
        for line in bout.splitlines():
            if re.match(r"^\d+\|[a-zA-Z]+\|$", line):
                self.state = SStates(line.split("|")[1])
                return
        self.state = SStates.UNKNOWN_STATE

    def ok(self) -> None:
        self.__ok = True

    def start_loop(self):
        logger = log.get_logger()
        if self.__allow:
            return self.__loop()
        else:
            logger.error("Did you entered context?")
            return False

    @property
    def state(self):
        return self.__current_state

    @state.setter
    def state(self, state: SStates):
        self.__current_state = state

    def inform_user(self, message: str):
        try:
            user = os.environ['user']
            bout, berr = wexec('who')
            ttys = [line.split()[1] for line in bout.splitlines() if line.startswith(user)]
            ttys = [f"/dev/{tty}" for tty in ttys]
            for tty in ttys:
                with open(tty, 'w') as term:
                    term.write(f"\n{message}\n")
        except Exception as e:
            pass

    def __loop(self) -> bool:
        logger = log.get_logger()
        last_state = self.state
        last_state_times: int = 0

        logger.info("Started main loop")

        try:
            while True:
                time.sleep(self.every)
                logger.info("Checking job")
                try:
                    self.perform_check()
                except Exception as e:
                    logger.critical("Check failed due to exception:")
                    logger.exception(e)
                    self.inform_user(f"spoll (PID: {os.getpid()}, jobid: {self.jobid}) check failed due to exception, cwd: {self.cwd.as_posix()}")
                    raise
                logger.info(f"Job state: {str(self.state)}")

                if self.state in states_to_end:
                    logger.info(f"Reached end state: {str(self.state)}. Exiting loop")
                    self.ok()
                    self.inform_user(f"spoll (PID: {os.getpid()}, jobid: {self.jobid}) reached end state, cwd: {self.cwd.as_posix()}")
                    return True
                elif self.state in failure_states:
                    logger.error(f"Something went wrong with slurm job. State: {str(self.state)} Exiting...")
                    self.ok()
                    self.inform_user(f"spoll (PID: {os.getpid()}, jobid: {self.jobid}) Something went wrong with slurm job. State: {str(self.state)}. cwd: {self.cwd.as_posix()}")
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
                            self.inform_user(f"spoll (PID: {os.getpid()}, jobid: {self.jobid}) State {self.state} was too long, cwd: {self.cwd.as_posix()}")
                            self.ok()
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


def main() -> int:
    parser = argparse.ArgumentParser(prog="spolld")
    Poller.set_args(parser)
    poller = Poller.from_args(parser.parse_args())
    with poller:
        poller.start_loop()

    logger = log.get_logger()
    logger.info("Exiting.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
