#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (c) 2023-2024 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 26-10-2024 09:32:44

from enum import StrEnum
import os
import sys
import json
import toml
import shlex
import shutil
import inspect
import logging
import itertools
import subprocess
from pathlib import Path
import paramiko
from paramiko import SSHClient
from typing import Literal, Any, Type

from marshmallow import fields

from .dumbdata import SStates, SlurmJobInfo


def minilog(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.handlers.clear()
    logger.setLevel(logging.DEBUG)
    formatter: logging.Formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s')
    soutHandler = logging.StreamHandler(stream=sys.stdout)
    soutHandler.setLevel(logging.DEBUG)
    soutHandler.setFormatter(formatter)
    logger.addHandler(soutHandler)
    serrHandler = logging.StreamHandler(stream=sys.stderr)
    serrHandler.setFormatter(formatter)
    serrHandler.setLevel(logging.WARNING)
    logger.addHandler(serrHandler)
    return logger


def get_call_stack(fname: str | None = None, skip: int = 0, skip_after: int = 0):
    stack = inspect.stack()
    func_list = [frame.function for frame in stack[1+skip:-1-skip_after]]
    s = ".".join(reversed(func_list))
    if fname is not None:
        s += f".{fname}"
    return s


class UpperLevelFilter(logging.Filter):
    def __init__(self, max_level: int):
        super().__init__()
        self.max_level = max_level

    def filter(self, record):
        return record.levelno <= self.max_level


class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        else:
            cls._instances[cls].__init__(*args, **kwargs)
        return cls._instances[cls]


log2type = Literal["file", "screen", "both", "off"]
log2list: list[log2type] = ["file", "screen", "both", "off"]
formatter: logging.Formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s')


logger = logging.getLogger("pysbatch")
logger.handlers.clear()
logger.setLevel(logging.DEBUG)
soutHandler = logging.StreamHandler(stream=sys.stdout)
soutHandler.setLevel(logging.DEBUG)
soutHandler.setFormatter(formatter)
soutHandler.addFilter(UpperLevelFilter(logging.WARNING))
logger.addHandler(soutHandler)
serrHandler = logging.StreamHandler(stream=sys.stderr)
serrHandler.setFormatter(formatter)
serrHandler.setLevel(logging.WARNING)
logger.addHandler(serrHandler)

def configure_logger(logto: log2type, logfile: Path | None = None, debug: bool = True):
    logger.handlers.clear()
    loglevel: int = logging.DEBUG if debug else logging.INFO
    logger.setLevel(loglevel)

    if logto == 'file' or logto == 'both':
        if logfile is None:
            raise ValueError("Logfile is not specified")
        FileHandler = logging.FileHandler(logfile)
        FileHandler.setFormatter(formatter)
        FileHandler.setLevel(logging.DEBUG)
        logger.addHandler(FileHandler)
    if logto == 'screen' or logto == 'both':
        soutHandler = logging.StreamHandler(stream=sys.stdout)
        soutHandler.setLevel(logging.DEBUG)
        soutHandler.setFormatter(formatter)
        soutHandler.addFilter(UpperLevelFilter(logging.WARNING))
        logger.addHandler(soutHandler)
        serrHandler = logging.StreamHandler(stream=sys.stderr)
        serrHandler.setFormatter(formatter)
        serrHandler.setLevel(logging.WARNING)
        logger.addHandler(serrHandler)

    if logto == 'off':
        logger.propagate = False
    logger.info(f"Initialized by {get_call_stack(skip=1, skip_after=1)}")


class FieldPath(fields.Field):
    def _deserialize(self, value: str, attr, data, **kwargs) -> Path:
        return Path(value).resolve(True)

    def _serialize(self, value: Path, attr, obj, **kwargs) -> str:
        return value.as_posix()


def ranges(i):
    for a, b in itertools.groupby(enumerate(i), lambda pair: pair[1] - pair[0]):
        b = list(b)
        yield b[0][1], b[-1][1]


# def ranges_as_list(i):
#     return list(ranges(i))


# def parse_timelimit(limit_str: str) -> int:
#     """Returns seconds"""
#     if limit_str == "UNLIMITED":
#         return -1
#     else:
#         pattern = r"^[a-zA-Z\*]*\s+(?:(\d+)-)?(\d{1,2}):(\d{2}):?(?:(\d{2}))?$"
#         match = re.match(pattern, limit_str)
#         if match:
#             days = int(match.group(1)) if match.group(1) else 0
#             hours = int(match.group(2)) if match.group(2) else 0
#             minutes = int(match.group(3)) if match.group(3) else 0
#             seconds = int(match.group(4)) if match.group(4) else 0

#             if 0 <= hours <= 23 and 0 <= minutes <= 59 and 0 <= seconds <= 59:
#                 return ((days * 24 + hours) * 60 + minutes) * 60 + seconds
#             else:
#                 raise RuntimeError(f"Invalid (time components out of range): {limit_str}")
#         else:
#             raise RuntimeError(f"Time limit retrieved does not match regular expression: {limit_str}")


# def parse_nodes(nodelist_str: str) -> dict[str, set[int]]:
#     if not re.match(r"^([a-z]+\[(?:\d+(?:-\d+)?,?)*\](?:,\s*[a-z]+\[(?:\d+(?:-\d+)?,?)*\])*)$", nodelist_str):
#         raise RuntimeError(f"Invalid nodelist: {nodelist_str}")
#     nodelist: dict[str, set[int]] = {}
#     for nsl in nodelist_str.split('],'):
#         nn, nr_s = nsl.strip().replace("]", "").split('[')
#         nodelist[nn] = set()
#         for item in nr_s.split(','):
#             if re.match(r"^\d+-\d+$", item.strip()):
#                 nra, nrb = item.split('-')
#                 for i in range(int(nra), int(nrb)+1):
#                     nodelist[nn].add(i)
#             elif re.match(r"\d+", item):
#                 nodelist[nn].add(int(item))
#             else:
#                 raise RuntimeError(f"Element not either an integer, nor range: {item}")

#     return nodelist


def parse_sacct_output(output: str) -> list[SlurmJobInfo]:
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


class Shell(metaclass=Singleton):
    class AuthMeth(StrEnum):
        NOPASS = "NOPASS"
        PASS = "PASS"
        KEYFILE = "KEYFILE"
        AGENT = "AGENT"

    __local: bool = True
    __host: str
    __port: int = 22
    __authmeth: AuthMeth = AuthMeth.NOPASS
    __username: str
    __password: str
    __keyfile: Path

    __connected: bool = False
    __context_present: bool = False
    __ssh: SSHClient

    def __init__(self, *args) -> None:
        if self.__connected: self.__disconnect()
        return self.configure(*args)

    def configure(
            self,
            local: bool = True,
            host: str | None = None,
            port: int = 22,
            authmeth: AuthMeth = AuthMeth.NOPASS,
            username: str | None = None,
            password: str | None = None,
            keyfile: Path | None = None,
        ) -> None:
        if self.__connected: self.__disconnect()
        self.__local = local
        if not self.__local:
            if host is None: raise ValueError("No host specified")
            self.__host = host
            self.__port = port
            self.__authmeth = authmeth
            if username is None: raise ValueError("No username specified")
            self.__username = username
            match self.__authmeth:
                case Shell.AuthMeth.PASS:
                    if password is None: raise ValueError("No password specified")
                    self.__password = password
                case Shell.AuthMeth.KEYFILE:
                    if keyfile is None: raise ValueError("No keyfile specified")
                    self.__keyfile = keyfile

        self.__connected = False
        self.__context_present = False
        self.__ssh = SSHClient()
        self.__ssh.set_missing_host_key_policy(paramiko.RejectPolicy())

    def __connect(self) -> None:
        if self.__local or self.__connected: return
        match self.__authmeth:
            case Shell.AuthMeth.PASS: self.__ssh.connect(hostname=self.__host, port=self.__port, username=self.__username, password=self.__password)
            case Shell.AuthMeth.NOPASS: self.__ssh.connect(hostname=self.__host, port=self.__port, username=self.__username)
            case Shell.AuthMeth.KEYFILE: self.__ssh.connect(hostname=self.__host, port=self.__port, username=self.__username, key_filename=self.__keyfile.resolve().as_posix())
            case Shell.AuthMeth.AGENT:
                agent_keys = paramiko.Agent().get_keys()

                if len(agent_keys) == 0: raise Exception(f"Cannot connect to {self.__host}: No keys available from ssh-agent")

                for key in agent_keys:
                    try:
                        logger.debug(f"Trying key: {key.get_fingerprint()}")
                        self.__ssh.connect(hostname=self.__host, port=self.__port, username=self.__username, pkey=key)
                        logger.info("Connection successful!")
                        break
                    except paramiko.AuthenticationException: logger.debug("Authentication failed with this key.")
                    except paramiko.SSHException as e:       logger.error(f"SSH error: {e}")
                    except Exception as e:                   logger.error(f"An unexpected error occurred: {e}")
                else: raise Exception("Failed to authenticate with any available keys.")
        self.__connected = True

    def __disconnect(self) -> None:
        if self.__local or (not self.__connected): return
        self.__ssh.close()
        self.__connected = False

    def __enter__(self) -> 'Shell':
        self.__connect()
        self.__context_present = True
        return self

    def __exit__(self, exc_type: Type[Exception], exc_value: Exception, exc_traceback) -> None:
        self.__context_present = False
        self.__disconnect()

    def local_exec(self, cmds: list[str]) -> tuple[str, str]:
        logger.debug(f"Calling '{cmds}'")
        try:
            proc = subprocess.run(cmds, capture_output=True, check=True, env=os.environ.copy())
        except subprocess.CalledProcessError as e:
            logger.error("Process returned non-zero exitcode")
            logger.error("Output from stdout:")
            logger.error(e.stdout)
            logger.error("Output from stderr:")
            logger.error(e.stderr)
            raise
        return proc.stdout.decode().strip(), proc.stderr.decode().strip()

    def remote_exec(self, cmds: list[str]) -> tuple[str, str]:
        if not self.__connected: self.__connect()
        stdin, stdout, stderr = self.__ssh.exec_command(shlex.join(cmds))
        if not self.__context_present: self.__disconnect()
        return stdout.read().decode().strip(), stderr.read().decode().strip()

    def exec(self, cmds: list[str]) -> tuple[str, str]: return self.local_exec(cmds) if self.__local else self.remote_exec(cmds)


shell = Shell()


# def wexec(cmds: list[str]) -> tuple[str, str]:
#     logger.debug(f"Calling '{cmds}'")
#     try:
#         proc = subprocess.run(cmds, capture_output=True, check=True, env=os.environ.copy())
#     except subprocess.CalledProcessError as e:
#         logger.error("Process returned non-zero exitcode")
#         logger.error("Output from stdout:")
#         logger.error(e.stdout)
#         logger.error("Output from stderr:")
#         logger.error(e.stderr)
#         raise
#     return proc.stdout.decode().strip(), proc.stderr.decode().strip()


def load_conf(file: Path) -> dict[str, Any] | None:
    with file.open('r') as fp:
        try:
            return json.load(fp)
        except json.JSONDecodeError:
            pass

        try:
            return toml.load(fp)
        except toml.TomlDecodeError:
            pass

    return None


def is_exe(fpath: str | Path) -> bool:
    if shutil.which(fpath if isinstance(fpath, str) else fpath.as_posix()):
        return True

    if (os.path.isfile(fpath) and os.access(fpath, os.X_OK)):
        return True

    return False


class ConfigurationError(RuntimeError):
    pass


if __name__ == "__main__":
    pass
