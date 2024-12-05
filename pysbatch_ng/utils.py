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
import shlex
import shutil
import inspect
import logging
import itertools
import subprocess
from pathlib import Path
from typing import Union, Literal

from marshmallow import fields


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


log2type = Literal["file", "screen", "both", "off"]
log2list: list[log2type] = ["file", "screen", "both", "off"]


class LogDaemon:
    __logger: logging.Logger
    __initalized: bool = False

    def __init__(self) -> None:
        self.__logger = logging.getLogger("pysbatch")

    def configure(self, logto: log2type, logfile: Path | None = None, debug: bool = True):
        if self.__initalized:
            return
        self.__logger.handlers.clear()
        loglevel: int = logging.DEBUG if debug else logging.INFO
        self.__logger.setLevel(loglevel)

        formatter: logging.Formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s')

        if logto == 'file' or logto == 'both':
            if logfile is None:
                raise ValueError("Logfile is not specified")
            FileHandler = logging.FileHandler(logfile)
            FileHandler.setFormatter(formatter)
            FileHandler.setLevel(logging.DEBUG)
            self.__logger.addHandler(FileHandler)
        if logto == 'screen' or logto == 'both':
            soutHandler = logging.StreamHandler(stream=sys.stdout)
            soutHandler.setLevel(logging.DEBUG)
            soutHandler.setFormatter(formatter)
            soutHandler.addFilter(UpperLevelFilter(logging.WARNING))
            self.__logger.addHandler(soutHandler)
            serrHandler = logging.StreamHandler(stream=sys.stderr)
            serrHandler.setFormatter(formatter)
            serrHandler.setLevel(logging.WARNING)
            self.__logger.addHandler(serrHandler)

        if logto == 'off':
            self.__logger.propagate = False
        self.__initalized = True
        self.__logger.info(f"Initialized by {get_call_stack(skip=1, skip_after=1)}")

    def get_logger(self):
        if not self.__initalized:
            raise RuntimeError("pysbatch logger is not configured. Do it by calling pysbatch.log.configure()")
        return self.__logger.getChild(get_call_stack(skip=1, skip_after=1))


log = LogDaemon()


class FieldPath(fields.Field):
    def _deserialize(self, value: str, attr, data, **kwargs) -> Path:
        return Path(value).resolve(True)

    def _serialize(self, value: Path, attr, obj, **kwargs) -> str:
        return value.as_posix()


def ranges(i):
    for a, b in itertools.groupby(enumerate(i), lambda pair: pair[1] - pair[0]):
        b = list(b)
        yield b[0][1], b[-1][1]


def ranges_as_list(i):
    return list(ranges(i))


def parse_timelimit(limit_str: str) -> int:
    logger = log.get_logger()
    if limit_str == "UNLIMITED":
        return -1
    else:
        pattern = r"^[a-zA-Z\*]*\s+(?:(\d+)-)?(\d{1,2}):(\d{2}):?(?:(\d{2}))?$"
        match = re.match(pattern, limit_str)
        if match:
            days = int(match.group(1)) if match.group(1) else 0
            hours = int(match.group(2)) if match.group(2) else 0
            minutes = int(match.group(3)) if match.group(3) else 0
            seconds = int(match.group(4)) if match.group(4) else 0

            if 0 <= hours <= 23 and 0 <= minutes <= 59 and 0 <= seconds <= 59:
                return ((days * 24 + hours) * 60 + minutes) * 60 + seconds
            else:
                raise RuntimeError(f"Invalid (time components out of range): {limit_str}")
        else:
            raise RuntimeError(f"Time limit retrieved does not match regular expression: {limit_str}")


def parse_nodes(nodelist_str: str) -> dict[str, set[int]]:
    if not re.match(r"^([a-z]+\[(?:\d+(?:-\d+)?,?)*\](?:,\s*[a-z]+\[(?:\d+(?:-\d+)?,?)*\])*)$", nodelist_str):
        raise RuntimeError(f"Invalid nodelist: {nodelist_str}")
    nodelist: dict[str, set[int]] = {}
    for nsl in nodelist_str.split('],'):
        nn, nr_s = nsl.strip().replace("]", "").split('[')
        nodelist[nn] = set()
        for item in nr_s.split(','):
            if re.match(r"^\d+-\d+$", item.strip()):
                nra, nrb = item.split('-')
                for i in range(int(nra), int(nrb)+1):
                    nodelist[nn].add(i)
            elif re.match(r"\d+", item):
                nodelist[nn].add(int(item))
            else:
                raise RuntimeError(f"Element not either an integer, nor range: {item}")

    return nodelist


def wexec(cmd: str) -> tuple[str, str]:
    logger = log.get_logger()
    logger.debug(f"Calling '{cmd}'")
    cmds = shlex.split(cmd)
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


def is_exe(fpath: str | Path) -> bool:
    if shutil.which(fpath if isinstance(fpath, str) else fpath.as_posix()):
        return True

    if (os.path.isfile(fpath) and os.access(fpath, os.X_OK)):
        return True

    return False


if __name__ == "__main__":
    pass
