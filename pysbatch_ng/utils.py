#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (c) 2023-2024 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 02-05-2024 23:40:24

import os
import re
import sys
import shlex
import shutil
import inspect
import logging
import itertools
import functools
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


logger: logging.Logger = minilog(__name__)
logto_type = Union[Literal['file'], Literal['screen'], Literal['both'], Literal['off']]


def loggerConf(logto: logto_type, logfile: Path, debug: bool = True):
    global logger
    logger = logging.root.getChild(__name__)
    logger.handlers.clear()
    loglevel: int = logging.DEBUG if debug else logging.INFO
    logger.setLevel(loglevel)

    formatter: logging.Formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s')

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
    if limit_str == "UNLIMITED":
        return -1
    else:
        pattern = r"^(?:(\d+)-)?(\d{1,2}):(\d{2}):(\d{2})$"
        match = re.match(pattern, limit_str)
        if match:
            days = int(match.group(1)) if match.group(1) else 0
            hours = int(match.group(2))
            minutes = int(match.group(3))
            seconds = int(match.group(4))

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


def get_call_stack(fname: str | None = None):
    stack = inspect.stack()
    func_list = [frame.function for frame in stack[1:] if frame.function != "wrapper"]
    s = ".".join(reversed(func_list))
    if fname is not None:
        s += f".{fname}"
    return s


def logs(func):
    global logger
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        global logger
        logger = logging.root.getChild(__name__).getChild(get_call_stack(func.__name__))
        # logger = minilog(get_call_stack())
        result = func(*args, **kwargs)
        # print(f"After calling {func.__name__}")
        return result
    return wrapper


@logs
def wexec(cmd: str) -> str:
    logger.debug(f"Calling '{cmd}'")
    cmds = shlex.split(cmd)
    proc = subprocess.run(cmds, capture_output=True)
    bout = proc.stdout.decode()
    berr = proc.stderr.decode()
    if proc.returncode != 0:
        logger.error("Process returned non-zero exitcode")
        logger.error("Output from stdout:")
        logger.error(bout)
        logger.error("Output from stderr:")
        logger.error(berr)
        raise RuntimeError("Process returned non-zero exitcode")
    return bout.strip()


def is_exe(fpath: str | Path) -> bool:
    if shutil.which(fpath if isinstance(fpath, str) else fpath.as_posix()):
        return True

    if (os.path.isfile(fpath) and os.access(fpath, os.X_OK)):
        return True

    return False


if __name__ == "__main__":
    pass
