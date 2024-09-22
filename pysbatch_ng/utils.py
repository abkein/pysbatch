#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 09-09-2023 21:55:21

import os
import shlex
import string
import logging
import itertools
import subprocess
from typing import Dict, List


def ranges(i):
    for a, b in itertools.groupby(enumerate(i), lambda pair: pair[1] - pair[0]):
        b = list(b)
        yield b[0][1], b[-1][1]


def ranges_as_list(i):
    return list(ranges(i))


def wexec(cmd: str, logger: logging.Logger) -> str:
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
    return bout


def is_exe(fpath: str, logger: logging.Logger, exit: bool = False) -> bool:
    logger.debug(f"Checking: '{fpath}'")
    if not (os.path.isfile(fpath) and os.access(fpath, os.X_OK)):
        logger.debug("This is not standard file")
        if not exit:
            logger.debug("Resolving via 'which'")
            cmd = f"which {fpath}"
            cmds = shlex.split(cmd)
            proc = subprocess.run(cmds, capture_output=True)
            bout = proc.stdout.decode()
            # berr = proc.stderr.decode()
            if proc.returncode != 0:
                logger.debug('Process returned nonzero returncode')
                return False
            else:
                return is_exe(bout.strip(), logger.getChild('2nd'), exit=True)
        else:
            return False
    else:
        return True


class confdict(dict):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.placeholders: Dict[str, List] = {}
        for k, v in super().items():
            self.add_placeholder(k, v)

    def __setitem__(self, __key, __value) -> None:
        self.add_placeholder(__key, __value)
        return super().__setitem__(__key, __value)

    def add_placeholder(self, __key, __value) -> None:
        if isinstance(__value, str):
            phs = [tup[1] for tup in string.Formatter().parse(__value) if tup[1] is not None]
            if len(phs) > 0:
                for ph in phs:
                    if len(ph) > 0:
                        if ph in self.placeholders:
                            self.placeholders[ph] += [__key]
                        else:
                            self.placeholders[ph] = [__key]
                    else:
                        raise RuntimeError("Unnamed placeholder detected")
            else:
                pass  # no placeholders were found
        else:
            pass  # not searching for placeholders in non-str objects


    def reconf(self, **kwargs) -> None:
        for ph, value in kwargs.items():
            for key in self.placeholders[ph]:
                obj = super().__getitem__(key)
                if isinstance(obj, str):
                    obj = obj.format(**{ph: value})
                # else:
                #     pass
                #     raise RuntimeError("")  # if item has changed bypassing __setitem__ method
                super().__setitem__(key, obj)

    def self_reconf(self) -> None:
        for ph, keys in self.placeholders.items():
            if super().__contains__(ph):
                for key in keys:
                    obj = super().__getitem__(key)
                    if isinstance(obj, str):
                        obj = obj.format(**{ph: super().__getitem__(ph)})
                    # else:
                    #     pass
                    #     raise RuntimeError("")  # if item has changed bypassing __setitem__ method
                    super().__setitem__(key, obj)


if __name__ == "__main__":
    pass
