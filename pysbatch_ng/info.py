#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 23-09-2024 03:21:51

import re
import logging
from typing import Set, Dict

from .utils import wexec
from . import constants as cs


def parse_nodes(nodelist_str: str) -> Dict[str, Set[int]]:
    if not re.match(r"^([a-z]+\[(?:\d+(?:-\d+)?,?)*\](?:,\s*[a-z]+\[(?:\d+(?:-\d+)?,?)*\])*)$", nodelist_str):
        raise RuntimeError(f"Invalid nodelist: {nodelist_str}")
    nodelist: Dict[str, Set[int]] = {}
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


def get_nodelist(logger: logging.Logger) -> Dict[str, Set[int]]:
    cmd = f"{cs.execs.sinfo} -h --hide -o %N"
    nodelist_out = wexec(cmd, logger.getChild('sinfo'))

    return parse_nodes(nodelist_out)


def get_partitions(logger: logging.Logger) -> Set[str]:
    cmd = f"{cs.execs.sinfo} -h --hide -o %P"
    partitions_out = wexec(cmd, logger.getChild('sinfo'))
    partitions = []
    for el in partitions_out.split():
        partitions.append(el.replace("*", ""))

    return set(partitions)


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


def get_timelimit(logger: logging.Logger, partition: str) -> int:
    cmd = f"{cs.execs.sinfo} -o '%P %l' --partition={partition}"
    cmd += " | awk 'NR==2 {print $2}'"
    limit_str = wexec(cmd, logger.getChild('sinfo'))
    try:
        limit = parse_timelimit(limit_str)
        return limit
    except RuntimeError as e:
        logger.exception(e)
    return 0

