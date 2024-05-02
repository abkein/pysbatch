#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 01-05-2024 01:35:21

import json
import logging
from pathlib import Path
from typing import Dict, Set, Any

from MPMU import wexec, is_exe

from .utils import ranges
from . import constants as cs


def spoll_check_conf(conf: Dict[str, Any], logger: logging.Logger) -> bool:
    sconf = conf[cs.fields.spoll]

    fl = True
    if cs.fields.debug in sconf:
        try:
            bool(sconf[cs.fields.debug])
        except Exception as e:
            logger.error(f"Failed to parse '{cs.fields.debug}' field due to an exception:")
            logger.exception(e)
            fl = False
    if cs.fields.cwd in sconf:
        try:
            _cwd = Path(sconf[cs.fields.cwd]).resolve()
            if not _cwd.exists():
                logger.error(f"Specified cwd does not exists: {_cwd.as_posix()}")
                fl = False
        except Exception as e:
            logger.error(f"Failed to parse '{cs.fields.cwd}' field due to an exception:")
            logger.exception(e)
            fl = False
    if cs.fields.jobid in sconf:
        try:
            int(sconf[cs.fields.jobid])
        except Exception as e:
            logger.error(f"Failed to parse '{cs.fields.jobid}' field due to an exception:")
            logger.exception(e)
            fl = False
    if cs.fields.ptag in sconf:
        try:
            int(sconf[cs.fields.ptag])
        except Exception as e:
            logger.error(f"Failed to parse '{cs.fields.ptag}' field due to an exception:")
            logger.exception(e)
            fl = False
    if cs.fields.logfolder in sconf:
        try:
            _logfolder = Path(sconf[cs.fields.logfolder]).resolve()
        except Exception as e:
            logger.error(f"Failed to parse '{cs.fields.logfolder}' field due to an exception:")
            logger.exception(e)
            fl = False
        # if not _logfolder.exists(): print(f"Specified logfolder does not exists: {_logfolder.as_posix()}")  # unnecessary, cuz created automatically if not exists
    if cs.fields.logto in sconf:
        if not (sconf[cs.fields.logto] == 'file' or sconf[cs.fields.logto] == 'screen' or sconf[cs.fields.logto] == 'both' or sconf[cs.fields.logto] == 'off'):
            logger.error(f"Cannot parse '{cs.fields.logto}' field, must be one of 'screen', 'file', 'both' or 'off'")
            fl = False
    if cs.fields.every in sconf:
        try:
            int(sconf[cs.fields.every])
        except Exception as e:
            logger.error(f"Failed to parse '{cs.fields.every}' field due to an exception:")
            logger.exception(e)
            fl = False
    if cs.fields.times_criteria in sconf:
        try:
            int(sconf[cs.fields.times_criteria])
        except Exception as e:
            logger.error(f"Failed to parse '{cs.fields.times_criteria}' field due to an exception:")
            logger.exception(e)
            fl = False
    if cs.fields.sbatch in conf:
        if cs.fields.execs in conf[cs.fields.sbatch]:
            cexecs(conf[cs.fields.sbatch], logging.Logger("execs_configuration"))
    return fl


def get_info(logger: logging.Logger):
    logger.debug("Getting nodelist")
    cmd = f"{cs.execs.sinfo} -h --hide -o %N"
    nodelist_out = wexec(cmd, logger.getChild('sinfo'))
    nodelist = {}
    for nsl in nodelist_out.split(','):
        nn, nr_s = nsl.strip().replace("]", "").split('[')
        nra, nrb = nr_s.split('-')
        nodelist[nn] = set(range(int(nra), int(nrb)+1))
    cs.obj.nodelist = nodelist
    logger.info(f"Following nodes were found: {cs.obj.nodelist}")

    logger.debug("Getting partitions list")
    cmd = f"{cs.execs.sinfo} -h --hide -o %P"
    partitions_out = wexec(cmd, logger.getChild('sinfo'))
    partitions = []
    for el in partitions_out.split():
        # if re.match(r"^[a-zA-Z_]+$", el):
        partitions.append(el.replace("*", ""))
    cs.obj.partitions = set(partitions)
    logger.info(f"Following partitions were found: {cs.obj.partitions}")


def nodelist_parse(conf, logger: logging.Logger) -> Dict[str, Set[Any]]:
    exclude = {}
    for node_name, node_num in conf.items():
        if isinstance(node_num, (list, set)):
            nodelist = set(node_num)
        else:
            try:
                nodelist = set([int(node_num)])
            except ValueError:
                try:
                    nodelist = set(json.loads(node_num))
                except json.decoder.JSONDecodeError:
                    try:
                        nra, nrb = node_num[1:-1].split('-')
                        nodelist = set(range(int(nra), int(nrb)+1))
                    except Exception:
                        logger.critical("Cannot transform given nodelist to unique set")
                        raise
                except Exception:
                    logger.critical("Cannot transform given nodelist to unique set")
                    raise
            except Exception:
                logger.critical("Cannot transform given nodelist to unique set")
                raise
        exclude[node_name] = nodelist

    return exclude


def checkin(main: Dict[str, Set[Any]], sub: Dict[str, Set[Any]], logger: logging.Logger):
    kys = set(sub.keys()) - set(main.keys())
    if len(kys) != 0:
        logger.error(f"There are no such nodes like {kys}")
        raise RuntimeError(f"There are no such nodes like {kys}")
    nds = {}
    for key in sub.keys():
        itr = sub[key] - main[key]
        if len(itr) != 0:
            nds[key] = itr
    raise RuntimeError(f"There are no such nodes like {nds}")


def __checkin(main: Dict[str, Set[Any]], sub: Dict[str, Set[Any]]):
    if set(sub.keys()) <= set(main.keys()):
        return all([sub[key] <= main[key] for key in sub.keys()])
    else:
        return False


def chuie(use: Dict[str, Set[Any]], exclude: Dict[str, Set[Any]], logger: logging.Logger):  # check if use nodes in exclude nodes
    inter = set(use.keys()) & set(exclude.keys())
    nds = {}
    for key in inter:
        its = use[key] & exclude[key]
        if len(its) != 0:
            nds[key] = its
    logger.error(f"There is intersection of excluded and used nodes: {nds}")
    raise RuntimeError(f"There is intersection of excluded and used nodes: {nds}")


def __chuie(use: Dict[str, Set[Any]], exclude: Dict[str, Set[Any]]):  # check if use nodes in exclude nodes
    inter = set(use.keys()) & set(exclude.keys())
    if len(inter) != 0:
        return not all([len(use[key] & exclude[key]) == 0 for key in inter])
    else:
        return False


def gnnis(nodelist: Dict[str, Set[Any]], exclude: Dict[str, Set[Any]]):  # get nodes not in subset
    nds = {key: (nodelist[key] if key not in exclude.keys() else nodelist[key] - exclude[key]) for key in nodelist.keys()}
    for key in nds.keys():
        if len(nds[key]) == 0:
            del nds[key]
    return nds


def excludes(conf: Dict[str, Any], logger: logging.Logger):
    if cs.fields.nodes_exclude in conf:
        main_nodes_exclude = nodelist_parse(conf[cs.fields.nodes_exclude], logger)
        if __checkin(cs.obj.nodelist, main_nodes_exclude):
            cs.obj.nodes_exclude = main_nodes_exclude
            logger.debug(f"Following nodes will be excluded for main runs: {cs.obj.nodes_exclude}")
        else:
            checkin(cs.obj.nodelist, main_nodes_exclude, logger)

    if cs.fields.nodes_use in conf:
        main_nodes_use = nodelist_parse(conf[cs.fields.nodes_use], logger)
        if __checkin(cs.obj.nodelist, main_nodes_use):
            if cs.obj.nodes_exclude is not None:
                if __chuie(main_nodes_use, cs.obj.nodes_exclude):
                    chuie(main_nodes_use, cs.obj.nodes_exclude, logger)
                cs.obj.nodes_exclude = gnnis(cs.obj.nodelist, main_nodes_use)
                logger.debug(f"Following nodes will be excluded for main runs: {cs.obj.nodes_exclude}")
            else:
                cs.obj.nodes_exclude = gnnis(cs.obj.nodelist, main_nodes_use)
                logger.debug(f"Following nodes will be excluded for main runs: {cs.obj.nodes_exclude}")
        else:
            checkin(cs.obj.nodelist, main_nodes_use, logger)


def gensline(nodelist: Dict[str, Set[Any]]):
    s = ""
    for k, v in nodelist.items():
        for a, b in ranges(v):
            if a == b:
                s += f"{k}{a},"
            else:
                s += f"{k}[{a}-{b}],"
    return s[:-1]


def cexecs(conf: Dict[str, Any], logger: logging.Logger) -> bool:
    fl = True
    if cs.fields.execs in conf:
        execs = conf[cs.fields.execs]
        if cs.fields.sinfo in execs:
            cs.execs.sinfo = execs[cs.fields.sinfo]
            if not is_exe(cs.execs.sinfo, logger.getChild('is_exe')):
                logger.error(f"sinfo executable not found: {cs.execs.sinfo}")
                fl = False
                # raise FileNotFoundError("sinfo executable not found")
        if cs.fields.sacct in execs:
            cs.execs.sacct = execs[cs.fields.sacct]
            if not is_exe(cs.execs.sacct, logger.getChild('is_exe')):
                logger.error(f"sacct executable not found: {cs.execs.sacct}")
                fl = False
                # raise FileNotFoundError("sacct executable not found")
        if cs.fields.sbatch in execs:
            cs.execs.sbatch = execs[cs.fields.sbatch]
            if not is_exe(cs.execs.sbatch, logger.getChild('is_exe')):
                logger.error(f"sbatch executable not found: {cs.execs.sbatch}")
                fl = False
                # raise FileNotFoundError("sbatch executable not found")
        if cs.fields.spoll in execs:
            cs.execs.spoll = execs[cs.fields.spoll]
            if not is_exe(cs.execs.spoll, logger.getChild('is_exe')):
                logger.error(f"spoll executable not found: {cs.execs.spoll}")
                fl = False
                # raise FileNotFoundError("spoll executable not found")
        if cs.fields.spolld in execs:
            cs.execs.spolld = execs[cs.fields.spolld]
            if not is_exe(cs.execs.spolld, logger.getChild('is_exe')):
                logger.error(f"spolld executable not found: {cs.execs.spolld}")
                fl = False
                # raise FileNotFoundError("spolld executable not found")
    return fl


def basic(conf: Dict[str, Any], logger: logging.Logger, is_check: bool = False):
    if not cexecs(conf, logger.getChild("execs_configuration")):
        logger.error("Unable to find some executables")
        raise RuntimeError("Unable to find some executables")
    if cs.fields.folder in conf:
        cs.folders.run = conf[cs.fields.folder]
    if cs.fields.jname in conf:
        cs.ps.jname = conf[cs.fields.jname]
    if cs.fields.nnodes in conf:
        cs.ps.nnodes = conf[cs.fields.nnodes]
    if cs.fields.ntpn in conf:
        cs.ps.ntpn = conf[cs.fields.ntpn]
    if cs.fields.partition in conf:
        cs.ps.partition = conf[cs.fields.partition]
    if cs.fields.preload in conf:
        cs.ps.pre = conf[cs.fields.preload]
    if not is_check:
        if cs.fields.executable in conf:
            if not is_exe(conf[cs.fields.executable], logger.getChild('is_exe')):
                logger.error(f"Specified executable {conf[cs.fields.executable]} is not an executable")
                raise RuntimeError(f"Specified executable {conf[cs.fields.executable]} is not an executable")
        else:
            logger.error("Executable is not specified")
            raise RuntimeError("Executable is not specified")


def configure(conf: Dict[str, Any], logger: logging.Logger, is_check: bool = False) -> bool:
    # logger.debug("Following configuration is set")
    # logger.debug(json.dumps(conf))
    logger.debug("Setting basic configuration")
    basic(conf, logger.getChild('basic'), is_check)
    logger.debug("Getting info about nodes, partitions, policies")
    get_info(logger)
    logger.debug("Getting partitions to exclude/use")
    excludes(conf, logger)
    if cs.obj.nodes_exclude is not None:
        cs.ps.exclude_str = gensline(cs.obj.nodes_exclude)
    logger.info("Configuration OK")
    return True


def genconf() -> Dict[str, Any]:
    conf: Dict[str, Any] = {}

    execs: Dict[str, str] = {}
    execs[cs.fields.sinfo] = cs.execs.sinfo
    execs[cs.fields.sbatch] = cs.execs.sbatch
    conf[cs.fields.execs] = execs
    conf[cs.fields.preload] = cs.ps.pre

    conf[cs.fields.jname] = 'SoMeNaMe'
    conf[cs.fields.nnodes] = 1
    conf[cs.fields.ntpn] = 4
    conf[cs.fields.partition] = 'test'
    conf[cs.fields.folder] = 'slurm'
    conf[cs.fields.nodes_exclude] = {
        'host': 1,
        'angr': [3, 4, 5],
        'ghost': "[6-18]"
    }
    conf[cs.fields.nodes_use] = {
        'host': 2,
        'angr': [1, 2, 6, 7, 8],
        'ghost': "[1-5]"
    }

    return conf


if __name__ == "__main__":
    pass
