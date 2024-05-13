#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 02-05-2024 23:40:20

import os
import sys
import shlex
import logging
import argparse
import subprocess
from pathlib import Path
from typing import Dict, Union, Any

import toml

from . import constants as cs
from . import config


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


def run_conf(conf: Dict[str, Any], conffileloc: Union[Path, None] = None, logger: logging.Logger = minilog("spoll")) -> bool:
    """Run detached spoll instance with configuration provided by dict.

    Args:
        conf (Dict[str, Union[str, int, bool]]): Arguments are represented using key=value convenience. Arguments without value (like --debug) must be stored as keys with True value.
        filename (Union[Path, None]): If conffile wasn't specified, spoll creates its own conffile and forwards arguments from cmd to it. This option allows specify this conffile location. Default: [tag_]jobid_poll_conf.toml"
    """

    if not config.spoll_check_conf(conf, logger.getChild("checkconf")):
        logger.error("Invalid configuration. Aborting...")
        return False

    sconf = conf[cs.fields.spoll]

    conffile: Path
    # if conffilename: conffile = Path(conffilename).resolve()
    # else:
    if cs.fields.jobid in sconf:
        conffilename = f"{sconf[cs.fields.jobid]}_poll_conf.toml"
        if cs.fields.ptag in sconf: conffilename = f"{sconf[cs.fields.ptag]}_" + conffilename
        conffile = conffileloc / conffilename if conffileloc else Path.cwd() / conffilename
    else: RuntimeError("Jobid wasn't specified")

    with conffile.open('w') as fp:
        toml.dump(conf, fp)

    if cs.fields.sbatch in conf:
        bconf = conf[cs.fields.sbatch]
        if cs.fields.execs in bconf:
            config.cexecs(bconf, logger.getChild("execs_configuration"))

    cmd = f"{cs.execs.spolld} --file={shlex.quote(conffile.as_posix())}"
    cmds = shlex.split(cmd)
    subprocess.Popen(cmds, start_new_session=True)

    return True


def main():
    parser = argparse.ArgumentParser(prog="spoll", description="Run detached spoll instance. Program runs detached spolld instance and lazy forwards cli arguments to it.")  # , formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--debug", action="store_true", help="Debug. Default: False")
    parser.add_argument("--genconf", action="store_true", help="Generate sample configuration file and exit (Default file: ./spoll_sample_conf.toml), see also --file and --section options")
    parser.add_argument("--checkconf", action="store_true", help="Check configuration file and exit, see also --file and --section options")
    parser.add_argument('--log', choices=['screen', 'file', 'both', 'off'], help="Whether to log to screen, to file or both or off logging. Default: file")
    parser.add_argument("--jobid", action="store", type=int, help="Slurm job ID")
    parser.add_argument("--tag", action="store", type=int, help="Project tag (arbitrary int number) (optional)")
    parser.add_argument("--every", action="store", type=int, help="Perform poll every N-minutes. Default: 5")
    parser.add_argument("--tc", action="store", type=int, help="Criteria to wait a normal state (times of chech). Default: 288 (a day)")
    parser.add_argument("--cmd", action="store", type=str, help="CMD to run after")
    parser.add_argument("--file", action="store", type=str, help="Read configuration from TOML configuration file (cli arguments owerwrite file ones). If --genconf was specified, generated configuration is stored in this file.")
    parser.add_argument("--logfolder", action="store", type=str, help="Folder whether to store logs. Default: cwd/log/slurm")
    parser.add_argument("--cwd", action="store", type=str, help="Current working directory. Default: cwd")
    parser.add_argument("--tmpfile", action="store", type=str, help="If conffile wasn't specified, spoll creates its own conffile and forwards arguments from cmd to it. This option allows specify this conffile location. Default: [tag_]jobid_poll_conf.toml")
    args = parser.parse_args()

    if args.genconf:
        sample_conf: Dict[str, Union[str, int, bool]] = {}
        sample_conf[cs.fields.debug] = False
        sample_conf[cs.fields.cwd] = "./"
        sample_conf[cs.fields.jobid] = 123456
        sample_conf[cs.fields.ptag] = 11111
        sample_conf[cs.fields.logfolder] = cs.folders.log
        sample_conf[cs.fields.logto] = 'file'
        sample_conf[cs.fields.cmd] = 'echo "It works!"'
        sample_conf[cs.fields.every] = 5
        sample_conf[cs.fields.times_criteria] = 288
        sample_conf_sec = {cs.fields.spoll: sample_conf}
        with Path(args.file if args.file else "spoll_sample_conf.toml").resolve().open('w', encoding='utf-8') as fp:
            toml.dump(sample_conf_sec, fp)
            return 0

    if args.cwd: os.chdir(Path(args.cwd).resolve())

    cwd = Path.cwd()

    logger = minilog("spoll")

    if args.checkconf:
        with Path(args.file).resolve().open('r', encoding='utf-8') as fp:
            conf = toml.load(fp)
            if not config.spoll_check_conf(conf, logger.getChild("checkconf")):
                logger.error("Invalid configuration.")
                return 1
            else:
                logger.info("Configuration ok")
                return 0

    if args.file:
        with Path(args.file).resolve().open('r', encoding='utf-8') as fp:
            conf = toml.load(fp)[cs.fields.spoll]

        if not config.spoll_check_conf(conf, logger.getChild("checkconf")):  # it appears here two times instead of just one before calling the main process, because 'spoll' executable can be overriden in the configuration file
            logger.error("Invalid configuration. Aborting...")
            return 1

        cmd = f"{cs.execs.spolld}"
        cmd += f" --file={shlex.quote(Path(args.file).resolve().as_posix())}"
    else:
        conf: Dict[str, Union[str, int, Path, bool]] = {}
        if args.debug: conf[cs.fields.debug] = True
        if args.log: conf[cs.fields.logto] = args.log
        if args.jobid: conf[cs.fields.jobid] = args.jobid
        else: raise RuntimeError("Jobid wasn't specified")
        if args.tag: conf[cs.fields.ptag] = args.ptag
        if args.every: conf[cs.fields.every] = args.every
        if args.tc: conf[cs.fields.times_criteria] = args.tc
        if args.cmd: conf[cs.fields.cmd] = args.cmd
        if args.cwd: conf[cs.fields.cwd] = args.cwd
        if args.logfolder: conf[cs.fields.logfolder] = Path(args.logfolder).resolve()

        if not config.spoll_check_conf(conf, logger.getChild("checkconf")):
            logger.error("Invalid configuration. Aborting...")

        conffile: Path
        if args.tmpfile: conffile = Path(args.tmpfile).resolve()
        else:
            conffile_name = f"{args.jobid}_poll_conf.toml"
            if args.tag: conffile_name = f"{args.tag}_" + conffile_name
            conffile = cwd / conffile_name
        with conffile.open('w') as fp:
            toml.dump({cs.fields.spoll: conf}, fp)

        cmd = f"{cs.execs.spolld} --file={shlex.quote(conffile.as_posix())}"

    cmds = shlex.split(cmd)
    subprocess.Popen(cmds, start_new_session=True)

    return 0

if __name__ == "__main__":
    sys.exit(main())
