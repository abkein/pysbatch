#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (c) 2023-2024 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

import os
import re
import sys
import json
import inspect

import argparse
from typing import Any
from pathlib import Path
from dataclasses import dataclass

import toml
from marshmallow import Schema, fields, post_load

from .polling import Poller
from .platform import Platform, PlatformSchema
from .utils import configure_logger, shell, FieldPath, logger, load_conf


regex_sbatch_jobid = r'Submitted batch jobid (\d+)'


@dataclass
class Options:
    cmd: str | None = None
    job_name: str | None = None
    nnodes:   int | None = None
    ntasks_per_node: int | None = None
    partition: str | None = None
    folder: str = "slurm"
    job_number: int | None = None
    tag: int | None = None

    def log(self) -> None:
        _props: list[tuple[str, Any]] = [
            ("Slurm folder: ", self.folder),
            ("Job name:     ", self.job_name),
            ("Nodes:        ", self.nnodes),
            ("N tasks/node: ", self.ntasks_per_node),
            ("Partition:    ", self.partition),
            ("Job number:   ", self.job_number),
            ("Tag:          ", self.tag),
            ("Cmd:          ", self.cmd)
        ]
        props: list[tuple[str, Any]] = []
        for el in _props:
            if el[1] is not None:
                props.append(el)
        logger.info("Current configured options:")
        for i, el in enumerate(props):
            prefix = "├─"
            if i == 0:
                prefix = "┌─"
            if i == len(props) - 1:
                prefix = "└─"
            logger.info(f"{prefix}{el[0]}{el[1]}")

    @property
    def job_folder_rel(self) -> str:
        bd = Path()
        if self.job_number is None:
            return (bd / self.folder / f"{self.job_name}").relative_to(bd).as_posix()
        else:
            return (bd / self.folder / f"{self.job_name}_{self.job_number}").relative_to(bd).as_posix()

    def job_folder(self, cwd: Path) -> Path:
        return cwd / self.job_folder_rel

    @classmethod
    def from_schema(cls, data: dict[str, Any]) -> "Options":
        options = OptionsSchema().load(data)
        if not isinstance(options, Options):
            raise ValueError(f"Unable to load {cls.__name__} from data (dev bug:{__file__}:{inspect.currentframe().f_code.co_name})")  # type: ignore
        return options

    def dump_schema(self) -> dict[str, Any]:
        data = OptionsSchema().dump(self)
        if not isinstance(data, dict):
            raise ValueError(f"Unable to dump {self.__class__.__name__} to data (dev bug:{__file__}:{inspect.currentframe().f_code.co_name})")  # type: ignore
        return data


class OptionsSchema(Schema):
    cmd = fields.String(allow_none=True, missing=None)
    job_name = fields.String(allow_none=True, missing=None)
    nnodes = fields.Integer(allow_none=True, missing=None)
    job_number = fields.Integer(allow_none=True, missing=None)
    tag = fields.Integer(allow_none=True, missing=None)
    ntasks_per_node = fields.Integer(allow_none=True, missing=None)
    partition = fields.String(allow_none=True, missing=None)
    folder = fields.String(missing="slurm")

    @post_load
    def create_options(self, data, **kwargs) -> Options:
        return Options(**data)


class Sbatch:
    default_options: Options
    platform: Platform
    cwd: Path

    def __init__(self, default_options: Options | None = None, platform: Platform | None = None, cwd: Path | None = None) -> None:
        super().__init__()
        self.default_options = default_options if default_options is not None else Options()
        self.platform = platform if platform is not None else Platform()
        self.cwd = cwd if cwd is not None else Path.cwd()

    def run(self, _options: Options, run_poll: bool = False, after_cmd: str | None = None, debug: bool = False) -> int:
        """Runs sbatch command via creating .job file

        Args:
            cwd (Path): current working directory
            conf (config): configuration
            number (Optional[int]): Number of task. At this stage there is no jobid yet, so it used instead. Defaults to None.
            add_conf (Optional[Dict]): Additional configuration, it merged to main configuration. Defaults to None.

        Raises:
            RuntimeError: Raised if sbatch command not returned jobid (or function cannot parse it from output)

        Returns:
            jobid (int): slurm's jobid
        """
        opt_base = self.default_options.dump_schema()
        opt_head = _options.dump_schema()
        opt_base.update(opt_head)
        options = Options.from_schema(opt_base)

        tdir = options.job_folder(self.cwd)
        tdir.mkdir(parents=True, exist_ok=True)

        configure_logger('both', tdir / "sbatch_launch.log")
        logger.debug('Configuring...')
        logger.info("Launching job with following options:")
        options.log()
        if options.cmd is None:
            logger.error("Options cmd is None")
            raise ValueError("Options cmd is None")

        job_file = tdir / f"{options.job_name}.job"

        if run_poll:
            poller = Poller(
                cmd = after_cmd,
                debug=debug,
                logto='file',
                tag=options.tag,
                logfolder=options.job_folder_rel,
                lockfilename=f"{options.tag}.lock" if options.tag is not None else None,
                cwd=self.cwd,
                platform=self.platform,
            )

        with job_file.open('w') as fh:
            fh.writelines("#!/usr/bin/env bash\n")
            fh.writelines(f"#SBATCH --job-name={options.job_name}\n")
            fh.writelines(f"#SBATCH --output={tdir}/{options.job_name}.out\n")
            fh.writelines(f"#SBATCH --error={tdir}/{options.job_name}.err\n")
            fh.writelines("#SBATCH --begin=now\n")
            if options.nnodes is not None:
                fh.writelines(f"#SBATCH --nodes={options.nnodes}\n")
            if options.ntasks_per_node is not None:
                fh.writelines(f"#SBATCH --ntasks-per-node={options.ntasks_per_node}\n")
            if options.partition is not None:
                fh.writelines(f"#SBATCH --partition={options.partition}\n")
            if len(self.platform.nodes_exclude) != 0:
                fh.writelines(f"#SBATCH --exclude={self.platform.exclude_str}\n")

        logger.info("Submitting task...")
        cmds = [
            f"{self.platform.execs.sbatch}",
            f"{job_file}"
        ]
        bout, _ = shell.exec(cmds)

        if re.match(r'Submitted batch job \d+', bout):
            try:
                jobid = int(bout.split()[-1])
            except ValueError as e:
                logger.error("Cannot parse sbatch jobid from:")
                logger.error(bout)
                logger.exception(e)
                raise RuntimeError("sbatch command not returned task jobid")
            print("Sbatch jobid: ", jobid)
            logger.info(f"Sbatch jobid: {jobid}")
        else:
            logger.error("Cannot parse sbatch jobid from:")
            logger.error(bout)
            raise RuntimeError("sbatch command not returned task jobid")

        if run_poll:
            poller.jobid = jobid
            poller.check()
            try:
                poller.detach_start()
            except Exception as e:
                logger.error("Unable to start poller")
                logger.exception(e)
            logger.info("Poller started")

        return jobid

    @classmethod
    def from_schema(cls, data: dict[str, Any]) -> "Sbatch":
        sbatch = SbatchSchema().load(data)
        if not isinstance(sbatch, Sbatch):
            raise ValueError(f"Unable to load {cls.__name__} from data (dev bug:{__file__}:{inspect.currentframe().f_code.co_name})")  # type: ignore
        return sbatch

    def dump_schema(self) -> dict[str, Any]:
        data = SbatchSchema().dump(self)
        if not isinstance(data, dict):
            raise ValueError(f"Unable to dump {self.__class__.__name__} to data (dev bug:{__file__}:{inspect.currentframe().f_code.co_name})")  # type: ignore
        return data

    @classmethod
    def load_base_conf(cls) -> dict[str, Any] | None:
        conf_path_str = os.getenv("PYSBATCH_USR_CONF_PATH")
        if conf_path_str is not None:
            conf_path = Path(conf_path_str)
            conf = load_conf(conf_path)
            if conf is not None:
                return conf
        cjs = Path("~/.config/pysbatch.json")
        ctm = Path("~/.config/pysbatch.toml")
        if cjs.exists():
            with cjs.open('r') as fp: return json.load(fp)
        elif ctm.exists():
            with ctm.open('r') as fp: return toml.load(fp)
        else: return None

    @classmethod
    def load_current_conf(cls, cwd: Path | None = None) -> dict[str, Any] | None:
        if cwd is None: cwd = Path.cwd()
        cjs = (cwd / "pysbatch.json")
        ctm = (cwd / "pysbatch.toml")
        if cjs.exists():
            with cjs.open('r') as fp: return json.load(fp)
        elif ctm.exists():
            with ctm.open('r') as fp: return toml.load(fp)
        else: return None

    @classmethod
    def load(cls, cwd: Path | None = None) -> "Sbatch":
        if cwd is None: cwd = Path.cwd()
        base_conf = Sbatch.load_base_conf()
        curr_conf = Sbatch.load_current_conf(cwd)
        conf = {}
        if base_conf is not None:
            conf.update(base_conf)
        if curr_conf is not None:
            conf.update(curr_conf)
        return Sbatch.from_schema(conf)


class SbatchSchema(Schema):
    default_options = fields.Nested(OptionsSchema, missing=None)
    platform = fields.Nested(PlatformSchema, missing=None)
    cwd = FieldPath(missing=Path.cwd(), default=None)

    @post_load
    def make_sbatch(self, data, **kwargs) -> Sbatch:
        return Sbatch(**data)


def_conf_name: str = "sbatch.toml"


def main():
    configure_logger('screen')
    parser = argparse.ArgumentParser(prog="sbatch", description="Only configuration checks and dumps via CLI currently")
    parser.add_argument("--cwd", action="store", type=str)
    parser.add_argument("--genconf", action="store_true")
    parser.add_argument("--drop_defs", action="store_true")

    args = parser.parse_args()

    cwd = Path.cwd()
    if args.cwd is not None:
        cwd = Path(args.cwd).resolve()

    sbatch = Sbatch.load(cwd)
    if not args.drop_defs:
        sbatch = Sbatch()

    if args.genconf:
        print(toml.dumps(sbatch.dump_schema()))


if __name__ == "__main__":
    sys.exit(main())
