#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (c) 2023-2024 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 26-10-2024 09:32:44

import re
import sys
import argparse
from typing import Any
from pathlib import Path
from dataclasses import dataclass

import toml
from marshmallow import Schema, fields, post_load, validates, ValidationError

from .utils import ranges, wexec, parse_nodes, parse_timelimit, FieldPath, log
from .execs import CMDSchema, Execs, ExecsSchema, CMD
from .polling import Poller


regex_sbatch_jobid = r'Submitted batch jobid (\d+)'


@dataclass
class Options:
    cmd: CMD | None = None
    job_name: str | None = None
    nnodes:   int | None = None
    ntasks_per_node: int | None = None
    partition: str | None = None
    folder: str = "slurm"
    job_number: int | None = None
    tag: int | None = None

    def check(self, strict: bool) -> bool:
        logger = log.get_logger()
        logger.debug(f"┌─Slurm folder: {self.folder}")
        logger.debug(f"├─Job name:     {self.job_name}")
        logger.debug(f"├─Nodes:        {self.nnodes}")
        logger.debug(f"├─N tasks/node: {self.ntasks_per_node}")
        logger.debug(f"├─Partition:    {self.partition}")
        if self.cmd is not None:
            logger.debug(f"└─CMD:")
            logger.debug(f"  ├─Preload:    {self.cmd.preload}")
            logger.debug(f"  ├─Executable: {self.cmd.executable}")
            logger.debug(f"  └─Args:       {self.cmd.args}")

        if strict:
            if self.cmd is None:
                logger.error(f"CMD is not specified")
                return False
            if not self.cmd.check():
                logger.error(f"Misconfigured CMD")
                return False

        return True

    @property
    def job_folder_rel(self) -> str:
        bd = Path()
        if self.job_number is None:
            return (bd / self.folder / f"{self.job_name}").relative_to(bd).as_posix()
        else:
            return (bd / self.folder / f"{self.job_name}_{self.job_number}").relative_to(bd).as_posix()

    def job_folder(self, cwd: Path) -> Path:
        return cwd / self.job_folder_rel


class OptionsSchema(Schema):
    cmd = fields.Nested(CMDSchema, allow_none=True, missing=None, read_only=True)
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


@dataclass
class Node:
    name: str
    idx: int

    def stringify(self) -> str:
        return f"{self.name}_{self.idx}"

    @classmethod
    def from_string(cls, string: str):
        instance = cls.__new__(cls)

        name, idx_str = string.split('_')
        instance.name = name
        instance.idx = int(idx_str)
        return instance


class Platform:
    execs: Execs = Execs()
    usr_nodes_include: dict[str, list[int]] = {}
    usr_nodes_exclude: dict[str, list[int]] = {}
    nodes_include: dict[str, set[int]] = {}
    nodes_exclude: dict[str, set[int]] = {}
    nodelist: dict[str, set[int]] = {}
    partitions: set[str] = set()

    def __init__(self, execs: Execs = Execs(), nodes_include: dict[str, list[int]] = {}, nodes_exclude: dict[str, list[int]] = {}) -> None:
        super().__init__()
        self.execs = execs
        self.usr_nodes_exclude = nodes_exclude
        self.usr_nodes_include = nodes_include

    def update(self, strict: bool) -> bool:
        logger = log.get_logger()
        if not self.execs.check(strict):
            logger.error("Could not find some executables")
            return False

        logger.debug("Getting nodelist")
        self.nodelist = self.get_nodelist()
        logger.info(f"Following nodes were found: {self.nodelist}")

        logger.debug("Getting partitions list")
        self.partitions = self.get_partitions()
        logger.info(f"Following partitions were found: {self.partitions}")

        long_usr_nodes_include: set = set()
        for name, ids in self.usr_nodes_include.items():
            if isinstance(ids, int):
                long_usr_nodes_include.update({Node(name, id).stringify() for id in self.nodelist[name]})
            else:
                long_usr_nodes_include.update({Node(name, id).stringify() for id in ids})

        long_usr_nodes_exclude: set = set()
        for name, ids in self.usr_nodes_exclude.items():
            if isinstance(ids, int):
                long_usr_nodes_exclude.update({Node(name, id).stringify() for id in self.nodelist[name]})
            else:
                long_usr_nodes_exclude.update({Node(name, id).stringify() for id in ids})

        long_nodelist: set = set()
        for name, ids in self.nodelist.items():
            long_nodelist.update({Node(name, id).stringify() for id in ids})

        was_empty = len(long_usr_nodes_include) == 0

        nonexistent = (long_usr_nodes_include - long_nodelist) | (long_usr_nodes_exclude - long_nodelist)
        if len(nonexistent) != 0:
            logger.info(f"Nonexistent nodes found in configuration: {nonexistent}")
            long_usr_nodes_include.difference_update(nonexistent)
            long_usr_nodes_exclude.difference_update(nonexistent)
        if len(long_usr_nodes_include & long_usr_nodes_exclude) != 0:
            logger.error(f"There were nodes both in include and exclude lists: {long_usr_nodes_include & long_usr_nodes_exclude}")
            return False

        if was_empty:
            logger.info(f"Include nodelist is empty, asumming use all, except exlude nodelist")
            long_usr_nodes_include = long_nodelist
            long_usr_nodes_include.difference_update(long_usr_nodes_exclude)

        long_usr_nodes_exclude = long_nodelist - long_usr_nodes_include

        if len(long_usr_nodes_include) == 0:
            logger.error("No nodes left to run on. Check your excludes and includes")
            return False

        self.nodes_include = {}
        for node_str in long_usr_nodes_include:
            _node = Node.from_string(node_str)
            if _node.name not in self.nodes_include:
                self.nodes_include[_node.name] = set()
            self.nodes_include[_node.name].add(_node.idx)

        self.nodes_exclude = {}
        for node_str in long_usr_nodes_exclude:
            _node = Node.from_string(node_str)
            if _node.name not in self.nodes_exclude:
                self.nodes_exclude[_node.name] = set()
            self.nodes_exclude[_node.name].add(_node.idx)

        return True

    def get_nodelist(self) -> dict[str, set[int]]:
        cmd = f"{self.execs.sinfo} -h --hide -o %N"
        bout, berr = wexec(cmd)

        return parse_nodes(bout)

    def get_partitions(self) -> set[str]:
        cmd = f"{self.execs.sinfo} -h --hide -o %P"
        bout, berr = wexec(cmd)
        partitions = []
        for el in bout.split():
            partitions.append(el.replace("*", ""))

        return set(partitions)

    def get_timelimit(self, partition: str) -> int:
        cmd = f"{self.execs.sinfo} -o '%P %l' --partition={partition}"
        bout, berr = wexec(cmd)

        try:
            s = bout.splitlines()[1]
            limit = parse_timelimit(s)
            return limit
        except Exception as e:
            logger = log.get_logger()
            logger.error("Unable to get timelimit")
            logger.exception(e)
        return 0

    @property
    def exclude_str(self):
        s = ""
        for k, v in self.nodes_exclude.items():
            for a, b in ranges(v):
                if a == b:
                    s += f"{k}{a},"
                else:
                    s += f"{k}[{a}-{b}],"
        return s[:-1]

    @classmethod
    def from_schema(cls, data: dict[str, Any], immidiate_update: bool = False, strict: bool = False):
        schema = PlatformSchema()
        platform = schema.load(data)
        if not isinstance(platform, Platform):
            raise ValueError("")
        if immidiate_update:
            if not platform.update(strict):
                raise RuntimeError("")


class PlatformSchema(Schema):
    execs = fields.Nested(ExecsSchema, missing=Execs())
    nodes_include_dump = fields.Dict(
        keys=fields.Str(),
        values=fields.List(fields.Int()),
        missing={},
        default={},
        attribute="usr_nodes_include",
        data_key="nodes_include",
        dump_only=True
    )
    nodes_exclude_dump = fields.Dict(
        keys=fields.Str(),
        values=fields.List(fields.Int()),
        missing={},
        default={},
        attribute="usr_nodes_exclude",
        data_key="nodes_exclude",
        dump_only=True
    )

    nodes_include_load = fields.Dict(
        keys=fields.Str(),
        values=fields.List(fields.Int()),
        missing={},
        default={},
        attribute="nodes_include",
        data_key="nodes_include",
        load_only=True
    )
    nodes_exclude_load = fields.Dict(
        keys=fields.Str(),
        values=fields.List(fields.Int()),
        missing={},
        default={},
        attribute="nodes_exclude",
        data_key="nodes_exclude",
        load_only=True
    )

    @post_load
    def make_platform_conf(self, data, **kwargs):
        return Platform(**data)

    @validates('nodes_include_load')
    def validate_usr_nodes_include(self, value: dict[str, list[int]]):
        for key, node_list in value.items():
            if not isinstance(key, str):
                raise ValidationError("All keys must be strings.")
            if not isinstance(node_list, list):
                raise ValidationError(f"Value for key '{key}' must be a list of integers.")
            if not all(isinstance(node, int) for node in node_list):
                raise ValidationError(f"All node IDs in '{key}' must be integers.")

    @validates('nodes_exclude_load')
    def validate_usr_nodes_exclude(self, value: dict[str, list[int]]):
        for key, node_list in value.items():
            if not isinstance(key, str):
                raise ValidationError("All keys must be strings.")
            if not isinstance(node_list, list):
                raise ValidationError(f"Value for key '{key}' must be a list of integers.")
            if not all(isinstance(node, int) for node in node_list):
                raise ValidationError(f"All node IDs in '{key}' must be integers.")


class Sbatch:
    options: Options = Options()
    platform: Platform = Platform()
    cwd: Path

    def __init__(self, options: Options = Options(), platform: Platform = Platform(), cwd: Path = Path.cwd()) -> None:
        super().__init__()
        self.options = options
        self.platform = platform
        self.cwd = cwd

    def check(self, strict: bool) -> bool:
        logger = log.get_logger()
        if not self.options.check(strict):
            logger.error("Some options are invalid")
            return False

        if not self.platform.update(strict):
            logger.error("There was some misconfiguration")
            return False

        logger.info("Configuration OK")
        return True

    def run(self, run_poll: bool = False, poller: Poller | None = None, poll_cmd: CMD | None = None) -> int:
        """Runs sbatch command via creating .job file

        Args:
            cwd (Path): current working directory
            logger (logging.Logger): Logger object
            conf (config): configuration
            number (Optional[int]): Number of task. At this stage there is no jobid yet, so it used instead. Defaults to None.
            add_conf (Optional[Dict]): Additional configuration, it merged to main configuration. Defaults to None.

        Raises:
            RuntimeError: Raised if sbatch command not returned jobid (or function cannot parse it from output)

        Returns:
            jobid (int): slurm's jobid
        """
        if not self.check(True):
            raise RuntimeError("Configuration check failed")

        tdir = self.options.job_folder(self.cwd)
        tdir.mkdir(parents=True, exist_ok=True)

        job_file = tdir / f"{self.options.job_name}.job"
        log.configure('both', tdir / "sbatch_launch.log")
        logger = log.get_logger()
        logger.debug('Configuring...')

        if run_poll:
            if poller is None:
                if poll_cmd is None:
                    raise RuntimeError("cmd cannot be None")
                poller = Poller(
                    cmd = poll_cmd,
                    debug=True,
                    logto='file',
                    tag=self.options.tag,
                    logfolder=self.options.job_folder_rel,
                    lockfilename=f"{self.options.tag}.lock" if self.options.tag is not None else None,
                    cwd=self.cwd,
                    execs=self.platform.execs,
                )
            poller.check(False)

        with job_file.open('w') as fh:
            fh.writelines("#!/usr/bin/env bash\n")
            fh.writelines(f"#SBATCH --job-name={self.options.job_name}\n")
            fh.writelines(f"#SBATCH --output={tdir}/{self.options.job_name}.out\n")
            fh.writelines(f"#SBATCH --error={tdir}/{self.options.job_name}.err\n")
            fh.writelines("#SBATCH --begin=now\n")
            if self.options.nnodes is not None:
                fh.writelines(f"#SBATCH --nodes={self.options.nnodes}\n")
            if self.options.ntasks_per_node is not None:
                fh.writelines(f"#SBATCH --ntasks-per-node={self.options.ntasks_per_node}\n")
            if self.options.partition is not None:
                fh.writelines(f"#SBATCH --partition={self.options.partition}\n")
            if len(self.platform.nodes_exclude) != 0:
                fh.writelines(f"#SBATCH --exclude={self.platform.exclude_str}\n")
            assert self.options.cmd is not None
            if self.options.cmd.preload == "":
                fh.writelines(f"srun -u {self.options.cmd.executable} {self.options.cmd.args}")
            else:
                fh.writelines(f"{self.options.cmd.preload} srun -u {self.options.cmd.executable} {self.options.cmd.args}")

        logger.info("Submitting task...")
        cmd = f"{self.platform.execs.sbatch} {job_file}"
        bout, berr = wexec(cmd)

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
            assert poller is not None
            poller.jobid = jobid
            poller.check(True)
            poller.detach_start()
            logger.info("Poller started")

        return jobid

    @classmethod
    def from_schema(cls, data: dict[str, Any], immidiate_check: bool = False, strict: bool = False):
        schema = SbatchSchema()
        sbatch = schema.load(data)
        if not isinstance(sbatch, Sbatch):
            raise ValueError("")
        if immidiate_check:
            if not sbatch.check(strict):
                raise RuntimeError("")
        return sbatch


class SbatchSchema(Schema):
    options = fields.Nested(OptionsSchema)
    platform = fields.Nested(PlatformSchema)
    cwd = FieldPath(missing=Path.cwd(), default=Path.cwd())

    @post_load
    def make_sbatch(self, data, **kwargs):
        return Sbatch(**data)


def_conf_name: str = "sbatch.toml"


def main():
    log.configure('screen')
    parser = argparse.ArgumentParser(prog="sbatch", description="Only configuration checks and dumps via CLI currently")
    parser.add_argument("--cwd", action="store", type=str)
    parser.add_argument("-c", "--conf", action="store", type=str)
    parser.add_argument("--genconf", action="store_true")
    parser.add_argument("--checkconf", action="store_true")
    parser.add_argument("-s", "--strict", action="store_true")

    args = parser.parse_args()

    cwd = Path.cwd()
    if args.cwd is not None:
        cwd = Path(args.cwd).resolve()

    conffile = cwd / def_conf_name
    if args.conf is not None:
        conffile = Path(args.conf).resolve()

    d: dict[str, Any]
    if args.genconf:
        sb = Sbatch(
            options=Options(
                CMD(
                    executable="echo",
                    args="'Hello, world!'",
                ),
                "echoer",
                1, 1, "test",
                tag=293431
            ),
            cwd=cwd
        )
        _d = SbatchSchema().dump(sb)
        assert isinstance(_d, dict)
        d = _d
        with conffile.open('w') as fp:
            toml.dump(d, fp)

    if args.checkconf:
        with conffile.open('r') as fp:
            d: dict[str, Any] = toml.load(fp)
        sbatch = Sbatch.from_schema(d)
        sbatch.check(args.strict)


if __name__ == "__main__":
    sys.exit(main())
