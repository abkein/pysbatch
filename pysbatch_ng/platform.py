import getpass
from typing import Any
from dataclasses import dataclass
from marshmallow import Schema, fields, post_load, validates, ValidationError

from .execs import Execs, ExecsSchema
from .utils import ranges, shell, logger
from .dumbdata import TimeSpec, Partition, NodeDict


@dataclass
class Node:
    name: str
    idx: int

    def stringify(self) -> str:
        return f"{self.name}_{self.idx}"

    def tpl(self) -> tuple[str, int]:
        return (self.name, self.idx)

    @classmethod
    def from_string(cls, string: str):
        instance = cls.__new__(cls)

        name, idx_str = string.split('_')
        instance.name = name
        instance.idx = int(idx_str)
        return instance


def node_tpl2str(name: str, idx: int) -> str:
    return f"{name}_{idx}"


def node_str2tpl(node: str) -> tuple[str, int]:
    name, idx_str = node.split('_')
    return (name, int(idx_str))


class Platform:
    execs: Execs
    usr_nodes_include: NodeDict | None = None
    usr_nodes_exclude: NodeDict | None = None
    nodes_include: NodeDict
    nodes_exclude: NodeDict
    nodelist: NodeDict
    partitions: list[Partition]

    def __init__(self, execs: Execs | None = None, nodes_include: str | None = None, nodes_exclude: str | None = None) -> None:
        super().__init__()
        self.execs = execs if execs is not None else Execs()
        if nodes_exclude is not None: self.usr_nodes_exclude = NodeDict.parse_str(nodes_exclude)
        if nodes_include is not None: self.usr_nodes_include = NodeDict.parse_str(nodes_include)
        self.__update()

    def __update(self) -> bool:
        if not self.execs.check():
            logger.error("Could not find some executables")
            return False

        logger.debug("Getting nodelist")
        self.nodelist = self.get_nodelist()
        logger.info(f"Following nodes were found: {self.nodelist}")

        logger.debug("Getting partitions list")
        self.partitions = self.get_partitions()
        logger.info(f"Following partitions were found: {[s.PartitionName for s in self.partitions]}")

        long_usr_nodes_include: set[str] = set()
        if self.usr_nodes_include is not None:
            for name, ids in self.usr_nodes_include.items(): long_usr_nodes_include.update({node_tpl2str(name, id) for id in ids})

        long_usr_nodes_exclude: set[str] = set()
        if self.usr_nodes_exclude is not None:
            for name, ids in self.usr_nodes_exclude.items(): long_usr_nodes_exclude.update({node_tpl2str(name, id) for id in ids})

        long_nodelist: set[str] = set()
        for name, ids in self.nodelist.items(): long_nodelist.update({node_tpl2str(name, id) for id in ids})

        was_empty = len(long_usr_nodes_include) == 0

        nonexistent = (long_usr_nodes_include - long_nodelist) | (long_usr_nodes_exclude - long_nodelist)
        if len(nonexistent) != 0:
            logger.warning(f"Nonexistent nodes found in configuration: {nonexistent}")
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

        new_include: dict[str, set[int]] = {}
        for k,v in [node_str2tpl(_node) for _node in long_usr_nodes_include]:
            if k not in new_include: new_include[k] = set()
            new_include[k].add(v)
        self.nodes_include  = NodeDict(new_include)

        new_exclude: dict[str, set[int]] = {}
        for k,v in [node_str2tpl(_node) for _node in long_usr_nodes_exclude]:
            if k not in new_exclude: new_exclude[k] = set()
            new_exclude[k].add(v)
        self.nodes_exclude  = NodeDict(new_exclude)

        return True

    def get_nodelist(self) -> NodeDict:
        bout, berr = shell.exec([f"{self.execs.sinfo}", "-h", "--hide", "-o", "%N"])
        return NodeDict.parse_str(bout)

    def get_partitions(self) -> list[Partition]:
        bout, berr = shell.exec([f"{self.execs.scontrol}", "show", "partitions"])
        return Partition.parse_multiple(bout)

    def get_default_partition(self) -> Partition | None:
        for part in self.partitions:
            if part.Default: return part
        return None

    def get_partition(self, name: str) -> Partition | None:
        for part in self.partitions:
            if part.PartitionName == name: return part
        return None

    def get_active_jobids(self) -> list[int]:
        """
        Returns a list of current active SLURM JOBIDs launched by the current user.
        """
        username = getpass.getuser()

        bout, berr = shell.exec([f'{self.execs.squeue}', '-u', username, '-h', '--format=%A'])

        job_ids: list[int] = []
        for line in bout.splitlines():
            line = line.strip()
            if line is not None:
                try:
                    job_ids.append(int(line))
                except ValueError:
                    continue
        return job_ids

    @property
    def exclude_str(self) -> str:
        s = ""
        for k, v in self.nodes_exclude.items():
            for a, b in ranges(v):
                if a == b:
                    s += f"{k}{a},"
                else:
                    s += f"{k}[{a}-{b}],"
        return s[:-1]

    @classmethod
    def from_schema(cls, data: dict[str, Any]) -> "Platform":
        platform = PlatformSchema().load(data)
        if not isinstance(platform, Platform):
            raise ValueError(f"Unable to load {cls.__name__} from data (dev bug:{__file__}:{inspect.currentframe().f_code.co_name})")  # type: ignore
        return platform

    def dump_schema(self) -> dict[str, Any]:
        data = PlatformSchema().dump(self)
        if not isinstance(data, dict):
            raise ValueError(f"Unable to dump {self.__class__.__name__} to data (dev bug:{__file__}:{inspect.currentframe().f_code.co_name})")  # type: ignore
        return data


class PlatformSchema(Schema):
    execs = fields.Nested(ExecsSchema, missing=None)
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
