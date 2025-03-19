#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (c) 2023-2024 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 26-10-2024 09:32:44

import inspect
from typing import Any
from pathlib import Path
from dataclasses import dataclass

from marshmallow import Schema, fields, post_load

from .utils import is_exe, logger  # , ConfigurationError


class StrPath(fields.Field):
    def _deserialize(self, value: str, attr, data, **kwargs) -> Path | str:
        try:
            a = Path(value).resolve(True)
            return a
        except Exception:
            return value

    def _serialize(self, value: str | Path, attr, obj, **kwargs) -> str:
        return value.as_posix() if isinstance(value, Path) else value


@dataclass
class Execs:
    sinfo:    str = "sinfo"
    sbatch:   str = "sbatch"
    sacct:    str = "sacct"
    spoll:    str = "spoll"
    spolld:   str = "spolld"
    squeue:   str = "squeue"
    scontrol: str = "scontrol"

    # def __post_init__(self):
    #     if not self.check():
    #         raise ConfigurationError()

    def check(self) -> bool:
        for exec in [self.sinfo, self.sbatch, self.sacct, self.spoll, self.spolld, self.squeue, self.scontrol]:
            if not is_exe(exec):
                logger.error(f"Executable {exec} not found")
                return False
        return True

    @classmethod
    def from_schema(cls, data: dict[str, Any]) -> "Execs":
        execs = ExecsSchema().load(data)
        if not isinstance(execs, Execs): raise ValueError(f"Unable to load {cls.__name__} from data (dev bug:{__file__}:{inspect.currentframe().f_code.co_name})")  # type: ignore
        return execs

    def dump_schema(self) -> dict[str, Any]:
        data = ExecsSchema().dump(self)
        if not isinstance(data, dict): raise ValueError(f"Unable to dump {self.__class__.__name__} to data (dev bug:{__file__}:{inspect.currentframe().f_code.co_name})")  # type: ignore
        return data


class ExecsSchema(Schema):
    sinfo  = fields.String(missing="sinfo")
    sbatch = fields.String(missing="sbatch")
    sacct  = fields.String(missing="sacct")
    spoll  = fields.String(missing="spoll")
    spolld = fields.String(missing="spolld")
    squeue = fields.String(missing="squeue")
    scontrol = fields.String(missing="scontrol")

    @post_load
    def make_execs_conf(self, data, **kwargs) -> Execs: return Execs(**data)

if __name__ == "__main__":
    pass
