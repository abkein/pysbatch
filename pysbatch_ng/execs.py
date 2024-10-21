#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (c) 2023-2024 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 21-10-2024 04:36:50

from typing import Any
from pathlib import Path
from dataclasses import dataclass

from marshmallow import Schema, fields, post_load

from .utils import is_exe, logs, logger


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
    sinfo:  str = "sinfo"
    sbatch: str = "sbatch"
    sacct:  str = "sacct"
    spoll:  str = "spoll"
    spolld: str = "spolld"

    @logs
    def check(self, strict: bool) -> bool:
        for exec in [self.sinfo, self.sbatch, self.sacct, self.spoll, self.spolld]:
            if not is_exe(exec):
                logger.error(f"Executable {exec} not found")
                return False
        return True

    @classmethod
    def from_schema(cls, data: dict[str, Any], immidiate_check: bool = False, strict: bool = False):
        schema = ExecsSchema()
        execs = schema.load(data)
        if not isinstance(execs, Execs):
            raise ValueError("")
        if immidiate_check:
            if not execs.check(strict):
                raise RuntimeError("")


class ExecsSchema(Schema):
    sinfo  = fields.String(missing="sinfo")
    sbatch = fields.String(missing="sbatch")
    sacct  = fields.String(missing="sacct")
    spoll  = fields.String(missing="spoll")
    spolld = fields.String(missing="spolld")

    @post_load
    def create_execs(self, data, **kwargs) -> Execs:
        return Execs(**data)


@dataclass
class CMD:
    preload: str = ""
    executable: str | None = None
    args: str = ""

    @logs
    def check(self) -> bool:
        if self.executable is None:
            logger.error(f"Executable is not specified")
            return False
        if not is_exe(self.executable):
            logger.error(f"Executable {self.executable} not found")
            return False

        return True

    def gen_line(self):
        return f"{self.preload} {self.executable} {self.args}"


class CMDSchema(Schema):
    preload = fields.String(missing="")
    executable = fields.String(allow_none=True, missing=None)
    args = fields.String(missing="")

    @post_load
    def create_cmd(self, data, **kwargs) -> CMD:
        return CMD(**data)


if __name__ == "__main__":
    pass

