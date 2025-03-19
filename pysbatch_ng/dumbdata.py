#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (c) 2023-2024 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 21-10-2024 04:36:43

import re
from enum import StrEnum
from dataclasses import dataclass


class SStates(StrEnum):
    BOOT_FAIL = "BOOT_FAIL"
    CANCELLED = "CANCELLED"
    COMPLETED = "COMPLETED"
    CONFIGURING = "CONFIGURING"
    COMPLETING = "COMPLETING"
    DEADLINE = "DEADLINE"
    FAILED = "FAILED"
    NODE_FAIL = "NODE_FAIL"
    OUT_OF_MEMORY = "OUT_OF_MEMORY"
    PENDING = "PENDING"
    PREEMPTED = "PREEMPTED"
    RUNNING = "RUNNING"
    RESV_DEL_HOLD = "RESV_DEL_HOLD"
    REQUEUE_FED = "REQUEUE_FED"
    REQUEUE_HOLD = "REQUEUE_HOLD"
    REQUEUED = "REQUEUED"
    RESIZING = "RESIZING"
    REVOKED = "REVOKED"
    SIGNALING = "SIGNALING"
    SPECIAL_EXIT = "SPECIAL_EXIT"
    STAGE_OUT = "STAGE_OUT"
    STOPPED = "STOPPED"
    SUSPENDED = "SUSPENDED"
    TIMEOUT = "TIMEOUT"
    UNKNOWN_STATE = "UNKNOWN_STATE"

    @staticmethod
    def from_string(state_str):
        for state in SStates:
            if state.value == state_str: return state
        return SStates.UNKNOWN_STATE


class SStatesShort(StrEnum):
    PENDING = "PD"
    RUNNING = "R"
    SUSPENDED = "S"
    COMPLETED = "CD"
    CANCELLED = "CA"
    FAILED = "F"
    TIMEOUT = "TO"
    NODE_FAIL = "NF"
    PREEMPTED = "PR"
    BOOT_FAIL = "BF"
    DEADLINE = "DL"
    OUT_OF_MEMORY = "OOM"
    COMPLETING = "CG"
    CONFIGURING = "CF"
    RESV_DEL_HOLD = "RD"
    REQUEUE_FED = "RF"
    REQUEUE_HOLD = "RH"
    REQUEUED = "RQ"
    RESIZING = "RS"
    REVOKED = "RV"
    SIGNALING = "SI"
    SPECIAL_EXIT = "SE"
    STAGE_OUT = "SO"
    STOPPED = "ST"
    UNKNOWN_STATE = "?"

    @staticmethod
    def from_string(state_str):
        for state in SStatesShort:
            print(f"Comparing: {state.value} and {state_str}")
            if state.value in state_str:
                print("Match")
                return state
        return SStatesShort.UNKNOWN_STATE


def get_job_state_description(state: SStates) -> str:
    """Return a description for the given SlurmJobState."""
    descriptions = {
        SStates.PENDING: "Job is waiting for resource allocation.",
        SStates.RUNNING: "Job currently has an allocation.",
        SStates.SUSPENDED: "Job has an allocation but execution has been suspended.",
        SStates.COMPLETED: "Job has terminated all processes on all nodes.",
        SStates.CANCELLED: "Job was explicitly cancelled by the user or system administrator.",
        SStates.FAILED: "Job terminated with a non-zero exit code or other failure condition.",
        SStates.TIMEOUT: "Job terminated upon reaching its time limit.",
        SStates.NODE_FAIL: "Job terminated due to a failure of one or more allocated nodes.",
        SStates.PREEMPTED: "Job terminated due to preemption.",
        SStates.BOOT_FAIL: "Job terminated due to a failure in booting up the allocated node.",
        SStates.DEADLINE: "Job terminated due to deadline violation.",
        SStates.OUT_OF_MEMORY: "Job experienced an out of memory error.",
        SStates.COMPLETING: "Job is in the process of completing.",
        SStates.CONFIGURING: "Job is being configured.",
        SStates.RESV_DEL_HOLD: "Job is in reservation delete hold.",
        SStates.REQUEUE_FED: "Job is being requeued by a federation.",
        SStates.REQUEUE_HOLD: "Held job is being requeued.",
        SStates.REQUEUED: "Job is requeued.",
        SStates.RESIZING: "Job is about to change size.",
        SStates.REVOKED: "Sibling was removed from cluster due to other cluster starting the job.",
        SStates.SIGNALING: "Job is being signaled.",
        SStates.SPECIAL_EXIT: "Job terminated with a special exit code.",
        SStates.STAGE_OUT: "Job is staging out files.",
        SStates.STOPPED: "Job has been stopped.",
        SStates.UNKNOWN_STATE: "Job state is unknown or pysbatch-ng was unable to determine it.",
    }

    return descriptions.get(state, "Unknown job state.")


@dataclass
class SlurmJobInfo:
    job_id: str
    job_name: str
    partition: str
    user: str
    account: str
    n_nodes: int
    state: SStates
    exit_code: int
    signal: int


class NodeDict(dict):
    def __init__(self, data: dict[str, set[int]] | None = None) -> None:
        super().__init__()
        if data is not None: self.update(data)

    @classmethod
    def parse_str(cls, nodelist_str: str) -> 'NodeDict':
        if not re.match(r"^\s*?([a-z]+(?:\[[\d\-\,\s]*\]|\d+)(?:\,\s*?)?)*\s*?$", nodelist_str): raise RuntimeError(f"Invalid nodelist: {nodelist_str}")
        nodedict: dict[str, set[int]] = {}
        groupspecs: list[tuple[str, str]] = re.findall(r"([a-z]+)(\[(?:(?:\d+|\d+\-\d+)(?:\,\s*?)?)*\]|\d+)", nodelist_str.replace(" ", ""))
        for hostname, numspec in groupspecs:
            if hostname not in nodedict: nodedict[hostname] = set()
            if re.match(r"\d+", numspec): nodedict[hostname].add(int(numspec))
            else:
                for st in numspec[1:-1].split(','):
                    nums: tuple[int, ...] = tuple(map(lambda x: int(x), st.split('-')))
                    match len(nums):
                        case 1: nodedict[hostname].add(nums[0])
                        case 2: nodedict[hostname].update(range(nums[0], nums[1]+1))
                        case _: raise RuntimeError(f"Invalid numspec: '{numspec}'")
        return NodeDict(nodedict)

    def __str__(self) -> str:
        preformat: list[str] = []
        for hostname, nums in self.items():
            if len(nums) == 1:
                preformat.append(f"{hostname}{nums.pop()}")
                continue
            sorted_nums = sorted(nums)
            end = start = sorted_nums[0]
            numspecs: list[str] = []
            for num in sorted_nums[1:]:
                if num == end + 1: end = num
                else:
                    numspecs.append(f'{start}' if start == end else f'{start}-{end}')
                    start = end = num
            numspecs.append(f'{start}' if start == end else f'{start}-{end}')
            preformat.append(f"{hostname}[{', '.join(numspecs)}]")
        return ", ".join(preformat)

    def total_num(self) -> int:
        return sum([len(s) for s in self.values()])


class TimeSpec:
    unlimited: bool = False
    days: int = 0
    hours: int = 0
    minutes: int = 0
    seconds: int = -1

    def __init__(self, timespec: str | None = None) -> None:
        if timespec is None: return
        if timespec == "UNLIMITED": self.unlimited = True
        else:
            pattern = r"^[a-zA-Z\*]*\s+(?:(\d+)-)?(\d{1,2}):(\d{2}):?(?:(\d{2}))?$"
            match = re.match(pattern, timespec)
            if match:
                self.days = int(match.group(1)) if match.group(1) else 0
                self.hours = int(match.group(2)) if match.group(2) else 0
                self.minutes = int(match.group(3)) if match.group(3) else 0
                self.seconds = int(match.group(4)) if match.group(4) else 0
                if not (0 <= self.hours <= 23 and 0 <= self.minutes <= 59 and 0 <= self.seconds <= 59): raise RuntimeError(f"Invalid (time components out of range): {timespec}")
            else: raise RuntimeError(f"Time limit retrieved does not match regular expression: {timespec}")

    def to_seconds(self) -> int: return ((self.days * 24 + self.hours) * 60 + self.minutes) * 60 + self.seconds

    def to_minutes(self) -> int: return (self.days * 24 + self.hours) * 60 + self.minutes

    def to_hours(self) -> int: return self.days * 24 + self.hours

    def to_days(self) -> int: return self.days

    def to_weeks(self) -> int: return self.days // 7

    def to_minutes_f(self) -> float: return (self.days * 24 + self.hours) * 60 + self.minutes + self.seconds / 60

    def to_hours_f(self) -> float: return self.days * 24 + self.hours + (self.minutes + self.seconds / 60) / 60

    def to_days_f(self) -> float: return self.days + (self.hours + (self.minutes + self.seconds / 60) / 60) / 60

    def to_weeks_f(self) -> float: return self.to_days_f() / 7


class PartitionState(StrEnum):
    UP = "UP"
    DOWN = "DOWN"
    DRAIN = "DRAIN"
    INACTIVE = "INACTIVE"


class Partition:
    PartitionName:        str
    MinNodes:             int            | None = None
    MaxNodes:             int            | None = None
    QoS:                  str            | None = None
    MaxCPUsPerNode:       int            | None = None
    State:                PartitionState | None = None
    TotalCPUs:            int            | None = None
    TotalNodes:           int            | None = None
    Nodes:                NodeDict       | None = None  # ALL
    AllowGroups:          list[str]      | None = None  # ALL
    AllowAccounts:        list[str]      | None = None  # ALL
    AllocNodes:           NodeDict       | None = None  # ALL
    Default:              bool                  = False
    DisableRootJobs:      bool                  = False
    ExclusiveUser:        bool                  = False
    Hidden:               bool                  = False
    LLN:                  bool                  = False
    MaxTime:              TimeSpec              = TimeSpec()
    DefaultTime:          TimeSpec              = TimeSpec()

    AllowQos:             str            | None = None  # uneval
    GraceTime:            str            | None = None  # uneval
    PriorityJobFactor:    str            | None = None  # uneval
    PriorityTier:         str            | None = None  # uneval
    RootOnly:             str            | None = None  # uneval
    ReqResv:              str            | None = None  # uneval
    OverSubscribe:        str            | None = None  # uneval
    OverTimeLimit:        str            | None = None  # uneval
    PreemptMode:          str            | None = None  # uneval
    SelectTypeParameters: str            | None = None  # uneval
    JobDefaults:          str            | None = None  # uneval
    DefMemPerNode:        str            | None = None  # uneval
    MaxMemPerNode:        str            | None = None  # uneval

    unknown_args:   dict[str, str] = {}

    def __init__(self, **kwargs: str) -> None:
        for k, v in kwargs.items():
            match k:
                case "QoS":                  self.QoS                  = v
                case "PartitionName":        self.PartitionName        = v
                case "Default":
                    if v == "YES":           self.is_default           = True
                case "DisableRootJobs":
                    if v == "YES":           self.DisableRootJobs      = True
                case "ExclusiveUser":
                    if v == "YES":           self.ExclusiveUser        = True
                case "Hidden":
                    if v == "YES":           self.Hidden               = True
                case "LLN":
                    if v == "YES":           self.LLN                  = True
                case "MinNodes":             self.MinNodes             = int(v)
                case "MaxNodes":             self.MaxNodes             = int(v)
                case "TotalCPUs":            self.TotalCPUs            = int(v)
                case "TotalNodes":           self.TotalNodes           = int(v)
                case "MaxCPUsPerNode":       self.MaxCPUsPerNode       = int(v)
                case "MaxTime":              self.MaxTime              = TimeSpec(v)
                case "DefaultTime":          self.DefaultTime          = TimeSpec(v)
                case "State":                self.State                = PartitionState(v)
                case "Nodes":                self.Nodes                = NodeDict.parse_str(v)
                case "ReqResv":              self.ReqResv              = v
                case "RootOnly":             self.RootOnly             = v
                case "AllowQos":             self.AllowQos             = v
                case "GraceTime":            self.GraceTime            = v
                case "AllocNodes":
                    if v != "ALL":           self.AllocNodes           = NodeDict.parse_str(v)
                case "PreemptMode":          self.PreemptMode          = v
                case "AllowGroups":
                    if v != "ALL":           self.AllowGroups          = v.split(",")
                case "AllowAccounts":
                    if v != "ALL":           self.AllowAccounts        = v.split(",")
                case "JobDefaults":          self.JobDefaults          = v
                case "PriorityTier":         self.PriorityTier         = v
                case "OverTimeLimit":        self.OverTimeLimit        = v
                case "OverSubscribe":        self.OverSubscribe        = v
                case "DefMemPerNode":        self.DefMemPerNode        = v
                case "MaxMemPerNode":        self.MaxMemPerNode        = v
                case "PriorityJobFactor":    self.PriorityJobFactor    = v
                case "SelectTypeParameters": self.SelectTypeParameters = v
                case _: self.unknown_args[k] = v
        if not hasattr(self, "PartitionName"):
            raise RuntimeError("No PartitionName specified")

    @classmethod
    def parse_str(cls, partition_str: str) -> 'Partition':
        return Partition(**{kv[0]: kv[1] for ss in partition_str.splitlines() for s in ss.strip().split() for kv in s.strip().split('=')})


    @classmethod
    def parse_multiple(cls, out_str: str) -> list['Partition']:
        return [Partition.parse_str(s.strip()) for s in out_str.strip().split('\n\n')]


all_states = [
    SStates.BOOT_FAIL,
    SStates.CANCELLED,
    SStates.COMPLETED,
    SStates.CONFIGURING,
    SStates.COMPLETING,
    SStates.DEADLINE,
    SStates.FAILED,
    SStates.NODE_FAIL,
    SStates.OUT_OF_MEMORY,
    SStates.PENDING,
    SStates.PREEMPTED,
    SStates.RUNNING,
    SStates.RESV_DEL_HOLD,
    SStates.REQUEUE_FED,
    SStates.REQUEUE_HOLD,
    SStates.REQUEUED,
    SStates.RESIZING,
    SStates.REVOKED,
    SStates.SIGNALING,
    SStates.SPECIAL_EXIT,
    SStates.STAGE_OUT,
    SStates.STOPPED,
    SStates.SUSPENDED,
    SStates.TIMEOUT,
]


states_str = [
    "BOOT_FAIL",
    "CANCELLED",
    "COMPLETED",
    "CONFIGURING",
    "COMPLETING",
    "DEADLINE",
    "FAILED",
    "NODE_FAIL",
    "OUT_OF_MEMORY",
    "PENDING",
    "PREEMPTED",
    "RUNNING",
    "RESV_DEL_HOLD",
    "REQUEUE_FED",
    "REQUEUE_HOLD",
    "REQUEUED",
    "RESIZING",
    "REVOKED",
    "SIGNALING",
    "SPECIAL_EXIT",
    "STAGE_OUT",
    "STOPPED",
    "SUSPENDED",
    "TIMEOUT",
]


failure_states = [
    SStates.BOOT_FAIL,
    SStates.DEADLINE,
    SStates.NODE_FAIL,
    SStates.OUT_OF_MEMORY,
    SStates.STOPPED,
    SStates.FAILED,
    SStates.CANCELLED,
]


states_to_end = [
    SStates.COMPLETED,
    SStates.TIMEOUT,
]


if __name__ == "__main__": pass
