#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (c) 2023-2024 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 21-10-2024 04:36:43

import subprocess
from dataclasses import dataclass
from enum import StrEnum


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
            if state.value == state_str:
                return state
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


if __name__ == "__main__":
    pass
