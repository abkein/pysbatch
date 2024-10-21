#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (c) 2023-2024 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 21-10-2024 04:36:43

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
