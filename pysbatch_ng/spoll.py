#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (c) 2023-2024 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 26-10-2024 09:32:44

import sys
import argparse

from .polling import Poller
from .utils import log


def main() -> int:
    parser = argparse.ArgumentParser(prog="spoll")
    Poller.set_args(parser)

    # parser.add_argument("--tmpfile", action="store", type=str, help="If conffile wasn't specified, spoll creates its own conffile and forwards arguments from cmd to it. This option allows specify this conffile location. Default: [tag_]jobid_poll_conf.toml")
    parser.add_argument("--genconf", action="store_true", help="Generate sample configuration file and exit (Default file: ./spoll_sample_conf.toml), see also --file and --section options")
    parser.add_argument("--checkconf", action="store_true", help="Check configuration file and exit, see also --file and --section options")
    args = parser.parse_args()

    poller = Poller.from_args(args)

    logger = log.get_logger()
    if args.checkconf:
        if not poller.check(False):
            logger.error("Configuration check wasn't successfull")
            return 1
        elif args.checkconf:
            logger.info("Configuration check was successfull")
            return 0

    if args.genconf:
        poller.genconf(True)
        return 0

    poller.detach_start()

    return 0


if __name__ == "__main__":
    sys.exit(main())
