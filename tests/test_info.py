#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 23-09-2024 03:28:01

import unittest

from pysbatch_ng import info


class TestInfo(unittest.TestCase):

    def test_parse_timelimit(self):
        self.assertEqual(info.parse_timelimit("UNLIMITED"), -1)
        self.assertEqual(info.parse_timelimit("10:00:00"), 36000)
        self.assertEqual(info.parse_timelimit("10:30:00"), 37800)
        self.assertEqual(info.parse_timelimit("1-00:00:00"), 86400)
        self.assertEqual(info.parse_timelimit("2-00:00:00"), 172800)
        self.assertEqual(info.parse_timelimit("2-10:30:15"), 210615)

        with self.assertRaises(RuntimeError):
            info.parse_timelimit("24:00:00")  # Invalid (hours > 23)

        with self.assertRaises(RuntimeError):
            info.parse_timelimit("1-25:30:00")  # Invalid (hours > 23)

        with self.assertRaises(RuntimeError):
            info.parse_timelimit("30:00")  # Invalid (missing seconds)

        with self.assertRaises(RuntimeError):
            info.parse_timelimit("-10:00:00")  # Invalid (negative days)

        with self.assertRaises(RuntimeError):
            info.parse_timelimit("invalid-format")

    def test_parse_nodes(self):
        self.assertEqual(info.parse_nodes("host[1-5]"), {'host': {1, 2, 3, 4, 5}})
        self.assertEqual(info.parse_nodes("host[1-5,7,9-11]"), {'host': {1, 2, 3, 4, 5, 7, 9, 10, 11}})
        self.assertEqual(info.parse_nodes("host[1-5],node[1-3]"), {'host': {1, 2, 3, 4, 5}, 'node': {1, 2, 3}})
        self.assertEqual(info.parse_nodes("host[1-5,7],node[1-3,8-10]"), {'host': {1, 2, 3, 4, 5, 7}, 'node': {1, 2, 3, 8, 9, 10}})

        with self.assertRaises(RuntimeError):
            info.parse_nodes("host[1-5")  # Invalid format

        with self.assertRaises(RuntimeError):
            info.parse_nodes("host[1-5],")  # Invalid format

        with self.assertRaises(RuntimeError):
            info.parse_nodes("host[1-5]node[1-3]")  # Missing comma


if __name__ == "__main__":
    unittest.main()
