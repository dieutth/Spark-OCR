#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os

dirpath = os.getcwd()
parent_path = os.path.abspath(os.path.join(dirpath, os.pardir))

INPUT_PATH = "{parent_path}/resources/input".format(parent_path=parent_path)
OUTPUT_PATH = "{parent_path}/resources/output".format(parent_path=parent_path)
