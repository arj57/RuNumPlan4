#! /usr/bin/python
# -*- coding: utf-8 -*-
import os


def get_abs_path(filename: str) -> str:
    if os.path.isabs(filename):
        abs_path = filename
    else:
        wd = os.path.abspath(os.path.curdir)
        src_basename = os.path.basename(filename)
        src_dirname = os.path.dirname(filename)
        if src_dirname:
            os.chdir(src_dirname)
        abs_path = os.path.abspath(src_basename)
        os.chdir(wd)
    return abs_path


class OptionalFieldEmptyException(Exception):
    pass
