#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from sync.accounts import sync_accounts


if __name__ == '__main__':
    sync_accounts()
