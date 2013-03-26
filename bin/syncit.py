#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import sync.accounts


if __name__ == '__main__':
    sync.accounts.main()
