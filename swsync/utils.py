# -*- coding: utf-8 -*-
# Copyright (C) 2013 eNovance SAS <licensing@enovance.com>
#
# Author: Chmouel Boudjnah <chmouel@enovance.com>
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
import os
import ConfigParser


CONFIG = {}
curdir = os.path.abspath(os.path.dirname(__file__))
INIFILE = os.path.abspath(os.path.join(curdir, '..', 'etc', "config.ini"))


class ConfigurationError(Exception):
    pass


def parse_ini(inifile):
    if os.path.exists(inifile):
        fp = open(inifile)
    elif type(inifile) is file:
        fp = inifile
    else:
        raise ConfigurationError("Cannot found inifile")

    config = ConfigParser.RawConfigParser()
    config.readfp(fp)
    return config


def get_config(section, option, default=None):
    """Get section/option from ConfigParser or print default if specified"""
    global CONFIG

    if not CONFIG:
        CONFIG = parse_ini()

    if not CONFIG.has_section(section):
        raise ConfigurationError("Invalid configuration, missing section: %s" %
                                 section)
    if CONFIG.has_option(section, option):
        return CONFIG.get(section, option)
    elif not default is None:
        return default
    else:
        raise ConfigurationError("Invalid configuration, missing "
                                 "section/option: %s/%s" % (section, option))
