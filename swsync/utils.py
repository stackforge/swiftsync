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
import ConfigParser
import logging
import os


CONFIG = None
curdir = os.path.abspath(os.path.dirname(__file__))
INIFILE = os.path.abspath(os.path.join(curdir, '..', 'etc', "config.ini"))
SAMPLE_INIFILE = os.path.abspath(os.path.join(curdir, '..',
                                              'etc', "config.ini-sample"))
REVERSE = False


class ConfigurationError(Exception):
    pass


def set_logging(level):
    logger = logging.getLogger()
    logger.setLevel({
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR,
        'critical': logging.CRITICAL}.get(
            level.lower()
        ))
    loghandler = logging.StreamHandler()
    logger.addHandler(loghandler)
    logger = logging.LoggerAdapter(logger, 'swfiller')
    logformat = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    loghandler.setFormatter(logformat)


def parse_ini(inicfg=None):
    if hasattr(inicfg, 'read'):
        fp = inicfg
    elif inicfg and os.path.exists(inicfg):
        fp = open(inicfg)
    elif inicfg is None and os.path.exists(INIFILE):
        fp = open(INIFILE)
    else:
        raise ConfigurationError("Cannot find inicfg")

    config = ConfigParser.RawConfigParser()
    config.readfp(fp)
    return config


def get_config(section, option, default=None, _config=None):
    """Get section/option from ConfigParser or print default if specified."""
    global CONFIG
    if _config:
        CONFIG = _config
    elif not CONFIG:
        CONFIG = parse_ini()

    if not CONFIG.has_section(section):
        raise ConfigurationError("Invalid configuration, missing section: %s" %
                                 section)
    if CONFIG.has_option(section, option):
        return CONFIG.get(section, option)
    elif default is not None:
        return default
    else:
        raise ConfigurationError("Invalid configuration, missing "
                                 "section/option: %s/%s" % (section, option))
