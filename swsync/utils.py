# -*- encoding: utf-8 -*-
__author__ = "Chmouel Boudjnah <chmouel@chmouel.com>"
import os
import ConfigParser


CONFIG = {}
curdir = os.path.abspath(os.path.dirname(__file__))
INIFILE = os.path.abspath(os.path.join(curdir, '..', 'etc', "config.ini"))


class ConfigurationError(Exception):
    pass


def parse_ini(inifile=INIFILE):
    if not os.path.exists(inifile):
        raise ConfigurationError("Error while parsing inifile")

    config = ConfigParser.RawConfigParser()
    config.read(inifile)
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


if __name__ == '__main__':
    get_config("foo", "bar")
