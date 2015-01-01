# -*- coding: utf-8 -*-
# Copyright (C) 2013 eNovance SAS <licensing@enovance.com>
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
import cStringIO as StringIO

import swsync.utils
import tests.units.base as test_base


class TestAccount(test_base.TestCase):
    def test_parse_ini_file_not_found(self):
        self.stubs.Set(swsync.utils.os.path, 'exists',
                       lambda x: False)
        self.assertRaises(swsync.utils.ConfigurationError,
                          swsync.utils.parse_ini, "/tmp/foo")

    def test_parse_ini_bad_file(self):
        s = StringIO.StringIO("foo=bar")
        self.assertRaises(ConfigParser.MissingSectionHeaderError,
                          swsync.utils.parse_ini, s)

    def test_parse_ini(self):
        s = StringIO.StringIO("[foo]\nfoo=bar")
        self.assertIsInstance(swsync.utils.parse_ini(s),
                              ConfigParser.RawConfigParser)

    def test_get_config(self):
        s = StringIO.StringIO("[foo]\nkey=bar")
        cfg = swsync.utils.parse_ini(s)
        self.assertEqual(swsync.utils.get_config('foo', 'key', _config=cfg),
                         'bar')

    def test_get_config_no_section(self):
        s = StringIO.StringIO("[pasla]\nkey=bar")
        cfg = swsync.utils.parse_ini(s)
        self.assertRaises(swsync.utils.ConfigurationError,
                          swsync.utils.get_config,
                          'foo', 'key', _config=cfg)

    def test_get_config_with_default(self):
        s = StringIO.StringIO("[foo]\n")
        cfg = swsync.utils.parse_ini(s)
        self.assertEqual(swsync.utils.get_config('foo', 'key', default='MEME',
                                                 _config=cfg),
                         'MEME')

    def test_get_config_auto_parsed(self):
        s = StringIO.StringIO("[foo]\nkey=bar")
        cfg = swsync.utils.parse_ini(s)
        self.stubs.Set(swsync.utils, 'CONFIG', cfg)
        self.assertEqual(swsync.utils.get_config('foo', 'key'), 'bar')

    def test_get_config_no_value(self):
        s = StringIO.StringIO("[foo]\n")
        cfg = swsync.utils.parse_ini(s)
        self.assertRaises(swsync.utils.ConfigurationError,
                          swsync.utils.get_config,
                          'foo', 'key', _config=cfg)

    def test_reversed_option_default_false(self):
        self.assertEqual(swsync.utils.REVERSE, False)
