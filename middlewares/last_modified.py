#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (C) 2013 eNovance SAS <licensing@enovance.com>
#
# Author: Fabien Boucher <fabien.boucher@enovance.com>
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import time
from swift.common.utils import get_logger
from swift.common.swob import Request, wsgify


class LastModified(object):
    """
    """

    def __init__(self, app, conf):
        self.app = app
        self.conf = conf
        self.logger = get_logger(self.conf, log_route='last_modified')

    def update_last_modified_meta(self, req, env):
        vrs, account, container, obj = req.split_path(1, 4, True)
        if obj:
            env['PATH_INFO'] = env['PATH_INFO'].split('/%s' % obj)[0]
        env['REQUEST_METHOD'] = 'POST'
        headers = {'X-Container-Meta-Last-Modified': str(time.time())}
        set_meta_req = Request.blank(env['PATH_INFO'],
                                     headers=headers,
                                     environ=env)
        return set_meta_req.get_response(self.app)

    def req_passthrough(self, req):
        return req.get_response(self.app)

    @wsgify
    def __call__(self, req):
        vrs, account, container, obj = req.split_path(1, 4, True)
        if req.method in ('POST', 'PUT') and container or \
         req.method == 'DELETE' and obj:
            new_env = req.environ.copy()
            user_resp = self.req_passthrough(req)
            if user_resp.status_int // 100 == 2:
                # Update Container Meta Last-Modified in case of
                # successful request
                update_resp = self.update_last_modified_meta(req,
                                                             new_env)
                if update_resp.status_int // 100 != 2:
                    self.logger.info('Unable to update Meta Last-Modified')
            return user_resp
        return self.app


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def last_modified_filter(app):
        return LastModified(app, conf)
    return last_modified_filter
