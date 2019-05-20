# -*- coding: utf-8 -*-

# Copyright (c) 2019 Future Internet Consulting and Development Solutions S.L.

# This file is part of BAE NGSI Dataset plugin.

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import unicode_literals

import requests
from urlparse import urlparse

from django.conf import settings as django_settings
from django.core.exceptions import PermissionDenied

from wstore.asset_manager.resource_plugins.plugin_error import PluginError

from settings import IDM_URL, IDM_PASSWORD, IDM_USER


class KeyrockClient(object):

    def __init__(self):
        self._login()

    def _login(self):
        body = {
            "name": IDM_USER,
            "password": IDM_PASSWORD
        }

        url = IDM_URL + '/v3/auth/tokens'
        response = requests.post(url, json=body, verify=django_settings.VERIFY_REQUESTS)

        response.raise_for_status()
        self._auth_token = response.headers['x-subject-token']

    def check_ownership(self, app_id, provider):
        path = '/v1/applications/{}/users/{}/roles'.format(app_id, provider)
        role_field = 'role_user_assignments'

        assingments_url = IDM_URL + path

        resp = requests.get(assingments_url, headers={
            'X-Auth-Token': self._auth_token
        }, verify=django_settings.VERIFY_REQUESTS)

        resp.raise_for_status()
        assingments = resp.json()

        for assingment in assingments[role_field]:
            if assingment['role_id'] == 'provider':
                break
        else:
            raise PermissionDenied('You are not the owner of the specified IDM application')

    def check_role(self, app_id, role_name):
        # Get available roles
        path = '/v1/applications/{}/roles'.format(app_id)
        roles_url = IDM_URL + path

        resp = requests.get(roles_url, headers={
            'X-Auth-Token': self._auth_token
        }, verify=django_settings.VERIFY_REQUESTS)

        # Get role id
        resp.raise_for_status()
        roles = resp.json()

        for role in roles['roles']:
            if role['name'].lower() == role_name.lower():
                role_id = role['id']
                break
        else:
            raise PluginError('The provided role is not registered in keystone')

        return role_id

    def grant_permission(self, app_id, user, role):
        # Get ids
        role_id = self.check_role(app_id, role)
        assign_url = IDM_URL + '/v1/applications/{}/users/{}/roles/{}'.format(app_id, user.username, role_id)

        resp = requests.post(assign_url, headers={
            'X-Auth-Token': self._auth_token,
            'Content-Type': 'application/json'
        }, verify=django_settings.VERIFY_REQUESTS)

        resp.raise_for_status()

    def revoke_permission(self, app_id, user, role):
        role_id = self.check_role(app_id, role)
        assign_url = IDM_URL + '/v1/applications/{}/users/{}/roles/{}'.format(app_id, user.username, role_id)

        resp = requests.delete(assign_url, headers={
            'X-Auth-Token': self._auth_token,
            'Content-Type': 'application/json'
        }, verify=django_settings.VERIFY_REQUESTS)

        resp.raise_for_status()
