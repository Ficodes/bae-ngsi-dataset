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
from urlparse import urljoin

from wstore.asset_manager.resource_plugins.plugin_error import PluginError

from settings import UMBRELLA_URL, UMBRELLA_KEY, UMBRELLA_TOKEN

class UmbrellaClient(object):

    def __init__(self):
        self._server = UMBRELLA_URL
        self._token = UMBRELLA_TOKEN
        self._key = UMBRELLA_KEY

        self._accounting_processor = {
            'api call': self._process_call_accounting
        }

    def _make_request(self, path, method, **kwargs):
        url = urljoin(self._server, path)
        try:
            resp = method(url, **kwargs)
        except requests.ConnectionError:
            raise PermissionDenied('Invalid resource: API Umbrella server is not responding')

        if resp.status_code == 404:
            raise PluginError('The provided Umbrella resource does not exist')
        elif resp.status_code != 200:
            raise PluginError('Umbrella gives an error accessing the provided resource')

        return resp

    def _get_request(self, path):
        resp = self._make_request(path, requests.get, headers={
            'X-Api-Key': self._key,
            'X-Admin-Auth-Token': self._token
        }, verify=False)

        return resp.json()

    def _paginate_data(self, url, err_msg, page_processor):
        page_len = 100
        start = 0
        processed = False
        matching_elem = None

        while not processed:
            result = self._get_request(url + '&start={}&length={}'.format(start, page_len))

            # There is no remaining elements
            if not len(result['data']):
                raise PluginError(err_msg)
            
            for elem in result['data']:
                processed = page_processor(elem)

                # The page element has been found
                if processed:
                    matching_elem = elem
                    break
            
            start += page_len

        return matching_elem

    def validate_service(self, path):
        err_msg = 'The provided asset is not supported. ' \
                  'Only services protected by API Umbrella are supported'

        # Split the path of the service 
        paths = [p for p in path.split('/') if p != '']
        if not len(paths):
            # API umbrella resources include a path for matching the service
            raise PluginError(err_msg)

        # Make paginated requests to API umbrella looking for the provided paths
        url = '/api-umbrella/v1/apis.json?search[value]={}&search[regex]=false'.format(paths[0])
        def page_processor(api):
            front_path = [p for p in api['frontend_prefixes'].split('/') if p != '']
            return len(front_path) <= len(paths) and front_path == paths[:len(front_path)]

        matching_elem = self._paginate_data(url, err_msg, page_processor)

        # If the API is configured to accept access tokens from an external IDP save its external id
        app_id = None
        if 'idp_app_id' in matching_elem['settings'] and len(matching_elem['settings']['idp_app_id']):
            app_id = matching_elem['settings']['idp_app_id']

        return app_id

    def _process_call_accounting(self, params, parsed_url):
        pass
