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
from urlparse import urlparse, urljoin

from django.conf import settings as django_settings

from wstore.asset_manager.resource_plugins.plugin import Plugin
from wstore.asset_manager.resource_plugins.plugin_error import PluginError
from wstore.model import User

from keyrock_client import KeyrockClient
from umbrella_client import UmbrellaClient

from settings import UNITS


class NGSIDataset(Plugin):

    def __init__(self, plugin_model):
        super(NGSIDataset, self).__init__(plugin_model)
        self._units = UNITS

    def _get_access_token(self):
        user = User.objects.get(name=self._user_id)
        return user.userprofile.access_token

    def create_dataset(self, product, data_url, data_info):
        name = product['name'].lower().replace(' ', '-')
        description = ''
        if 'description' in product and product['description'] is not None:
            description = product['description']

        service = ''
        if 'service' in data_info['service'] and data_info['service'] is not None:
            service = data_info['service']

        service_path = ''
        if 'service_path' in data_info['service_path'] and data_info['service_path'] is not None:
            service_path = data_info['service_path']

        # Build URL query using data info
        url = data_url
        if not data_url.endswith('/'):
            data_url = data_url + '/'

        data_url = data_url + 'v2/entities'
        query = ''

        if 'entities' in data_info and data_info['entities'] is not None:
            query = '?type=' + data_info['entities']

        if 'attrs' in data_info and data_info['attrs'] is not None:
            if query == '':
                query = query + '?attrs=' + data_info['attrs']
            else:
                query = query + '&attrs=' + data_info['attrs']

        if 'expression' in data_info and data_info['expression'] is not None:
            if query == '':
                query = query + '?' + data_info['expression']
            else:
                query = query + '&' + data_info['expression']

        data_url = data_url + query

        dataset_info = {
	        "private": True,
	        "acquire_url": "",
	        "name": name,
	        "title": product["name"],
	        "notes": description,
	        "isopen": True,
            "searchable": "True",
            "resources": [{
                "auth_type": "oauth2",
                "entity": [],
                "format": "fiware-ngsi",
                "name": "NGSI query",
                "url": data_url,
                "tenant": data_info['service'],
                "service_path": data_info['service_path']
            }]
        }

        ckan_url = data_info['ckan_url']
        if not ckan_url.endswith('/'):
            ckan_url = ckan_url + '/'

        ckan_url = ckan_url + 'api/3/action/package_create'

        resp = requests.post(ckan_url, json=dataset_info, headers={
            'Authorization': 'Bearer ' + self._get_access_token()
        })

        if resp.status_code != 200:
            raise PluginError('It had not being possible to create CKAN dataset')

        return resp.json()['result']

    def update_dataset_acquire_url(self, url, dataset_id, product_id):
        # Get updated dataset info
        ckan_url = url
        if not ckan_url.endswith('/'):
            ckan_url = ckan_url + '/'

        show_url = ckan_url + 'api/3/action/package_show?id={}'.format(dataset_id)
        resp = requests.post(show_url, headers={
            'Authorization': 'Bearer ' + self._get_access_token()
        })

        if resp.status_code != 200:
            raise PluginError('It has not been posible to update acquire URL')

        dataset_info = resp.json()['result']

        # Generate Acquire URL
        parsed_site = urlparse(django_settings.SITE)
        acquire_url = '{}://{}/#/offering?productSpecId={}'.format(parsed_site.schema, parsed_site.netloc, product_id)

        dataset_info['acquire_url'] = acquire_url

        update_url = ckan_url + 'api/3/action/package_update?id={}'.format(dataset_id)
        resp = requests.post(show_url, json=dataset_info, headers={
            'Authorization': 'Bearer ' + self._get_access_token()
        })

        if resp.status_code != 200:
            raise PluginError('It has not been posible to update acquire URL')

    def on_post_product_spec_validation(self, provider, asset):
        self._user_id = provider.name

        parsed_url = urlparse(asset.get_url())

        # Validate that the provided URL is a valid API in API Umbrella
        client = UmbrellaClient()
        app_id = client.validate_service(parsed_url.path)

        # Check that the provider is authorized to create an offering in the current App
        keyrock_client = KeyrockClient()
        keyrock_client.check_ownership(app_id, provider.name)

        # Check that the provided role is registered in the specified App
        keyrock_client.check_role(app_id, asset.meta_info['role'])

        asset.meta_info['app_id'] = app_id
        asset.save()

    def update_product(self, product_spec, data_info):
        product_url = django_settings.CATALOG
        if not product_url.endswith('/'):
            product_url = product_url + '/'
        
        product_url = product_url + 'api/catalogManagement/v2/productSpecification/{}'.format(product_spec['id'])
        charact = product_spec['productSpecCharacteristic']

        if 'entities' in data_info and data_info['entities'] is not None:
            charact.append({
                'configurable': False,
                'description': 'NGSI Entities provided'
                'name': 'Entities'
                'productSpecCharacteristicValue': [{
                    'value': data_info['entities'], 'unitOfMeasure': "", 'valueFrom': "", 'valueTo': "", 'default': True}]
                'valueType': "string"
            })

        if 'attrs' in data_info and data_info['attrs'] is not None:
            charact.append({
                'configurable': False,
                'description': 'NGSI Atributtes provided'
                'name': 'Attributes'
                'productSpecCharacteristicValue': [{
                    'value': data_info['attrs'], 'unitOfMeasure': "", 'valueFrom': "", 'valueTo': "", 'default': True}]
                'valueType': "string"
            })

        if 'expression' in data_info and data_info['expression'] is not None:
            charact.append({
                'configurable': False,
                'description': 'NGSI Expression'
                'name': 'Expression'
                'productSpecCharacteristicValue': [{
                    'value': data_info['expression'], 'unitOfMeasure': "", 'valueFrom': "", 'valueTo': "", 'default': True}]
                'valueType': "string"
            })

        requests.patch(url, json={
            'productSpecCharacteristic': charact
        })

    def on_post_product_spec_attachment(self, asset, asset_t, product_spec):
        self._user_id = asset.provider.name
        # Include NGSI specific info as characteristics of the product
        # If CKAN URL has been included register a new dataset
        if 'ckan_url' in asset.meta_info and asset.meta_info['ckan_url'] is not None and asset.meta_info['ckan_url'] != '':
            dataset = self.create_dataset(product_spec, asset.get_url(), asset.meta_info)
            asset.meta_info['dataset_id'] = dataset['id']
            asset.save()

        #self.update_product(product_spec, asset.meta_info)

    def on_post_product_offering_validation(self, asset, product_offering):
        self._user_id = asset.provider.name

        # Validate that the pay-per-use model (if any) is supported by the backend
        if 'productOfferingPrice' in product_offering:
            has_usage = False
            supported_units = [unit['name'].lower() for unit in self._units]

            for price_model in product_offering['productOfferingPrice']:
                if price_model['priceType'] == 'usage':
                    has_usage = True

                    if price_model['unitOfMeasure'].lower() not in supported_units:
                        raise PluginError('Unsupported accounting unit ' +
                                          price_model['unit'] + '. Supported units are: ' + ','.join(supported_units))

        # If CKAN dataset has been registered, attach acquisition URL
        if 'dataset_id' in asset.meta_info:
            self.update_dataset_acquire_url(data_info['ckan_url'], asset.meta_info['dataset_id'], asset.product_id)

    def activate_ckan_dataset(self, ckan_url, dataset_id, customer):
        notification_url = urljoin(ckan_url, '/api/action/package_acquired')
        dataset_url = urljoin(ckan_url, '/dataset/{}'.format(dataset_id))

        # Build notification data
        data = {
            'customer_name': customer,
            'resources': [{
                'url': dataset_url
            }]
        }

        # Notify the dataset acquisition to CKAN
        headers = {'Content-type': 'application/json'}
        response = requests.post(
            notification_url,
            json=data,
            headers=headers,
            verify=django_settings.VERIFY_REQUESTS,
            cert=(django_settings.NOTIF_CERT_FILE, django_settings.NOTIF_CERT_KEY_FILE)
        )
        response.raise_for_status()

    def on_product_acquisition(self, asset, contract, order):
        self._user_id = order.owner_organization.name

        # Activate API resources
        client = KeyrockClient()
        client.grant_permission(order.customer, asset.meta_info['role'])

        if 'dataset_id' in asset.meta_info:
            # User need to be included in the authorized users list of the dataset
            self.activate_ckan_dataset(
                asset.meta_info['ckan_url'], asset.meta_info['asset_id'], order.owner_organization.name)

    def on_product_suspension(self, asset, contract, order):
        self._user_id = order.owner_organization.name

        # Suspend API Resources
        client = KeyrockClient()
        client.revoke_permission(order.customer, asset.meta_info['role'])

    def get_usage_specs(self):
        return self._units

    def get_pending_accounting(self, asset, contract, order):
        return []

