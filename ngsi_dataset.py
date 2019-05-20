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

from urlparse import urlparse

from wstore.asset_manager.resource_plugins.plugin import Plugin
from wstore.asset_manager.resource_plugins.plugin_error import PluginError

from keyrock_client import KeyrockClient
from umbrella_client import UmbrellaClient

from settings import UNITS


class NGSIDataset(Plugin):

    def __init__(self, plugin_model):
        super(NGSIDataset, self).__init__(plugin_model)
        self._units = UNITS

    def create_dataset(self, data_info):
        pass

    def on_post_product_spec_validation(self, provider, asset):
        parsed_url = urlparse(asset.get_url())

        # Validate that the provided URL is a valid API in API Umbrella
        client = UmbrellaClient()
        app_id = client.validate_service(parsed_url.path)

        # Check that the provider is authorized to create an offering in the current App
        keyrock_client = KeyrockClient()
        keyrock_client.check_ownership(app_id, provider.name)

        # Check that the provided role is registered in the specified App
        keyrock_client.check_role(app_id, asset.meta_info['role'])

        # If CKAN URL has been included register a new dataset
        if 'ckan_url' in asset.meta_info and asset.meta_info['ckan_url'] is not None and asset.meta_info['ckan_url'] != '':
            self.create_dataset(asset.meta_info)

        asset.meta_info['app_id'] = app_id
        asset.save()

    def on_post_product_spec_attachment(self, asset, asset_t, product_spec):
        # Include NGSI specific info as characteristics of the product
        pass

    def on_post_product_offering_validation(self, asset, product_offering):
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

    def on_product_acquisition(self, asset, contract, order):
        # Activate API resources
        client = KeyrockClient()
        client.grant_permission(order.customer, asset.meta_info['role'])

    def on_product_suspension(self, asset, contract, order):
        # Suspend API Resources
        client = KeyrockClient()
        client.revoke_permission(order.customer, asset.meta_info['role'])

    def get_usage_specs(self):
        return self._units

    def get_pending_accounting(self, asset, contract, order):
        pass
