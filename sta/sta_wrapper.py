# =================================================================
#
# Authors: Benjamin Webb <benjamin.miller.webb@gmail.com>
#
# Copyright (c) 2021 Benjamin Webb
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

from requests import Session, get, codes
from requests.compat import urljoin

from json.decoder import JSONDecodeError

ENTITY = {
    'Thing', 'Things', 'Observation', 'Observations',
    'Location', 'Locations', 'Sensor', 'Sensors',
    'Datastream', 'Datastreams', 'ObservedProperty',
    'ObservedProperties', 'FeatureOfInterest', 'FeaturesOfInterest',
    'HistoricalLocation', 'HistoricalLocations'
    }

ENTITY_LOCATION = {
    'Things': 'Locations/location', 
    'Observations': 'FeatureOfInterest/feature',
    'Locations': 'location', 
    'Sensors': 'Datastreams/Thing/Locations/location',
    'Datastreams': 'Thing/Locations/location',
    'ObservedProperties': 'Datastreams/Thing/Locations/location', 
    'FeaturesOfInterest': 'feature',
    'HistoricalLocations': 'Locations/location'
    }

class SensorThingsProvider:
    """SensorThings API (STA) Provider
    """

    def __init__(self, provider_def):
        """
        STA Class constructor

        :param provider_def: provider definitions from yml pygeoapi-config.
                             data,id_field, name set in parent class

        :returns: SensorThingsProvider
        """
        try:
            self._url = provider_def['data']
            self.time_field = provider_def.get('timefield', None)
        except KeyError:
            raise RuntimeError('name/type/data are required')

    def query(self, startindex=0, limit=10, resulttype='results',
              bbox=[], datetime_=None, properties=[], sortby=[],
              select_properties=[], skip_geometry=False, q=None, **kwargs):
        """
        STA query

        :param startindex: starting record to return (default 0)
        :param limit: number of records to return (default 10)
        :param resulttype: return results or hit limit (default results)
        :param bbox: bounding box [minx,miny,maxx,maxy]
        :param datetime_: temporal (datestamp or extent)
        :param properties: list of tuples (name, value)
        :param sortby: list of dicts (property, order)
        :param select_properties: list of property names
        :param skip_geometry: bool of whether to skip geometry (default False)
        :param q: full-text search term(s)

        :returns: dict of GeoJSON FeatureCollection
        """

        return self._load(startindex, limit, resulttype, bbox=bbox,
                          datetime_=datetime_, properties=properties,
                          sortby=sortby, select_properties=select_properties,
                          skip_geometry=skip_geometry)

    def get(self, entity, identifier, **kwargs):
        """
        Query the STA by id

        :param identifier: feature id
        :returns: dict of single GeoJSON feature
        """
        return self._load(entity, identifier=identifier)

    def _load(self, entity, startindex=0, limit=10, resulttype='results',
              identifier=None, bbox=[], datetime_=None, properties=[],
              sortby=[], select_properties=[], skip_geometry=False, expand=None):
        """
        Private function: Load STA data

        :param entity: sensorthings entity
        :param startindex: starting record to return (default 0)
        :param limit: number of records to return (default 10)
        :param resulttype: return results or hit limit (default results)
        :param bbox: bounding box [minx,miny,maxx,maxy]
        :param datetime_: temporal (datestamp or extent)
        :param properties: list of tuples (name, value)
        :param sortby: list of dicts (property, order)
        :param select_properties: list of property names
        :param skip_geometry: bool of whether to skip geometry (default False)
        :param expand: full-text expand term(s)

        :returns: dict of GeoJSON FeatureCollection
        """
        feature_collection = {
            'type': 'FeatureCollection', 'features': []
        }
        # Make params
        params = {
            '$skip': str(startindex),
            '$top': str(limit),
            '$count': 'true'
        }
        if expand:
            params['$expand'] = expand
        if properties or bbox or datetime_:
            params['$filter'] = self._make_filter(entity, properties, bbox, datetime_)
        if sortby:
            params['$orderby'] = self._make_orderby(sortby)

        # Start session
        s = Session()

        # Form URL for GET request
        url = urljoin(self._url,entity)
        if identifier:
            r = s.get(f'{url}({identifier})', params=params)
        else:
            r = s.get(f'{url}', params=params)

        if r.status_code == codes.bad:
            print('Bad http response code')
            raise ConnectionError('Bad http response code')
        response = r.json()

        # if hits, return count
        if resulttype == 'hits':
            feature_collection['numberMatched'] = response.get('@iot.count')
            return feature_collection

        # Query if values are less than expected
        v = [response, ] if identifier else response.get('value')
        hits_ = 1 if identifier else min(limit, response.get('@iot.count'))
        while len(v) < hits_:
            next_ = response.get('@iot.nextLink', None)
            if next_ is None:
                break
            else:
                with s.get(next_) as r:
                    response = r.json()
                    v.extend(response.get('value'))

        # End session
        s.close()
        return v[:hits]

    def _make_filter(self, entity, properties, bbox=[], datetime_=None):
        """
        Private function: Make STA filter from query properties

        :param entity: sensorthings entity
        :param properties: list of tuples (name, value)
        :param bbox: bounding box [minx,miny,maxx,maxy]
        :param datetime_: temporal (datestamp or extent)

        :returns: STA $filter string of properties
        """
        ret = []
        for (name, value) in properties:
            if name in ENTITY:
                ret.append(f'{name}/@iot.id eq {value}')
            else:
                ret.append(f'{name} eq {value}')

        if bbox and entity in ENTITY_LOCATION.keys():
            location = ENTITY_LOCATION[entity]
            minx, miny, maxx, maxy = bbox
            bbox_ = f'POLYGON (({minx} {miny}, {maxx} {miny}, \
                     {maxx} {maxy}, {minx} {maxy}, {minx} {miny}))'
            ret.append(f"st_within({location}, geography'{bbox_}')")

        if datetime_ is not None:
            if self.time_field is None:
                print('time_field not enabled for collection')
                raise LookupError()

            if '/' in datetime_:
                time_start, time_end = datetime_.split('/')
                if time_start != '..':
                    ret.append(f'{self.time_field} ge {time_start}')
                if time_end != '..':
                    ret.append(f'{self.time_field} le {time_end}')
            else:
                ret.append(f'{self.time_field} eq {datetime_}')

        return ' and '.join(ret)

    @staticmethod
    def _make_orderby(sortby):
        """
        Private function: Make STA filter from query properties

        :param sortby: list of dicts (property, order)

        :returns: STA $orderby string
        """
        ret = []
        _map = {'+': 'asc', '-': 'desc'}
        for _ in sortby:
            prop, order = _['property'], _map[_['order']]
            if _['property'] in ENTITY:
                prop += '/@iot.id'
            ret = [f"{prop} {order}" ]
        return ','.join(ret)

    def __repr__(self):
        return '<SensorThingsProvider> {}'.format(self.data)
