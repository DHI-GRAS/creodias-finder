import datetime
from six.moves.urllib.parse import urljoin, urlencode
from six import string_types
import requests
import dateutil.parser
from shapely.geometry import shape

import re

API_URL = 'http://finder.creodias.eu/resto/api/collections/{collection}/search.json?maxRecords=1000'
ONLINE_STATUS_CODES = '34|37|0'


class RequestError(Exception):
    def __init__(self, message, errors):
        super().__init__(message)
        self.errors = errors


def query(collection, start_date=None, end_date=None,
          geometry=None, status=ONLINE_STATUS_CODES, progress_bar=True, **kwargs):
    """ Query the EOData Finder API

    Parameters
    ----------
    collection: str, optional
        the data collection, corresponding to various satellites
    start_date: str or datetime
        the start date of the observations, either in iso formatted string or datetime object
    end_date: str or datetime
        the end date of the observations, either in iso formatted string or datetime object
        if no time is specified, time 23:59:59 is added.
    geometry: WKT polygon or object impementing __geo_interface__
        area of interest as well-known text string
    **kwargs
        Additional arguments can be used to specify other query parameters,
        e.g. productType=L1GT
        See https://creodias.eu/eo-data-finder-api-manual for a full list

    Returns
    -------
    dict[string, dict]
        Products returned by the query as a dictionary with the product ID as the key and
        the product's attributes (a dictionary) as the value.
    """
    query_str = _build_query(
        API_URL.format(collection=collection),
        start_date,
        end_date,
        geometry,
        status,
        **kwargs
    )

    query_response = {}
    while query_str:
        response = requests.get(query_str)
        response.raise_for_status()
        data = response.json()
        for feature in data['features']:
            query_response[feature['id']] = feature
        query_str = _find_next(data['properties']['links'])
    return query_response


def _build_query(base_url, start_date=None, end_date=None, geometry=None, status=None, **kwargs):
    query_params = {}



    if start_date is not None:
        start_date = _parse_date(start_date)
        query_params['startDate'] = start_date.isoformat()
    if end_date is not None:
        end_date = _parse_date(end_date)
        end_date = _add_time(end_date)
        query_params['completionDate'] = end_date.isoformat()

    if geometry is not None:
        query_params['geometry'] = _parse_geometry(geometry)

    if status is not None:
        query_params['status'] = status

    for attr, value in sorted(kwargs.items()):
        value = _parse_argvalue(value)
        query_params[attr] = value

    if query_params:
        base_url += f'&{urlencode(query_params)}'
    return base_url


def _find_next(links):
    for link in links:
        if link['rel'] == 'next':
            return link['href']
    return False


def _parse_date(date):
    if isinstance(date, datetime.datetime):
        return date
    try:
        return dateutil.parser.parse(date)
    except ValueError:
        raise ValueError('Date {date} is not in a valid format. Use Datetime object or iso string')


def _add_time(date):
    if date.hour == 0 and date.minute == 0 and date.second == 0:
        date = date + datetime.timedelta(hours=23, minutes=59, seconds=59)
        return date
    return date


def _tastes_like_wkt_polygon(geometry):
    try:
        return geometry.replace(", ", ",").replace(" ", "", 1).replace(" ", "+")
    except Exception:
        raise ValueError('Geometry must be in well-known text format')


def _parse_geometry(geom):
    try:
        # If geom has a __geo_interface__
        return shape(geom).wkt
    except AttributeError:
        if _tastes_like_wkt_polygon(geom):
            return geom
        raise ValueError('geometry must be a WKT polygon str or have a __geo_interface__')


def _parse_argvalue(value):

    if isinstance(value, string_types):
        value = value.strip()
        if not any(
            value.startswith(s[0]) and value.endswith(s[1])
            for s in ["[]", "{}", "//", "()"]
        ):
            value = re.sub(r"\s", r"\ ", value, re.M)
        return value

    elif isinstance(value, (list, tuple)):
        # Handle value ranges
        if len(value) == 2:
            value = "[{},{}]".format(*value)
            return value
        else:
            raise ValueError(
                "Invalid number of elements in list. Expected 2, received "
                "{}".format(len(value))
            )

    else:
        raise ValueError(
            "Additional arguments can be either string or tuple/list of 2 values"
        )
