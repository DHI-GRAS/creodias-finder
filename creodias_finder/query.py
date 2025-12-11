import datetime

import dateutil.parser
import requests
from shapely.geometry import shape

API_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1"
ATTRIBUTE_URL = (
    "https://catalogue.dataspace.copernicus.eu/odata/v1/Attributes({collection})"
)


def query(
    collection,
    start_date=None,
    end_date=None,
    geometry=None,
    status="ONLINE",
    metadata=False,
    **kwargs,
):
    """Query the EOData Finder API

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
    status : str
        allowed online/offline/all status (ONLINE || OFFLINE || ALL)
    **kwargs
        Additional arguments can be used to specify other query parameters,
        e.g. productType=L1GT
        See https://documentation.dataspace.copernicus.eu/APIs/OpenSearch.html for details

    Returns
    -------
    dict[string, dict]
        Products returned by the query as a dictionary with the product ID as the key and
        the product's attributes (a dictionary) as the value.
    """
    query_url = _build_query(
        collection,
        start_date,
        end_date,
        geometry,
        status,
        **kwargs,
    )
    if metadata:
        query_url += "&$expand=Attributes"

    query_response = {}
    while query_url:
        response = requests.get(query_url)
        response.raise_for_status()
        data = response.json()
        for feature in data["value"]:
            query_response[feature["Id"]] = feature
        query_url = data.get("@odata.nextLink")
    return query_response


def _build_query(
    collection=None,
    start_date=None,
    end_date=None,
    geometry=None,
    status=None,
    **kwargs,
):
    product_type = kwargs.get("productType")
    level = kwargs.get("level", 1)
    instrument = kwargs.get("instrument")

    if collection is None:
        raise ValueError(
            "You need to provide a collection. Check 'https://documentation.dataspace.copernicus.eu/APIs/OData.html#query-collection-of-products' for possible values"
        )
    match collection:
        case "Sentinel3":
            collection = "SENTINEL-3"
        case "Sentinel2":
            collection = "SENTINEL-2"
        case "Sentinel1":
            collection = "SENTINEL-1"

    collection = collection.upper()

    if product_type is None:
        ## use defaults
        if collection == "SENTINEL-2":
            if level == 1:
                product_type = "S2MSI1C"
            if level == 2:
                product_type = "S2MSI2A"

        if collection == "SENTINEL-3":
            if instrument is None:
                instrument = "OLCI"

            if instrument == "OLCI":
                if level == 1:
                    product_type = "OL_1_EFR___"
                if level == 2:
                    product_type = "OL_2_LFR___"
            if instrument == "SLSTR":
                if level == 1:
                    product_type = "SL_1_RBT___"

    query_list = []
    if geometry is not None:
        wkt = _parse_geometry(geometry)
        query_list.append(f"OData.CSC.Intersects(area=geography'SRID=4326;{wkt}')")

    if start_date is not None:
        start_date = _parse_date(start_date)
        query_list.append(f"ContentDate/Start gt '{start_date.isoformat()}'")

    if end_date is not None:
        end_date = _parse_date(end_date)
        end_date = _add_time(end_date)
        query_list.append(f"ContentDate/Start lt '{end_date.isoformat()}'")

    if status is not None:
        if status == "ONLINE":
            query_list.append("Online eq true")
        elif status == "OFFLINE":
            query_list.append("Online eq false")

    response = requests.get(ATTRIBUTE_URL.format(collection=collection))
    response.raise_for_status()
    attr_dict = response.json()
    for key, value in sorted(kwargs.items()):
        attr_type = _parse_argtype(key, attr_dict=attr_dict)
        value = _parse_argvalue(value)
        if not attr_type:
            raise ValueError(f"Kwarg {key} wasn't found in allowed attributes")
        if attr_type == "String":
            query_list.append(
                f"Attributes/OData.CSC.{attr_type}Attribute/any(att:att/Name eq '{key}' and att/OData.CSC.{attr_type}Attribute/Value eq '{value}')"
            )
        else:
            if isinstance(value, (list, tuple)):
                query_list.append(
                    f"Attributes/OData.CSC.{attr_type}Attribute/any(att:att/Name eq '{key}' and att/OData.CSC.{attr_type}Attribute/Value ge {value[0]}) and "
                    + f"Attributes/OData.CSC.{attr_type}Attribute/any(att:att/Name eq '{key}' and att/OData.CSC.{attr_type}Attribute/Value le {value[1]})"
                )
            else:
                query_list.append(
                    f"Attributes/OData.CSC.{attr_type}Attribute/any(att:att/Name eq '{key}' and att/OData.CSC.{attr_type}Attribute/Value eq {value})"
                )

    ## create query url
    query = f"{API_URL}/Products?$filter={' and '.join(query_list)}&$top=500"
    return query


def _parse_argtype(key, attr_dict):
    for obj in attr_dict:
        if obj.get("Name") == key:
            return obj.get("ValueType")


def _parse_argvalue(value):
    if isinstance(value, (list, tuple)):
        # Handle value ranges
        if len(value) == 2:
            return value
        else:
            raise ValueError(
                "Invalid number of elements in list. Expected 2, received {}".format(
                    len(value)
                )
            )
    else:
        return value


def _parse_date(date):
    if isinstance(date, datetime.datetime):
        return date
    elif isinstance(date, datetime.date):
        return datetime.datetime.combine(date, datetime.time())
    try:
        return dateutil.parser.parse(date)
    except ValueError:
        raise ValueError(
            "Date {date} is not in a valid format. Use Datetime object or iso string"
        )


def _add_time(date):
    if date.hour == 0 and date.minute == 0 and date.second == 0:
        date = date + datetime.timedelta(hours=23, minutes=59, seconds=59)
        return date
    return date


def _tastes_like_wkt_polygon(geometry):
    try:
        return geometry.replace(", ", ",").replace(" ", "", 1).replace(" ", "+")
    except Exception:
        raise ValueError("Geometry must be in well-known text format")


def _parse_geometry(geom):
    try:
        # If geom has a __geo_interface__
        return shape(geom).wkt
    except AttributeError:
        if _tastes_like_wkt_polygon(geom):
            return geom
        raise ValueError(
            "geometry must be a WKT polygon str or have a __geo_interface__"
        )
