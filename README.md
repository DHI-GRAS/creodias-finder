# creodias-finder

A Python library for querying and downloading Earth observation products from the
[Copernicus Data Space Ecosystem (CDSE)](https://dataspace.copernicus.eu/), using CDSE's
[OData API](https://documentation.dataspace.copernicus.eu/APIs/OData.html).

**Note:**
The name of the package (creodias-finder) comes from its heritage, when it was used to search for
data stored in the Creodias service. With CDSE reaching maturity it was decided to start using the
Copernicus service instead. To download data, an [account on CDSE](https://dataspace.copernicus.eu/)
is required.

This is a proof of concept, not thoroughly tested or fully developed. You are welcome to use it
and to submit pull requests fixing bugs you find.

## What it does

- `creodias_finder.query` — builds and executes OData queries against the CDSE catalogue
  (`https://catalogue.dataspace.copernicus.eu/odata/v1`), filtering by collection, date range,
  geometry (WKT string or an object implementing `__geo_interface__`), online/offline status, and
  arbitrary collection-specific attributes (e.g. `productType`, `cloudCover`).
- `creodias_finder.download` — downloads a single product or a list of products through CDSE's
  zipper service, authenticating with a CDSE username/password against CDSE's Keycloak token
  endpoint. Downloads can run multithreaded and show `tqdm` progress bars.
- `creodias_finder.download` (S3 variants) — downloads a product, or a list of products, directly
  from CDSE's S3-compatible object storage (`data.cloudferro.com`). This only works when run from
  a machine on the CreoDIAS/CDSE infrastructure.
- `creodias_finder.creodias_storage.S3Storage` — a thin wrapper around a `boto3` S3 client used by
  the S3 download functions to list and fetch the files that make up a product.

## Tech stack / dependencies

- Python 3.10+ (the query module uses `match`/`case` statements)
- [requests](https://pypi.org/project/requests/)
- [tqdm](https://pypi.org/project/tqdm/) — download progress bars
- [python-dateutil](https://pypi.org/project/python-dateutil/) — flexible date parsing
- [shapely](https://pypi.org/project/Shapely/) — geometry handling for spatial queries
- [boto3](https://pypi.org/project/boto3/) — S3 downloads
- [six](https://pypi.org/project/six/)

## Installation

Install directly from GitHub:

```bash
pip install git+https://github.com/DHI-GRAS/creodias-finder.git
```

Or clone and install locally:

```bash
git clone git@github.com:DHI-GRAS/creodias-finder.git
cd creodias-finder
pip install .
```

## Usage

Query Sentinel-1 products for a given time range:

```python
from datetime import datetime

from creodias_finder import query

results = query.query(
    "Sentinel1",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2019, 1, 2),
)
```

`results` is a dictionary keyed by product ID, where each value is the full OData product record
returned by CDSE (fields such as `Id`, `Name`, `S3Path`, `ContentDate`, `Online`, ...). Pass
`metadata=True` to also expand each product's `Attributes`.

Download selected products (pass the full product records returned by `query`, not just their
IDs):

```python
from creodias_finder import download

products = list(results.values())

CREDENTIALS = {
    "username": "my-cdse-email",
    "password": "my-cdse-password",
}

# download a single product
download.download(products[0], outfile="/home/user/data/file.zip", **CREDENTIALS)

# download a list of products, multithreaded
download.download_list(products[1:11], outdir="/home/user/data", threads=10, **CREDENTIALS)
```

When running on CreoDIAS/CDSE infrastructure, products can instead be pulled directly from S3
storage (no CDSE credentials required):

```python
from creodias_finder import download

download.download_from_s3(products[0], outdir="/home/user/data")
download.download_list_from_s3(products[1:11], outdir="/home/user/data", threads=5)
```

## Testing

The tests in `tests/` exercise the download functions against the real CDSE service and require
the `CREODIAS_USERNAME` and `CREODIAS_PASSWORD` environment variables to be set. Run with:

```bash
pytest
```

## License

Distributed under the MIT License. See [LICENSE](LICENSE) for details.

## Changelog

15-12-2025 - BREAKING CHANGES; updated the API to use CDSE's OData API instead of OpenSearch, due
to the [deprecation](https://dataspace.copernicus.eu/news/2025-10-16-opensearch-catalogue-api-decommissioning-notice)
of OpenSearch starting in 2026. Query results and download arguments changed accordingly — pass
full query result objects (not bare IDs) to the download functions.
