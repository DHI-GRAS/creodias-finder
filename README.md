# creodias-finder
Query the [CREO Data finder API](https://creodias.eu/eo-data-finder-api-manual) for available 
products.

## Usage
Exposes a single function `query`

```python
from creodias_finder.download import download, download_list
from creodias_finder.query import query
import geojson

import os

from datetime import datetime


q = query('Sentinel1',
      start_date=datetime(2019, 1, 1),
      end_date=datetime(2019, 1, 2))

ids = [q[key]['id'] for key in q.keys()]

username = os.environ['CREODIAS_USERNAME'] 
password = os.environ['CREODIAS_PASSWORD']

download(ids[0], username=username, password=password, outfile='/home/andreas/data/')

download_list(ids[1:11], username=username, password=password, threads=10)
```

```python
[
    ...,
    {'geometry': {'coordinates': [[[-66.400017, -65.643265],
                               [-58.727936, -63.775444],
                               [-56.687397, -65.090927],
                               [-64.635124, -67.052101],
                               [-66.400017, -65.643265]]],
              'type': 'Polygon'},
     'id': '639595ae-da84-5eac-a96d-aa5e323ca0e9',
     'properties': {'centroid': {'coordinates': [-61.543707, -65.4137725],
                                 'type': 'Point'},
                    'cloudCover': -1,
                    'collection': 'Sentinel1',
                    'completionDate': '2019-01-03T00:00:18.941Z',
                    'description': None,
                    'gmlgeometry': '<gml:Polygon '
                                   'srsName="EPSG:4326"><gml:outerBoundaryIs><gml:LinearRing><gml:coordinates>-66.400017,-65.643265 '
                                   '-58.727936,-63.775444 -56.687397,-65.090927 '
                                   '-64.635124,-67.052101 '
                                   '-66.400017,-65.643265</gml:coordinates></gml:LinearRing></gml:outerBoundaryIs></gml:Polygon>',
                    'instrument': 'SAR',
                    'keywords': [{'href': 'https://finder.creodias.eu/resto/api/collections/Sentinel1/search.json?&lang=en&q=Antarctica',
                                  'id': 'ab3b4ea14403e2e',
                                  'name': 'Antarctica',
                                  'normalized': 'antarctica',
                                  'type': 'continent'},
                                 {'gcover': 0.25,
                                  'href': 'https://finder.creodias.eu/resto/api/collections/Sentinel1/search.json?&lang=en&q=Antarctica',
    ...
]
```
