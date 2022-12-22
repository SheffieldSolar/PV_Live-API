
# PV_Live
A Python implementation of the PV_Live web API. See https://www.solar.sheffield.ac.uk/pvlive/

**Latest Version: 0.12**

**New! Updated 2022-12-10 to provide support for proxy connections.**

## About this repository

* This Python library provides a convenient interface for the PV_Live web API to facilitate accessing PV_Live results in Python code.
* Developed and tested with Python 3.8, should work with Python 3.7+. Support for Python 2.7+ has been discontinued as of 2021-01-15.

## How do I get set up?

* Make sure you have Git installed - [Download Git](https://git-scm.com/downloads)
* Run `pip install git+https://github.com/SheffieldSolar/PV_Live-API`

## Usage

There are three methods for extracting raw data from the PV_Live API:

|Method|Description|Docs Link|
|------|-----------|---------|
|`PVLive.latest(entity_type="pes", entity_id=0, extra_fields="", period=30, dataframe=False)`|Get the latest PV_Live generation result from the API.|[&#128279;](https://sheffieldsolar.github.io/PV_Live-API/build/html/modules.html#pvlive_api.pvlive.PVLive.latest)|
|`PVLive.at_time(dt, entity_type="pes", entity_id=0, extra_fields="", period=30, dataframe=False)`|Get the PV_Live generation result for a given time from the API.|[&#128279;](https://sheffieldsolar.github.io/PV_Live-API/build/html/modules.html#pvlive_api.pvlive.PVLive.at_time)|
|`PVLive.between(start, end, entity_type="pes", entity_id=0, extra_fields="", period=30, dataframe=False)`|Get the PV_Live generation result for a given time interval from the API.|[&#128279;](https://sheffieldsolar.github.io/PV_Live-API/build/html/modules.html#pvlive_api.pvlive.PVLive.between)|

There are two methods for extracting derived statistics:

|Method|Description|Docs Link|
|------|-----------|---------|
|`PVLive.day_peak(d, entity_type="pes", entity_id=0, extra_fields="", period=30, dataframe=False)`|Get the peak PV_Live generation result for a given day from the API.|[&#128279;](https://sheffieldsolar.github.io/PV_Live-API/build/html/modules.html#pvlive_api.pvlive.PVLive.day_peak)|
|`PVLive.day_energy(d, entity_type="pes", entity_id=0)`|Get the cumulative PV generation for a given day from the API.|[&#128279;](https://sheffieldsolar.github.io/PV_Live-API/build/html/modules.html#pvlive_api.pvlive.PVLive.day_energy)|

These methods include the following optional parameters:

|Parameter|Usage|
|---------|-----|
|`entity_type`|Choose between `"pes"` or `"gsp"`. If querying for national data, this parameter can be set to either value (or left to it's default value) since setting `entity_id` to `0` will always return national data.|
|`entity_id`|Set `entity_id=0` (the default value) to return nationally aggregated data. If `entity_type="pes"`, specify a _pes_id_ to retrieve data for, else if `entity_id="gsp"`, specify a _gsp_id_. For a full list of GSP and PES IDs, refer to the lookup table hosted on National Grid ESO's data portal [here](https://data.nationalgrideso.com/system/gis-boundaries-for-gb-grid-supply-points).|
|`extra_fields`|Use this to extract additional fields from the API such as _installedcapacity_mwp_. For a full list of available fields, see the [PV_Live API Docs](https://www.solar.sheffield.ac.uk/pvlive/api/).|
|`period`|Set the desired temporal resolution (in minutes) for PV outturn estimates. Options are 30 (default) or 5.|
|`dataframe`|Set `dataframe=True` and the results will be returned as a Pandas DataFrame object which is generally much easier to work with. The columns of the DataFrame will be _pes_id_ or _gsp_id_, _datetime_gmt_, _generation_mw_, plus any extra fields specified.|

## Code Examples

See [pvlive_api_demo.py](https://github.com/SheffieldSolar/PV_Live-API/blob/master/pvlive_api_demo.py) for more example usage.

The examples below assume you have imported the PVLive class and created a local instance called `pvl`:

```Python
from datetime import datetime
import pytz

from pvlive_api import PVLive

pvl = PVLive()
```

|Example|Code|Example Output|
|-------|----|------|
|Get the latest nationally aggregated GB PV outturn|`pvl.latest()`|`(0, '2021-01-20T11:00:00Z', 203.0)`|
|Get the latest aggregated outturn for **PES** region **23** (Yorkshire)|`pvl.latest(entity_id=23)`|`(23, '2021-01-20T14:00:00Z', 5.8833031)`
|Get the latest aggregated outturn for **GSP** ID **120** (INDQ1 or "Indian Queens")|`pvl.latest(entity_type="gsp", entity_id=120)`|`(120, '2021-01-20T14:00:00Z', 1, 3.05604)`
|Get the nationally aggregated GB PV outturn for all of 2020 as a DataFrame|`pvl.between(start=datetime(2020, 1, 1, 0, 30, tzinfo=pytz.utc), end=datetime(2021, 1, 1, tzinfo=pytz.utc), dataframe=True)`|![Screenshot of output](https://raw.githubusercontent.com/SheffieldSolar/PV_Live-API/master/misc/code_example_output.png)|
|Get a list of GSP IDs|`pvl.gsp_ids`|`array([  0,   1,   2,   3,   ..., 336, 337, 338])`|
|Get a list of PES IDs|`pvl.pes_ids`|`array([  0,   1,   2,   3,   ..., 336, 337, 338])`|

To download data for all GSPs, use something like:

```python
def download_pvlive_by_gsp(start, end, include_national=True, extra_fields=""):
    data = None
    pvl = PVLive()
    min_gsp_id = 0 if include_national else 1
    for gsp_id in pvl.gsp_ids:
        if gsp_id < min_gsp_id:
            continue
        data_ = pvl.between(start=start, end=end, entity_type="gsp", entity_id=gsp_id,
                            dataframe=True, extra_fields=extra_fields)
        if data is None:
            data = data_
        else:
            data = pd.concat((data, data_), ignore_index=True)
    return data
```

## Command Line Utilities

### pv_live

This utility can be used to download data to a CSV file:

```
>> pv_live -h
usage: pvlive.py [-h] [-s "<yyyy-mm-dd HH:MM:SS>"]
                 [-e "<yyyy-mm-dd HH:MM:SS>"] [--entity_type <entity_type>]
                 [--entity_id <entity_id>] [--period <5|30>] [-q]
                 [-o </path/to/output/file>] [-http <http_proxy>]
                 [-https <https_proxy>]

This is a command line interface (CLI) for the PV_Live API module

optional arguments:
  -h, --help            show this help message and exit
  -s "<yyyy-mm-dd HH:MM:SS>", --start "<yyyy-mm-dd HH:MM:SS>"
                        Specify a UTC start date in 'yyyy-mm-dd HH:MM:SS'
                        format (inclusive), default behaviour is to retrieve
                        the latest outturn.
  -e "<yyyy-mm-dd HH:MM:SS>", --end "<yyyy-mm-dd HH:MM:SS>"
                        Specify a UTC end date in 'yyyy-mm-dd HH:MM:SS' format
                        (inclusive), default behaviour is to retrieve the
                        latest outturn.
  --entity_type <entity_type>
                        Specify an entity type, either 'gsp' or 'pes'. Default
                        is 'gsp'.
  --entity_id <entity_id>
                        Specify an entity ID, default is 0 (i.e. national).
  --period <5|30>       Desired temporal resolution (in minutes) for PV
                        outturn estimates. Default is 30.
  -q, --quiet           Specify to not print anything to stdout.
  -o </path/to/output/file>, --outfile </path/to/output/file>
                        Specify a CSV file to write results to.
  -http <http_proxy>, -http-proxy <http_proxy>
                        HTTP Proxy address
  -https <https_proxy>, -https-proxy <https_proxy>
                        HTTPS Proxy address

Jamie Taylor & Ethan Jones, 2018-06-04
```

## Using the Docker Image

There is also a Docker Image hosted on Docker Hub which can be used to download data from the PV_Live API with minimal setup:

```
>> docker run -it --rm sheffieldsolar/pv_live-api:<release> pv_live -h
```

## Documentation

* [https://sheffieldsolar.github.io/PV_Live-API/](https://sheffieldsolar.github.io/PV_Live-API/)

## How do I upgrade?

Sheffield Solar will endeavour to update this library in sync with the [PV_Live API](https://www.solar.sheffield.ac.uk/pvlive/api/ "PV_Live API webpage") and ensure the latest version of this library always supports the latest version of the PV_Live API, but cannot guarantee this. To make sure you are forewarned of upcoming changes to the API, you should email [solar@sheffield.ac.uk](mailto:solar@sheffield.ac.uk?subject=PV_Live%20API%20email%20updates "Email Sheffield Solar") and request to be added to the PV_Live user mailing list.

To upgrade the code:
* Run `pip install --upgrade git+https://github.com/SheffieldSolar/PV_Live-API`

## Who do I talk to?

* Jamie Taylor - [jamie.taylor@sheffield.ac.uk](mailto:jamie.taylor@sheffield.ac.uk "Email Jamie") - [SheffieldSolar](https://github.com/SheffieldSolar)

## Authors

* **Jamie Taylor** - [SheffieldSolar](https://github.com/SheffieldSolar)
* **Ethan Jones** - [SheffieldSolar](https://github.com/SheffieldSolar)

## License

No license is defined yet - use at your own risk.