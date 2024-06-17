"""
A Python interface for the PV_Live web API from Sheffield Solar.

- Jamie Taylor <jamie.taylor@sheffield.ac.uk>
- Ethan Jones <ejones18@sheffield.ac.uk>
- First Authored: 2018-06-04
"""

import sys
import os
import json
from datetime import datetime, timedelta, date, time
from time import sleep
from typing import List, Union, Tuple, Dict, Optional, Literal
import inspect
import argparse
import re
from io import BytesIO

import pytz
import requests
from numpy import nan, int64
import pandas as pd
from bs4 import BeautifulSoup

class PVLiveException(Exception):
    """An Exception specific to the PVLive class."""
    def __init__(self, msg):
        try:
            caller_file = inspect.stack()[2][1]
        except:
            caller_file = os.path.basename(__file__)
        self.msg = f"{msg} (in '{caller_file}')"

    def __str__(self):
        return self.msg

class PVLive:
    """
    Interface with the PV_Live web API.

    Parameters
    ----------
    retries : int
        Optionally specify the number of retries to use should the API respond with anything
        other than status code 200. Exponential back-off applies inbetween retries.
    proxies : Optional[Dict]
        Optionally specify a Dict of proxies for http and https requests in the format:
        {"http": "<address>", "https": "<address>"}
    """
    def __init__(self, retries: int = 3, proxies: Optional[Dict] = None, ssl_verify: bool = True):
        self.domain_url = "https://api.solar.sheffield.ac.uk"
        self.base_url = f"{self.domain_url}/pvlive/api/v4"
        self.max_range = {"national": timedelta(days=365), "regional": timedelta(days=30)}
        self.retries = retries
        self.proxies = proxies
        self.ssl_verify = ssl_verify
        self.timeout = 30
        self.gsp_list = self._get_gsp_list()
        self.pes_list = self._get_pes_list()
        self.gsp_ids = self.gsp_list.gsp_id.dropna().astype(int64).unique()
        self.pes_ids = self.pes_list.pes_id.dropna().astype(int64).unique()
        self.deployment_releases = None

    def _get_gsp_list(self):
        """Fetch the GSP list from the API and convert to Pandas DataFrame."""
        url = f"{self.base_url}/gsp_list"
        response = self._fetch_url(url)
        return pd.DataFrame(response["data"], columns=response["meta"])

    def _get_pes_list(self):
        """Fetch the PES list from the API and convert to Pandas DataFrame."""
        url = f"{self.base_url}/pes_list"
        response = self._fetch_url(url)
        return pd.DataFrame(response["data"], columns=response["meta"])

    def _get_deployment_releases(self):
        """Get a list of deployment releases as datestamps (YYYYMMDD)."""
        if self.deployment_releases is None:
            url = f"{self.domain_url}/capacity/"
            response = self._fetch_url(url, parse_json=False)
            soup = BeautifulSoup(response.content, "html.parser")
            releases = [r["href"].strip("/") for r in soup.find_all("a", href=True)
                        if re.match(r"[0-9]{8}/", r["href"])]
            self.deployment_releases = sorted(releases, reverse=True)
        return self.deployment_releases

    def _get_deployment_filenames(self, release):
        """Get a list of filenames for a given release."""
        url = f"{self.domain_url}/capacity/{release}/"
        response = self._fetch_url(url, parse_json=False)
        soup = BeautifulSoup(response.content, "html.parser")
        filenames = [r["href"] for r in soup.find_all("a", href=True)
                     if re.match(r".+\.csv.gz", r["href"])]
        return filenames

    def _validate_deployment_inputs(self, region, include_history, by_system_size, release):
        """Validate input parameters to `deployment()`."""
        releases = self._get_deployment_releases()
        if not isinstance(region, str):
            raise TypeError("`region` must be a string.")
        supported_regions = ["gsp", "llsoa"]
        if region not in supported_regions:
            raise ValueError(f"The region must be one of {supported_regions}")
        if not isinstance(include_history, bool):
            raise TypeError("`include_history` must be True or False.")
        if not isinstance(by_system_size, bool):
            raise TypeError("`by_system_size` must be True or False.")
        if by_system_size and region != "gsp":
            raise ValueError(f"`by_system_size` can only be True if `region`='gsp'")
        if by_system_size and not include_history:
            raise ValueError(f"`by_system_size` can only be True if `include_history`=True")
        if not isinstance(release, (str, int)):
            raise TypeError("`release` must be str or int.")
        if isinstance(release, str) and release not in releases:
            raise ValueError(f"The requested release ({release}) was not found on the API")
        if isinstance(release, int) and release not in range(len(releases)):
            raise ValueError("The requested release index ({release}) was not found on the API, "
                             f"release index must be between 0 and {len(releases)-1}")

    def deployment(self,
                   region: Literal["gsp", "llsoa"] = "gsp",
                   include_history: bool = False,
                   by_system_size: bool = False,
                   release: Union[str, int] = 0) -> pd.DataFrame:
        """
        Download deployment data from the `/capacity` endpoint.

        Parameters
        ----------
        region : str
            The aggregation region for the deployment data, either 'gsp' (default) or 'llsoa'.
        include_history : bool
            Set to True to include historical deployment data. Defaults to False.
        by_system_size : bool
            If `region` == "gsp", set to True to also include the breakdown by system size.
        release : Union[str, int]
            The datestamp (YYYYMMDD) of the capacity update you wish to download. Pass a string
            (e.g. "20231116") to get a specific release, or pass an int to get the latest
            (release=0), next-latest (release==1) etc. Defaults to 0.

        Returns
        -------
        Pandas DataFrame
            Columns vary depending on the input parameters, but shoudl include at least release,
            GSPs/llsoa and dc_capacity_mwp.
        """
        self._validate_deployment_inputs(region, include_history, by_system_size, release)
        releases = self._get_deployment_releases()
        release = releases[release] if isinstance(release, int) else release
        filenames = self._get_deployment_filenames(release)
        region_ = "20220314_GSP" if region == "gsp" else region
        history_ = "_and_month" if include_history else ""
        system_size_ = "_and_system_size" if by_system_size else ""
        filename_ending = f"_capacity_by_{region_}{history_}{system_size_}.csv.gz"
        filename = [f for f in filenames if f.endswith(filename_ending)][0]
        url = f"{self.domain_url}/capacity/{release}/{filename}"
        kwargs = dict(parse_dates=["install_month"]) if include_history else {}
        response = self._fetch_url(url, parse_json=False)
        mock_file = BytesIO(response.content)
        deployment_data = pd.read_csv(mock_file, compression={"method": "gzip"}, **kwargs)
        deployment_data.insert(0, "release", release)
        deployment_data.rename(columns={"dc_capacity_MWp": "dc_capacity_mwp"}, inplace=True)
        deployment_data.system_count = deployment_data.system_count.astype("Int64")
        return deployment_data

    def latest(self,
               entity_type: Literal["gsp", "pes"] = "gsp",
               entity_id: int = 0,
               extra_fields: str = "",
               period: int = 30,
               dataframe: bool = False) -> Union[Tuple, pd.DataFrame]:
        """
        Get the latest PV_Live generation result from the API.

        Parameters
        ----------
        entity_type : string
            The aggregation entity type of interest, either "pes" or "gsp". Defaults to "gsp".
        entity_id : int
            The numerical ID of the entity of interest. Defaults to 0.
        extra_fields : string
            Comma-separated string listing any extra fields.
        period : int
            Time-resolution to retrieve, either 30 or 5 (minutely). Default is 30.
        dataframe : boolean
            Set to True to return data as a Python DataFrame. Default is False, i.e. return a tuple.

        Returns
        -------
        tuple
            Tuple containing the pes_id, datetime_gmt and generation_mw fields of the latest
            PV_Live result, plus any extra_fields in the order specified.
        OR
        Pandas DataFrame
            Contains the columns pes_id, datetime_gmt and generation_mw, plus any extra_fields in
            the order specified.
        OR
        None
            If no data found, return None.

        Notes
        -----
        For list of optional *extra_fields*, see `PV_Live API Docs
        <https://www.solar.sheffield.ac.uk/pvlive/api/>`_.
        """
        self._validate_inputs(entity_type=entity_type, entity_id=entity_id,
                              extra_fields=extra_fields, period=period)
        params = self._compile_params(extra_fields, period=period)
        response = self._query_api(entity_type, entity_id, params)
        if response["data"]:
            data, meta = response["data"], response["meta"]
            data = tuple(data[0])
            if dataframe:
                return self._convert_tuple_to_df(data, meta)
            return data
        return None

    def at_time(self,
                dt: datetime,
                entity_type: Literal["gsp", "pes"] = "gsp",
                entity_id: int = 0,
                extra_fields: str = "",
                period: int = 30,
                dataframe: bool = False) -> Union[Tuple, pd.DataFrame]:
        """
        Get the PV_Live generation result for a given time from the API.

        Parameters
        ----------
        dt : datetime
            A timezone-aware datetime object. Will be corrected to the END of the half hour in which
            *dt* falls, since Sheffield Solar use end of interval as convention.
        entity_type : string
            The aggregation entity type of interest, either "pes" or "gsp". Defaults to "gsp".
        entity_id : int
            The numerical ID of the entity of interest. Defaults to 0.
        extra_fields : string
            Comma-separated string listing any extra fields.
        period : int
            Time-resolution to retrieve, either 30 or 5 (minutely). Default is 30.
        dataframe : boolean
            Set to True to return data as a Python DataFrame. Default is False, i.e. return a tuple.

        Returns
        -------
        tuple
            Tuple containing the pes_id, datetime_gmt and generation_mw fields of the PV_Live
            result, plus any extra_fields in the order specified.
        OR
        Pandas DataFrame
            Contains the columns pes_id, datetime_gmt and generation_mw, plus any extra_fields in
            the order specified.

        Notes
        -----
        For list of optional *extra_fields*, see `PV_Live API Docs
        <https://www.solar.sheffield.ac.uk/pvlive/api/>`_.
        """
        result = self.between(start=dt, end=dt, entity_type=entity_type, entity_id=entity_id,
                              extra_fields=extra_fields, period=period, dataframe=dataframe)
        if dataframe:
            return result
        return tuple(result[0])

    def between(self,
                start: datetime,
                end: datetime,
                entity_type: Literal["gsp", "pes"] = "gsp",
                entity_id: int = 0,
                extra_fields: str = "",
                period: int = 30,
                dataframe: bool = False) -> Union[List, pd.DataFrame]:
        """
        Get the PV_Live generation result for a given time interval from the API.

        Parameters
        ----------
        start : datetime
            A timezone-aware datetime object. Will be corrected to the END of the half hour in which
            *start* falls, since Sheffield Solar use end of interval as convention.
        end : datetime
            A timezone-aware datetime object. Will be corrected to the END of the half hour in which
            *end* falls, since Sheffield Solar use end of interval as convention.
        entity_type : string
            The aggregation entity type of interest, either "pes" or "gsp". Defaults to "gsp".
        entity_id : int
            The numerical ID of the entity of interest. Defaults to 0.
        extra_fields : string
            Comma-separated string listing any extra fields.
        period : int
            Time-resolution to retrieve, either 30 or 5 (minutely). Default is 30.
        dataframe : boolean
            Set to True to return data as a Python DataFrame. Default is False, i.e. return a tuple.

        Returns
        -------
        list
            Each element of the outter list is a list containing the pes_id, datetime_gmt and
            generation_mw fields of a PV_Live result, plus any extra_fields in the order specified.
        OR
        Pandas DataFrame
            Contains the columns pes_id, datetime_gmt and generation_mw, plus any extra_fields in
            the order specified.

        Notes
        -----
        For list of optional *extra_fields*, see `PV_Live API Docs
        <https://www.solar.sheffield.ac.uk/pvlive/api/>`_.
        """
        return self._between(start, end, entity_type, entity_id, extra_fields, period, dataframe)[0]

    def day_peak(self,
                 d: date,
                 entity_type: Literal["gsp", "pes"] = "gsp",
                 entity_id: int = 0,
                 extra_fields: str = "",
                 period: int = 30,
                 dataframe: bool = False) -> Union[Tuple, pd.DataFrame]:
        """
        Get the peak PV_Live generation result for a given day from the API.

        Parameters
        ----------
        d : date
            The day of interest as a date object.
        entity_type : string
            The aggregation entity type of interest, either "pes" or "gsp". Defaults to "gsp".
        entity_id : int
            The numerical ID of the entity of interest. Defaults to 0.
        extra_fields : string
            Comma-separated string listing any extra fields.
        period : int
            Time-resolution to retrieve, either 30 or 5 (minutely). Default is 30.
        dataframe : boolean
            Set to True to return data as a Python DataFrame. Default is False, i.e. return a tuple.

        Returns
        -------
        tuple
            Tuple containing the pes_id, datetime_gmt and generation_mw fields of the latest
            PV_Live result, plus any extra_fields in the order specified.
        OR
        Pandas DataFrame
            Contains the columns pes_id, datetime_gmt and generation_mw, plus any extra_fields in
            the order specified.
        OR
        None
            If no data found for the day, return None.

        Notes
        -----
        For list of optional *extra_fields*, see `PV_Live API Docs
        <https://www.solar.sheffield.ac.uk/pvlive/api/>`_.
        """
        if not isinstance(d, date):
            raise TypeError("`d` must be a Python date object.")
        start = datetime.combine(d, time(0, 30, tzinfo=pytz.UTC))
        end = start + timedelta(days=1) - timedelta(minutes=30)
        data, meta = self._between(start, end, entity_type, entity_id, extra_fields, period=period)
        if data:
            gen_index = meta.index("generation_mw")
            gens = [x[gen_index] if x[gen_index] is not None else -1e308 for x in data]
            index_max = max(range(len(gens)), key=gens.__getitem__)
            maxdata = tuple(data[index_max])
            if dataframe:
                return self._convert_tuple_to_df(maxdata, meta)
            return maxdata
        return None

    def day_energy(self, d: date, entity_type: Literal["gsp", "pes"] = "gsp", entity_id: int = 0) -> float:
        """
        Get the cumulative PV generation for a given day from the API.

        Parameters
        ----------
        d : date
            The day of interest as a date object.
        entity_type : string
            The aggregation entity type of interest, either "pes" or "gsp". Defaults to "gsp".
        entity_id : int
            The numerical ID of the entity of interest. Defaults to 0.

        Returns
        -------
        float
            The cumulative PV generation on the given day in MWh.
        OR
        None
            If no data found, return None.
        """
        if not isinstance(d, date):
            raise TypeError("`d` must be a Python date object.")
        start = datetime.combine(d, time(0, 30, tzinfo=pytz.UTC))
        end = start + timedelta(days=1) - timedelta(minutes=30)
        data, meta = self._between(start, end, entity_type=entity_type, entity_id=entity_id)
        if data:
            gen_index = meta.index("generation_mw")
            pv_energy = sum([x[gen_index] if x[gen_index] is not None else 0 for x in data]) * 0.5
            return pv_energy
        return None

    def _between(self, start, end, entity_type="gsp", entity_id=0, extra_fields="", period=30,
                 dataframe=False):
        """
        Get the PV_Live generation result for a given time interval from the API, returning both the
        data and the columns.
        """
        self._validate_inputs(entity_type=entity_type, entity_id=entity_id,
                              extra_fields=extra_fields, period=period)
        type_check = not (isinstance(start, datetime) and isinstance(end, datetime))
        tz_check = start.tzinfo is None or end.tzinfo is None
        if type_check or tz_check:
            raise ValueError("`start` and `end` must be timezone-aware Python datetime objects.")
        if end < start:
            raise ValueError("Start must be later than end.")
        start = self._nearest_interval(start, period=period)
        end = self._nearest_interval(end, period=period)
        data = []
        request_start = start
        max_range = self.max_range["national"] if entity_id == 0 and entity_type == 0 else \
            self.max_range["regional"]
        while request_start <= end:
            request_end = min(end, request_start + max_range)
            params = self._compile_params(extra_fields, request_start, request_end, period)
            response = self._query_api(entity_type, entity_id, params)
            data += response["data"]
            request_start += max_range + timedelta(minutes=period)
        if dataframe:
            return self._convert_tuple_to_df(data, response["meta"]), response["meta"]
        return data, response["meta"]

    def _compile_params(self, extra_fields="", start=None, end=None, period=30):
        """Compile parameters into a Python dict, formatting where necessary."""
        params = {}
        if extra_fields:
            params["extra_fields"] = extra_fields
        if start is not None:
            params["start"] = start.isoformat().replace("+00:00", "Z")
        end = start if (start is not None and end is None) else end
        if end is not None:
            params["end"] = end.isoformat().replace("+00:00", "Z")
        params["period"] = period
        return params

    def _query_api(self, entity_type, entity_id, params):
        """Query the API with some REST parameters."""
        url = self._build_url(entity_type, entity_id, params)
        return self._fetch_url(url)

    def _convert_tuple_to_df(self, data, columns):
        """Converts a tuple of values to a data-frame object."""
        data = [data] if isinstance(data, tuple) else data
        data = [tuple(nan if d is None else d for d in t) for t in data]
        data = pd.DataFrame(data, columns=columns)
        if "datetime_gmt" in data.columns:
            data.datetime_gmt = pd.to_datetime(data.datetime_gmt)
        return data

    def _build_url(self, entity_type, entity_id, params):
        """Construct the appropriate URL for a given set of parameters."""
        base_url = f"{self.base_url}/{entity_type}/{entity_id}"
        url = base_url + "?" + "&".join(["{}={}".format(k, params[k]) for k in params])
        return url

    def _fetch_url(self, url, parse_json=True):
        """Fetch the URL with GET request."""
        success = False
        try_counter = 0
        delay = 1
        while not success and try_counter < self.retries + 1:
            try_counter += 1
            try:
                page = requests.get(url, proxies=self.proxies, verify=self.ssl_verify,
                                    timeout=self.timeout)
                page.raise_for_status()
                success = True
            except requests.exceptions.HTTPError as err:
                if page.status_code == 400:
                    helper = re.search(r"<p>(.*)</p>", page.text).group(1)
                    raise PVLiveException(f"PV_Live API received Bad Request (400)... {helper}")
                sleep(delay)
                delay *= 2
                continue
        if not success:
            raise PVLiveException("Error communicating with the PV_Live API.")
        try:
            if parse_json:
                return json.loads(page.text)
            else:
                return page
        except Exception as e:
            raise PVLiveException("Error communicating with the PV_Live API.") from e

    def _nearest_interval(self, dt, period=30):
        """Round to either the nearest 30 or 5 minute interval."""
        if not(dt.minute % period == 0 and dt.second == 0 and dt.microsecond == 0):
            dt = dt - timedelta(minutes=dt.minute % period, seconds=dt.second,
                                microseconds=dt.microsecond) + timedelta(minutes=period)
        return dt

    def _validate_inputs(self, entity_type="gsp", entity_id=0, extra_fields="", period=30):
        """Validate common input parameters."""
        if not isinstance(entity_type, str):
            raise TypeError("The entity_type must be a string.")
        if entity_type not in ["pes", "gsp"]:
            raise ValueError("The entity_type must be either 'pes' or 'gsp'.")
        if not isinstance(extra_fields, str):
            raise TypeError("The extra_fields must be a comma-separated string (with no spaces).")
        if entity_type == "pes":
            if entity_id != 0 and entity_id not in self.pes_ids:
                raise PVLiveException(f"The pes_id {entity_id} was not found.")
        elif entity_type == "gsp":
            if entity_id not in self.gsp_ids:
                raise PVLiveException(f"The gsp_id {entity_id} was not found.")
        periods = [5, 30]
        if period not in periods:
            raise ValueError("The period parameter must be one of: "
                             f"{', '.join(map(str, periods))}.")

def parse_options():
    """Parse command line options."""
    parser = argparse.ArgumentParser(description=("This is a command line interface (CLI) for the "
                                                  "PV_Live API module"),
                                     epilog="Jamie Taylor & Ethan Jones, 2018-06-04")
    parser.add_argument("-s", "--start", metavar="\"<yyyy-mm-dd HH:MM:SS>\"", dest="start",
                        action="store", type=str, required=False, default=None,
                        help="Specify a UTC start date in 'yyyy-mm-dd HH:MM:SS' format "
                             "(inclusive), default behaviour is to retrieve the latest outturn.")
    parser.add_argument("-e", "--end", metavar="\"<yyyy-mm-dd HH:MM:SS>\"", dest="end",
                        action="store", type=str, required=False, default=None,
                        help="Specify a UTC end date in 'yyyy-mm-dd HH:MM:SS' format (inclusive), "
                        "default behaviour is to retrieve the latest outturn.")
    parser.add_argument("--entity_type", metavar="<entity_type>", dest="entity_type",
                        action="store", type=str, required=False, default="gsp",
                        choices=["gsp", "pes"],
                        help="Specify an entity type, either 'gsp' or 'pes'. Default is 'gsp'.")
    parser.add_argument("--entity_id", metavar="<entity_id>", dest="entity_id", action="store",
                        type=int, required=False, default=0,
                        help="Specify an entity ID, default is 0 (i.e. national).")
    parser.add_argument("--extra_fields", metavar="<field1[,field2, ...]>", dest="extra_fields",
                        action="store", type=str, required=False, default="installedcapacity_mwp",
                        help="Specify an extra_fields (as a comma-separated list to include when "
                             "requesting data from the API, defaults to 'installedcapacity_mwp'.")
    parser.add_argument("--period", metavar="<5|30>", dest="period", action="store",
                        type=int, required=False, default=30, choices=(5, 30),
                        help="Desired temporal resolution (in minutes) for PV outturn estimates. "
                             "Default is 30.")
    parser.add_argument("-q", "--quiet", dest="quiet", action="store_true",
                        required=False, help="Specify to not print anything to stdout.")
    parser.add_argument("-o", "--outfile", metavar="</path/to/output/file>", dest="outfile",
                        action="store", type=str, required=False,
                        help="Specify a CSV file to write results to.")
    parser.add_argument('-http', '--http-proxy', metavar="<http_proxy>", dest="http",
                        type=str, required=False, default=None, action="store",
                        help="HTTP Proxy address")
    parser.add_argument('-https', '--https-proxy', metavar="<https_proxy>", dest="https",
                        type=str, required=False, default=None, action="store",
                        help="HTTPS Proxy address")
    options = parser.parse_args()

    def handle_options(options):
        """Validate command line args and pre-process where necessary."""
        if (options.outfile is not None and os.path.exists(options.outfile)) and not options.quiet:
            try:
                input(f"The output file '{options.outfile}' already exists and will be "
                      "overwritten, are you sure you want to continue? Press enter to continue or "
                      "ctrl+c to abort.")
            except KeyboardInterrupt:
                print()
                print("Aborting...")
                sys.exit(0)
        if options.start is not None:
            try:
                options.start = pytz.utc.localize(
                    datetime.strptime(options.start, "%Y-%m-%d %H:%M:%S")
                )
            except:
                raise Exception("OptionsError: Failed to parse start datetime, make sure you use "
                                "'yyyy-mm-dd HH:MM:SS' format.")
        if options.end is not None:
            try:
                options.end = pytz.utc.localize(datetime.strptime(options.end, "%Y-%m-%d %H:%M:%S"))
            except:
                raise Exception("OptionsError: Failed to parse end datetime, make sure you use "
                                "'yyyy-mm-dd HH:MM:SS' format.")
        proxies = {} if options.http is not None or options.https is not None else None
        if options.http is not None:
            proxies.update({"http": options.http})
        if options.https is not None:
            proxies.update({"https": options.https})
        options.proxies = proxies
        return options
    return handle_options(options)

def main():
    """Load CLI options and access the API accordingly."""
    options = parse_options()
    pvl = PVLive(proxies=options.proxies)
    if options.start is None and options.end is None:
        data = pvl.latest(entity_type=options.entity_type, entity_id=options.entity_id,
                          extra_fields=options.extra_fields, dataframe=True)
    else:
        start = datetime(2014, 1, 1, 0, 30, tzinfo=pytz.utc) if options.start is None \
            else options.start
        end = pytz.utc.localize(datetime.utcnow()) if options.end is None else options.end
        data = pvl.between(start, end, entity_type=options.entity_type, entity_id=options.entity_id,
                           extra_fields=options.extra_fields, period=options.period,
                           dataframe=True)
    if options.outfile is not None:
        data.to_csv(options.outfile, float_format="%.3f", index=False)
    if not options.quiet:
        print(data)

if __name__ == "__main__":
    main()
