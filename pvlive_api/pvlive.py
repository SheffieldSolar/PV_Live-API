"""
A Python interface for the PV_Live web API from Sheffield Solar.

- Jamie Taylor <jamie.taylor@sheffield.ac.uk>
- Ethan Jones <ejones18@sheffield.ac.uk>
- First Authored: 2018-06-04
- Updated: 2020-10-20 to return Pandas dataframe object
- Updated: 2021-01-15 to use v3 of the PV_Live APi and expose GSP endpoints as well as PES
"""

import sys
import os
import json
from datetime import datetime, timedelta, date, time
from time import sleep
from typing import List, Union, Tuple
import inspect
import pytz
import requests
import argparse
from numpy import nan, int64
import pandas as pd


class PVLiveException(Exception):
    """An Exception specific to the PVLive class."""
    def __init__(self, msg):
        try:
            caller_file = inspect.stack()[2][1]
        except:
            caller_file = os.path.basename(__file__)
        self.msg = "%s (in '%s')" % (msg, caller_file)

    def __str__(self):
        return self.msg


class PVLive:
    """
    Interface with the PV_Live web API.

    Parameters
    ----------
    `retries` : int
        Optionally specify the number of retries to use should the API respond with anything
        other than status code 200. Exponential back-off applies inbetween retries.
    """
    def __init__(self, retries: int = 3):
        self.base_url = "https://api0.solar.sheffield.ac.uk/pvlive/api/v4/"
        self.max_range = {"national": timedelta(days=365), "regional": timedelta(days=30)}
        self.retries = retries
        self.ggd_lookup = self._get_ggd_lookup()
        self.gsp_ids = self.ggd_lookup.gsp_id.dropna().astype(int64).unique()
        self.pes_ids = self.ggd_lookup.pes_id.dropna().astype(int64).unique()

    def _get_ggd_lookup(self):
        """Fetch the GGD lookup from the API and convert to Pandas DataFrame."""
        url = f"{self.base_url}ggd_list"
        response = self._fetch_url(url)
        ggd_lookup = pd.DataFrame(response["data"], columns=response["meta"])
        return ggd_lookup

    def latest(self,
               entity_type: str = "pes",
               entity_id: int = 0,
               extra_fields: str = "",
               period: int = 30,
               dataframe: bool = False) -> Union[Tuple, pd.DataFrame]:
        """
        Get the latest PV_Live generation result from the API.

        Parameters
        ----------
        `entity_type` : string
            The aggregation entity type of interest, either "pes" or "gsp". Defaults to "pes".
        `entity_id` : int
            The numerical ID of the entity of interest. Defaults to 0.
        `extra_fields` : string
            Comma-separated string listing any extra fields.
        `period` : int
            Time-resolution to retrieve, either 30 or 5 (minutely). Default is 30.
        `dataframe` : boolean
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
            data, meta = self._remove_n_ggds(response["data"], response["meta"])
            data = tuple(data[0])
            if dataframe:
                return self._convert_tuple_to_df(data, meta)
            return data
        return None

    def at_time(self,
                dt: datetime,
                entity_type: str = "pes",
                entity_id: int = 0,
                extra_fields: str = "",
                period: int = 30,
                dataframe: bool = False) -> Union[Tuple, pd.DataFrame]:
        """
        Get the PV_Live generation result for a given time from the API.

        Parameters
        ----------
        `dt` : datetime
            A timezone-aware datetime object. Will be corrected to the END of the half hour in which
            *dt* falls, since Sheffield Solar use end of interval as convention.
        `entity_type` : string
            The aggregation entity type of interest, either "pes" or "gsp". Defaults to "pes".
        `entity_id` : int
            The numerical ID of the entity of interest. Defaults to 0.
        `extra_fields` : string
            Comma-separated string listing any extra fields.
        `period` : int
            Time-resolution to retrieve, either 30 or 5 (minutely). Default is 30.
        `dataframe` : boolean
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
                entity_type: str = "pes",
                entity_id: int = 0,
                extra_fields: str = "",
                period: int = 30,
                dataframe: bool = False) -> Union[List, pd.DataFrame]:
        """
        Get the PV_Live generation result for a given time interval from the API.

        Parameters
        ----------
        `start` : datetime
            A timezone-aware datetime object. Will be corrected to the END of the half hour in which
            *start* falls, since Sheffield Solar use end of interval as convention.
        `end` : datetime
            A timezone-aware datetime object. Will be corrected to the END of the half hour in which
            *end* falls, since Sheffield Solar use end of interval as convention.
        `entity_type` : string
            The aggregation entity type of interest, either "pes" or "gsp". Defaults to "pes".
        `entity_id` : int
            The numerical ID of the entity of interest. Defaults to 0.
        `extra_fields` : string
            Comma-separated string listing any extra fields.
        `period` : int
            Time-resolution to retrieve, either 30 or 5 (minutely). Default is 30.
        `dataframe` : boolean
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
                 entity_type: str = "pes",
                 entity_id: int = 0,
                 extra_fields: str = "",
                 period: int = 30,
                 dataframe: bool = False) -> Union[Tuple, pd.DataFrame]:
        """
        Get the peak PV_Live generation result for a given day from the API.

        Parameters
        ----------
        `d` : date
            The day of interest as a date object.
        `entity_type` : string
            The aggregation entity type of interest, either "pes" or "gsp". Defaults to "pes".
        `entity_id` : int
            The numerical ID of the entity of interest. Defaults to 0.
        `extra_fields` : string
            Comma-separated string listing any extra fields.
        `period` : int
            Time-resolution to retrieve, either 30 or 5 (minutely). Default is 30.
        `dataframe` : boolean
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

    def day_energy(self, d: date, entity_type: str = "pes", entity_id: int = 0) -> float:
        """
        Get the cumulative PV generation for a given day from the API.

        Parameters
        ----------
        `d` : date
            The day of interest as a date object.
        `entity_type` : string
            The aggregation entity type of interest, either "pes" or "gsp". Defaults to "pes".
        `entity_id` : int
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

    def _between(self, start, end, entity_type="pes", entity_id=0, extra_fields="", period=30,
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
        data, meta = self._remove_n_ggds(data, response["meta"])
        if dataframe:
            return self._convert_tuple_to_df(data, meta), meta
        return data, meta

    @staticmethod
    def _remove_n_ggds(data, meta):
        """Remove the n_ggds column from the API response (not useful for most users)."""
        if "n_ggds" in meta:
            ind = meta.index("n_ggds")
            data = [d[:ind] + d[ind + 1:] for d in data]
            meta.remove("n_ggds")
        return data, meta

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
        return data.drop(columns="n_ggds", errors="ignore")

    def _build_url(self, entity_type, entity_id, params):
        """Construct the appropriate URL for a given set of parameters."""
        base_url = "{}{}/{}".format(self.base_url, entity_type, entity_id)
        url = base_url + "?" + "&".join(["{}={}".format(k, params[k]) for k in params])
        return url

    def _fetch_url(self, url):
        """Fetch the URL with GET request."""
        success = False
        try_counter = 0
        delay = 1
        while not success and try_counter < self.retries + 1:
            try_counter += 1
            try:
                page = requests.get(url)
                page.raise_for_status()
                success = True
            except requests.exceptions.HTTPError:
                sleep(delay)
                delay *= 2
                continue
        if not success:
            raise PVLiveException("Error communicating with the PV_Live API.")
        try:
            return json.loads(page.text)
        except Exception as e:
            raise PVLiveException("Error communicating with the PV_Live API.") from e

    def _nearest_interval(self, dt, period=30):
        """Round to either the nearest 30 or 5 minute interval."""
        if not(dt.minute % period == 0 and dt.second == 0 and dt.microsecond == 0):
            dt = dt - timedelta(minutes=dt.minute % period, seconds=dt.second,
                                microseconds=dt.microsecond) + timedelta(minutes=period)
        return dt

    def _validate_inputs(self, entity_type="pes", entity_id=0, extra_fields="", period=30):
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
                                     epilog="Jamie Taylor, 2018-06-04")
    parser.add_argument("-s", "--start", metavar="\"<yyyy-mm-dd HH:MM:SS>\"", dest="start",
                        action="store", type=str, required=False, default=None,
                        help="Specify a UTC start date in 'yyyy-mm-dd HH:MM:SS' format "
                             "(inclusive), default behaviour is to retrieve the latest outturn.")
    parser.add_argument("-e", "--end", metavar="\"<yyyy-mm-dd HH:MM:SS>\"", dest="end",
                        action="store", type=str, required=False, default=None,
                        help="Specify a UTC end date in 'yyyy-mm-dd HH:MM:SS' format (inclusive), "
                        "default behaviour is to retrieve the latest outturn.")
    parser.add_argument("--entity_type", metavar="<entity_type>", dest="entity_type",
                        action="store", type=str, required=False, default="pes",
                        choices=["gsp", "pes"],
                        help="Specify an entity type, either 'gsp' or 'pes'. Default is 'pes'.")
    parser.add_argument("--entity_id", metavar="<entity_id>", dest="entity_id", action="store",
                        type=int, required=False, default=0,
                        help="Specify an entity ID, default is 0 (i.e. national).")
    parser.add_argument("--period", metavar="<5|30>", dest="period", action="store",
                        type=int, required=False, default=30, choices=(5, 30),
                        help="Desired temporal resolution (in minutes) for PV outturn estimates. "
                             "Default is 30.")
    parser.add_argument("-q", "--quiet", dest="quiet", action="store_true",
                        required=False, help="Specify to not print anything to stdout.")
    parser.add_argument("-o", "--outfile", metavar="</path/to/output/file>", dest="outfile",
                        action="store", type=str, required=False,
                        help="Specify a CSV file to write results to.")
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
        return options
    return handle_options(options)


def main():
    """Load CLI options and access the API accordingly."""
    options = parse_options()
    pvl = PVLive()
    if options.start is None and options.end is None:
        data = pvl.latest(entity_type=options.entity_type, entity_id=options.entity_id,
                          extra_fields="installedcapacity_mwp", dataframe=True)
    else:
        start = datetime(2014, 1, 1, 0, 30, tzinfo=pytz.utc) if options.start is None \
            else options.start
        end = pytz.utc.localize(datetime.utcnow()) if options.end is None else options.end
        data = pvl.between(start, end, entity_type=options.entity_type, entity_id=options.entity_id,
                           extra_fields="installedcapacity_mwp", period=options.period,
                           dataframe=True)
    if options.outfile is not None:
        data.to_csv(options.outfile, float_format="%.3f", index=False)
    if not options.quiet:
        print(data)


if __name__ == "__main__":
    main()
