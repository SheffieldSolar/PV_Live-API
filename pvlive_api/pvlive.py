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
import inspect
import pytz
import requests
from numpy import nan
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
    def __init__(self, retries=3):
        self.base_url = "https://api0.solar.sheffield.ac.uk/pvlive/v3/"
        self.max_range = {"national": timedelta(days=365), "regional": timedelta(days=30)}
        self.retries = retries
        self.ggd_lookup = self._get_ggd_lookup()

    def _get_ggd_lookup(self):
        """Fetch the GGD lookup from the API and convert to Pandas DataFrame."""
        url = "https://api0.solar.sheffield.ac.uk/pvlive/v3/ggd_list"
        response = self._fetch_url(url)
        ggd_lookup = pd.DataFrame(response["data"], columns=response["meta"])
        return ggd_lookup

    def latest(self, entity_type="pes", entity_id=0, extra_fields="", dataframe=False):
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
                              extra_fields=extra_fields)
        params = self._compile_params(extra_fields)
        response = self._query_api(entity_type, entity_id, params)
        if response["data"]:
            data = tuple(response["data"][0])
            if dataframe:
                return self._convert_tuple_to_df(data, response["meta"])
            if entity_type == "pes":
                return data
            return data[:2] + data[3:]
        return None

    def at_time(self, dt, entity_type="pes", entity_id=0, extra_fields="", dataframe=False):
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
                              extra_fields=extra_fields, dataframe=dataframe)
        if dataframe:
            return result
        return tuple(result[0])

    def between(self, start, end, entity_type="pes", entity_id=0, extra_fields="", dataframe=False):
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
        return self._between(start, end, entity_type, entity_id, extra_fields, dataframe)[0]

    def day_peak(self, d, entity_type="pes", entity_id=0, extra_fields="", dataframe=False):
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
            raise PVLiveException("The d must be a Python date object.")
        start = datetime.combine(d, time(0, 30, tzinfo=pytz.UTC))
        end = start + timedelta(days=1) - timedelta(minutes=30)
        data, meta = self._between(start, end, entity_type, entity_id, extra_fields)
        if data:
            gen_index = 2
            gens = [x[gen_index] if x[gen_index] is not None else -1e308 for x in data]
            index_max = max(range(len(gens)), key=gens.__getitem__)
            maxdata = tuple(data[index_max])
            if dataframe:
                return self._convert_tuple_to_df(maxdata, meta)
            return maxdata
        return None

    def day_energy(self, d, entity_type="pes", entity_id=0):
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
            raise PVLiveException("The d must be a Python date object.")
        start = datetime.combine(d, time(0, 30, tzinfo=pytz.UTC))
        end = start + timedelta(days=1) - timedelta(minutes=30)
        data, _ = self._between(start, end, entity_type=entity_type, entity_id=entity_id)
        if data:
            gen_index = 2
            pv_energy = sum([x[gen_index] if x[gen_index] is not None else 0 for x in data]) * 0.5
            return pv_energy
        return None

    def _between(self, start, end, entity_type="pes", entity_id=0, extra_fields="",
                 dataframe=False):
        """
        Get the PV_Live generation result for a given time interval from the API, returning both the
        data and the columns.
        """
        self._validate_inputs(entity_type=entity_type, entity_id=entity_id,
                              extra_fields=extra_fields)
        type_check = not (isinstance(start, datetime) and isinstance(end, datetime))
        tz_check = start.tzinfo is None or end.tzinfo is None
        if type_check or tz_check:
            raise PVLiveException("Start and end must be timezone-aware Python datetime objects.")
        if end < start:
            raise PVLiveException("Start must be later than end.")
        start = self._nearest_hh(start)
        end = self._nearest_hh(end)
        data = []
        request_start = start
        max_range = self.max_range["national"] if entity_id == 0 and entity_type == 0 else \
                    self.max_range["regional"]
        while request_start <= end:
            request_end = min(end, request_start + max_range)
            params = self._compile_params(extra_fields, request_start, request_end)
            response = self._query_api(entity_type, entity_id, params)
            data += response["data"]
            request_start += max_range + timedelta(minutes=30)
        if dataframe:
            columns = response["meta"]
            return self._convert_tuple_to_df(data, columns), response["meta"]
        if entity_type == "pes":
            return data, response["meta"]
        response["meta"].remove("n_ggds")
        return [d[:2] + d[3:] for d in data], response["meta"]

    def _compile_params(self, extra_fields="", start=None, end=None):
        """Compile parameters into a Python dict, formatting where necessary."""
        params = {}
        if extra_fields:
            params["extra_fields"] = extra_fields
        if start is not None:
            params["start"] = start.isoformat().replace("+00:00", "Z")
        end = start if (start is not None and end is None) else end
        if end is not None:
            params["end"] = end.isoformat().replace("+00:00", "Z")
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

    def _nearest_hh(self, dt):
        """Round a given datetime object up to the nearest half hour."""
        if not(dt.minute % 30 == 0 and dt.second == 0 and dt.microsecond == 0):
            dt = dt - timedelta(minutes=dt.minute%30, seconds=dt.second) + timedelta(minutes=30)
        return dt

    @staticmethod
    def _validate_inputs(entity_type="pes", entity_id=0, extra_fields=""):
        """Validate common input parameters."""
        if not isinstance(entity_type, str):
            raise PVLiveException("The entity_type must be a string.")
        if entity_type not in ["pes", "gsp"]:
            raise PVLiveException("The entity_type must be either 'pes' or 'gsp'.")
        if not isinstance(extra_fields, str):
            raise PVLiveException("The extra_fields must be a comma-separated string.")
        if entity_type == "pes":
            if entity_id != 0 and entity_id not in range(10, 24):
                raise PVLiveException("The pes_id must be an integer between 10 and 23 (inclusive) "
                                      "or 0 (For national).")
        elif entity_type == "gsp":
            if entity_id not in range(0, 328):
                raise PVLiveException("The gsp_id must be an integer between 0 and 327 "
                                      "(inclusive).")

def main():
    """Placeholder for CLI."""
    print("There is no CLI for this module yet.")
    sys.exit()

if __name__ == "__main__":
    main()
