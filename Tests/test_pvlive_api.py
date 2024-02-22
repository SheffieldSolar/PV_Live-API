"""
Unit tests for the pvlive library.
Author: Ethan Jones
Written: 07/11/2020
"""

import unittest
from datetime import datetime, date
import pytz

import pandas.api.types as ptypes
from pvlive_api import PVLive

class PVLiveTestCase(unittest.TestCase):
    """Tests for `pvlive.py`."""

    def setUp(self):
        """
        Setup the instance of class.
        """
        self.api = PVLive()
        self.expected_dtypes = {
            "pes_id": ptypes.is_integer_dtype,
            "gsp_id": ptypes.is_integer_dtype,
            "datetime_gmt": ptypes.is_datetime64_any_dtype,
            "generation_mw": ptypes.is_float_dtype,
            "bias_error": ptypes.is_float_dtype,
            "capacity_mwp": ptypes.is_float_dtype,
            "installedcapacity_mwp": ptypes.is_float_dtype,
            "lcl_mw": ptypes.is_float_dtype,
            "stats_error": ptypes.is_float_dtype,
            "ucl_mw": ptypes.is_float_dtype,
            "uncertainty_MW": ptypes.is_float_dtype,
            "site_count": ptypes.is_integer_dtype,
            "release": ptypes.is_string_dtype,
            "GSPs": ptypes.is_string_dtype,
            "llsoa": ptypes.is_string_dtype,
            "system_size": ptypes.is_string_dtype,
            "install_month": ptypes.is_datetime64_dtype,
            "dc_capacity_mwp": ptypes.is_float_dtype,
            "system_count": ptypes.is_integer_dtype,
            "cumul_capacity_mwp": ptypes.is_float_dtype,
        }

    def check_df_dtypes(self, api_df):
        """
        Check the dtypes of a Pandas DataFrame
        against the expected dtypes from the API.
        """
        for column in api_df.columns:
            if column in self.expected_dtypes:
                with self.subTest(column=column):
                    assert self.expected_dtypes[column](api_df[column])

    def check_gsp_tuple_dtypes(self, data):
        """
        Check the dtypes of a gsp tuple
        against the expected dtypes from the API.
        """
        with self.subTest(column="gsp_id"):
            assert isinstance(data[0], int)
        with self.subTest(column="datetime_gmt"):
            assert isinstance(data[1], str)
        with self.subTest(column="generation_mw"):
            assert isinstance(data[2], float)

    def check_pes_tuple_dtypes(self, data):
        """
        Check the dtypes of a pes tuple
        against the expected dtypes from the API.
        """
        with self.subTest(column="pes_id"):
            assert isinstance(data[0], int)
        with self.subTest(column="datetime_gmt"):
            assert isinstance(data[1], str)
        with self.subTest(column="generation_mw"):
            assert isinstance(data[2], float)

    def check_pes_tuple(self, data):
        """
        Check the length of the returned pes tuple
        against the expected length.
        """
        with self.subTest():
            assert isinstance(data, tuple) and len(data) == 3

    def check_gsp_tuple(self, data):
        """
        Check the length of the returned gsp tuple
        against the expected length.
        """
        with self.subTest():
            assert isinstance(data, tuple) and len(data) == 3

    def check_df_columns(self, data):
        """
        Check the columns of the returned DataFrame
        against the expected columns.
        """
        with self.subTest():
            assert (("pes_id" in data or "gsp_id" in data) and "datetime_gmt" in data
                    and "generation_mw" in data)

    def check_deployment_df_columns(self, data):
        """
        Check the columns of the returned DataFrame
        against the expected columns.
        """
        with self.subTest():
            assert (("llsoa" in data or "GSPs" in data) and "release" in data
                    and "dc_capacity_mwp" in data)

    def test_latest(self):
        """Tests the latest function."""
        data = self.api.latest(entity_type="pes", entity_id=0)
        self.check_pes_tuple(data)
        self.check_pes_tuple_dtypes(data)
        data = self.api.latest(entity_type="pes", entity_id=0, dataframe=True)
        self.check_df_columns(data)
        self.check_df_dtypes(data)
        data = self.api.latest(entity_type="pes", entity_id=0,
                               extra_fields="ucl_mw,lcl_mw,installedcapacity_mwp,stats_error",
                               dataframe=True)
        self.check_df_columns(data)
        self.check_df_dtypes(data)
        data = self.api.latest(entity_type="pes", entity_id=0, period=5)
        self.check_pes_tuple(data)
        self.check_pes_tuple_dtypes(data)
        data = self.api.latest(entity_type="pes", entity_id=0, period=5, dataframe=True)
        self.check_df_columns(data)
        self.check_df_dtypes(data)
        data = self.api.latest(entity_type="pes", entity_id=0,
                               extra_fields="ucl_mw,lcl_mw,installedcapacity_mwp,stats_error",
                               period=5, dataframe=True)
        self.check_df_columns(data)
        self.check_df_dtypes(data)
        data = self.api.latest(entity_type="gsp", entity_id=103)
        self.check_gsp_tuple(data)
        self.check_gsp_tuple_dtypes(data)
        data = self.api.latest(entity_type="gsp", entity_id=103, dataframe=True)
        self.check_df_columns(data)
        self.check_df_dtypes(data)

    def test_day_peak(self):
        """Tests the day_peak function."""
        data = self.api.day_peak(d=date(2023, 12, 1), entity_type="pes", entity_id=0)
        self.check_pes_tuple(data)
        self.check_pes_tuple_dtypes(data)
        data = self.api.day_peak(d=date(2023, 12, 1), entity_type="pes", entity_id=0, dataframe=True)
        self.check_df_columns(data)
        self.check_df_dtypes(data)
        data = self.api.day_peak(d=date(2023, 12, 1),
                                 extra_fields="ucl_mw,lcl_mw,installedcapacity_mwp,stats_error",
                                 entity_type="pes", entity_id=0, dataframe=True)
        data = self.api.day_peak(d=date(2023, 12, 1), entity_type="pes", entity_id=0, period=5)
        self.check_pes_tuple(data)
        self.check_pes_tuple_dtypes(data)
        data = self.api.day_peak(d=date(2023, 12, 1), entity_type="pes", entity_id=0, period=5,
                                 dataframe=True)
        self.check_df_columns(data)
        self.check_df_dtypes(data)
        data = self.api.day_peak(d=date(2023, 12, 1),
                                 extra_fields="ucl_mw,lcl_mw,installedcapacity_mwp,stats_error",
                                 entity_type="pes", entity_id=0, period=5, dataframe=True)
        self.check_df_dtypes(data)
        data = self.api.day_peak(d=date(2023, 12, 1), entity_type="gsp", entity_id=54)
        self.check_gsp_tuple(data)
        self.check_gsp_tuple_dtypes(data)
        data = self.api.day_peak(d=date(2023, 12, 1), entity_type="gsp", entity_id=54,
                                 dataframe=True)
        self.check_df_columns(data)
        self.check_df_dtypes(data)

    def test_day_energy(self):
        """Tests the day_energy function."""
        data = self.api.day_energy(d=date(2023, 12, 1), entity_type="pes", entity_id=0)
        assert isinstance(data, float)

    def test_between(self):
        """Test the between function."""
        data = self.api.between(start=datetime(2023, 12, 1, 12, 20, tzinfo=pytz.utc),
                                end=datetime(2023, 12, 1, 14, 0, tzinfo=pytz.utc),
                                entity_type="pes", entity_id=0)
        with self.subTest():
            assert isinstance(data, list)
        data = self.api.between(start=datetime(2023, 12, 1, 12, 20, tzinfo=pytz.utc),
                                end=datetime(2023, 12, 1, 14, 0, tzinfo=pytz.utc),
                                entity_type="pes", entity_id=0, dataframe=True)
        self.check_df_columns(data)
        self.check_df_dtypes(data)
        data = self.api.between(start=datetime(2023, 12, 1, tzinfo=pytz.utc),
                                end=datetime(2023, 12, 1, 14, 00, tzinfo=pytz.utc),
                                entity_type="pes", entity_id=0, period=5)
        with self.subTest():
            assert isinstance(data, list)
        data = self.api.between(start=datetime(2023, 12, 1, 12, 20, tzinfo=pytz.utc),
                                end=datetime(2023, 12, 1, 14, 00, tzinfo=pytz.utc),
                                entity_type="pes", entity_id=0, period=5, dataframe=True)
        self.check_df_columns(data)
        self.check_df_dtypes(data)

    def test_at_time(self):
        """Test the at_time function."""
        data = self.api.at_time(dt=datetime(2023, 12, 1, 12, 35, tzinfo=pytz.utc), entity_type="pes",
                                            entity_id=0)
        self.check_pes_tuple(data)
        self.check_pes_tuple_dtypes(data)
        data = self.api.at_time(datetime(2023, 12, 1, 12, 35, tzinfo=pytz.utc), entity_type="pes",
                                entity_id=0, dataframe=True)
        self.check_df_columns(data)
        self.check_df_dtypes(data)
        data = self.api.at_time(datetime(2023, 12, 1, 12, 35, tzinfo=pytz.utc), entity_type="pes",
                                entity_id=0,
                                extra_fields="ucl_mw,lcl_mw,installedcapacity_mwp,stats_error",
                                dataframe=True)
        self.check_df_dtypes(data)
        data = self.api.at_time(dt=datetime(2023, 12, 1, 12, 35, tzinfo=pytz.utc), entity_type="pes",
                                            entity_id=0, period=5)
        self.check_pes_tuple(data)
        self.check_pes_tuple_dtypes(data)
        data = self.api.at_time(datetime(2023, 12, 1, 12, 35, tzinfo=pytz.utc), entity_type="pes",
                                entity_id=0, period=5, dataframe=True)
        self.check_df_columns(data)
        self.check_df_dtypes(data)
        data = self.api.at_time(datetime(2023, 12, 1, 12, 35, tzinfo=pytz.utc), entity_type="pes",
                                entity_id=0,
                                extra_fields="ucl_mw,lcl_mw,installedcapacity_mwp,stats_error",
                                period=5, dataframe=True)
        self.check_df_dtypes(data)
        data = self.api.at_time(dt=datetime(2023, 12, 1, 12, 35, tzinfo=pytz.utc), entity_type="gsp",
                                entity_id=26)
        self.check_gsp_tuple(data)
        self.check_gsp_tuple_dtypes(data)
        data = self.api.at_time(datetime(2023, 12, 1, 12, 35, tzinfo=pytz.utc), entity_type="gsp",
                                         entity_id=26, dataframe=True)
        self.check_df_columns(data)
        self.check_df_dtypes(data)

    def test_deployment(self):
        """Tests the deployment function."""
        # import pdb; pdb.set_trace()
        data = self.api.deployment(region="gsp")
        self.check_deployment_df_columns(data)
        self.check_df_dtypes(data)
        data = self.api.deployment(region="llsoa")
        self.check_deployment_df_columns(data)
        self.check_df_dtypes(data)
        data = self.api.deployment(region="gsp", include_history=True)
        self.check_deployment_df_columns(data)
        self.check_df_dtypes(data)
        with self.subTest(test_type="errors"):
            with self.assertRaises(ValueError):
                data = self.api.deployment(region="gsp", by_system_size=True)
        data = self.api.deployment(region="gsp", include_history=True, by_system_size=True)
        self.check_deployment_df_columns(data)
        self.check_df_dtypes(data)
        data = self.api.deployment(region="gsp", release=1)
        self.check_deployment_df_columns(data)
        self.check_df_dtypes(data)

if __name__ == "__main__":
    unittest.main(verbosity=2)
