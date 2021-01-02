"""
Demo script for the PVLive-API library.
See https://github.com/SheffieldSolar/PV_Live-API for installation instructions.

- Jamie Taylor <jamie.taylor@sheffield.ac.uk>
- Ethan Jones <ejones18@sheffield.ac.uk>
- First Authored: 2018-06-04
- Updated: 2020-10-20 to return Pandas dataframe object
"""

from datetime import datetime, date
import pytz

from pvlive_api import PVLive

def main():
    pvlive = PVLive()
    print("---------- NATIONAL ----------")
    print("\nLatest: ")
    print(pvlive.latest())
    print("\nAt 2019-03-18 12:00: ")
    print(pvlive.at_time(datetime(2019, 3, 18, 12, 0, tzinfo=pytz.utc)))
    print("\nAt 2019-03-18 12:00 as a Pandas DataFrame object: ")
    print(pvlive.at_time(datetime(2019, 3, 18, 12, 0, tzinfo=pytz.utc), dataframe=True))
    print("\nAt 2019-03-18 12:35: ")
    print(pvlive.at_time(datetime(2019, 3, 18, 12, 35, tzinfo=pytz.utc)))
    print("\nBetween 2019-03-18 10:30 and 2019-03-18 14:00: ")
    print(pvlive.between(datetime(2019, 3, 18, 10, 30, tzinfo=pytz.utc),
                         datetime(2019, 3, 18, 14, 00, tzinfo=pytz.utc)))
    print("\nBetween 2019-03-18 10:30 and 2019-03-18 14:00 as a Pandas DataFrame object: ")
    print(pvlive.between(datetime(2019, 3, 18, 10, 30, tzinfo=pytz.utc),
                         datetime(2019, 3, 18, 14, 00, tzinfo=pytz.utc), dataframe=True))
    print("\nPeak on 2019-03-18: ")
    print(pvlive.day_peak(date(2019, 3, 18)))
    print("\nPeak on 2019-03-18 as a Pandas DataFrame object: ")
    print(pvlive.day_peak(date(2019, 3, 18), dataframe=True))
    print("\nCumulative generation on 2019-03-18: ")
    print(pvlive.day_energy(date(2019, 3, 18)))
    print("\n\n---------- REGIONAL - PES REGION 22 ----------")
    print("\nLatest PES region 22: ")
    print(pvlive.latest(region_type=0, region_id=22))
    print("\nLatest PES region 22 as a Pandas DataFrame object: ")
    print(pvlive.latest(region_type=0, region_id=22, dataframe=True))
    print("\nPES region 22 at 2019-03-18 12:00: ")
    print(pvlive.at_time(datetime(2019, 3, 18, 12, 0, tzinfo=pytz.utc), region_type=0, region_id=22))
    print("\nPES region 22 at 2019-03-18 12:00 as a Pandas DataFrame object: ")
    print(pvlive.at_time(datetime(2019, 3, 18, 12, 0, tzinfo=pytz.utc), region_type=0, region_id=22, dataframe=True))
    print("\nPES region 22 between 2019-03-18 10:30 and 2019-03-18 14:00: ")
    print(pvlive.between(datetime(2019, 3, 18, 10, 30, tzinfo=pytz.utc),
                         datetime(2019, 3, 18, 14, 00, tzinfo=pytz.utc), region_type=0, region_id=22))
    print("\nPES region 22 between 2019-03-18 10:30 and 2019-03-18 14:00 as a Pandas DataFrame object: ")
    print(pvlive.between(datetime(2019, 3, 18, 10, 30, tzinfo=pytz.utc),
                         datetime(2019, 3, 18, 14, 00, tzinfo=pytz.utc), region_type=0, region_id=22, dataframe=True))
    print("\nPES region 22 peak on 2019-03-18: ")
    print(pvlive.day_peak(date(2019, 3, 18), region_type=0, region_id=22))
    print("\nPES region 22 peak on 2019-03-18 as a Pandas DataFrame object: ")
    print(pvlive.day_peak(date(2019, 3, 18), region_type=0, region_id=22, dataframe=True))
    print("\nPES region 22 cumulative generation on 2019-03-18: ")
    print(pvlive.day_energy(date(2019, 3, 18), region_type=0, region_id=22))
    print("\n\n---------- REGIONAL - GSP REGION 134 ----------")
    print("\nLatest GSP region 134: ")
    print(pvlive.latest(region_type=1, region_id=134))
    print("\nLatest GSP region 134 as a Pandas DataFrame object: ")
    print(pvlive.latest(region_type=1, region_id=134, dataframe=True))
    print("\nGSP region 134 at 2019-03-18 12:00: ")
    print(pvlive.at_time(datetime(2019, 3, 18, 12, 0, tzinfo=pytz.utc), region_type=1, region_id=134))
    print("\nGSP region 134 at 2019-03-18 12:00 as a Pandas DataFrame object: ")
    print(pvlive.at_time(datetime(2019, 3, 18, 12, 0, tzinfo=pytz.utc), region_type=1, region_id=134, dataframe=True))
    print("\nGSP region 134 between 2019-03-18 10:30 and 2019-03-18 14:00: ")
    print(pvlive.between(datetime(2019, 3, 18, 10, 30, tzinfo=pytz.utc),
                         datetime(2019, 3, 18, 14, 00, tzinfo=pytz.utc), region_type=1, region_id=134))
    print("\nGSP region 134 between 2019-03-18 10:30 and 2019-03-18 14:00 as a Pandas DataFrame object: ")
    print(pvlive.between(datetime(2019, 3, 18, 10, 30, tzinfo=pytz.utc),
                         datetime(2019, 3, 18, 14, 00, tzinfo=pytz.utc), region_type=1, region_id=134, dataframe=True))
    print("\nGSP region 134 peak on 2019-03-18: ")
    print(pvlive.day_peak(date(2019, 3, 18), region_type=1, region_id=134))
    print("\nGSP region 134 peak on 2019-03-18 as a Pandas DataFrame object: ")
    print(pvlive.day_peak(date(2019, 3, 18), region_type=1, region_id=134, dataframe=True))
    print("\nGSP region 134 cumulative generation on 2019-03-18: ")
    print(pvlive.day_energy(date(2019, 3, 18), region_type=1, region_id=134))

if __name__ == "__main__":
    main()
