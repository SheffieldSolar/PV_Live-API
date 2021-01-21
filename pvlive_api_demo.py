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
    """
    Demo API's functionality.
    """
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
                         datetime(2019, 3, 18, 14, 0, tzinfo=pytz.utc)))
    print("\nBetween 2019-03-18 10:30 and 2019-03-18 14:00 as a Pandas DataFrame object: ")
    print(pvlive.between(datetime(2019, 3, 18, 10, 30, tzinfo=pytz.utc),
                         datetime(2019, 3, 18, 14, 0, tzinfo=pytz.utc), dataframe=True))
    print("\nPeak on 2019-03-18: ")
    print(pvlive.day_peak(date(2019, 3, 18)))
    print("\nPeak on 2019-03-18 as a Pandas DataFrame object: ")
    print(pvlive.day_peak(date(2019, 3, 18), dataframe=True))
    print("\nCumulative generation on 2019-03-18: ")
    print(pvlive.day_energy(date(2019, 3, 18)))

    print("\n\n---------- REGIONAL - PES REGION 23 ----------")
    print("\nLatest PES region 23: ")
    print(pvlive.latest(entity_type="pes", entity_id=23))
    print("\nLatest PES region 23 as a Pandas DataFrame object: ")
    print(pvlive.latest(entity_type="pes", entity_id=23, dataframe=True))
    print("\nPES region 23 at 2019-03-18 12:00: ")
    print(pvlive.at_time(datetime(2019, 3, 18, 12, 0, tzinfo=pytz.utc), entity_type="pes",
                         entity_id=23))
    print("\nPES region 23 at 2019-03-18 12:00 as a Pandas DataFrame object: ")
    print(pvlive.at_time(datetime(2019, 3, 18, 12, 0, tzinfo=pytz.utc), entity_type="pes",
                         entity_id=23, dataframe=True))
    print("\nPES region 23 between 2019-03-18 10:30 and 2019-03-18 14:00: ")
    print(pvlive.between(datetime(2019, 3, 18, 10, 30, tzinfo=pytz.utc),
                         datetime(2019, 3, 18, 14, 0, tzinfo=pytz.utc), entity_type="pes",
                         entity_id=23))
    print("\nPES region 23 between 2019-03-18 10:30 and 2019-03-18 14:00 as a Pandas DataFrame object: ")
    print(pvlive.between(datetime(2019, 3, 18, 10, 30, tzinfo=pytz.utc),
                         datetime(2019, 3, 18, 14, 0, tzinfo=pytz.utc), entity_type="pes",
                         entity_id=23, dataframe=True))
    print("\nPES region 23 peak on 2019-03-18: ")
    print(pvlive.day_peak(date(2019, 3, 18), entity_type="pes", entity_id=23))
    print("\nPES region 23 peak on 2019-03-18 as a Pandas DataFrame object: ")
    print(pvlive.day_peak(date(2019, 3, 18), entity_type="pes", entity_id=23, dataframe=True))
    print("\nPES region 23 cumulative generation on 2019-03-18: ")
    print(pvlive.day_energy(date(2019, 3, 18), entity_type="pes", entity_id=23))

    print("\n\n---------- REGIONAL - GSP ID 120 ----------")
    print("\nLatest GSP ID 120: ")
    print(pvlive.latest(entity_type="gsp", entity_id=120))
    print("\nLatest GSP ID 120 as a Pandas DataFrame object: ")
    print(pvlive.latest(entity_type="gsp", entity_id=120, dataframe=True))
    print("\nGSP ID 120 at 2019-03-18 12:00: ")
    print(pvlive.at_time(datetime(2019, 3, 18, 12, 0, tzinfo=pytz.utc), entity_type="gsp",
                         entity_id=120))
    print("\nGSP ID 120 at 2019-03-18 12:00 as a Pandas DataFrame object: ")
    print(pvlive.at_time(datetime(2019, 3, 18, 12, 0, tzinfo=pytz.utc), entity_type="gsp",
                         entity_id=120, dataframe=True))
    print("\nGSP ID 120 between 2019-03-18 10:30 and 2019-03-18 14:00: ")
    print(pvlive.between(datetime(2019, 3, 18, 10, 30, tzinfo=pytz.utc),
                         datetime(2019, 3, 18, 14, 0, tzinfo=pytz.utc), entity_type="gsp",
                         entity_id=120))
    print("\nGSP ID 120 between 2019-03-18 10:30 and 2019-03-18 14:00 as a Pandas DataFrame object: ")
    print(pvlive.between(datetime(2019, 3, 18, 10, 30, tzinfo=pytz.utc),
                         datetime(2019, 3, 18, 14, 0, tzinfo=pytz.utc), entity_type="gsp",
                        entity_id=120, dataframe=True))
    print("\nGSP ID 120 peak on 2019-03-18: ")
    print(pvlive.day_peak(date(2019, 3, 18), entity_type="gsp", entity_id=120))
    print("\nGSP ID 120 peak on 2019-03-18 as a Pandas DataFrame object: ")
    print(pvlive.day_peak(date(2019, 3, 18), entity_type="gsp", entity_id=120, dataframe=True))
    print("\nGSP ID 120 cumulative generation on 2019-03-18: ")
    print(pvlive.day_energy(date(2019, 3, 18), entity_type="gsp", entity_id=120))

if __name__ == "__main__":
    main()
