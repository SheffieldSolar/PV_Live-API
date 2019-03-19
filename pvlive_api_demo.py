"""
Demo script for the PVLive-API library.
See https://github.com/SheffieldSolar/PV_Live-API for installation instructions.

- Jamie Taylor <jamie.taylor@sheffield.ac.uk>
- First Authored: 2018-06-04
- Updated: 2019-03-19 to use PV_Live API v2
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
    print("\nAt 2019-03-18 12:35: ")
    print(pvlive.at_time(datetime(2019, 3, 18, 12, 35, tzinfo=pytz.utc)))
    print("\nBetween 2019-03-18 10:30 and 2019-03-18 14:00: ")
    print(pvlive.between(datetime(2019, 3, 18, 10, 30, tzinfo=pytz.utc),
                         datetime(2019, 3, 18, 14, 00, tzinfo=pytz.utc)))
    print("\nPeak on 2019-03-18: ")
    print(pvlive.day_peak(date(2019, 3, 18)))
    print("\nCumulative generation on 2019-03-18: ")
    print(pvlive.day_energy(date(2019, 3, 18)))
    print("\n\n---------- REGIONAL ----------")
    print("\nLatest PES region 22: ")
    print(pvlive.latest(pes_id=22))
    print("\nPES region 22 at 2019-03-18 12:00: ")
    print(pvlive.at_time(datetime(2019, 3, 18, 12, 0, tzinfo=pytz.utc), pes_id=22))
    print("\nPES region 22 between 2019-03-18 10:30 and 2019-03-18 14:00: ")
    print(pvlive.between(datetime(2019, 3, 18, 10, 30, tzinfo=pytz.utc),
                         datetime(2019, 3, 18, 14, 00, tzinfo=pytz.utc), pes_id=22))
    print("\nPES region 22 peak on 2019-03-18: ")
    print(pvlive.day_peak(date(2019, 3, 18), pes_id=22))
    print("\nPES region 22 cumulative generation on 2019-03-18: ")
    print(pvlive.day_energy(date(2019, 3, 18), pes_id=22))

if __name__ == "__main__":
    main()
