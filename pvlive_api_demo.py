"""
Demo script for the PVLive-API library.
See https://github.com/SheffieldSolar/PV_Live-API for installation instructions.

- Jamie Taylor <jamie.taylor@sheffield.ac.uk>
- First Authored: 2018-06-04
"""

from datetime import datetime, date
import pytz

from pvlive_api import PVLive

def main():
    pvlive = PVLive()
    print("Latest:", pvlive.latest())
    print("At 2018-06-03 12:00: ", pvlive.at_time(datetime(2018, 6, 3, 12, 0, tzinfo=pytz.utc)))
    print("At 2018-06-03 12:30: ", pvlive.at_time(datetime(2018, 6, 3, 12, 30, tzinfo=pytz.utc)))
    print("At 2018-06-03 12:35: ", pvlive.at_time(datetime(2018, 6, 3, 12, 35, tzinfo=pytz.utc)))
    print("Peak on 2018-06-03: ", pvlive.day_peak(date(2018, 6, 3)))

if __name__ == "__main__":
    main()
