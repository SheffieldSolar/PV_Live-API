# PV_Live
A Python implementation of the PV_Live web API. See https://www.solar.sheffield.ac.uk/pvlive/

### What is this repository for? ###

* A Python interface for the PV_Live web API to enable accessing PV_Live results in Python code.
* Version 0.2
* Works with Python 2.7+ or 3.5+

### How do I get set up? ###

* Make sure you have Git installed - [Download Git](https://git-scm.com/downloads)
* Run `pip install git+https://github.com/SheffieldSolar/PV_Live-API`
    - NOTE: You may need to run this command as sudo on Linux machines depending, on your Python installation i.e. `sudo pip install git+https://github.com/SheffieldSolar/PV_Live-API`

### Getting started ###

See [pvlive_api_demo.py](https://github.com/SheffieldSolar/PV_Live-API/blob/master/pvlive_api_demo.py) for example usage.
```Python
from pvlive_api import PVLive

pvl = PVLive()
pvl.latest()
```

### Documentation ###

* [https://sheffieldsolar.github.io/PV_Live-API/](https://sheffieldsolar.github.io/PV_Live-API/)

### How do I update? ###

Sheffield Solar will endeavour to update this library in sync with the [PV_Live API](https://www.solar.sheffield.ac.uk/pvlive/api/ "PV_Live API webpage"), but cannot guarantee this. To make sure you are forewarned of upcoming changes to the API, you should email [solar@sheffield.ac.uk](mailto:solar@sheffield.ac.uk?subject=PV_Live%20API%20email%20updates "Email Sheffield Solar") and request to be added to the PV_Live user mailing list.

To upgrade the code:
* Run `pip install --upgrade git+https://github.com/SheffieldSolar/PV_Live-API`

### Contribution guidelines ###

* To Do

### Who do I talk to? ###

* Jamie Taylor - [jamie.taylor@sheffield.ac.uk](mailto:jamie.taylor@sheffield.ac.uk "Email Jamie") - [SheffieldSolar](https://github.com/SheffieldSolar)

### Authors ###

* **Jamie Taylor** - *Initial work* - [SheffieldSolar](https://github.com/SheffieldSolar)

### License ###

No license is defined yet - use at your own risk.
