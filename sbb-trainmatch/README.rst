=======================
Train Schedule Matching
=======================

.. contents:: **Table of Contents**

**Overview**

This bot runs through train trips detected by the mode of transportation bot (MoT) in Switzerland and tries to find the
best match from candidate journey using the SBB API for routing, i.e. given a start (and end) latitude, longitude as
well as a time, the API returns a list of candidate journeys which SM analyzes.

Documentation here: https://axonvibe.atlassian.net/wiki/display/VIBE/Schedule+Matching


Start the bot
=============

The file to run is: ``event/run_bot.py``

All the parameters are defined in ``application.ini.template`` and ``%%VALUE%%`` fields are populated from a hiera .yaml
file when deployed. To run locally an ``application.ini`` with appropriate values is needed. Similarly the logging
parameters are defined in ``logging_config.json.template``.

Triggering a Schedule Matching event
------------------------------------

The SM bot gets triggered when it receives JSON payloads from the MoT bot output queue at:
::
    [rabbitmq]
    rabbit_mot_exchange=%%RABBIT_MOT_EXCHANGE%%

A sample JSON is shown below for a single MoT segment (the list can contain multiple ones):
::
    {"batch_id": 1494447,
     "mot_segments": [{"end_lon": 8.4856714788068395,
                       "start_lon": 8.5166242782020394,
                       "mot_segment_id": "f80a58ef-7ab4-44ed-b09b-dc2049301054",
                       "mot": "walking",
                       "end_lat": 47.226723748118403,
                       "start_lat": 47.171813462698097}]}

Only train trips starting and ending in switzerland will be processed. This is controlled by
::
    [geovalidity]
    VALID_REGION_WKT=%%VALID_REGION_WKT_SM%%

Metrics are pushed to grafana through programatically-generated namespaces at ``%%STATSD_NAMESPACE%%.schedule-matching``.


Required tables
---------------

Input:
  - public.locations
  - public.visits
  - public.mot_segments
  - public.train_routes
  - public.route_geoms
  - public.hotspots_gh16

  - sbb.stn_hops
  - sbb.stops

Output:
  - public.train_trips
  - public.train_trips_leg
  - public.train_trips_failed

  - sbb.sm_trips
  - sbb.sm_trip_link
  - sbb.sm_itineraries
  - sbb.sm_legs
  - sbb.sm_segments
  - sbb.sm_points
  - sbb.sm_points_fpga
  - sbb.sm_point_meta
  - sbb.sm_stats
  - sbb.sm_diagnostics


Sample application.ini file
---------------------------

Sensitive fields replaced with --LOOK UP PASSPACK--
::
    [database]
    --LOOK UP PASSPACK--

    [sbb]
    --LOOK UP PASSPACK--

    [params]
    MAX_WALK_DIST=2500
    START_TRIP_BUFFER=600
    TRAIN_LONG_STOP_THRESHOLD=300
    TRAIN_LONG_STOP_WEIGHT=2.0
    ORDER_FILTER_CUTOFF=0.75
    ORDER_FILTER_MIN_STD=2.0
    VISIT_TIME_OVERLAP_BUFFER=10
    N_ROW_MIN=10
    N_DIST_MAX=8000
    N_TIME_MIN=0.5

    [mapbox]
    API_URL= --LOOK UP PASSPACK--
    START_END_ZOOM_LEVEL=14

    [geovalidity]
    VALID_REGION_WKT=switzerland

    [statsd]
    --LOOK UP PASSPACK--

    [smtp]
    host=localhost
    port=25
    sender=trainmatch
    recipients=your_email@axonvibe.com

    [rabbitmq]
    --LOOK UP PASSPACK--


Visualize the Schedule-Matched train trips
==========================================

The file to run is: ``event/schedule_match_viz.py``

usage: ``schedule_matcher_viz.py [-h] [-v VID] [-m MOT] [-t TIME] [-x] [-w]``

optional arguments:
      -h, --help  show this help message and exit
      -v VID      Look up a single vid
      -m MOT      Look up a single mot segment id
      -t TIME     Set a start time, YYYY-MM-DD HH:MM:SS
      -x          Also outputs an excel spreadsheet
      -w          Shows journeys with warning flags in (otherwise skips them)

Documentation and examples: https://axonvibe.atlassian.net/wiki/display/VIBE/Visualization+Tools


Step-by-step installation
-------------------------

In case you're having issues running the code, the method below should work.

Create a temporary file containing the conda-installable modules from requirements.txt, for example a
temprequirements.txt that contains the following:
::
    uuid==1.30
    psycopg2==2.6.1
    matplotlib==1.4.3
    pandas==0.18.0
    numpy==1.10.4
    scipy==0.17.0
    joblib==0.9.4
    requests==2.9.1
    shapely==1.5.13

Create the virtual environment from the temporary requirements file:
::
    conda create -n trainviz --file temprequirements.txt
    source activate trainviz

Install non-conda modules with pip within the virtual env
::
    pip install vibepy
    pip install vibebot

If python is installed as a framework (typical on Mac OS for example), need to run pythonw
::
    conda install wxpython

Make sure you ``~/.matplotlib/matplotlibrc`` file has:
::
    backend : WXAgg

Running the visualization tool will now require you to use ``pythonw`` rather than ``python``.
::
    pythonw event/schedule_match_viz.py [options]


Run a batch of MoT segments
===========================

The file to run is: ``event/schedule_matcher_batch.py``

**Note:** *This is not a command-line friendly code. You need to define the segments you're interested within the schedule_matcher_batch code by either entering the mot_segment_id or a sql query that does so.*

