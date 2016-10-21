Train Schedule Matching SETUP
=============================

This folder contains all the codes which need to be run once before schedule matching can run.

The code relies on the availability of a GTFS feed for all swiss train which can be found here:
- http://gtfs.geops.ch/

The following S3 bucket holds all the large-volume files that are needed:
- S3 bucket info here

build_sbb_stn_graph.py
    - Creates the stn_hops table containing the shortest number of connections required to get between pairs of stations
    - Generates two files:
        - station_ij_dist.csv
        - create_stn_hops_table.sql
    - The sql file must be run first to create the required table
    - The code also outputs a \copy statement (to be run in psql) to fill the table with the data


Write this later. NOTE: This is a restructured text formatted file. Lets be super PEP8 people! Read more at

http://docutils.sourceforge.net/docs/user/rst/quickstart.html