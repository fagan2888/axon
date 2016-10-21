import os
import argparse
import pandas as pd

from vibepy.load_logger import load_logger
from vibepy.read_config import read_config
import vibepy.class_postgres as class_postgres
from trainvis.analyze_trip import display_trips
from trainvis.load_save_data import write_xls, load_agg_data


def main(args):
    """
    Loads all the sm_XXXXX tables into dataframe and display trips one by one

    :return:
    """

    # TODO add some output options (e.g. images as .png in S3 bucket)
    load_logger(log_config_folder=os.path.dirname(__file__))
    CONFIG = read_config(ini_filename='application.ini', ini_path=os.path.dirname(__file__))

    if args.from_pkl:
        # To pickle
        trip_info = pd.read_pickle('trip_info.pkl')
        itin_info = pd.read_pickle('itin_info.pkl')
        legs_info = pd.read_pickle('legs_info.pkl')
        pts_info = pd.read_pickle('pts_info.pkl')
    else:
        DB = class_postgres.PostgresManager(CONFIG, 'database')
        trip_info, itin_info, legs_info, pts_info = load_agg_data(args, DB)

        if args.to_pkl:
            # From pickle
            trip_info.to_pickle('trip_info.pkl')
            itin_info.to_pickle('itin_info.pkl')
            legs_info.to_pickle('legs_info.pkl')
            pts_info.to_pickle('pts_info.pkl')

    if args.xlsx:
        write_xls(trip_info)

    display_trips(trip_info, itin_info, legs_info, pts_info, CONFIG)
    return


def get_args():
    """
    Parses command line arguments
    (based on https://pymotw.com/2/argparse/)

    Possible options:
        - output to png, local / s3 bucket
        - only iOS / Android

    :return:
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('-v', action='store', dest='vid', default='', help='Look up a single vid')
    parser.add_argument('-m', action='store', dest='mot', default='', help='Look up a single mot segment id')
    parser.add_argument('-t', action='store', dest='time', default='', help='Set a start time, YYYY-MM-DD HH:MM:SS')
    parser.add_argument('-x', action='store_true', dest='xlsx', default=False, help='Also outputs an excel spreadsheet')
    parser.add_argument('-w', action='store_false', dest='warning', default=True,
                        help='Shows journeys with warning flags in (otherwise skips them)')
    parser.add_argument('-s', action='store_true', dest='to_pkl', default=False, help='Save raw data as .pkl files')
    parser.add_argument('-r', action='store_true', dest='from_pkl', default=False, help='Read raw data from .pkl files')


    args = parser.parse_args()
    return args


if __name__ == "__main__":

    args = get_args()
    main(args)