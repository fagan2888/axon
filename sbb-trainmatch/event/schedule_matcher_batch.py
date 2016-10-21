import os

from vibepy.load_logger import load_logger
from vibepy.read_config import read_config
import vibepy.class_postgres as class_postgres
from run_single_batch import process_batch
from traineval.output_to_postgres import truncate_all_sm_tables


def main():

    load_logger(log_config_folder=os.path.dirname(__file__))
    CONFIG = read_config(ini_filename='application.ini', ini_path=os.path.dirname(__file__))
    DB = class_postgres.PostgresManager(CONFIG, 'database')

    # Either process [all] since Jan 1st 2016 or only [new] ones.
    # batch_id, list_mot_id = get_all_ids(DB)  # ALL
    # batch_id, list_mot_id = get_new_ids(DB) # NEW
    # batch_id, list_mot_id = get_all_ids_since_date(DB) # SINCE DATE
    # batch_id, list_mot_id = get_specific_vid(DB)
    list_mot_id = ['28d1cf6e-21f2-4309-ac00-165ab93a59f7','90d34597-d901-4d64-873d-f16caac3a219']
    # list_mot_id = list_mot_id[:6]
    process_batch(list_mot_id, CONFIG, DB)

    # for li in list_mot_id:
    #     try:
    #         process_batch(batch_id, [li], CONFIG, DB)
    #     except:
    #         print 'error with id', li

    return


def get_specific_vid(DB):
    sql = """SELECT DISTINCT mot_segments.id
        FROM mot_segments
        JOIN locations
            ON locations.mot_segment_id=mot_segments.id
            AND locations.datetime_created>'2016-06-01T00:00:00'
        WHERE locations.vid IN ('AVANDDE3-6C12-5288-C199-B032CFC3269E')
        ;
        """

    query_output = DB.query_fetchall(sql)
    list_mot_id = [x[0] for x in query_output]

    return list_mot_id


def get_all_ids_since_date(DB):
    sql = """SELECT DISTINCT mot_segments.id
        FROM mot_segments
        JOIN locations
            ON locations.mot_segment_id=mot_segments.id
            AND locations.datetime_created>'2016-05-14T00:00:00'
        WHERE mot='train';
        """

    query_output = DB.query_fetchall(sql)
    list_mot_id = [x[0] for x in query_output]

    return list_mot_id


def get_all_ids(DB):
    sql = """SELECT DISTINCT mot_segments.id
        FROM mot_segments
        JOIN locations
            ON locations.mot_segment_id=mot_segments.id
            AND locations.timezone_coordinate='Europe/Zurich'
            AND locations.datetime_created>'2016-01-01T00:00:00'
        JOIN vid_whitelist w on w.vid=locations.vid
        WHERE mot='train'
            --AND locations.vid NOT LIKE 'AVAND%';
        """

    query_output = DB.query_fetchall(sql)
    list_mot_id = [x[0] for x in query_output]

    return list_mot_id


def get_new_ids(DB):

    # TODO this is at risk of constantly re-running failed points... (when get_stops returns nothing...) double check!
    # Get new MoT IDs
    sql_mot = """
        SELECT DISTINCT m.id
        FROM mot_segments m
        JOIN locations l
            ON l.mot_segment_id=m.id
            AND l.timezone_coordinate='Europe/Zurich'
            AND l.datetime_created>'2016-01-01T00:00:00'
        WHERE mot='train'
            AND vid LIKE 'AVAND%'
            AND NOT EXISTS (
                SELECT 1  -- it's mostly irrelevant what you put here
                FROM   sbb.sm_trips t
                WHERE  m.id = t.mot_segment_id::uuid
            );
        """
    query_output = DB.query_fetchall(sql_mot)
    list_mot_id = [x[0] for x in query_output]

    return list_mot_id


if __name__ == "__main__":
    main()
