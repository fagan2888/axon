import pandas as pd
import logging


def load_agg_data(args, DB):

    trip_info = load_trip_info(args, DB)
    mot_segment_list = trip_info['mot_segment_id'].unique()
    itin_info, legs_info, pts_info = load_other_info(mot_segment_list, DB)

    trip_info.set_index('mot_segment_id', inplace=True)
    itin_info.set_index('itinerary_id', inplace=True)

    return trip_info, itin_info, legs_info, pts_info


def load_trip_info(args, DB):

    sql = DB.get_query('trip_info', __file__)

    where_clauses = []
    where_clauses_values = {
        'v': args.vid,
        'm': args.mot,
        't': args.time
    }

    if args.vid != '':
        where_clauses.append("""t.vid=%(v)s""")
    if args.mot != '':
        where_clauses.append("""t.mot_segment_id=%(m)s""")
    if args.time != '':
        where_clauses.append("""t.time_start>=%(t)s""")
    if args.warning:
        where_clauses.append("""s.warning_bool IS FALSE""")

    if where_clauses:
        sql += 'WHERE ' + ' AND '.join(where_clauses)

    sql += ';'

    trip_info = DB.postgres2pandas(sql, params=where_clauses_values)

    return trip_info


def load_other_info(mot_segment_list, DB):

    sql_var = {'mot_id': list(mot_segment_list)}

    sql  = DB.get_query('itin_info', __file__)
    itin_info = DB.postgres2pandas(sql, params=sql_var)

    sql  = DB.get_query('legs_info', __file__)
    legs_info = DB.postgres2pandas(sql, params=sql_var)

    sql  = DB.get_query('pts_info', __file__)
    pts_info = DB.postgres2pandas(sql, params=sql_var)

    sql  = DB.get_query('pts_fpga_info', __file__)
    pts_fpga_info = DB.postgres2pandas(sql, params=sql_var)

    if set(pts_info.columns) != set(pts_fpga_info.columns):
        logging.error('trainvis.load_save_data.load_other_info(): mismatch between pts_info and pts_fpga_info column names')
        raise

    pts_info = pd.concat([pts_info, pts_fpga_info], axis=0)

    return itin_info, legs_info, pts_info


def write_xls(trip_info):
    """
    Generates an excel spreadsheet used to facilitate analysis

    :param trip_info:
    :return:
    """

    trip_info[['vid', 'time_start', 'warning_str', 'delta_next']].sort_values(['vid', 'time_start'])\
        .to_excel('out.xlsx')

    return
