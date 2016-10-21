import numpy as np
from datetime import timedelta
import xml_path


def skip_duplicates_itineraries(itinerary_nodes, previous_itineraries_cr):

    # Do not recalculate itineraries already included in that trip
    itinerary_nodes = [node for node in itinerary_nodes
                       if not np.in1d(xml_path.get_itin_context_reconstruction(node), previous_itineraries_cr)]

    return itinerary_nodes


def skip_visit_overlap_itineraries(trip, itinerary_nodes, CONFIG):

    time_buffer = timedelta(minutes=int(CONFIG.get('params', 'VISIT_TIME_OVERLAP_BUFFER')))
    min_time = trip['trip_time_start'] - time_buffer
    max_time = trip['trip_time_end'] + time_buffer

    itinerary_nodes = [node for node in itinerary_nodes
                       if (xml_path.get_itin_start_datetime(node) > min_time) and
                       (xml_path.get_itin_end_datetime(node) < max_time)]

    return itinerary_nodes


# TODO the code below is not used anymore...
def remove_itineraries(itinerary_list, trip_link_df, itinerary_df, legs_df, segments_df):
    """
    In-place removal of all points associated with selected itineraries (excepting 1st occurrence of itinerary)

    :param itinerary_list:
    :param trip_link_df:
    :param itinerary_df:
    :param legs_df:
    :param segments_df:
    :return:
    """

    # gets the indexes (itinerary uuid) associated with points to be removed
    to_remove_itins = itinerary_list.index[itinerary_list.values]
    # Builds a boolean list for the trip link table
    to_remove_trip_link_bool = np.in1d(trip_link_df['itinerary_id'], to_remove_itins)
    # Builds a list of legs uuids to be removed
    to_remove_legs = trip_link_df.loc[to_remove_trip_link_bool, 'leg_id'].dropna().unique()
    # Builds a list of segments uuids to be removed
    to_remove_segs = trip_link_df.loc[to_remove_trip_link_bool, 'segment_id'].dropna().unique()

    # remove duplicates
    trip_link_df.drop(trip_link_df.index[to_remove_trip_link_bool], inplace=True)
    itinerary_df.drop(to_remove_itins, inplace=True)
    legs_df.drop(to_remove_legs, inplace=True)
    segments_df.drop(to_remove_segs, inplace=True)

    return


def remove_visit_overlap(trip, trip_link_df, itinerary_df, legs_df, segments_df, CONFIG):

    time_buffer = timedelta(minutes=int(CONFIG.get('params', 'VISIT_TIME_OVERLAP_BUFFER')))
    # True when trip needs to be removed (overlaps with either visit by more than buffer)
    out_of_bounds_itineraries = (itinerary_df['time_start'] < (trip['trip_time_start']-time_buffer)) | \
                                (itinerary_df['time_end'] > (trip['trip_time_end']+time_buffer))

    remove_itineraries(out_of_bounds_itineraries, trip_link_df, itinerary_df, legs_df, segments_df)

    return
