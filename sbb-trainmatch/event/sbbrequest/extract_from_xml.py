import uuid
import xml.etree.cElementTree as ET

import pandas as pd

import init_data_struct as ids
import remove_itineraries as ri
import xml_eval
import xml_path


def initialize_all_df(itinerary_nodes):
    """
    Returns the 4 dataframes (trip_link_df, itinerary_df, legs_df, segments_df)
    initialized as defined in init_data_struct.py
    with each itinerary/leg/segment assigned a unique IDs (uuid4() converted to string)

    :param itinerary_nodes:
    :return:
    """

    # Build nested bundles and assign IDs

    # (itinerary_ID, itinerary_node)
    itin_nodes = [(str(uuid.uuid4()), itin) for itin in itinerary_nodes]

    # (itinerary_ID, leg_ID, leg_node, numbering)
    leg_nodes = [(itin[0], str(uuid.uuid4()), leg, i)
                 for itin in itin_nodes
                 for i, leg in enumerate(xml_path.get_leg_nodes(itin[1]))]

    # (leg_ID, segment_ID, segment_node, numbering)
    seg_nodes = [(leg[1], str(uuid.uuid4()), seg, i * 2)
                 for leg in leg_nodes
                 for i, seg in enumerate(xml_path.get_segment_nodes(leg[2]))]

    # Turn the itineraries into a dataframe
    itin_nodes_df = pd.DataFrame(itin_nodes, columns=['itinerary_id', 'node'])
    itinerary_df = ids.init_itineraries_df({'itinerary_id': itin_nodes_df['itinerary_id'],
                                            'node': itin_nodes_df['node']
                                            })

    # Turn the legs into a dataframe
    leg_nodes_df = pd.DataFrame(leg_nodes, columns=['itinerary_id', 'leg_id', 'node', 'leg_number'])
    legs_df = ids.init_legs_df({'leg_id': leg_nodes_df['leg_id'],
                                'node': leg_nodes_df['node'],
                                'leg_number': leg_nodes_df['leg_number']
                                })

    # Turn the segments into a dataframe
    seg_nodes_df = pd.DataFrame(seg_nodes, columns=['leg_id', 'segment_id', 'node', 'segment_number'])
    segments_df = ids.init_segments_df({'segment_id': seg_nodes_df['segment_id'],
                                        'node': seg_nodes_df['node'],
                                        'segment_number': seg_nodes_df['segment_number']
                                        })

    # Build the link table dataframe
    merged_link = pd.merge(leg_nodes_df[['itinerary_id', 'leg_id']],
                           seg_nodes_df[['leg_id', 'segment_id']],
                           on='leg_id', how='outer')
    trip_link_df = ids.init_trip_link_df({'itinerary_id': merged_link['itinerary_id'],
                                          'leg_id': merged_link['leg_id'],
                                          'segment_id': merged_link['segment_id']
                                          })

    return trip_link_df, itinerary_df, legs_df, segments_df


def populate_itinerary_df(itinerary_df):
    itinerary_df['time_start'] = itinerary_df['node'].apply(xml_path.get_itin_start_datetime)
    itinerary_df['time_end'] = itinerary_df['node'].apply(xml_path.get_itin_end_datetime)
    itinerary_df['context_reconstruction'] = itinerary_df['node'].apply(xml_path.get_itin_context_reconstruction)

    itinerary_df.set_index('itinerary_id', inplace=True)

    return


def populate_legs_df(legs_df):
    legs_df['route_full_name'] = legs_df['node'].apply(xml_path.get_leg_route_full_name)
    legs_df['route_category'] = legs_df['node'].apply(xml_path.get_leg_route_category)
    legs_df['route_line'] = legs_df['node'].apply(xml_path.get_leg_route_line)
    legs_df['route_number'] = legs_df['node'].apply(xml_path.get_leg_route_number)
    legs_df['agency_id'] = legs_df['node'].apply(xml_path.get_leg_agency_id)

    legs_df['time_start'] = legs_df['node'].apply(xml_path.get_leg_time_start)
    legs_df['time_planned_start'] = legs_df['node'].apply(xml_path.get_leg_planned_time_start)
    legs_df['stop_id_start'] = legs_df['node'].apply(xml_path.get_leg_stop_id_start)
    legs_df['station_name_start'] = legs_df['node'].apply(xml_path.get_leg_station_name_start)
    legs_df['platform_start'] = legs_df['node'].apply(xml_path.get_leg_platform_start)

    legs_df['time_end'] = legs_df['node'].apply(xml_path.get_leg_time_end)
    legs_df['time_planned_end'] = legs_df['node'].apply(xml_path.get_leg_planned_time_end)
    legs_df['stop_id_end'] = legs_df['node'].apply(xml_path.get_leg_stop_id_end)
    legs_df['station_name_end'] = legs_df['node'].apply(xml_path.get_leg_station_name_end)
    legs_df['platform_end'] = legs_df['node'].apply(xml_path.get_leg_platform_end)

    xml_eval.eval_leg_route_name(legs_df)  # in-place evaluates legs_df['route_name']
    legs_df['leg_type'] = legs_df['node'].apply(xml_eval.eval_leg_type_error)

    legs_df.set_index('leg_id', inplace=True)

    # TODO the following can be NaN, investigate: route_name, agency_id, route_category -- Figure out right thing to do
    # legs_df[['route_name','agency_id','route_category']].fillna('', inplace=True)

    return


def populate_segments_df(segments_df):

    # The segment being built first are those of all 'stops' at station, with a departure and arrival time
    segments_df['time_start'] = segments_df['node'].apply(xml_path.get_seg_time_arrival)
    segments_df['time_end'] = segments_df['node'].apply(xml_path.get_seg_time_departure)
    segments_df['stop_id_start'] = segments_df['node'].apply(xml_path.get_seg_stop_id)
    segments_df['stop_id_end'] = segments_df['stop_id_start']
    xml_eval.eval_segment_is_waypoint(segments_df)  # fills ['waypoint'] column

    # 1st/last segment of a leg have a start/end time as NaT.
    xml_eval.eval_nat_replace(segments_df)

    segments_df.set_index('segment_id', inplace=True)

    return


def build_single_itinerary(response, trip, previous_itineraries_cr, CONFIG):
    """
    Takes in the XML response and turns it into an etree. Then builds tables containing itinerary/leg/segment including
    the node in the tree where they reside.
    The build_ functions then populate all fields of the dataframes with the data associated with that node in the
    etree. All the paths for the various fields are stored as functions in the xml_path.py file.
    Unique IDs are associated to each item (i/l/s) using a uuid4() generator.

    :param response: XML response content from the SBB API call
    :param trip:
    :param previous_itineraries_cr:
    :param CONFIG:
    :return: trip_link_df, itinerary_df, legs_df, segments_df
    """

    # Build a tree from the XML
    root = ET.fromstring(response)

    # Extracts the nodes corresponding to itineraries from the tree
    itinerary_nodes = xml_path.get_itinerary_nodes(root)

    # Removes itineraries that have been previously added to this trip
    itinerary_nodes = ri.skip_duplicates_itineraries(itinerary_nodes, previous_itineraries_cr)
    # Remove itineraries that overlap with previous/next visit by more than (buffer), a quantity found in CONFIG
    itinerary_nodes = ri.skip_visit_overlap_itineraries(trip, itinerary_nodes, CONFIG)
    # Skip if no itineraries are left
    if len(itinerary_nodes) == 0:
        return ids.initialize_all_empty_df()

    # Initialize all the dataframes and populate them with (unique identifier, node) for all itinerary/legs/segments
    trip_link_df, itinerary_df, legs_df, segments_df = initialize_all_df(itinerary_nodes)

    # Adds the data in place (dataframe not returned from function)
    populate_itinerary_df(itinerary_df)
    populate_legs_df(legs_df)
    populate_segments_df(segments_df)

    # Node position in the etree no longer needed. Drop them before returning the dataframes
    itinerary_df.drop('node', axis=1, inplace=True)
    legs_df.drop('node', axis=1, inplace=True)
    segments_df.drop('node', axis=1, inplace=True)

    # Counts the number of stops te
    xml_eval.eval_n_seg(trip_link_df, legs_df, segments_df, 'nb_train_stops')

    segments_df, trip_link_df = xml_eval.add_moving_segments(segments_df, legs_df, trip_link_df, CONFIG)

    # Calculates the number of legs/segments, inplace=True
    xml_eval.eval_n_leg(trip_link_df, itinerary_df, legs_df)
    xml_eval.eval_n_seg(trip_link_df, legs_df, segments_df, 'num_segments')

    return trip_link_df, itinerary_df, legs_df, segments_df
