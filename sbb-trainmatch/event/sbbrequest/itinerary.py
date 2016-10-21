# Core python
import uuid
from datetime import timedelta

# Sci stack
import pandas as pd
from numpy import logical_and

# SBB
import init_data_struct as ids

LEG_SUB_TYPES = ['S','IR','R','RE','EC','RJ','ICE','IC','ICN','VAE','TGV']

class Itinerary(object):

    def __init__(self, itinerary_nodes, CONFIG, response):
        """
        Initialize the Itinerary object with 4 dataframes
        each itinerary / leg / segment assigned a unique ID
        """

        # config parser
        self.config = CONFIG

        # sbb response
        self.response = response

        # Tuple of (itinerary_ID, itinerary_node)
        itin_nodes = [(str(uuid.uuid4()), itin) for itin in itinerary_nodes]

        # Tuple of (itineary_ID, leg_ID, leg_node, numbering)
        leg_nodes = [(itin[0], str(uuid.uuid4()), leg, i)
                     for itin in itin_nodes
                     for i, leg in enumerate(self.response.get_leg_nodes(itin[1]))]

        # Tuple of (leg_ID, segment_ID, segment_node, numbering)
        seg_nodes = [(leg[1], str(uuid.uuid4()), seg, i * 2)
                     for leg in leg_nodes
                     for i, seg in enumerate(self.response.get_segment_nodes(leg[2]))]

        # Build dataframes
        # Turn the itineraries into a dataframe
        itin_nodes_df = pd.DataFrame(itin_nodes, columns=['itinerary_id', 'node'])
        self.itinerary_df = ids.init_itineraries_df({'itinerary_id': itin_nodes_df['itinerary_id'],
                                                     'node': itin_nodes_df['node']
                                                     })

        # Turn the legs into a dataframe
        leg_nodes_df = pd.DataFrame(leg_nodes, columns=['itinerary_id', 'leg_id', 'node', 'leg_number'])
        self.legs_df = ids.init_legs_df({'leg_id': leg_nodes_df['leg_id'],
                                         'node': leg_nodes_df['node'],
                                         'leg_number': leg_nodes_df['leg_number']
                                         })

        # Turn the segments into a dataframe
        seg_nodes_df = pd.DataFrame(seg_nodes, columns=['leg_id', 'segment_id', 'node', 'segment_number'])
        self.segments_df = ids.init_segments_df({'segment_id': seg_nodes_df['segment_id'],
                                                 'node': seg_nodes_df['node'],
                                                 'segment_number': seg_nodes_df['segment_number']
                                                 })

        # Build the link table dataframe
        merged_link = pd.merge(leg_nodes_df[['itinerary_id', 'leg_id']],
                               seg_nodes_df[['leg_id', 'segment_id']],
                               on='leg_id', how='outer')
        self.trip_link_df = ids.init_trip_link_df({'itinerary_id': merged_link['itinerary_id'],
                                                   'leg_id': merged_link['leg_id'],
                                                   'segment_id': merged_link['segment_id']
                                                   })

        # Populate data frames
        self.populate_dfs()

        # Node position in the etree is no longer needed, drop them
        self.drop_nodes()

        # counts the number of stops
        self.eval_n_seg('nb_train_stops')

        # add moving segs
        self.add_moving_segments()

        # calculate the number of legs/segs
        self.eval_n_leg()
        self.eval_n_seg('num_segments')

    def populate_dfs(self):
        self.__populate_itineraries__()
        self.__populate_legs__()
        self.__populate_segments__()

    def drop_nodes(self):
        self.itinerary_df.drop('node', axis=1, inplace=True)
        self.legs_df.drop('node', axis=1, inplace=True)
        self.segments_df.drop('node', axis=1, inplace=True)

    def eval_n_seg(self, seg_count_name):
        seg_mask = self.trip_link_df['segment_id'].isin(self.segments_df[~self.segments_df['waypoint']].index)
        # Count segments
        seg_count = self.trip_link_df.loc[seg_mask, ['leg_id', 'segment_id']].groupby('leg_id').count()
        seg_count.rename(columns={'segment_id': 'num_legs'}, inplace=True)
        self.legs_df[seg_count_name] = seg_count
        # Legs that have 0 segments won't get counted to fill NaN with 0
        self.legs_df[seg_count_name].fillna(0, inplace=True)

    def add_moving_segments(self):
        """
            Segments are only the static stays at the station, and their IDs are all even.
            Adds odd-numbered movements segments that link station stops.
            Updates the trip_link_df with the new IDs

        """

        # TODO test that waypoint inclusion works well

        leg_subset = self.legs_df.loc[self.legs_df['leg_type'] == '', ['leg_number']]
        seg_subset = self.segments_df.loc[~self.segments_df['waypoint'],
                                     ['segment_number', 'time_start', 'time_end', 'stop_id_start', 'stop_id_end']]

        merged = pd.merge(self.trip_link_df, leg_subset, left_on='leg_id', right_index=True, suffixes=('', '_leg'),
                          sort=False)
        merged = pd.merge(merged, seg_subset, left_on='segment_id', right_index=True, suffixes=('', '_seg'), sort=False)

        # values need to be ordered before using .shift()
        merged.sort_values(['itinerary_id', 'leg_number', 'segment_number'], ascending=True, inplace=True)

        # Pads with START_TRIP_BUFFER the 1st and last segment to include the wait at station.
        time_buffer = timedelta(seconds=int(self.config.get('params', 'START_TRIP_BUFFER')))
        merged_groupby = merged.copy().groupby('itinerary_id')  # TODO -- why is COPY needed?
        first_pts_list = merged_groupby['segment_id'].first()
        self.segments_df.loc[first_pts_list.values, 'time_start'] = self.segments_df.loc[first_pts_list.values, 'time_end'] \
                                                               - time_buffer
        last_pts_list = merged_groupby['segment_id'].last()
        self.segments_df.loc[last_pts_list.values, 'time_end'] = self.segments_df.loc[last_pts_list.values, 'time_start'] \
                                                            + time_buffer

        # starts from the end of previous segment and goes to start of next one
        temp_col_names = {'time_end': 'time_start',
                          'stop_id_end': 'stop_id_start',
                          'time_start': 'time_end',
                          'stop_id_start': 'stop_id_end'
                          }
        merged.rename(columns=temp_col_names, inplace=True)

        merged[['time_end', 'stop_id_end']] = merged[['time_end', 'stop_id_end']].shift(-1).values
        merged['segment_number'] += 1

        # Drop segments that link different itineraries
        merged = merged[merged['itinerary_id'] == merged['itinerary_id'].shift(-1)]
        # Initialize new uuid for the segments that were created
        merged['segment_id'] = [str(uuid.uuid4()) for i in range(merged['segment_id'].shape[0])]
        merged['waypoint'] = False

        new_seg_view = merged[['segment_id', 'segment_number', 'time_start', 'time_end', 'stop_id_start', 'stop_id_end',
                               'waypoint']]
        new_segments = ids.init_segments_df(values=new_seg_view, set_index=True, drop_node=True)

        self.segments_df = pd.concat([self.segments_df, new_segments])
        self.trip_link_df = pd.concat([self.trip_link_df, merged[self.trip_link_df.columns]])

        # Identify long_pause segments
        # # (these are weighted more heavily later because 'static' points are deemed more reliable)
        train_long_stop_threshold = timedelta(seconds=int(self.config.get('params', 'TRAIN_LONG_STOP_THRESHOLD')))
        self.segments_df['is_long_stop'] = logical_and(
            (self.segments_df['time_end'] - self.segments_df['time_start']) >= train_long_stop_threshold,
            (self.segments_df['segment_number'] % 2) == 0)

    def eval_n_leg(self):
        """
            This currently calculates all legs, including those labeled as 'FUSSWEG' ot other ones to be skipped...
        """
        # takes only legs that are well conditioned
        leg_mask = self.trip_link_df['leg_id'].isin(self.legs_df[self.legs_df['leg_type'] == ''].index)
        if any(leg_mask):
            # count legs
            leg_count = self.trip_link_df.loc[leg_mask, ['itinerary_id', 'leg_id']].groupby('itinerary_id')['leg_id'].nunique()
            self.itinerary_df['num_legs'] = leg_count
            # Itineraries that have 0 legs won't get counted to fill NaN with 0
            self.itinerary_df['num_legs'].fillna(0, inplace=True)
        else:
            #nunique() not defined on a groupby empty series
            self.itinerary_df['num_legs'] = 0

    def __populate_itineraries__(self):
        self.itinerary_df['time_start'] = self.itinerary_df['node'].apply(self.response.get_itin_start_datetime)
        self.itinerary_df['time_end'] = self.itinerary_df['node'].apply(self.response.get_itin_end_datetime)
        self.itinerary_df['context_reconstruction'] = self.itinerary_df['node'].apply(self.response.get_itin_context_reconstruction)

        self.itinerary_df.set_index('itinerary_id', inplace=True)

    def __populate_legs__(self):
        self.legs_df['route_full_name'] = self.legs_df['node'].apply(self.response.get_leg_route_full_name)
        self.legs_df['route_category'] = self.legs_df['node'].apply(self.response.get_leg_route_category)
        self.legs_df['route_line'] = self.legs_df['node'].apply(self.response.get_leg_route_line)
        self.legs_df['route_number'] = self.legs_df['node'].apply(self.response.get_leg_route_number)
        self.legs_df['agency_id'] = self.legs_df['node'].apply(self.response.get_leg_agency_id)

        self.legs_df['time_start'] = self.legs_df['node'].apply(self.response.get_leg_time_start)
        self.legs_df['time_planned_start'] = self.legs_df['node'].apply(self.response.get_leg_planned_time_start)
        self.legs_df['stop_id_start'] = self.legs_df['node'].apply(self.response.get_leg_stop_id_start)
        self.legs_df['station_name_start'] = self.legs_df['node'].apply(self.response.get_leg_station_name_start)
        self.legs_df['platform_start'] = self.legs_df['node'].apply(self.response.get_leg_platform_start)

        self.legs_df['time_end'] = self.legs_df['node'].apply(self.response.get_leg_time_end)
        self.legs_df['time_planned_end'] = self.legs_df['node'].apply(self.response.get_leg_planned_time_end)
        self.legs_df['stop_id_end'] = self.legs_df['node'].apply(self.response.get_leg_stop_id_end)
        self.legs_df['station_name_end'] = self.legs_df['node'].apply(self.response.get_leg_station_name_end)
        self.legs_df['platform_end'] = self.legs_df['node'].apply(self.response.get_leg_platform_end)

        self.eval_leg_route_name()  # in-place evaluates legs_df['route_name']
        self.legs_df['leg_type'] = self.legs_df['node'].apply(self.eval_leg_type_error)
        self.legs_df = self.legs_df[self.legs_df['leg_type'] == '']  # TODO : testing, clean up

        self.legs_df.set_index('leg_id', inplace=True)

    def __populate_segments__(self):
        # The segment being built first are those of all 'stops' at station, with a departure and arrival time
        self.segments_df['time_start'] = self.segments_df['node'].apply(self.response.get_seg_time_arrival)
        self.segments_df['time_end'] = self.segments_df['node'].apply(self.response.get_seg_time_departure)
        self.segments_df['stop_id_start'] = self.segments_df['node'].apply(self.response.get_seg_stop_id)
        self.segments_df['stop_id_end'] = self.segments_df['stop_id_start']
        self.eval_segment_is_waypoint()  # fills ['waypoint'] column

        # 1st/last segment of a leg have a start/end time as NaT.
        self.eval_nat_replace()

        self.segments_df.set_index('segment_id', inplace=True)

    def eval_leg_route_name(self):
        # initialize and ensures str() rather than NaN-float
        self.legs_df['route_name'] = self.legs_df['route_category'] + ' '
        # unlike previous version of code, this is a Null value and not a 'None' str
        mask = self.legs_df['route_line'].isnull()
        self.legs_df.ix[mask, 'route_name'] += self.legs_df.ix[mask, 'route_number']
        self.legs_df.ix[~mask, 'route_name'] += self.legs_df.ix[~mask, 'route_line']

    def eval_leg_type_error(self, leg):

        if self.response.get_leg_type(leg) == 'FUSSWEG':
            return 'FUSSWEG'
        elif self.response.get_leg_route_category(leg) not in LEG_SUB_TYPES:
            return 'SKIP'
        else:
            return ''

    def eval_segment_is_waypoint(self):

        self.segments_df['waypoint'] = self.segments_df['node'].apply(self.response.get_seg_type) != 'STATION'
        self.segments_df.loc[(self.segments_df['time_start'].isnull() & self.segments_df['time_end'].isnull()), 'waypoint'] = True

    def eval_nat_replace(self):

        # set start time to end time if missing
        self.segments_df.loc[self.segments_df['time_start'].isnull(), 'time_start'] = \
            self.segments_df.loc[self.segments_df['time_start'].isnull(), 'time_end']

        # set end time to start time if missing
        self.segments_df.loc[self.segments_df['time_end'].isnull(), 'time_end'] = \
            self.segments_df.loc[self.segments_df['time_end'].isnull(), 'time_start']

        return