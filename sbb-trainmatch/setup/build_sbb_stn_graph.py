import pandas as pd
import numpy as np
from scipy.sparse.csgraph import dijkstra


def build_stn_graph(fpath_gtfs, fname_out=None):
    ''' Uses the two following GTFS files:
        - transfers.txt
        - stop_times.txt
    that are found in the fpath_gtfs directory
    '''

    # edges between different station IDs that are connected
    transfers = pd.read_csv(fpath_gtfs+'transfers.txt')

    # edges between a train trip and each station it stops at
    stop_times = pd.read_csv(fpath_gtfs+'stop_times.txt')
    stop_times['stop_parent_id'] = stop_times['stop_id'].apply(lambda x: x.split(':')[0])

    # Since we're interested in parent station, de-duplicate items in list
    unique_parent_id = stop_times['stop_parent_id'].unique()
    x = pd.DataFrame(np.inf, columns=unique_parent_id, index=unique_parent_id)

    # Assign a distance of 1 between stations which are directly connected by one trip
    for trip in stop_times[['trip_id','stop_parent_id']].groupby('trip_id')['stop_parent_id']:
        x.loc[trip[1].values,trip[1].values] = 1

    # Assigns a distance of 0 for stops which are connected by a transfer
    # (i.e. assume they're part of the same parent station)
    for transfer in transfers.ix[transfers['from_stop_id'] != transfers['to_stop_id'],
                                 ['from_stop_id','to_stop_id']].iterrows():
        x.ix[transfer[1][0],transfer[1][1]] = 0
        x.ix[transfer[1][1],transfer[1][0]] = 0

    # Scipy seems to expect infinite, rather than 0 distance on teh diagonal (a station with itself)
    np.fill_diagonal(x.values, np.inf)

    # Batch shortest-path calculation for all possible pairs of station
    dj = dijkstra(x.values, directed=False)

    # Disconnected stations are assigned a -1 distance for easy filtering out (rather than an arbitrary large number)
    dj[np.logical_or(np.isinf(dj), np.isnan(dj))] = -1

    # Converts into a pandas dataframe for convenient stacking of the n by n matrix into n*(n-1) rows (one per i,j pair)
    dj_df = pd.DataFrame(dj.astype(int), columns=unique_parent_id, index=unique_parent_id).stack().reset_index()

    # Saves the output into a .csv for convenient copy into a postgres table
    if fname_out: dj_df.to_csv(fname_out, index=False, header=False)

    return dj_df


def create_table(table_name, schema, fname_out=None):
    """
    Table creation statement

    :param table_name: (str) Name of the station hops table to be created
    :param schema: (str) Schema in which the table will be created
    :param fname_out: (str) Path including file name where the .sql file for table creation should be saved
    :return:
    """

    sql_str = '''CREATE TABLE {schema}.{table_name} (
    station_i varchar(20),
    station_j varchar(20),
    n_hops integer
);
CREATE INDEX stn_i_idx ON {schema}.{table_name} (station_i);
CREATE INDEX stn_j_idx ON {schema}.{table_name} (station_j);
CREATE INDEX stn_i_j_idx ON {schema}.{table_name} (station_i, station_j);'''.format(table_name=table_name, schema=schema)

    if fname_out:
        with open(fname_out, "w") as text_file:
            text_file.write(sql_str)

    return sql_str


# This builds the tables. Should be a one off thing
if __name__ == "__main__":

    # Parameters
    fpath_gtfs = 'gtfs_train_file_path/'
    fname_out_graph = fpath_gtfs+'station_ij_dist.csv'
    fname_out_table = fpath_gtfs + 'create_stn_hops_table.sql'
    table_name = 'stn_hops'
    schema = 'sbb'

    # Build and save the graph locally
    build_stn_graph(fpath_gtfs, fname_out=fname_out_graph)

    # Table creation statement with appropriate schemas
    create_table(table_name, schema, fname_out=fname_out_table)

    # Copy the data from file to table
    print '\npsql import statement:\n'
    print "\COPY {n} FROM '{f}' CSV DELIMITER ',';".format(n=table_name,f=fname_out_graph)