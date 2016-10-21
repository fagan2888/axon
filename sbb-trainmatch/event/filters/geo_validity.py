import pandas as pd
import numpy as np
import logging
import os
from shapely.geometry.point import Point
from shapely.wkt import loads


def geo_valid(json_input, CONFIG):
    """
    In order to make a guess at MoT we have to have OSM data. geoms is a list of geometries (closed polygons)
    that define the regions of validity of this model. This function returns a list of visit id's that are valid
    TODO: If we don't specify a lat and lon, go look this up.
    :param visits: A list of dictionaries containing a lat, a lon and a visit id
    :param CONFIG: The parsed config file
    :return: Will output a list of visits that are within the regions of validity
    """


    geoms = read_geo_valid(CONFIG)
    p = pd.DataFrame(json_input)

    p = p[p['mot'] == 'train']
    if p.empty:
        logging.debug('No train journey within json')
        return []

    is_within = p.apply(lambda x: np.all([geom.contains(Point(x['start_lon'], x['start_lat'])) and
                                          geom.contains(Point(x['end_lon'], x['end_lat'])) for geom in geoms]), axis=1)

    return p.loc[is_within, 'mot_segment_id'].tolist()


def read_geo_valid(CONFIG):
    """
    :param CONFIG: The parsed config file
    :return: A list of geometries (polygons) defining out geo valid regions
    """
    path = os.path.dirname(os.path.realpath(__file__))
    filenames = CONFIG.get('geovalidity', 'VALID_REGION_WKT').split(',')
    geoms = []
    for filename in filenames:
        filepath = path + '/wkt/' + filename + '.wkt'
        try:
            f = open(filepath)
            geoms.append(loads(f.read()))
        except:
            logging.critical('Could not read shapefile {filepath}'.format(filepath=filepath))
            raise IOError
    return geoms


