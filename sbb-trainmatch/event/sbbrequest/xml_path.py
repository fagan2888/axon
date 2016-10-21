from datetime import datetime

from xml_get import get_nodes, remove_non_ascii, get_node_text_value


def get_time_from_short_path(itinerary, short_path):
    """
    Time formatting

    :param itinerary:
    :param short_path:
    :return:
    """
    # TODO : Fix/Add Timezones!
    # TODO ensure it doesn't break comparison for visit overlap (uses str...)
    tz = 'Europe/Zurich'
    date_str = get_nodes(itinerary, short_path+['NS1:Datum'])
    time_str = get_nodes(itinerary, short_path+['NS1:Zeit'])

    if date_str and time_str:
        datetime_str = '{d} {t}'.format(d=date_str[0].text, t=time_str[0].text)
        return datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")

    return


# ROOT :: get_...(root) methods

def get_itinerary_nodes(root):
    itinerary_tree_path_short = ['soapenv:Body',
                                 'NS1:FindVerbindungenResponse',
                                 'NS1:Verbindungen',
                                 'NS1:Verbindung']
    return get_nodes(root, itinerary_tree_path_short)


# ITINERARY :: get_...(itinerary) methods

def get_leg_nodes(itinerary):
    leg_tree_path_short = ['NS1:Verbindungsabschnitte',
                           'NS1:Verbindungsabschnitt']
    return get_nodes(itinerary, leg_tree_path_short)


def get_itin_start_datetime(itinerary):
    short_path = ['NS1:Zusammenfassung',
                  'NS1:Abfahrt',
                  'NS1:DatumZeit',
                  'NS1:Aktuell']
    return get_time_from_short_path(itinerary, short_path)


def get_itin_end_datetime(itinerary):
    short_path = ['NS1:Zusammenfassung',
                  'NS1:Ankunft',
                  'NS1:DatumZeit',
                  'NS1:Aktuell']
    return get_time_from_short_path(itinerary, short_path)


def get_itin_context_reconstruction(itinerary):
    short_path = ['NS1:ContextReconstruction']
    [context_reconstruction] = get_nodes(itinerary, short_path)
    return context_reconstruction.text


# LEG :: get_...(leg) methods

def get_segment_nodes(leg):
    segment_tree_path_short = ['NS1:Haltepunkte',
                               'NS1:Haltepunkt']
    return get_nodes(leg, segment_tree_path_short)


def get_leg_type(leg):
    short_path = ['NS1:Verkehrsmittel',
                  'NS1:Typ']
    return get_node_text_value(leg, short_path)


def get_leg_route_full_name(leg):
    short_path = ['NS1:Verkehrsmittel',
                  'NS1:Informationen',
                  'NS1:Name']
    return get_node_text_value(leg, short_path)


def get_leg_route_category(leg):
    short_path = ['NS1:Verkehrsmittel',
                  'NS1:Informationen',
                  'NS1:Kategorie',
                  'NS1:Abkuerzung']
    return get_node_text_value(leg, short_path)


def get_leg_route_line(leg):
    short_path = ['NS1:Verkehrsmittel',
                  'NS1:Informationen',
                  'NS1:Linie']
    return get_node_text_value(leg, short_path)


def get_leg_route_number(leg):
    short_path = ['NS1:Verkehrsmittel',
                  'NS1:Informationen',
                  'NS1:Nummer']  # seems to be identical to 'NS1:ExterneNummer'
    return get_node_text_value(leg, short_path)


def get_leg_agency_id(leg):
    short_path = ['NS1:Verkehrsmittel',
                  'NS1:Informationen',
                  'NS1:TransportUnternehmungCode']
    return get_node_text_value(leg, short_path)


def get_leg_time_start(leg):
    short_path = ['NS1:Abfahrt',
                  'NS1:DatumZeit',
                  'NS1:Aktuell']
    return get_time_from_short_path(leg, short_path)


def get_leg_time_end(leg):
    short_path = ['NS1:Ankunft',
                  'NS1:DatumZeit',
                  'NS1:Aktuell']
    return get_time_from_short_path(leg, short_path)


def get_leg_planned_time_start(leg):
    short_path = ['NS1:Abfahrt',
                  'NS1:DatumZeit',
                  'NS1:Geplant']
    return get_time_from_short_path(leg, short_path)


def get_leg_planned_time_end(leg):
    short_path = ['NS1:Ankunft',
                  'NS1:DatumZeit',
                  'NS1:Geplant']
    return get_time_from_short_path(leg, short_path)


def get_leg_stop_id_start(leg):
    short_path = ['NS1:Abfahrt',
                  'NS1:Haltestelle',
                  'NS1:Standort',
                  'NS1:Id',
                  'NS1:ExterneStationId']
    return get_node_text_value(leg, short_path).lstrip('0')


def get_leg_station_name_start(leg):
    short_path = ['NS1:Abfahrt',
                  'NS1:Haltestelle',
                  'NS1:Standort',
                  'NS1:Name']
    # return remove_non_ascii(get_node_text_value(leg, short_path))
    return get_node_text_value(leg, short_path)


def get_leg_platform_start(leg):
    short_path = ['NS1:Abfahrt',
                  'NS1:Haltestelle',
                  'NS1:Gleis',
                  'NS1:Aktuell']
    return get_node_text_value(leg, short_path)


def get_leg_stop_id_end(leg):
    short_path = ['NS1:Ankunft',
                  'NS1:Haltestelle',
                  'NS1:Standort',
                  'NS1:Id',
                  'NS1:ExterneStationId']
    return get_node_text_value(leg, short_path).lstrip('0')


def get_leg_station_name_end(leg):
    short_path = ['NS1:Ankunft',
                  'NS1:Haltestelle',
                  'NS1:Standort',
                  'NS1:Name']
    # return remove_non_ascii(get_node_text_value(leg, short_path))
    return get_node_text_value(leg, short_path)


def get_leg_platform_end(leg):
    short_path = ['NS1:Ankunft',
                  'NS1:Haltestelle',
                  'NS1:Gleis',
                  'NS1:Aktuell']
    return get_node_text_value(leg, short_path)

# SEGMENT :: get_...(segment) methods


def get_seg_stop_id(segment):
    short_path = ['NS1:Haltestelle',
                  'NS1:Standort',
                  'NS1:Id',
                  'NS1:ExterneStationId']
    return get_node_text_value(segment, short_path).lstrip('0')


def get_seg_time_departure(segment):
    short_path = ['NS1:AbfahrtsZeitpunkt',
                  'NS1:Aktuell']
    return get_time_from_short_path(segment, short_path)


def get_seg_time_arrival(segment):
    short_path = ['NS1:AnkunftsZeitpunkt',
                  'NS1:Aktuell']
    return get_time_from_short_path(segment, short_path)


def get_seg_type(segment):
    short_path = ['NS1:Haltestelle',
                  'NS1:Standort',
                  'NS1:Typ']
    return get_node_text_value(segment, short_path)
