# Core python
from datetime import datetime
import xml.etree.cElementTree as ET

class SBBResponse(object):

    namespaces = {
        "soapenc": "http://schemas.xmlsoap.org/soap/encoding/",
        "soapenv": "http://schemas.xmlsoap.org/soap/envelope/",
        "xsd": "http://www.w3.org/2001/XMLSchema",
        "xsi": "http://www.w3.org/2001/XMLSchema-instance",
        "NS1": "http://spf.sbb.ch/kundeninformation/fahrplan/v2/FahrplanService",
        "NS2": "http://common.sbb.ch/types/CommonTypes/v1",
        "NS3": "http://common.sbb.ch/types/CommonTypes/v1",
        "NS4": "http://common.sbb.ch/types/CommonTypes/v1",
        "NS5": "http://common.sbb.ch/types/CommonTypes/v1"
    }

    def __init__(self, response):
        self.response = response


    # helper methods
    def __parse_datetime__(self, elem, path):
        date_elem = elem.find(path + '/NS1:Datum', self.namespaces)
        time_elem = elem.find(path + '/NS1:Zeit', self.namespaces)

        # normal checking of 'None' does not appear to work here or in __parse_text__
        if date_elem is not None and time_elem is not None:
            datetime_str = '{d} {t}'.format(d=date_elem.text, t=time_elem.text)
            return datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
        else:
            return None

    def __parse_text__(self, elem, path):
        child_elem = elem.find(path, self.namespaces)

        if child_elem is not None:
            return child_elem.text
        else:
            return ''


    def get_itinerary_nodes(self, root):
        # return root.findall('.//soapenv:Body/NS1:FindVerbindungenResponse/NS1:Verbindungen/NS1:Verbindung', self.namespaces)
        return root.findall('.//NS1:Verbindungen/NS1:Verbindung', self.namespaces)

    # Itinerary methods
    def get_leg_nodes(self, itinerary):
        return itinerary.findall('.//NS1:Verbindungsabschnitte/NS1:Verbindungsabschnitt', self.namespaces)

    def get_itin_start_datetime(self, itinerary):
        path = './/NS1:Zusammenfassung/NS1:Abfahrt/NS1:DatumZeit/NS1:Aktuell'
        return self.__parse_datetime__(itinerary, path)

    def get_itin_end_datetime(self, itinerary):
        path = './/NS1:Zusammenfassung/NS1:Ankunft/NS1:DatumZeit/NS1:Aktuell'
        return self.__parse_datetime__(itinerary, path)

    def get_itin_context_reconstruction(self, itinerary):
        path = './/NS1:ContextReconstruction'

        return self.__parse_text__(itinerary, path)


    # Leg methods
    def get_segment_nodes(self, leg):
        return leg.findall('.//NS1:Haltepunkte/NS1:Haltepunkt', self.namespaces)

    def get_leg_type(self, leg):
        path = './/NS1:Verkehrsmittel/NS1:Typ'

        return self.__parse_text__(leg, path)

    def get_leg_route_full_name(self, leg):
        path = './/NS1:Verkehrsmittel/NS1:Informationen/NS1:Name'
        return self.__parse_text__(leg, path)

    def get_leg_route_category(self, leg):
        path = './/NS1:Verkehrsmittel/NS1:Informationen/NS1:Kategorie/NS1:Abkuerzung'
        return self.__parse_text__(leg, path)

    def get_leg_route_line(self, leg):
        path = './/NS1:Verkehrsmittel/NS1:Informationen/NS1:Linie'
        return self.__parse_text__(leg, path)

    def get_leg_route_number(self, leg):
        path = './/NS1:Verkehrsmittel/NS1:Informationen/NS1:Nummer'
        return self.__parse_text__(leg, path)

    def get_leg_agency_id(self, leg):
        path = './/NS1:Verkehrsmittel/NS1:Informationen/NS1:TransportUnternehmungCode'
        return self.__parse_text__(leg, path)

    def get_leg_time_start(self, leg):
        path = './/NS1:Abfahrt/NS1:DatumZeit/NS1:Aktuell'
        return self.__parse_datetime__(leg, path)

    def get_leg_time_end(self, leg):
        path = './/NS1:Ankunft/NS1:DatumZeit/NS1:Aktuell'
        return self.__parse_datetime__(leg, path)

    def get_leg_planned_time_start(self, leg):
        path = './/NS1:Abfahrt/NS1:DatumZeit/NS1:Geplant'
        return self.__parse_datetime__(leg, path)

    def get_leg_planned_time_end(self, leg):
        path = './/NS1:Ankunft/NS1:DatumZeit/NS1:Geplant'
        return self.__parse_datetime__(leg, path)

    def get_leg_stop_id_start(self, leg):
        path = './/NS1:Abfahrt/NS1:Haltestelle/NS1:Standort/NS1:Id/NS1:ExterneStationId'
        return self.__parse_text__(leg, path)

    def get_leg_station_name_start(self, leg):
        path = './/NS1:Abfahrt/NS1:Haltestelle/NS1:Standort/NS1:Name'
        return self.__parse_text__(leg, path)

    def get_leg_platform_start(self, leg):
        path = './/NS1:Abfahrt/NS1:Haltestelle/NS1:Gleis/NS1:Aktuell'
        return self.__parse_text__(leg, path)

    def get_leg_stop_id_end(self, leg):
        path = './/NS1:Ankunft/NS1:Haltestelle/NS1:Standort/NS1:Id/NS1:ExterneStationId'
        return self.__parse_text__(leg, path)

    def get_leg_station_name_end(self, leg):
        path = './/NS1:Ankunft/NS1:Haltestelle/NS1:Standort/NS1:Name'
        return self.__parse_text__(leg, path)

    def get_leg_platform_end(self, leg):
        path = './/NS1:Ankunft/NS1:Haltestelle/NS1:Gleis/NS1:Aktuell'
        return self.__parse_text__(leg, path)

    # Segment methods
    def get_seg_stop_id(self, seg):
        path = './/NS1:Haltestelle/NS1:Standort/NS1:Id/NS1:ExterneStationId'
        txt = self.__parse_text__(seg, path)
        txt.lstrip('0')

        return txt

    def get_seg_time_departure(self, seg):
        path = './/NS1:AbfahrtsZeitpunkt/NS1:Aktuell'
        return self.__parse_datetime__(seg, path)

    def get_seg_time_arrival(self, seg):
        path = './/NS1:AnkunftsZeitpunkt/NS1:Aktuell'
        return self.__parse_datetime__(seg, path)

    def get_seg_type(self, seg):
        path = './/NS1:Haltestelle/NS1:Standort/NS1:Typ'
        return self.__parse_text__(seg, path)

    ## ERRORS RETURNED ##
    def check_if_error(self):
        rt = ET.fromstring(self.response)
        err_code = self.get_error_code(rt)
        if not err_code:
            return 0  # we are good
        err_str = self.get_error_string(rt)
        err_msg = self.get_error_msg(rt)

        # decide behavior
        if err_code == 'SPF-1000_C1':
            return 1  # retry
        elif err_code == 'SPF-1000_F2':
            return 1
        elif err_code == 'SPF-3000':
            return 1
        elif err_code == 'SPF-1000_W-K890':
            return 2  # skip
        else:
            return 2

    def get_error_code(self, err):
        path = './/faultcode'
        return self.__parse_text__(err, path)

    def get_error_string(self, err):
        path = './/faultstring'
        return self.__parse_text__(err, path)

    def get_error_msg(self, err):
        path = './/detail/NS5:technicalDetails/text'
        return self.__parse_text__(err, path)

