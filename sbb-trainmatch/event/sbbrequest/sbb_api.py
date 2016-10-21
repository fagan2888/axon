import logging
import os
import json

import requests

from vibepy.write_grafana import write_grafana


def query_sbb_api(params, CONFIG):
    """
    Calls the SBB API and returns the response as a 'request' object
    """

    KEY_DIR = CONFIG.get('sbb', 'SBB_API_KEY_DIR')
    SBB_API_URI = CONFIG.get('sbb', 'SBB_API_URI')

    headers = {
        'content-type': 'text/xml;charset=UTF-8',
        'SOAPAction': 'FindStandorte'
    }

    xml_str_fname = os.path.dirname(os.path.realpath(__file__)) + '/xml/sbb_api.xml'
    body = gen_query_xml_str(params, xml_str_fname)
    cert = (os.path.join(KEY_DIR, 'sbb.crt'), os.path.join(KEY_DIR, 'sbb.pem'))

    if cert[0] and cert[1]:
        response = requests.post(SBB_API_URI, headers=headers, data=body, cert=cert)
        increment_grafana_api_call_counter(CONFIG)
    else:
        logging.error('pem/crt files not found')
        raise IOError

    return response


def gen_query_xml_str(params, xml_str_fname):
    """
    XML query that calls the SBB API
    params are the lat/lon/timestamp/MaxResultNumber which specify the query
    """

    if os.path.isfile(xml_str_fname):
        with open(xml_str_fname, "r") as myfile:
            request_str = myfile.read()
        return request_str.format(**params)

    else:
        logging.error('XML template file not found at: {p}'.format(p=xml_str_fname))
        raise IOError


def fulltag(tag):
    """
    Handles the namespaces that etree can't seem to return properly. SBB-specific namespace values / shorthands
    Feed in a shorthand format tag, e.g. 'NS1:Verbindung',
    and returns full namespage in the etree node name format, e.g. '{http://schemas.xmlsoap.org/soap/encoding/}Body'

    :param tag:
    :return:
    """

    # Load the XML namespaces used by SBB
    xml_str_fname = os.path.dirname(os.path.realpath(__file__)) + '/xml/sbb_namespace.json'

    if os.path.isfile(xml_str_fname):
        with open(xml_str_fname, "r") as sbb_namespace:
            xmlns = json.load(sbb_namespace)
    else:
        logging.error('SBB XML namespace json -- file not found at: {p}'.format(p=xml_str_fname))
        raise IOError

    # Define XML namespaces used by SBB
    # xmlns = {'soapenc': 'http://schemas.xmlsoap.org/soap/encoding/',
    #          'soapenv': 'http://schemas.xmlsoap.org/soap/envelope/',
    #          'xsd': 'http://www.w3.org/2001/XMLSchema',
    #          'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
    #          'NS1': 'http://spf.sbb.ch/kundeninformation/fahrplan/v2/FahrplanService',
    #          'NS2': 'http://common.sbb.ch/types/CommonTypes/v1',
    #          'NS3': 'http://common.sbb.ch/types/CommonTypes/v1',
    #          'NS4': 'http://common.sbb.ch/types/CommonTypes/v1',
    #          'NS5': 'http://common.sbb.ch/types/CommonTypes/v1'
    #          }

    # current format in - xmlns:tag
    split_tag = tag.split(':')

    if len(split_tag) == 1:
        # no namespace
        ft = '{tag}'.format(tag=split_tag[0])

    elif len(split_tag) == 2:
        # namespace
        ns = xmlns.get(split_tag[0])
        if not ns:
            logging.error('XML namespace not found for tag - {t}'.format(t=tag))
        # full tag including namespace
        ft = '{{{ns}}}{tag}'.format(ns=ns, tag=split_tag[1])

    else:
        logging.error('tag error - {t}'.format(t=tag))
        ft = None

    return ft


def increment_grafana_api_call_counter(CONFIG):

    metrics = {'hit': 1}
    write_grafana(CONFIG, metrics, output_type='incr')
    return