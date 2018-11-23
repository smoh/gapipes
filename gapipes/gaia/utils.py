"""
Utilities for parsing Tap and Gaia TapPlus HTML and XML responses
"""
import logging
import re
from functools import partial
from collections import namedtuple
import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from astropy.table import Table

logger = logging.getLogger(__name__)

__all__ = [
    'parse_html_response_error',
    'parse_tableset',
    'ColumnMeta',
    'TableMeta',
    'TableSet'
    'Job'
]

# NOTE: Unique name spaces in all xml files in tests/data
# xmlns="http://www.ivoa.net/xml/VOTable/v1.3">
# xmlns:uws="http://www.ivoa.net/xml/UWS/v1.0"
# xmlns:xlink="http://www.w3.org/1999/xlink"
# xmlns:xs="http://www.w3.org/2001/XMLSchema"
# xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.ivoa.net/xml/UWS/v1.0 http://www.ivoa.net/xml/UWS/v1.0"  total="1">
# xmlns:vod="http://www.ivoa.net/xml/VODataService/v1.1" xsi:type="vod:TableSet"
# xmlns:esatapplus="http://esa.int/xml/EsaTapPlus" xsi:schemaLocation="http://www.ivoa.net/xml/VODataService/v1.1 http://www.ivoa.net/xml/VODataService/v1.1 http://esa.int/xml/EsaTapPlus http://gea.esac.esa.int/tap-server/xml/esaTapPlusAttributes.xsd">
ns = {
    'uws': "http://www.ivoa.net/xml/UWS/v1.0",
    'xlink': "http://www.w3.org/1999/xlink",
    'xs': "http://www.w3.org/2001/XMLSchema",
    'xsi': "http://www.w3.org/2001/XMLSchema-instance",
    'vod': "http://www.ivoa.net/xml/VODataService/v1.1", 
    'esatapplus': "http://esa.int/xml/EsaTapPlus"
}


def xstr(s):
    return '' if s is None else str(s)


def parse_html_response_error(html):
    """Return a useful message from failed TAP request"""
    soup = BeautifulSoup(html, 'html.parser')
    # this is not robust at all....
    message_li = soup.find(string=re.compile('Message')).parent.parent
    return message_li.text

    
class ColumnMeta(
    namedtuple('ColumnMeta', ['name', 'unit', 'datatype', 'description'],
               defaults=['', '', '', ''])):
    """
    Column meta data
    """

    def __repr__(self):
        # NOTE: Column description can be very long. In order to have
        # nice and useful info, truncate description.
        s = 'Column(name="{s.name:s}", unit="{s.unit:s}", description='\
            .format(s=self)
        remaining_length = 80-len(s)
        s += '"{desc}"'.format(desc=self.description[:remaining_length])
        s += "..." if len(self.description) > remaining_length else "" + ")"
        return s
    
    def __str__(self):
        # Descriptions are printed in full when this class is printed.
        return "Column name: {s.name}\nunit: {s.unit}"\
               "\ndesription: {s.description}".format(s=self)


class TableMeta(
    namedtuple('TableMeta', ['name', 'schema', 'description', 'columns'])):
    """
    Table meta data
    """

    @property
    def as_table(self):
        rows = list(map(
            lambda c: (c.name, c.datatype, c.unit, c.description[:60]),
            self.columns)) 
        return Table(rows=rows,
                     names=['name', 'datatype', 'unit', 'short_description'])
    
    def __repr__(self):
        return "{schema:s}.{name:s}, {ncolumns:d} columns".format(
            schema=self.schema, name=self.name, ncolumns=len(self.columns))


class TableSet(list):
    """
    A list of TableMeta that supports filtering
    """
    
    def filter(self, schema=None, table=None):
        """
        Filter tables by schema or table name

        Paramters
        ---------
        schema : str or list of str
            schemas to get
        table : str or list of str
            tables to get
        """
        if schema is None and table is None:
            raise ValueError('Specify at least one of `schema` or `table`.')
        def check_schema(schema, t):
            return t.schema in schema
        def check_table(table, t):
            return t.name in table
        filtered = self.copy()
        if schema is not None: 
            schema = set([schema]) if isinstance(schema, str) else set(schema)
            filtered = list(filter(partial(check_schema, schema), filtered))
        if table is not None:
            table = set([table]) if isinstance(table, str) else set(table)
            filtered = list(filter(partial(check_table, table), filtered))
        return TableSet(filtered)
        

def parse_tableset(xml):
    """
    Parse vod:tableset XML and return a list of tables

    Parameters
    ----------
    xml : str
        XML string to be parsed
    
    Returns
    -------
    tables : TableSet
        list of tables
    """
    root = ET.fromstring(xml)
    # save (schema, table name, table description) for each table
    tables = []
    for schema in root.findall('.//schema'):
        schema_name = schema.find('name').text
        if schema_name in ['tap_schema', 'external']:
            continue
        for table in schema.findall('.//table'):
            table_name = table.find('name').text
            # NOTE: The actual response from Gaia /tables table names are already
            # qualified names as in [schema].[table].
            if '.' in table_name:
                table_name = table_name.split('.')[-1]
            table_desc = table.find('description').text
            table_desc = xstr(table_desc).strip()
            columns = []
            for col in table.findall('.//column'):
                # columns has these tags
                # {('name', 'description', 'unit', 'dataType'),
                # ('name', 'description', 'unit', 'dataType', 'flag', 'flag'),
                # ('name', 'description', 'unit', 'ucd', 'utype', 'dataType'),
                # ('name', 'description', 'unit', 'ucd', 'utype', 'dataType', 'flag', 'flag')}
                col_name = xstr(col.find('name').text).strip()
                col_desc = xstr(col.find('description').text).strip()
                col_unit = xstr(col.find('unit').text).strip()
                col_dtype = xstr(col.find('dataType').text).strip()
                columns.append(ColumnMeta(col_name, col_unit, col_dtype, col_desc))
            tables.append(
                TableMeta(table_name, schema_name, table_desc, columns))
    return TableSet(tables)


class Job(object):
    """Job on TAP server

    """
    _lookup = {
        "jobid":            "uws:jobId",
        "runid":            "uws:runId",
        "ownerid":          "uws:ownerId",
        "phase":            "uws:phase",
        "quote":            "uws:quote",
        "starttime":        "uws:startTime",
        "endtime":          "uws:endTime",
        "executionduration":"uws:executionDuration",
        "destruction":      "uws:destruction",
        "creationtime":     "uws:creationTime",
        "locationid":       "uws:locationId",
        "name":             "uws:name",
    }

    def __init__(self):
        self.url = None
        self.result_url = None
    
    @classmethod
    def from_response(cls, response):
        """
        Create Job from response of TAP server

        Parameters
        ----------
        response : requests.Response
            response from POST to /async
        """
        job = cls()

        logger.debug('Try to get job location from redirect header')
        job.url = response.history[0].headers['Location']
        logger.debug(job.url)
        logger.debug(response.url)
        
        assert response.headers['Content-Type'] == 'text/xml;charset=UTF-8'

        parsed = Job.parse_xml(response.text)
        for k, v in parsed.items():
            setattr(job, k, v)
        return job
    
    @staticmethod
    def parse_xml(xml):
        out = {}
        root = ET.fromstring(xml)
        for k, v in Job._lookup.items():
            out[k] = root.find(v, ns).text
        
        out['query'] = root.find(".//uws:parameter[@id='query']", ns).text
        logger.debug('Current phase: {0}'.format(out['phase']))
    
        if out['phase'] == 'COMPLETED':
            result = root.find('.//uws:results//uws:result', ns)
            out['result_url'] = result.attrib['{{{xlink}}}href'.format(**ns)]
        return out
        
    @classmethod
    def from_xml(cls, xml):
        job = cls()
        parsed = Job.parse_xml(xml)
        for k, v in parsed.items():
            setattr(job, k, v)
        return job


class TapPlusJob(Job):

    def __init__(self):
        pass
