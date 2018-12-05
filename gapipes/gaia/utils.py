"""
Utilities for parsing Tap and Gaia TapPlus HTML and XML responses
"""
import logging
import re
import time
from functools import partial
from collections import namedtuple
import requests
from requests.exceptions import HTTPError
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from astropy.table import Table

logger = logging.getLogger(__name__)

__all__ = [
    'parse_html_error_response',
    'parse_votable_error_response',
    'parse_tableset',
    'ColumnMeta',
    'TableMeta',
    'TableSet',
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
    'esatapplus': "http://esa.int/xml/EsaTapPlus",
    'votable': 'http://www.ivoa.net/xml/VOTable/v1.2'
}


def xstr(s):
    return '' if s is None else str(s)


def parse_html_error_response(html):
    """Return a useful message from failed TAP request"""
    soup = BeautifulSoup(html, 'html.parser')
    # this is not robust at all....
    message_li = soup.find(string=re.compile('Message')).parent.parent
    return message_li.text


def parse_votable_error_response(response):
    """Return a useful message from server when response is not OK"""
    # elif 'votable' in response.headers['Content-Type'].lower():
    # synchronous wrong query
    # NOTE: although the response is VOTABLE, there is not table
    # and it is not parsed with astropy votable
    root = ET.fromstring(response.text)
    message = root.find('.//votable:INFO[@name="QUERY_STATUS"]', ns).text
    return message.strip()


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

        Parameters
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
    """Job on a TAP server

    Attributes
    ----------
    # TODO
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

    def __init__(self, *args, **kwargs):

        self.jobid = kwargs.pop('jobid', None)
        self.runid = kwargs.pop('runid', None)
        self.ownerid = kwargs.pop('ownerid', None)
        self.url = kwargs.pop('url', None)
        self.result_url = kwargs.pop('result_url', None)

        session = kwargs.pop('session', None)
        if session is None:
            self.session = requests.session()
        else:
            self.session = session
        
        #NOTE: possible phases are EXECUTING, PENDING, COMPLETED, ABORTED, ERROR
        self._phase = kwargs.pop('phase', None)

        self.query = kwargs.pop('query', None)
        self.output_format = kwargs.pop('format', None)
        self.message = kwargs.pop('message', None)
    
    def __repr__(self):
        #TODO: change when errored
        s = "Job(jobid='{s.jobid}', phase='{s.phase}')".format(s=self)
        return s
    
    @classmethod
    def from_response(cls, response, session=None):
        """
        Create Job from response from a TAP server

        Parameters
        ----------
        response : requests.Response
            response from POST to /async
        session : requests.Session
            session object
        
        Returns Job instance
        """
        logger.debug('Try to get job location from redirect header')
        url = response.history[0].headers['Location']
        logger.debug(url)
        logger.debug(response.url)
        
        assert response.headers['Content-Type'] == 'text/xml;charset=UTF-8'

        parsed = Job.parse_xml(response.text)
        return cls(url=url, session=session, **parsed)
    
    @staticmethod
    def parse_xml(xml):
        """Parse XML response from an async TAP job
        """
        out = {}
        root = ET.fromstring(xml)
        for k, v in Job._lookup.items():
            out[k] = root.find(v, ns).text
        
        out['query'] = root.find(".//uws:parameter[@id='query']", ns).text
        out['format'] = root.find(".//uws:parameter[@id='format']", ns).text
        logger.debug('Current phase: {0}'.format(out['phase']))
    
        if out['phase'] == 'COMPLETED':
            result = root.find('.//uws:results//uws:result', ns)
            out['result_url'] = result.attrib['{{{xlink}}}href'.format(**ns)]
        
        has_error_message = root.find(".//uws:errorSummary/uws:message", ns)
        if has_error_message is not None:
            out['message'] = has_error_message.text
        else:
            out['message'] = None
        return out
    
    @property
    def phase(self):
        """Current status of the job"""
        if self.url is None:
            raise TypeError("Job url is not found")
        else:
            if self._phase is None or self._phase == 'EXECUTING':
                r = self.session.get(self.url)
                try:
                    r.raise_for_status()
                    parsed = Job.parse_xml(r.text)
                    self._phase = parsed['phase']
                    if parsed['phase'] == 'COMPLETED':
                        self.result_url = parsed['result_url']
                    return self._phase
                except requests.exceptions.HTTPError as e:
                    # TODO: some useful message
                    raise e
            else:
                return self._phase
    
    @property
    def finished(self):
        return self.phase == 'COMPLETED'
    
    #TODO: this should be cached
    def get_result(self, sleep=0.5, wait=True):
        """
        Get the result or wait until ready

        Parameters
        ----------
        sleep: float
            Delay between status update for a given number of seconds
        wait: bool
            set to wait until result is ready

        Returns
        -------
        table: Astropy.Table
            votable result
        """
        while (not self.finished) & wait:
            time.sleep(sleep)
        if not self.finished:
            return
        # Get results
        try:
            r = self.session.get(self.result_url)
            r.raise_for_status()
            return Tap.parse_result_table(r, self.output_format)
        except HTTPError as e:
            raise e
    

class TapPlusJob(Job):
    # TODO: add session to Job for authenticated access: at Job class?

    def __init__(self):
        pass
