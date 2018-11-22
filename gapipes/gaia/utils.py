"""
Utilities for parsing Tap and Gaia TapPlus HTML and XML responses
"""

import re
from functools import partial
from collections import namedtuple
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from astropy.table import Table

__all__ = [
    'parse_html_response_error',
    'parse_tableset'
]

# NOTE: Unique name spaces in all xml files in tests/data
# xmlns="http://www.ivoa.net/xml/VOTable/v1.3">
# xmlns:uws="http://www.ivoa.net/xml/UWS/v1.0"
# xmlns:xlink="http://www.w3.org/1999/xlink"
# xmlns:xs="http://www.w3.org/2001/XMLSchema"
# xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.ivoa.net/xml/UWS/v1.0 http://www.ivoa.net/xml/UWS/v1.0"  total="1">
# xmlns:vod="http://www.ivoa.net/xml/VODataService/v1.1" xsi:type="vod:TableSet"
# xmlns:esatapplus="http://esa.int/xml/EsaTapPlus" xsi:schemaLocation="http://www.ivoa.net/xml/VODataService/v1.1 http://www.ivoa.net/xml/VODataService/v1.1 http://esa.int/xml/EsaTapPlus http://gea.esac.esa.int/tap-server/xml/esaTapPlusAttributes.xsd">


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

    def __repr__(self):
        s = 'Column(name="{s.name:s}", unit="{s.unit:s}", description='\
            .format(s=self)
        remaining_length = 80-len(s)
        s += '"{desc}"'.format(desc=self.description[:remaining_length])
        s += "..." if len(self.description) > remaining_length else "" + ")"
        return s
    
    def __str__(self):
        return "Column name: {s.name}\nunit: {s.unit}"\
               "\ndesription: {s.description}".format(s=self)


class TableMeta(
    namedtuple('TableMeta', ['name', 'schema', 'description', 'columns'])):

    @property
    def as_table(self):
        rows = list(map(
            lambda c: (c.name, c.datatype, c.unit, c.description[:60]),
            self.columns)) 
        return Table(rows=rows,
                     names=['name', 'datatype', 'unit', 'short_description'])
    
    def __repr__(self):
        return "{name:s}, {ncolumns:d} columns".format(
                   name=self.name, ncolumns=len(self.columns))


class TableSet(list):
    """
    a list of TableMeta that supports filtering
    """
    
    def filter(self, schema=None, table=None):
        """
        Filter tables by schema or table name
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
    
    Returns a list of TODO
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


ns = {'uws': "http://www.ivoa.net/xml/UWS/v1.0",
      'xlink': "http://www.w3.org/1999/xlink"}

items = [
    "uws:jobId",
    "uws:runId",
    "uws:ownerId",
    "uws:phase",
    "uws:quote",
    "uws:startTime",
    "uws:endTime",
    "uws:executionDuration",
    "uws:destruction",
    "uws:creationTime",
    "uws:locationId",
    "uws:name"]

lookup = {
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




class Job(object):
    def __init__(self):
        pass


def test_parse_job():
    tree = ET.parse('../../top5gaia_async.xml')
    root = tree.getroot()

    job = Job()
    for k, v in lookup.items():
        print(k)
        setattr(job, k, root.find(v, ns).text)

    result_urls = [r.attrib['{{{xlink}}}href'.format(**ns)]
            for r in root.findall('.//uws:results//uws:result', ns)]

