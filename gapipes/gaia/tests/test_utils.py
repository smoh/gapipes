import os
import pickle
import pytest
from functools import partial

from gapipes.gaia import utils


@pytest.fixture
def stored_responses():
    fn = os.path.join(os.path.dirname(__file__), 'data', 'responses.pkl')
    with open(fn, 'rb') as f:
        d = pickle.load(f)
    return d


def data_path(filename):
    data_dir = os.path.join(os.path.dirname(__file__), 'data')
    return os.path.join(data_dir, filename)


def test_parse_tableset():
    with open(data_path('test_tables.xml'), 'r') as f:
        tables = utils.parse_tableset(f.read())
    assert len(tables) == 2, \
        "Number of tables expected: %d, found: %d" % (2, len(tables))
    
    t1 = tables.filter('public', 'table1')[0]
    assert t1.description == 'Table1 desc', \
        "Wrong description for table1. Expected: %s, found %s" % \
        ('Table1 desc', t1.description)
    assert len(t1.columns) == 2, \
        "Number of columns for table1. Expected: %d, found: %d" % \
        (2, len(t1.columns))
    
    with open(data_path('gaia_tables.xml'), 'r') as f:
        tables = utils.parse_tableset(f.read())
    
    public_tables = tables.filter('public')
    assert all(list(map(lambda x: x.schema, tables)))
    assert len(public_tables) == 7

    gaia_source_tables = tables.filter(table='gaia_source')
    assert all(list(map(lambda x: x.name, gaia_source_tables)))
    assert len(gaia_source_tables) == 2


def test_parse_votable_error_response(stored_responses):

    message = utils.parse_votable_error_response(stored_responses['sync_wrong_query'])
    assert message == \
        "Cannot parse query 'select top 5 * from gg.gaia_source' for"\
        " job '1543935706506O': 1 unresolved identifiers: gaia_source"\
        " [l.1 c.21 - l.1 c.35] !"


def test_job_error_message(stored_responses):

    message = utils.Job.parse_xml(
        stored_responses['async_query'].text)['message']
    assert message is None

    message = utils.Job.parse_xml(
        stored_responses['async_wrong_query'].text)['message']
    assert message == \
        "Cannot parse query 'select top 5 * from gg.gaia_source' for job"\
        " '1543935708674O': 1 unresolved identifiers: gaia_source "\
        "[l.1 c.21 - l.1 c.35] !"
