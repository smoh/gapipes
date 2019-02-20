import os
import pickle
import pytest

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
        tables, columns = utils.parse_tableset(f.read())
    assert len(tables) == 2, \
        "Number of tables expected: %d, found: %d" % (2, len(tables))

    with open(data_path('gaia_tables.xml'), 'r') as f:
        tables, columns = utils.parse_tableset(f.read())

    public_tables = tables.query('schema == "public"')
    assert len(public_tables) == 7

    gaia_source_tables = tables.query('table_name == "gaia_source"')
    assert len(gaia_source_tables) == 2


def test_parse_votable_error_response(stored_responses):

    message = utils.parse_votable_error_response(stored_responses['sync_wrong_query'])
    assert message == \
        "Cannot parse query 'select top 5 * from gg.gaia_source' for"\
        " job '1550663796751O': 1 unresolved identifiers: gaia_source"\
        " [l.1 c.21 - l.1 c.35] !"


def test_job_error_message(stored_responses):

    message = utils.Job.parse_xml(
        stored_responses['async_query'].text)['message']
    assert message is None

    message = utils.Job.parse_xml(
        stored_responses['async_wrong_query'].text)['message']
    assert message == \
        "Cannot parse query 'select top 5 * from gg.gaia_source' for job"\
        " '1550663798739O': 1 unresolved identifiers: gaia_source "\
        "[l.1 c.21 - l.1 c.35] !"
