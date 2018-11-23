import os
import pytest
from functools import partial

from gapipes.gaia import utils


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


class TestJob(object):
    def test_job(self):
        with open(data_path('top5gaia_async.xml'), 'r') as f:
            job = utils.Job.from_xml(f.read())
        assert job.jobid == '1542458555634O'
        assert job.phase == 'COMPLETED'
        assert job.ownerid == 'anonymous'
        assert job.result_url == "http://gea.esac.esa.int/tap-server/tap/async/1542458555634O/results/result"
        assert job.url is None
        assert job.session is None