import requests
import pytest
from unittest.mock import patch, MagicMock, create_autospec
import pickle
import os
import pandas as pd
from astropy.table import Table
from gapipes.gaia.core import Tap
from gapipes.gaia.utils import Job

@pytest.fixture
def mock_post_query():
    fn = os.path.join(os.path.dirname(__file__), 'data', 'responses.pkl')
    with open(fn, 'rb') as f:
        d = pickle.load(f)
    # mock = create_autospec(Tap._post_query)
    mock = MagicMock()
    def ff(query, **kwargs):
        if query not in d.keys():
            raise ValueError("No stored response for the query")
        return d[query]
    mock.side_effect = ff
    return mock


def test_from_url():
    tap = Tap.from_url("http://gea.esac.esa.int:80/tap-server/tap")
    assert tap.protocol == 'http', "Tap has a wrong protocol"
    assert tap.host == 'gea.esac.esa.int', "Tap has a wrong host"
    assert tap.port == 80, "Tap has a wrong port"
    assert tap.path == '/tap-server/tap', "Tap has a wrong path"
    assert tap.tap_endpoint == "http://gea.esac.esa.int/tap-server/tap"

    tap = Tap.from_url("https://gea.esac.esa.int/tap-server/tap")
    assert tap.protocol == 'https', "Tap has a wrong protocol"
    assert tap.port == 443, "Tap has a wrong port"


@pytest.fixture
def tap():
    return Tap('foo.bar', 'foo')


def test_query(tap, mock_post_query):
    with patch('gapipes.Tap._post_query', mock_post_query):
        r = tap.query("sync_query")
        assert isinstance(r, pd.DataFrame), "Failed to parse csv result table"

        r = tap.query("sync_query_votable", output_format='votable')
        args, kwargs = mock_post_query.call_args
        assert isinstance(r, Table), "Failed to parse votable result table"
        assert kwargs['output_format'] == 'votable'

        # NOTE: Server-side bug that the response is empty for sync + fits query
        # r = tap.query("sync_query_fits", output_format='fits')
        # args, kwargs = mock_post_query.call_args
        # assert isinstance(r, Table), "Failed to parse fits result table"
        # assert kwargs['output_format'] == 'fits'

        with pytest.raises(requests.exceptions.HTTPError):
            r = tap.query("sync_wrong_query")

        mytable = Table({'col1': [1,2,3], 'col2': [4,5,6]})
        r = tap.query("sync_query_with_upload",
                      upload_resource=mytable, upload_table_name='foobar')
        args, kwargs = mock_post_query.call_args
        assert set(r.columns) == set(['col1', 'col2'])

        # queries that time out?


def test_query_async(tap):

    with patch('gapipes.Tap._post_query', mock_post_query):

        job = tap.query_async("async_query")
        assert isinstance(job, Job), "Async query did not return a Job"
        assert job.jobid is not None
        assert job.url is not None
        assert job.phase == 'COMPLETED'
        assert job.result_url is not None



        r = tap.query("sync_query_votable", output_format='votable')
        args, kwargs = mock_post_query.call_args
        assert isinstance(r, Table), "Failed to parse votable result table"
        assert kwargs['output_format'] == 'votable'


        # queries that time out?
    # simple query, job parsing
    # simple query, output format

    # long query, waiting jobs

    # test job attributes
    pass
