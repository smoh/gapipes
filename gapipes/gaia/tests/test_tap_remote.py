import pytest
import pandas as pd
from gapipes.gaia import Tap
from astropy.table import Table


@pytest.fixture
def gaiatap():
    return Tap.from_url("http://gea.esac.esa.int:80/tap-server/tap")


class TestTap(object):
    def test_simple_query(self, gaiatap):
        query = "select top 5 * from gaiadr1.tgas_source;"
        r = gaiatap.query(query)
        assert isinstance(r, pd.DataFrame)
        assert len(r) == 5


def test_query_with_upload(gaiatap):
    query = "select * from TAP_UPLOAD.mytable;"
    mytable = Table({"a": [1, 2, 3], "b": [4, 5, 6]})
    # r = gaiatap._post_query(
    #     query, upload_resource=mytable, upload_table_name='mytable')
    r = gaiatap.query(query, upload_resource=mytable, upload_table_name="mytable")
    assert isinstance(r, pd.DataFrame)
    assert len(r) == 3
    assert len(r.columns) == 2


def test_query_async(gaiatap):
    query = "select top 5 * from gaiadr1.tgas_source;"
    j = gaiatap.query(query, async_=True)
    assert j.query == query


def test_query_async_with_upload(gaiatap):
    query = "select top 5 * from TAP_UPLOAD.mytable;"
    mytable = Table({"a": [1, 2, 3], "b": [4, 5, 6]})
    j = gaiatap.query(
        query, upload_resource=mytable, upload_table_name="mytable", async_=True
    )
    assert j.query == query
