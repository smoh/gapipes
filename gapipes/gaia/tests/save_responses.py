#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Save responses for predefined queries to Gaia Archive
"""

import pickle
from gapipes import Tap
from astropy.table import Table

tap = Tap.from_url("http://gea.esac.esa.int:80/tap-server/tap")
d = {}
mytable = Table({'col1': [1,2,3], 'col2': [4,5,6]})

d['tables'] = tap.session.get("{s.tap_endpoint}/tables".format(s=tap))

d['sync_query'] = tap._post_query(
    "select top 5 * from gaiadr1.gaia_source")
d['sync_query_votable'] = tap._post_query(
    "select top 5 * from gaiadr1.gaia_source", output_format='votable')
d['sync_query_fits'] = tap._post_query(
    "select top 5 * from gaiadr1.gaia_source", output_format='fits')
d['sync_wrong_query'] = tap._post_query(
    "select top 5 * from gg.gaia_source")
d['sync_query_with_upload'] = tap._post_query(
    "select top 5 * from tap_upload.foobar",
    upload_resource=mytable, upload_table_name='foobar')

d['async_query'] = tap._post_query(
    "select top 5 * from gaiadr1.gaia_source", async_=True)
d['async_query_votable'] = tap._post_query(
    "select top 5 * from gaiadr1.gaia_source", output_format='votable', async_=True)
d['async_query_fits'] = tap._post_query(
    "select top 5 * from gaiadr1.gaia_source", output_format='fits', async_=True)
d['async_wrong_query'] = tap._post_query(
    "select top 5 * from gg.gaia_source", async_=True)
d['async_query_with_upload'] = tap._post_query(
    "select top 5 * from tap_upload.foobar",
    upload_resource=mytable, upload_table_name='foobar', async_=True)

with open('data/responses.pkl', 'wb') as f:
    pickle.dump(d, f)