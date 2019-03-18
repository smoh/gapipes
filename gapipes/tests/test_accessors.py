import os

import pytest
import pandas as pd
from astropy.table import Table
import astropy.coordinates as coord

import gapipes as gp

fn = os.path.join(os.path.dirname(__file__), 'top5.fits')
df = Table.read(fn).to_pandas()


def test_icrs():
    assert isinstance(df.g.icrs, coord.ICRS)

    assert isinstance(df.drop(columns=['radial_velocity']).g.icrs,
                      coord.ICRS)
    # some missing columns
    assert isinstance(
        df.drop(columns=[
            'radial_velocity', 'pmra', 'pmdec'
            ]).g.icrs,
        coord.ICRS)

    assert isinstance(df.g.galactic, coord.Galactic)
    assert isinstance(
        df.drop(columns=[
            'radial_velocity', 'pmra', 'pmdec'
            ]).g.galactic,
        coord.Galactic)
