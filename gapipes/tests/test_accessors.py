import os

import numpy as np
import pytest
import pandas as pd
from astropy.table import Table
import astropy.coordinates as coord

import gapipes as gp

fn = os.path.join(os.path.dirname(__file__), "top5.fits")
df = Table.read(fn).to_pandas()
# remove negative parallaxes for testing purposes
df = df.loc[df["parallax"] > 0]


def test_icrs():
    assert isinstance(df.g.icrs, coord.ICRS)

    assert isinstance(df.drop(columns=["radial_velocity"]).g.icrs, coord.ICRS)
    # some missing columns
    assert isinstance(
        df.drop(columns=["radial_velocity", "pmra", "pmdec"]).g.icrs, coord.ICRS
    )

    assert isinstance(df.g.galactic, coord.Galactic)
    assert isinstance(
        df.drop(columns=["radial_velocity", "pmra", "pmdec"]).g.galactic, coord.Galactic
    )

    # for series
    assert df.g.icrs[0].ra == df.iloc[0].g.icrs.ra


def test_make_cov():
    df = pd.DataFrame(
        dict(
            parallax_error=np.ones(3),
            pmra_error=np.ones(3),
            pmdec_error=np.ones(3),
            parallax_pmra_corr=np.zeros(3),
            parallax_pmdec_corr=np.zeros(3),
            pmra_pmdec_corr=np.zeros(3),
        )
    )
    cov = df.g.make_cov()
    assert cov.shape == (3, 3, 3)
    assert np.allclose(cov, np.repeat(np.eye(3)[None, :], 3, axis=0))

    s = df.iloc[0]
    assert np.allclose(s.g.make_cov(), np.eye(3))

    cov = df.g.make_cov(columns=["pmra", "pmdec"])
    assert np.allclose(cov, np.eye(2))

    # test random ordering of columns
    cov = df.g.make_cov(columns=["pmra", "parallax"])
    assert np.allclose(cov, np.eye(2))


def test_distmod():
    # At 100 pc, distmod is zero.
    df = pd.DataFrame(dict(parallax=[100]))
    assert df.g.distmod.values[0] == 0


def test_correct_brightsource_pm():
    df = pd.DataFrame(
        {
            "ra": [12.34, 56.78],
            "dec": [0.0, 12.32],
            "pmra": [-1.0, -1.2],
            "pmdec": [0.0, 0.0],
            "phot_g_mean_mag": [8.0, 15.2],
        }
    )
    df.g.correct_brightsource_pm()
    # only first source should be modified in pm
    assert not np.allclose(df.loc[0, ["pmra", "pmdec"]].values, [-1, 0.0])
    assert np.allclose(df.loc[1, ["pmra", "pmdec"]].values, [-1.2, 0.0])
