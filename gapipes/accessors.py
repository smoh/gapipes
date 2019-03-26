""" Custom accessors for pandas """
import webbrowser
import pandas as pd
import numpy as np
import astropy.coordinates as coord
import astropy.units as u
from . import pipes as pp

__all__ = [
    'GaiaData',
    'GaiaSource'
]

# conversion factor from mas/yr * mas to km/s
_tokms = (u.kpc * (u.mas).to(u.rad)/u.yr).to(u.km/u.s).value

@pd.api.extensions.register_dataframe_accessor("g")
class GaiaData(object):
    """Gaia data table class/accessor

    This class is designed to be accessed through a custom accessor "g"
    to a pandas DataFrame containing a gaia_source table, e.g.,

    >>> df.g.icrs

    However, it should also work with any table class containing Gaia data
    that has dict-like access:

    >>> type(t)
    astropy.table.table.Table
    >>> GaiaData(t).icrs

    Parameters
    ----------
    df : pandas.DataFrame, dict-like
        Gaia data table
    """
    def __init__(self, df):
        self._validate(df)
        self._df = df

    @staticmethod
    def _validate(df):
        pass
        # if 'ra' not in obj.columns or 'dec' not in obj.columns:
        #     raise AttributeError("Must have 'ra' and 'dec'.")

    @property
    def icrs(self):
        """ICRS coordinates of sources using all available columns

        Raises
        ------
        AttributeError
            If the table does not have ra, dec and parallax columns

        Returns
        -------
        astropy.coordinates.ICRS
            coordinates
        """
        return pp.make_icrs(self._df)

    @property
    def galactic(self):
        return self.icrs.transform_to(coord.Galactic)

    def make_cov(self):
        """Generate covariance matrix from Gaia table columns

        Returns
        -------
        numpy.array
            (N, 3, 3) array of parallax, pmra, pmdec covariance matrices
        """
        return pp.make_cov(self._df)

    @property
    def distmod(self):
        """Distance modulus M = m + DM
        """
        return pp.get_distmod(self._df)

    @property
    def vra(self):
        """Velocity [km/s] in R.A. direction"""
        return self._df['pmra']/self._df['parallax']*_tokms

    @property
    def vdec(self):
        """Velocity [km/s] in Decl. direction"""
        return self._df['pmdec']/self._df['parallax']*_tokms


@pd.api.extensions.register_series_accessor("g")
class GaiaSource(object):
    """Gaia row class/accessor

    This class is designed to be accessed through a custom accessor "g"
    to a pandas Series containing a gaia_source row, e.g.,

    >>> df.iloc[0].g.icrs

    However, it should also work with any table class containing Gaia data
    that has dict-like access:

    Parameters
    ----------
    s : pandas.Series, dict-like
        Gaia data table row for one source
    """
    def __init__(self, s):
        self._d = s
        self._keys = list(s.keys())

    def open_simbad(self):
        """Open Simbad search for this source in default web browser.
        """
        url = 'http://simbad.u-strasbg.fr/simbad/sim-basic?Ident={radec}&submit=SIMBAD+search'\
            .format(radec='{}{:+}'.format(self._d['ra'], self._d['dec']))
        webbrowser.open_new_tab(url)

    @property
    def icrs(self):
        """ICRS coordinate of the source
        """
        return pp.make_icrs(self._d)

    @property
    def distmod(self):
        """Distance modulus M = m + DM
        """
        return pp.get_distmod(self._d)

    def make_cov(self):
        """Generate covariance matrix from Gaia table columns

        Returns
        -------
        numpy.array
            (3, 3) array of parallax, pmra, pmdec covariance matrices
        """
        return pp.make_cov(self._d)

