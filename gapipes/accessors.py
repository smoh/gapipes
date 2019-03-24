""" Custom accessors for pandas """

import pandas as pd
import numpy as np
import astropy.coordinates as coord
import astropy.units as u
from . import pipes as pp

# conversion factor from mas/yr * mas to km/s
_tokms = (u.kpc * (u.mas).to(u.rad)/u.yr).to(u.km/u.s).value

@pd.api.extensions.register_dataframe_accessor("g")
class GaiaAccessor(object):
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

        df = self._df
        necessary_columns = set([
            'parallax_error', 'pmra_error', 'pmdec_error',
            'parallax_pmra_corr', 'parallax_pmdec_corr',
            'pmra_pmdec_corr'])
        s = set(df.columns)
        assert s >= necessary_columns, \
            "Columns missing: {:}".format(necessary_columns-s)
        C = np.zeros([len(df), 3, 3])
        C[:, [0,1,2], [0,1,2]] = df[['parallax_error', 'pmra_error', 'pmdec_error']].values**2
        C[:, [0, 1], [1, 0]] = (df['parallax_error']*df['pmra_error']*df['parallax_pmra_corr']).values[:, None]
        C[:, [0, 2], [2, 0]] = (df['parallax_error']*df['pmdec_error']*df['parallax_pmdec_corr']).values[:, None]
        C[:, [1, 2], [2, 1]] = (df['pmra_error']*df['pmdec_error']*df['pmra_pmdec_corr']).values[:, None]
        return C

    @property
    def distmod(self):
        """Distance modulus M = m + DM
        """
        return 5*np.log10(self._df['parallax'])-10

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
    def __init__(self, s):
        self._d = s
        self._keys = list(s.keys())

    def open_simbad(self):
        """Open Simbad search for this source
        """
        # TODO: finish
        # url =
        # webbrowser.open_new_tab(url)

    @property
    def icrs(self):
        """ICRS coordinate of the source
        """
        return pp.make_icrs(self._d)
