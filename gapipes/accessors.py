""" Custom accessors for pandas """

import pandas as pd
import numpy as np
import astropy.coordinates as coord
import astropy.units as u

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

        columns = set(self._df.columns.values)
        df = self._df
        if not set(['ra', 'dec', 'parallax']) < columns:
            raise AttributeError("Must have 'ra', 'dec', 'parallax'.")
        args = {}
        args['ra'] = df['ra'].values*u.deg
        args['dec'] = df['dec'].values*u.deg
        args['distance']=1e3/df['parallax'].values*u.pc
        if 'pmra' in columns:
            args['pm_ra_cosdec'] = df['pmra'].values*u.mas/u.year
        if 'pmdec' in columns:
            args['pm_dec'] = df['pmdec'].values*u.mas/u.yr
        if 'radial_velocity' in columns:
            args['radial_velocity'] = df['radial_velocity'].values*u.km/u.s
        c = coord.ICRS(**args)
        return c

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


