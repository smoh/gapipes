""" Custom accessors for pandas """
import warnings
import webbrowser
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
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

    def correct_brightsource_pm(self, gmag_threshold=12.):
        """Correct bright source proper motions for rotation bias.

        This function modifies the original DataFrame!

        Parameters
        ----------
        gmag_threshold : float, optional
            Threshold G mag below which the correction is applied.

        Returns
        -------
        pandas.DataFrame
            original dataframe with pmra, pmdec modified for bright sources.
        """
        if gmag_threshold > 13.:
            warnings.warn("This correction should only be applied to bright (G <= 12) sources!")
        bright = np.array(self._df['phot_g_mean_mag']) < gmag_threshold
        pmra, pmdec = pp.correct_brightsource_pm(self._df.loc[bright])
        self._df.loc[bright, 'pmra'] = pmra
        self._df.loc[bright, 'pmdec'] = pmdec
        return self._df

    def plot_xyz_icrs(self, *args, **kwargs):
        """Plot xyz coordinates in ICRS
        """
        fig, ax = plt.subplots(1, 2, figsize=(8, 4))
        ax[0].scatter(self.icrs.cartesian.x, self.icrs.cartesian.y, s=2);
        ax[1].scatter(self.icrs.cartesian.x, self.icrs.cartesian.z, s=2);
        return fig



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

