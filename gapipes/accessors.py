""" Custom accessors for pandas """

import pandas as pd
import numpy as np
import astropy.coordinates as coord
import astropy.units as u

@pd.api.extensions.register_dataframe_accessor("g")
class GaiaAccessor(object):
    def __init__(self, df):
        self._validate(df)
        self._df = df
        self._icrs = None

    @staticmethod
    def _validate(df):
        pass
        # if 'ra' not in obj.columns or 'dec' not in obj.columns:
        #     raise AttributeError("Must have 'ra' and 'dec'.")

    @property
    def icrs(self):
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