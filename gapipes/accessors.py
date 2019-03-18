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
        if self._icrs is None:
            req = set(['ra', 'dec', 'parallax', 'pmra', 'pmdec', 'radial_velocity'])
            columns = set(self._df.columns.values)
            if req < columns:
                df = self._df
                c = coord.ICRS(
                    ra=df['ra'].values*u.deg,
                    dec=df['dec'].values*u.deg,
                    distance=1000./df['parallax'].values*u.pc,
                    pm_ra_cosdec=df['pmra'].values*u.mas/u.year,
                    pm_dec=df['pmdec'].values*u.mas/u.year,
                    radial_velocity=df['radial_velocity'].values*u.km/u.s)
                self._icrs = c
            else:
                raise AttributeError("columns missing")
        return self._icrs