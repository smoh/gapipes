""" Custom accessors for pandas """

import pandas as pd
import numpy as np

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
        # return the geographic center point of this DataFrame
        ra, dec, parallax = self._df[['ra', 'dec', 'parallax']].values.T
        if self._icrs is None:
            self._icrs = coord.ICRS(ra*u.deg, dec*u.deg, 1e3/parallax*u.pc)
        return self._icrs

    def plot(self):
        # plot this array's data on a map, e.g., using Cartopy
        pass
