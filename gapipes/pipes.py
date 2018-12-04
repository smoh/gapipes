"""
Module containing frequent calculations on Gaia DataFrame
"""

import numpy as np
import pandas as pd
import astropy.coordinates as coord
import astropy.units as u

__all__ = [
    'pipe', 'calculate_vtan_error', 'add_vtan_errors', 'add_vtan',
    'make_icrs', 'add_x', 'add_xv', 'add_a_g_error', 'add_gMag', 'flag_good_phot']


def pipe(func):
    """Decorator to validate the first argument is pandas.DataFrame"""
    def newfunc(*args, **kwargs):
        if not isinstance(args[0], pd.DataFrame):
            raise ValueError("df should be a pandas.DataFrame")
        return func(*args, **kwargs)
    return newfunc


@pipe
def calculate_vtan_error(df):
    """
    Calculate tangential velocity errors with small error propagation

    Returns (vra_error, vdec_error) in km/s
    """
    vra_error = np.hypot(df.pmra_error/df.parallax,
                         df.parallax_error/df.parallax**2*df.pmra)*4.74
    vdec_error = np.hypot(df.pmdec_error/df.parallax,
                          df.parallax_error/df.parallax**2*df.pmdec)*4.74
    return vra_error, vdec_error


@pipe
def add_vtan_errors(df):
    """ Add 'vra_error' and 'vdec_error' columns to Gaia DataFrame """
    df = df.copy()
    vra_error, vdec_error = calculate_vtan_error(df)
    df['vra_error'] = vra_error
    df['vdec_error'] = vdec_error
    return df


@pipe
def add_vtan(df):
    """ Add 'vra' and 'vdec' columns to Gaia DataFrame """
    df = df.copy()
    vra, vdec = df.pmra/df.parallax*4.74, df.pmdec/df.parallax*4.74
    df['vra'] = vra
    df['vdec'] = vdec
    return df


@pipe
def make_icrs(df, include_pm_rv=True):
    """Returns ICRS instance from Gaia DataFrame"""
    if not include_pm_rv:
        return coord.ICRS(
            ra=df.ra.values*u.deg,        
            dec=df.dec.values*u.deg,
            distance=1000./df.parallax.values*u.pc)
    return coord.ICRS(
            ra=df.ra.values*u.deg,        
            dec=df.dec.values*u.deg,
            distance=1000./df.parallax.values*u.pc,
            pm_ra_cosdec=df.pmra.values*u.mas/u.year,
            pm_dec=df.pmdec.values*u.mas/u.year,
            radial_velocity=df.radial_velocity.values*u.km/u.s)


@pipe
def add_x(df, frame, unit=u.pc):
    """Add cartesian coordinates `x`, `y`, `z` of a given `frame`"""
    df = df.copy()
    c = make_icrs(df, include_pm_rv=False).transform_to(frame)
    df['x'], df['y'], df['z'] = c.cartesian.xyz.to(unit).value
    return df


@pipe
def add_xv(df, frame, unit=u.pc):
    """
    Add cartesian coordinates x, y, z, vx, vy, vz for a given `frame`
    
    df : pd.DataFrame
        Gaia DR2 data
    frame : astropy coordinate frame
        Frame to calculate coordinates in
    
    Returns df with x, y, z, vx, vy, vz columns added.
    """
    df = df.copy()
    c = make_icrs(df).transform_to(frame)
    df['x'], df['y'], df['z'] = c.cartesian.xyz.to(unit).value
    df['vx'], df['vy'], df['vz'] = c.velocity.d_xyz.value
    return df


@pipe
def add_a_g_error(df):
    """Extract lerr and uerr of a_g from percentiles.

    Returns df with a_g_lerr, a_g_uerr columns added.
    """
    df = df.copy()
    lerr = df['a_g_val'] - df['a_g_percentile_lower']
    uerr = df['a_g_percentile_upper'] - df['a_g_val']
    df['a_g_lerr'], df['a_g_uerr'] = lerr, uerr
    return df


@pipe
def add_distmod(df):
    """Add distance modulus `distmod`"""
    df = df.copy()
    df['distmod'] = 5*np.log10(df['parallax']) - 10
    return df


@pipe
def add_gMag(df):
    """Add absolute G Mag `gMag`"""
    df = df.copy()
    df['gMag'] = df['phot_g_mean_mag'] + 5*np.log10(df['parallax']) - 10
    return df


@pipe
def flag_good_phot(df):
    """Add """
    df = df.copy()
    good_phot = ((df.phot_bp_rp_excess_factor > 1+0.015*df.bp_rp**2)
                 & (df.phot_bp_rp_excess_factor < 1.3+0.06*df.bp_rp**2))
    df['good_phot'] = good_phot
    return df
