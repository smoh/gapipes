"""
Module containing frequent calculations on Gaia DataFrames
"""
import os
import numpy as np
import pandas as pd
import astropy.coordinates as coord
import astropy.units as u

__all__ = [
    "calculate_vtan_error",
    "add_vtan_errors",
    "add_vtan",
    "make_icrs",
    "add_x",
    "add_xv",
    "add_a_g_error",
    "get_distmod",
    "make_cov",
    "add_gMag",
    "flag_good_phot",
    "UWE0Calculator",
    "calculate_uwe0",
    "add_ruwe",
    "add_uwe",
    "correct_brightsource_pm",
]

# conversion factor from mas/yr * mas to km/s
_tokms = (u.kpc * (u.mas).to(u.rad) / u.yr).to(u.km / u.s).value


def calculate_vtan_error(df):
    """Calculate tangential velocity errors with small error propagation

    Returns (vra_error, vdec_error) in km/s
    """
    vra_error = (
        np.hypot(
            df["pmra_error"] / df["parallax"],
            df["parallax_error"] / df["parallax"] ** 2 * df["pmra"],
        )
        * _tokms
    )
    vdec_error = (
        np.hypot(
            df["pmdec_error"] / df["parallax"],
            df["parallax_error"] / df["parallax"] ** 2 * df["pmdec"],
        )
        * _tokms
    )
    return vra_error, vdec_error


def add_vtan_errors(df):
    """Add 'vra_error' and 'vdec_error' columns to Gaia DataFrame """
    df = df.copy()
    vra_error, vdec_error = calculate_vtan_error(df)
    df["vra_error"] = vra_error
    df["vdec_error"] = vdec_error
    return df


def add_vtan(df):
    """Add 'vra' and 'vdec' columns to Gaia DataFrame """
    df = df.copy()
    vra, vdec = df.pmra / df.parallax * _tokms, df.pmdec / df.parallax * _tokms
    df["vra"] = vra
    df["vdec"] = vdec
    return df


def make_icrs(df, include_pm_rv=True):
    """Make ICRS coordinates from Gaia data

    Parameters
    ----------
    df : pandas.DataFrame, dict-like
        Gaia DataFrame
    include_pm_rv : bool, optional
        Include proper motions and radial velocities

    Returns
    -------
    coordinates : astropy.coordinates.ICRS
        coordinates
    """
    columns = set(df.keys())
    if not set(["ra", "dec", "parallax"]) <= columns:
        raise AttributeError("Must have 'ra', 'dec', 'parallax'.")
    args = {}
    args["ra"] = np.array(df["ra"]) * u.deg
    args["dec"] = np.array(df["dec"]) * u.deg
    args["distance"] = 1e3 / np.array(df["parallax"]) * u.pc
    if "pmra" in columns:
        args["pm_ra_cosdec"] = np.array(df["pmra"]) * u.mas / u.year
    if "pmdec" in columns:
        args["pm_dec"] = np.array(df["pmdec"]) * u.mas / u.yr
    if "radial_velocity" in columns:
        args["radial_velocity"] = np.array(df["radial_velocity"]) * u.km / u.s
    c = coord.ICRS(**args)
    return c


def make_cov(df, columns=["parallax", "pmra", "pmdec"]):
    """Generate covariance matrix from Gaia data

    columns : list
        list of columns to calculate covariance.
        Must be a subset of 'ra', 'dec' 'parallax', 'pmra', 'pmdec'.

    Returns
    -------
    numpy.array
        (N, number of columns) array of covariance matrices
    """
    gaia_order = ["ra", "dec", "parallax", "pmra", "pmdec"]
    N = len(np.atleast_1d(df[columns[0] + "_error"]))  # N could be 1
    n = len(columns)
    C = np.zeros([N, n, n])

    for i, j in zip(*np.triu_indices(n)):
        if i == j:
            C[:, [i], [j]] = np.atleast_1d(
                df[f"{columns[i]}_error"] * df[f"{columns[j]}_error"]
            )[:, None]
        else:
            corr_name = (
                "_".join(
                    sorted([columns[i], columns[j]], key=lambda x: gaia_order.index(x))
                )
                + "_corr"
            )
            C[:, [i, j], [j, i]] = np.atleast_1d(
                df[f"{columns[i]}_error"] * df[f"{columns[j]}_error"] * df[corr_name]
            )[:, None]
    return C.squeeze()


def add_x(df, frame, unit=u.pc):
    """Add cartesian coordinates `x`, `y`, `z` of a given `frame`"""
    df = df.copy()
    c = make_icrs(df, include_pm_rv=False).transform_to(frame)
    df["x"], df["y"], df["z"] = c.cartesian.xyz.to(unit).value
    return df


def add_xv(df, frame, unit=u.pc):
    """Add cartesian coordinates x, y, z, vx, vy, vz for a given `frame`

    df : pd.DataFrame
        Gaia DR2 data
    frame : astropy coordinate frame
        Frame to calculate coordinates in

    Returns df with x, y, z, vx, vy, vz columns added.
    """
    if not isinstance(df, pd.DataFrame):
        raise ValueError("df should be a pandas.DataFrame")
    df = df.copy()
    c = make_icrs(df).transform_to(frame)
    df["x"], df["y"], df["z"] = c.cartesian.xyz.to(unit).value
    df["vx"], df["vy"], df["vz"] = c.velocity.d_xyz.value
    return df


def add_a_g_error(df):
    """Extract lerr and uerr of a_g from percentiles.

    Returns df with a_g_lerr, a_g_uerr columns added.
    """
    df = df.copy()
    lerr = df["a_g_val"] - df["a_g_percentile_lower"]
    uerr = df["a_g_percentile_upper"] - df["a_g_val"]
    df["a_g_lerr"], df["a_g_uerr"] = lerr, uerr
    return df


def get_distmod(df):
    """Calculate distance modulus from parallax
    """
    return 5 * np.log10(df["parallax"]) - 10


def add_distmod(df):
    """Add distance modulus `distmod`"""
    df = df.copy()
    df["distmod"] = get_distmod(df)
    return df


def add_gMag(df):
    """Add absolute G Mag `gMag`"""
    df = df.copy()
    df["gMag"] = df["phot_g_mean_mag"] + 5 * np.log10(df["parallax"]) - 10
    return df


def flag_good_phot(df):
    """Add 'good_phot' boolean column to the dataframe

    TODO: explain
    """
    df = df.copy()
    good_phot = (df["phot_bp_rp_excess_factor"] > 1 + 0.015 * df["bp_rp"] ** 2) & (
        df["phot_bp_rp_excess_factor"] < 1.3 + 0.06 * df["bp_rp"] ** 2
    )
    df["good_phot"] = good_phot
    return df


class UWE0Calculator(object):
    """Calculate unit weight error normalization for Gaia DR2 sources"""

    def __init__(self):
        from scipy.interpolate import NearestNDInterpolator

        fn = os.path.join(
            os.path.dirname(__file__), "data/DR2_RUWE_V1", "table_u0_g_col.txt"
        )
        self.data = pd.read_csv(fn, skipinitialspace=True)
        self.interp = NearestNDInterpolator(
            self.data[["bp_rp", "g_mag"]].values, self.data["u0"].values
        )

    def __call__(self, bp_rp, g_mag):
        """Calculate unit weight error normalization factor

        Parameters
        ----------
        bp_rp : array-like
            BP-RP colors of sources
        g_mag : array-like
            Gaia G magnitudes of sources

        Raises
        ------
        ValueError
            if g_mag contains any NaNs

        Returns
        -------
        array-like
            normalization factor
        """
        bp_rp, g_mag = np.atleast_1d(bp_rp), np.atleast_1d(g_mag)
        if np.isnan(g_mag).any():
            raise ValueError("g_mag should not contain NaNs")
        bp_rp_inan = np.isnan(bp_rp)
        bp_rp[bp_rp_inan] = 0.9
        return self.interp(np.vstack([bp_rp, g_mag]).T)


calculate_uwe0 = UWE0Calculator()


def add_ruwe(df):
    """Add renormalized unit weight error 'ruwe' column to df"""
    df = df.copy()
    uwe0 = calculate_uwe0(df["bp_rp"].values, df["phot_g_mean_mag"].values)
    df["ruwe"] = (
        np.sqrt(df["astrometric_chi2_al"] / (df["astrometric_n_good_obs_al"] - 5))
        / uwe0
    )
    return df


def add_uwe(df):
    """Add unit weight error 'uwe' column to df"""
    df = df.copy()
    df["uwe"] = np.sqrt(
        df["astrometric_chi2_al"] / (df["astrometric_n_good_obs_al"] - 5)
    )
    return df


def correct_brightsource_pm(df):
    """Get proper motions corrected for DR2 bright source rotational bias.

    For G <= 12, proper motions have a significant (0.15 mas/yr) rotation bias.
    See Lindegren et al. for details.
    """
    omegax, omegay, omegaz = -0.086, -0.114, -0.037  # mas/yr
    ra_rad, dec_rad = np.deg2rad(np.array(df["ra"])), np.deg2rad(np.array(df["dec"]))
    pmra, pmdec = np.array(df["pmra"]), np.array(df["pmdec"])
    pmra_new = (
        pmra
        + omegax * np.sin(dec_rad) * np.cos(ra_rad)
        + omegay * np.sin(dec_rad) * np.sin(ra_rad)
        - omegaz * np.cos(dec_rad)
    )
    pmdec_new = pmdec - omegax * np.sin(ra_rad) + omegay * np.cos(ra_rad)
    return pmra_new, pmdec_new
