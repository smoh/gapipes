# gapipes

<a href="https://smoh.space/gapipes"><img src="https://github.com/smoh/gapipes/workflows/docs/badge.svg"></a>


**gapipes** is a [pandas](http://pandas.pydata.org)-centric lightweight collection of routines
to fetch and work with the [Gaia](https://www.cosmos.esa.int/web/gaia/data) data.

It provides

- sensible and tested [Gaia TAP+](http://gea.esac.esa.int/archive/) client to fetch the data
- [custom accessors](https://pandas.pydata.org/pandas-docs/stable/development/extending.html) to pandas DataFrame and Series to do common operations on the Gaia data such as

  - making astropy [coordinates](http://docs.astropy.org/en/stable/coordinates/index.html) objects
  - calculating [renormalized unit weight error](https://www.cosmos.esa.int/web/gaia/dr2-known-issues)
  - making covariance matrix from errors and correlation coefficients.

## A quick tour

Say you have some Gaia data. Let's quickly fetch some from the Gaia archive.


```python
import gapipes as gp

df = gp.gaia.query("""
select top 50 *
from gaiadr2.gaia_source
where
1=contains(point('', ra, dec),
           circle('', 130.226, 19.665, 1))
and parallax between 4.613 and 7.312
""")
```

`df` is a pandas Dataframe containing gaia sources.


```python
print(type(df), len(df), 'rows')
```

    <class 'pandas.core.frame.DataFrame'> 50 rows


When you import gapipes, it registers a custom accessor `g`, under which there is a collection of common operations.
Let's just work with first 5 rows for display purpose.


```python
d = df.head()
```

- Make astropy ICRS coordinate instance with proper motion and radial velocity:


```python
d.g.icrs
```




    <ICRS Coordinate: (ra, dec, distance) in (deg, deg, pc)
        [(130.12157181, 18.79627847, 181.87376145),
         (130.05526312, 18.72393993, 190.20501526),
         (130.13982678, 18.67443893, 188.26622276),
         (130.24423701, 18.67500043, 152.59152456),
         (130.06723764, 18.79161546, 209.60959948)]
     (pm_ra_cosdec, pm_dec, radial_velocity) in (mas / yr, mas / yr, km / s)
        [(-60.7075949 , -52.76753264,         nan),
         (-35.80150307, -12.40964744,         nan),
         (-37.06980874, -11.88689953, 29.54114051),
         (-33.0232434 ,  -5.58953023, 36.23858577),
         (-22.05408719, -19.82573781,  9.80905085)]>



- Make astropy Galactic coordinate instance:


```python
d.g.galactic
```




    <Galactic Coordinate: (l, b, distance) in (deg, deg, pc)
        [(206.89277308, 32.19810551, 181.87376145),
         (206.94629392, 32.1137144 , 190.20501526),
         (207.03459301, 32.17095501, 188.26622276),
         (207.07558656, 32.26359033, 152.59152456),
         (206.87635259, 32.14836265, 209.60959948)]
     (pm_l_cosb, pm_b, radial_velocity) in (mas / yr, mas / yr, km / s)
        [(        nan,          nan,         nan),
         (        nan,          nan,         nan),
         (-2.08990742, -38.87288757, 29.54114051),
         (-6.52731056, -32.85074839, 36.23858577),
         (10.70417661, -27.65616106,  9.80905085)]>



- Make `[parallax, pmra, pmdec]` 3x3 covariance matrix (using `"*_error"` and `"parallax_pmra_corr"`, ... columns)


```python
d.g.make_cov()
```




    array([[[ 0.03407061,  0.03978523, -0.01119716],
            [ 0.03978523,  0.0693536 , -0.01736009],
            [-0.01119716, -0.01736009,  0.01528202]],
    
           [[ 0.05322305,  0.04000269, -0.01155556],
            [ 0.04000269,  0.1277317 , -0.03569367],
            [-0.01155556, -0.03569367,  0.04880749]],
    
           [[ 0.00363713,  0.00340469, -0.00060153],
            [ 0.00340469,  0.00880243, -0.00189437],
            [-0.00060153, -0.00189437,  0.0036257 ]],
    
           [[ 0.06099286,  0.06141351, -0.01816473],
            [ 0.06141351,  0.13105763, -0.03839579],
            [-0.01816473, -0.03839579,  0.04169299]],
    
           [[ 0.00185897,  0.00182671, -0.00045218],
            [ 0.00182671,  0.00437075, -0.00137452],
            [-0.00045218, -0.00137452,  0.00190053]]])



The same accessor is available for pandas.Series that contains one row of gaia_source table, i.e., data for a single source.


```python
s = d.iloc[0]
s
```




    solution_id                                           1635721458409799680
    designation                                   Gaia DR2 659494740858299264
    source_id                                              659494740858299264
    random_index                                                    642953719
    ref_epoch                                                          2015.5
                                                  ...                        
    lum_val                                                               NaN
    lum_percentile_lower                                                  NaN
    lum_percentile_upper                                                  NaN
    datalink_url            https://gea.esac.esa.int/data-server/datalink/...
    epoch_photometry_url                                                  NaN
    Name: 0, Length: 96, dtype: object



- Make astropy ICRS coordinate object


```python
s.g.icrs
```




    <ICRS Coordinate: (ra, dec, distance) in (deg, deg, pc)
        (130.12157181, 18.79627847, 181.87376145)
     (pm_ra_cosdec, pm_dec, radial_velocity) in (mas / yr, mas / yr, km / s)
        (-60.7075949, -52.76753264, nan)>



- Open Simbad coordinate search for this source in default web browser.


```python
s.g.open_simbad() # will open the page in your browser
```

Installation
------------

```sh
pip install git+https://github.com/smoh/gapipes.git@master
```