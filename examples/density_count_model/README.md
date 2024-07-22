# Modelled density to lice count comparison

Requires SciPy to be installed.

Usage is,

```
python density_count_ode.py deployment_2011-10-26_2011-11-01_cage_10.csv TOTAL density.csv time density 
```

Returns a JSON-encoded string with the behaviour of the reference model
and the model forced by the density data including fitted values of the
rate constants, weighted distance (EMD) to the measured data, and
model-generated distribution.
