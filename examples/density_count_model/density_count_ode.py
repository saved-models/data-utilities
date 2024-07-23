import argparse
import os
import csv
import sys
import json
import numpy as np
import datetime
from scipy.integrate import ode as solve
from scipy.optimize import minimize, Bounds
from scipy.stats import wasserstein_distance


'''
See https://github.com/linkml/linkml/issues/1994
'''
NA_NONE = [" ", "", "#N/A", "#N/A N/A", "#NA", "-1.#IND", "-1.#QNAN"
         , "-NaN", "-nan", "1.#IND", "1.#QNAN", "<NA>", "N/A", "NA"
         , "NULL", "NaN", "None", "n/a", "nan", "null"]

def derivative(k_inf, acc, density, volume, most_observed):
    """
    Return a function implementing the ODE version of the sea lice accumulation
    model. This ODE system generates the histogram as a function of time.

    It requires the rate constant k_inf and the accumulation factor, as well as
    a `density`, a function of time, and the volume of the cage. Number of lice is
    computed as `density(t)*volume`.
    """
    kn = np.array([k_inf] + [n*k_inf*acc for n in range(1,most_observed+1)])
    def ode(t, F):
        C = density(t) * volume
        dF0dt = -kn[0]*F[0]*C
        dFndt = [kn[n-1]*F[n-1]*C - kn[n]*F[n]*C for n in range(1,most_observed)]
        df50dt = kn[most_observed-1]*F[most_observed-1]*C
        return np.array([dF0dt] + dFndt + [df50dt])
    return ode

def downsample(hist, n):
    """
    Downsample the histogram to make it more coarse, with n buckets. Clearly, n
    should be less than the size of the histogram.
    """
    assert n <= len(hist)
    size = int(len(hist)/n)
    new  = [hist[0]] + [hist[1+size*i:1+size*(i+1)].sum() for i in range(n)]
    return np.array(new)

def cagedist(filename, column, max_count):
    """
    Read the named column from the file and return a probability distribution of
    the sampled lice counts.
    """
    raw_counts = {}
    with open(filename) as fp:
        colno = None
        for row in csv.reader(fp):
            if colno is None:
                colno = row.index(column)
                continue
            if row[colno] in NA_NONE:
                continue
            total = int(row[colno])
            raw_counts[total] = raw_counts.get(total, 0) + 1

    count_items   = raw_counts.items()
    total_lice    = sum([k*v for (k,v) in count_items])
    most_observed = max([k for (k,v) in count_items])
    if (max_count < 10 and most_observed > 10): ## otherwise hist() below fails
        nominal_max = most_observed +1
    elif (max_count < 10):
        nominal_max = 11
    else:
        nominal_max = max_count +1
    probabilities = {k: v for (k, v) in zip(range(0, nominal_max), [0]*nominal_max)}
    all_counts    = {k: v for (k, v) in zip(range(0, nominal_max), [0]*nominal_max)}
    for (freq, count) in count_items:
        if(freq < nominal_max):
            prob_count = count / total_lice
            probabilities[freq] = prob_count
            all_counts[freq]    = count
    hist = np.array([raw_counts.get(i, 0) for i in range(nominal_max)])
    hist = downsample(hist, 10)
    res = (nominal_max-1, all_counts, probabilities, hist/hist.sum())
    return res

def objective(y0, t0, t1, density, volume, weight, p_measured, most_observed):
    """
    Return an objective function to be minimised. Requires the initial
    conditions, `y0`, `t0`, and stopping time, `t1`. Also requires parameters, a
    density function, volume, relative weight of zero values compared to
    non-zero values, and measured probability distribution.
    """
    p_measured_zero   = p_measured[0]
    p_measured_renorm = p_measured[1:]/p_measured[1:].sum()
    def f(x):
        if any(c < 0 for c in x):
            return np.inf
        params = { "k_inf": x[0], "acc": x[1] }

        ode = derivative(params["k_inf"], params["acc"], density, volume, most_observed)
        solver = solve(ode)
        solver.set_initial_value(y0, t0)
        y = solver.integrate(t1)

        hist = downsample(y, len(p_measured) - 1)

        p_simulated = hist/hist.sum()
        p_simulated_zero = p_simulated[0]
        p_simulated_renorm = p_simulated[1:]/p_simulated[1:].sum()

        dz = weight*wasserstein_distance([p_measured_zero , 1.0 - p_measured_zero],
                                         [p_simulated_zero, 1.0 - p_simulated_zero])
        dc = (1-weight)*wasserstein_distance(p_measured_renorm, p_simulated_renorm)

        return dz + dc
    return f

def density(filename, timecol, denscol, fmt=None):
    """
    Return a function giving the density as a function of time from the starting
    value of the file. The `timecol` should name the column containing time, and
    `denscol` should name the column containing density. `timecol` can contain a
    numeric value or a date and time. In the latter case, a format `fmt`
    suitable for `strptime` should be passed.
    """
    times = []
    densities = []
    with open(filename) as fp:
        timeno = None
        densno = None
        start  = None
        for row in csv.reader(fp):
            if timeno is None:
                timeno = row.index(timecol)
                densno = row.index(denscol)
                continue
            if row[timeno] in NA_NONE or row[densno] in NA_NONE:
                continue
            if fmt is not None:
                time = datetime.strptime(row[timeno], fmt)
                if start is None:
                    start = time
                delta = time - start
                times.append(delta.seconds / 3600)
            else:
                times.append(float(row[timeno]))
            densities.append(float(row[densno]))
    times = np.array(times)
    densities = np.array(densities)
    def f(t):
        filtered = densities[times >= t]
        return filtered[0] if filtered.size > 0 else densities[-1]
    return f

def cli():
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", default=False, action="store_true", help="Verbose output")
    parser.add_argument("-l", "--limit", type=int, default=168, help="Simulation time (hours)")
    parser.add_argument("-f", "--format", default=None, help="Time format for density data (see strptime)")
    parser.add_argument("--k_inf", type=float, default=0.002, help="Infection rate")
    parser.add_argument("--acc", type=float, default=1, help="Increase in infection rate per louse")
    parser.add_argument("--weight", type=float, default=0.5, help="Relative weight of zeros")
    parser.add_argument("--max-count", type=int, default=-1, help="Maximum count")
    parser.add_argument("cagedata", help="Cage data file")
    parser.add_argument("cagecolumn", help="Column in cage data file to use for counts")
    parser.add_argument("densitydata", help="Density data file")
    parser.add_argument("densitytime", help="Column in density data to use for time")
    parser.add_argument("densitycolumn", help="Column in density data to use for density")
    args = parser.parse_args()

    result = { "obs": {}, "ref": {}, "test": {} }
    most_observed, observed_counts, observed_probabilities, p_measured = cagedist(args.cagedata, args.cagecolumn, args.max_count)
    
    result["obs"]["counts"] = observed_counts
    result["obs"]["probs"]  = observed_probabilities
    
    y0 = np.array([100] + [0 for _ in range(1,most_observed+1)])
    t0 = 0
    t1 = args.limit

    if args.weight < 0 or args.weight > 1:
        raise ValueError(f"bad value for weight ({args.weight}), should be between [0,1]")
    
    density_const = lambda t: 0.1
    volume = 10

    obj = objective(y0, t0, t1, density_const, volume, args.weight, p_measured, most_observed)
    x0 = [args.k_inf, args.acc]
    bounds = Bounds([1e-4,0.25], [1e-4,0.25])
    res = minimize(obj, x0, method='nelder-mead',
                   options={'disp': args.verbose})

    result["ref"]["k_inf"] = res.x[0]
    result["ref"]["acc"]   = res.x[1]
    result["ref"]["dist"]  = res.fun

    ode = derivative(res.x[0], res.x[1], density_const, volume, most_observed)
    solver = solve(ode)
    solver.set_initial_value(y0, t0)
    y = solver.integrate(t1)
    result["ref"]["totals"] = { n: c/100 for n, c in enumerate(y) if c > 0 }
    
    density_csv = density(args.densitydata, args.densitytime, args.densitycolumn, args.format)
    obj = objective(y0, t0, t1, density_csv, volume, args.weight, p_measured, most_observed)
    x0 = [args.k_inf, args.acc]
    bounds = Bounds([1e-4,0.25], [1e-4,0.25])
    res = minimize(obj, x0, method='nelder-mead',
                   options={'disp': args.verbose})

    result["test"]["k_inf"] = res.x[0]
    result["test"]["acc"]   = res.x[1]
    result["test"]["dist"]  = res.fun

    ode = derivative(res.x[0], res.x[1], density_csv, volume, most_observed)
    solver = solve(ode)
    solver.set_initial_value(y0, t0)
    y = solver.integrate(t1)
    result["test"]["totals"] = { n: c/100 for n, c in enumerate(y) if c > 0 }

    print(json.dumps(result, indent=4))

if __name__ == '__main__':
    cli()
