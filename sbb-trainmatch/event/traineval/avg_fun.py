import numpy as np
from scipy import stats


def kde_wavg(group, wgt=1):
    """
    Weighted averaging function, including:
        w1: kernel density estimate (KDE) using on time-axis density
        w2: log-scale uncertainty of location points
        w3: over-weighting stationary points at stations
    :param group:
    :return:
    """

    group = group[outlier_flag(group['distance'])]

    if group['distance'].count()>1:
        d = group['distance']
        w1 = 1. / kde_weights(group['time'])
        w2 = 1. / np.log10(10. + group['horizontal_accuracy'].values.astype(float)) # stupid numpy log10
        w3 = 1. + (group['is_long_stop']).astype(int)*(wgt-1) #Bool to int conversion
        w = w1*w2*w3
        return (d * w).sum() / w.sum()
    else:
        return group['distance'].sum()


def define_outlier_threshold(x):
    return 3*np.percentile(x, 90)


def outlier_flag(x):
    """ A value of 4 is chosen as a first test. Check if it's right!!!!
    maybe add a hard min, e.g. 1 or 10km, so that weird distributions don't get artificially messed up
    """
    # Should we change this to only remove the points and not perform aggregation?
    if x.shape[0]>9:
        # V1 -- works best for many points in (factors in the actual spread around the median)
        # Replaced with 90th percentile, can't fix trips where >>10% of points are outliers...
        # Also ensures that never more than 15% of points are removed
        y = x < define_outlier_threshold(x)

    elif x.shape[0]>2:
        # V2 -- works best for few points
        med = x.median()
        y = x < 4*med
    else:
        y = np.ones(len(x), dtype=bool)
    return y


def kde_weights(times):
    """ Kernel Density Estimate of point density along an itinerary
    """

    # KDE makes no sense for 2 points in this context (it also crashed with singular matrix)
    if (times.nunique()>1) and (times.count()>2):
        # Reformat times in a kde-friendly notation (small number near zero)
        np64_times = times.apply(lambda x: np.datetime64(x)).values.astype(int)/10.**12
        np64_times = np64_times - np64_times[0]

        kernel = stats.gaussian_kde(np64_times)
        w = kernel(np64_times)
    else:
        w = np.ones(times.count())

    return w