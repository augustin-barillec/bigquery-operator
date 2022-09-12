from copy import deepcopy
from pandas.testing import assert_frame_equal


def sort(df):
    res = deepcopy(df)
    cols = list(res.columns)
    res = res.sort_values(cols)
    return res


def reset_index(df):
    res = deepcopy(df)
    return res.reset_index(drop=True)


def normalize(df):
    res = sort(df)
    res = reset_index(res)
    return res


def assert_equal(df1, df2):
    assert_frame_equal(normalize(df1), normalize(df2), check_dtype=False)
