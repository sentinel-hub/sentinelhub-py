"""
Module with statistics to dataframe transformation.
"""
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd

from .time_utils import parse_time
from .type_utils import JsonDict


def _histogram_to_dataframe(
        hist_cols: Tuple[str, str], hist_data: List[JsonDict]
) -> Dict[str, List[Union[int, float]]]:
    """Transform Statistical API histogram into a pandas.DataFrame

    :param hist_cols: Column names to store histogram as a dataframe. Example: ("ndvi_B0_bins", "ndvi_B0_counts").
    :param hist_data: An input representation of Statistical API histogram.
    :return: Statistical histogram stored as bins and counts.
    """

    nbins = len(hist_data)
    low_edges, high_edges, counts = ([0] * nbins for _ in range(3))
    for idx in range(nbins):
        low_edges[idx] = hist_data[idx]["lowEdge"]
        high_edges[idx] = hist_data[idx]["highEdge"]
        counts[idx] = hist_data[idx]["count"]
    return {hist_cols[0]: sorted(set(low_edges + high_edges)), hist_cols[1]: counts}


def _statistical_to_dataframe(
    res_data: List[JsonDict], excl_stats: List[str] = None, incl_histogram: bool = False
) -> Optional[pd.DataFrame]:
    """Transform Statistical API response into a pandas.DataFrame

    :param res_data: An input representation of Statistical API response.
    :param excl_stats: Unwanted statistical name.
    :param incl_histogram: Flag to transform histogram.
    :return: Statistical dataframe
    """
    if not excl_stats:
        excl_stats = []

    nintervals = len(res_data)
    interval_dfs = [0] * nintervals
    for idx in range(nintervals):
        if "outputs" in res_data[idx]:
            df_entry = dict()
            df_entry["interval_from"] = parse_time(res_data[idx]["interval"]["from"]).date()
            df_entry["interval_to"] = parse_time(res_data[idx]["interval"]["to"]).date()

            is_valid_entry = False
            for output_name, output_data in res_data[idx]["outputs"].items():
                for band_name, band_values in output_data["bands"].items():
                    band_stats = band_values["stats"]
                    if band_stats["sampleCount"] == band_stats["noDataCount"]:
                        break

                    is_valid_entry = True
                    for stat_name, value in band_stats.items():
                        if stat_name not in excl_stats:
                            col_name = f"{output_name}_{band_name}_{stat_name}"
                            if stat_name == "percentiles":
                                for perc, perc_val in value.items():
                                    perc_col_name = f"{col_name}_{perc}"
                                    df_entry[perc_col_name] = perc_val
                            else:
                                df_entry[col_name] = value

                    if incl_histogram:
                        band_bins = band_values["histogram"]["bins"]
                        hist_col_names = (f"{output_name}_{band_name}_bins", f"{output_name}_{band_name}_counts")
                        df_entry.update(_histogram_to_dataframe(hist_col_names, band_bins))

            if is_valid_entry:
                interval_dfs[idx] = df_entry

    valid_dfs = [entry for entry in interval_dfs if entry != 0]

    return pd.DataFrame(valid_dfs)


def result_to_dataframe(
    rslt_data: List[JsonDict], excl_stats: List[str] = None, incl_histogram: bool = False
) -> Tuple[Optional[pd.DataFrame], List[Union[str, None]]]:
    """Transform Batch Statistical API get_data results into a pandas.DataFrame

    :param rslt_data: An input representation of Batch Statistical API result.
    :param excl_stats: Unwanted statistical name.
    :param incl_histogram: Flag to transform histogram.
    :return: Statistical dataframe and identifiers that failed on request.
    """
    if not excl_stats:
        excl_stats = []

    nrslt = len(rslt_data)
    dfs = [0] * nrslt
    nulls = []
    for idx in range(nrslt):
        identifier, response = rslt_data[idx]["identifier"], rslt_data[idx]["response"]
        if response:
            rslt_df = _statistical_to_dataframe(response["data"], excl_stats, incl_histogram)
            rslt_df["identifier"] = identifier
            dfs[idx] = rslt_df
        else:
            nulls.append(identifier)
    return pd.concat(dfs), nulls
