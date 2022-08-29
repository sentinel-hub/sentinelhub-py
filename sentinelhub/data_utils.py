"""
Module with statistics to dataframe transformation.
"""
from typing import Dict, List, Optional, Tuple

import pandas as pd

from .time_utils import parse_time
from .type_utils import JsonDict


def _histogram_to_dataframe(hist_cols: Tuple[str, str], hist_data: List[JsonDict]) -> Dict[str, List[float]]:
    """Transform Statistical API histogram into a pandas.DataFrame

    :param hist_cols: Column names to store histogram as a dataframe. Example: ("ndvi_B0_bins", "ndvi_B0_counts").
    :param hist_data: An input representation of Statistical API histogram.
    :return: Statistical histogram stored as bins and counts.
    """

    low_edges, high_edges, counts = ([] for _ in range(3))
    for hist_bin in hist_data:
        low_edges.append(hist_bin["lowEdge"])
        high_edges.append(hist_bin["highEdge"])
        counts.append(hist_bin["count"])
    return {hist_cols[0]: sorted(set(low_edges + high_edges)), hist_cols[1]: counts}


def _statistical_to_dataframe(
    res_data: List[JsonDict], excl_stats: Optional[List[str]] = None, incl_histogram: bool = False
) -> pd.DataFrame:
    """Transform Statistical API response into a pandas.DataFrame

    :param res_data: An input representation of Statistical API response.
    :param excl_stats: Unwanted statistical name.
    :param incl_histogram: Flag to transform histogram.
    :return: Statistical dataframe
    """
    if not excl_stats:
        excl_stats = []

    dfs = []
    for interval in res_data:
        if "outputs" in interval:
            df_entry = {
                "interval_from": parse_time(interval["interval"]["from"]),
                "interval_to": parse_time(interval["interval"]["to"])
            }

            is_valid_entry = False
            for output_name, output_data in interval["outputs"].items():
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
                dfs.append(df_entry)

    return pd.DataFrame(dfs)


def result_to_dataframe(
    result_data: List[JsonDict], excl_stats: Optional[List[str]] = None, incl_hist: bool = False
) -> Tuple[pd.DataFrame, List[str]]:
    """Transform Batch Statistical API get_data results into a pandas.DataFrame

    :param result_data: An input representation of Batch Statistical API result.
    :param excl_stats: Unwanted statistical name.
    :param incl_hist: Flag to transform histogram.
    :return: Statistical dataframe and identifiers that failed on request.
    """
    if not excl_stats:
        excl_stats = []

    nresults = len(result_data)
    dfs = [0] * nresults
    nulls = []
    for idx in range(nresults):
        identifier, response = result_data[idx]["identifier"], result_data[idx]["response"]
        if response:
            result_df = _statistical_to_dataframe(response["data"], excl_stats, incl_hist)
            result_df["identifier"] = identifier
            dfs[idx] = result_df
        else:
            nulls.append(identifier)

    if len(nulls) == nresults:
        raise RuntimeError("Batch Statistical API response for all geometries is empty.")

    return pd.concat(dfs), nulls
