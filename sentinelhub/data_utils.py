"""
Module with statistics to dataframe transformation.
"""
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd

from .time_utils import parse_time
from .type_utils import JsonDict


def _extract_hist(hist_cols: Tuple[str, str], hist_data: List[JsonDict]) -> Dict[str, List[float]]:
    """Transform Statistical API histogram into pandas.DataFrame entry

    :param hist_cols: Column names to store histogram as a dataframe. Example: ("ndvi_B0_bins", "ndvi_B0_counts").
    :param hist_data: An input representation of Statistical API histogram.
    :return: Statistical histogram stored as bins and counts.
    """

    nbins = len(hist_data)
    bins, counts = [], []
    for idx in range(nbins):
        bins.append(hist_data[idx]["lowEdge"])
        if idx == nbins - 1:
            bins.append(hist_data[idx]["highEdge"])
        counts.append(hist_data[idx]["count"])

    return {hist_cols[0]: bins, hist_cols[1]: counts}


def _extract_percentiles(col_name: str, perc_data: Dict[str, float]) -> Dict[str, float]:
    """Transform statistical percentiles into pandas.DataFrame entry

    :param col_name: Output name and band name. Example: ndvi_B0.
    :param perc_data: An input representation of percentile data of <col_name>.
    :return: Percentile data as a pandas.DataFrame entry. Examples: {"ndvi_B0_<perc>": perc_val}.
    """
    perc_entry = {}
    for perc, perc_val in perc_data.items():
        perc_col_name = f"{col_name}_{perc}"
        perc_entry[perc_col_name] = perc_val
    return perc_entry


def _extract_stats(
    interval_output: JsonDict, excl_stats: List[str], incl_histogram: bool
) -> Dict[str, Union[List[float], float]]:
    """Transform statistics into pandas.DataFrame entry

    :param interval_output: An input representation of statistics of an aggregation interval.
    :param excl_stats: Unwanted statistical name.
    :param incl_histogram: Flag to transform histogram.
    :return: Statistics as a pandas.DataFrame entry.
    """
    stat_entry: Dict[str, Union[List[float], float]] = {}
    for output_name, output_data in interval_output.items():
        for band_name, band_values in output_data["bands"].items():
            band_stats = band_values["stats"]
            if band_stats["sampleCount"] == band_stats["noDataCount"]:
                break

            for stat_name, value in band_stats.items():
                if stat_name not in excl_stats:
                    col_name = f"{output_name}_{band_name}_{stat_name}"
                    if stat_name == "percentiles":
                        stat_entry.update(_extract_percentiles(col_name, value))
                    else:
                        stat_entry[col_name] = value

                if incl_histogram:
                    band_bins = band_values["histogram"]["bins"]
                    hist_col_names = (f"{output_name}_{band_name}_bins", f"{output_name}_{band_name}_counts")
                    stat_entry.update(_extract_hist(hist_col_names, band_bins))

    return stat_entry


def _extract_response_data(
    response_data: List[JsonDict], excl_stats: Optional[List[str]], incl_histogram: bool
) -> pd.DataFrame:
    """Transform Statistical API response into a pandas.DataFrame

    :param response_data: An input representation of Statistical API response.
    :param excl_stats: Unwanted statistical name.
    :param incl_histogram: Flag to transform histogram.
    :return: Statistical dataframe
    """
    if not excl_stats:
        excl_stats = []

    dfs = []
    for interval in response_data:
        if "outputs" in interval:
            df_entry: Dict[str, Any] = _extract_stats(interval["outputs"], excl_stats, incl_histogram)
            if df_entry:
                df_entry["interval_from"] = parse_time(interval["interval"]["from"])
                df_entry["interval_to"] = parse_time(interval["interval"]["to"])
                dfs.append(df_entry)

    return pd.DataFrame(dfs)


def statistical_to_dataframe(
    result_data: List[JsonDict], excl_stats: Optional[List[str]] = None, incl_hist: bool = False
) -> Tuple[pd.DataFrame, List[str]]:
    """Transform (Batch) Statistical API get_data results into a pandas.DataFrame

    :param result_data: An input representation of (Batch) Statistical API result.
    :param excl_stats: Unwanted statistical name.
    :param incl_hist: Flag to transform histogram.
    :return: Statistical dataframe and identifiers that failed on request.
    """
    if not excl_stats:
        excl_stats = []

    nresults = len(result_data)
    dfs = [None] * nresults
    nulls = []
    for idx in range(nresults):
        identifier, response = result_data[idx]["identifier"], result_data[idx]["response"]
        if response:
            result_df = _extract_response_data(response["data"], excl_stats, incl_hist)
            result_df["identifier"] = identifier
            dfs[idx] = result_df
        else:
            nulls.append(identifier)

    if len(nulls) == nresults:
        raise RuntimeError("All statistical responses are empty.")

    return pd.concat(dfs), nulls
