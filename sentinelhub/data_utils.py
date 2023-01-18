"""
Module with statistics to dataframe transformation.
"""
from typing import Any, Dict, List, Optional, Tuple, Union

from .time_utils import parse_time
from .types import JsonDict

_PANDAS_IMPORT_MESSAGE = (
    "To use this function you need to install the `pandas` library, which is not a dependency of sentinelhub-py."
)


def _extract_hist(hist_data: List[Dict[str, float]]) -> Tuple[List[float], List[float]]:
    """Transform Statistical API histogram into sequences of bins and counts

    :param hist_data: An input representation of Statistical API histogram data in a form of the low edges,
        the high edges, and the counts for each bin.
    :return: Statistical histogram bins and counts value as sequences.
    """

    nbins = len(hist_data)
    bins, counts = [], []
    for idx in range(nbins):
        bins.append(hist_data[idx]["lowEdge"])
        if idx == nbins - 1:
            bins.append(hist_data[idx]["highEdge"])
        counts.append(hist_data[idx]["count"])

    return bins, counts


def _extract_stats(interval_output: JsonDict, exclude_stats: List[str]) -> Dict[str, Union[List[float], float]]:
    """Transform statistics into pandas.DataFrame entry

    :param interval_output: An input representation of statistics of an aggregation interval.
    :param exclude_stats: Statistics that will be excluded from output.
    :return: Statistics as a pandas.DataFrame entry.
    """
    stat_entry: Dict[str, Union[List[float], float]] = {}
    for output_name, output_data in interval_output.items():  # pylint: disable=too-many-nested-blocks
        for band_name, band_values in output_data["bands"].items():
            band_stats = band_values["stats"]
            # statistics are not valid when sample count equals to no data count
            if band_stats["sampleCount"] == band_stats["noDataCount"]:
                break

            for stat_name, value in band_stats.items():
                if stat_name not in exclude_stats:
                    col_name = f"{output_name}_{band_name}_{stat_name}"
                    if stat_name == "percentiles":
                        for percentile_name, percentile_value in value.items():
                            stat_entry[f"{col_name}_{percentile_name}"] = percentile_value
                    else:
                        stat_entry[col_name] = value

            if "histogram" in band_values:
                band_bins = band_values["histogram"]["bins"]
                hist_bins, hist_counts = _extract_hist(band_bins)
                stat_entry[f"{output_name}_{band_name}_bins"] = hist_bins
                stat_entry[f"{output_name}_{band_name}_counts"] = hist_counts

    return stat_entry


def _extract_response_data(response_data: List[JsonDict], exclude_stats: List[str]) -> List[Dict[str, Any]]:
    """Transform Statistical API response into a pandas.DataFrame

    :param response_data: An input representation of Statistical API response. The response is a list of JsonDict and
        each contains the statistics of an aggregation interval.
    :param exclude_stats: Statistics that will be excluded from output.
    :return: DataFrame entries of all aggregation intervals of a single geometry.
    """
    df_entries = []
    for interval in response_data:
        if "outputs" in interval:
            df_entry: Dict[str, Any] = _extract_stats(interval["outputs"], exclude_stats)
            if df_entry:
                df_entry["interval_from"] = parse_time(interval["interval"]["from"])
                df_entry["interval_to"] = parse_time(interval["interval"]["to"])
                df_entries.append(df_entry)

    return df_entries


def statistical_to_dataframe(result_data: List[JsonDict], exclude_stats: Optional[List[str]] = None) -> Any:
    """Transform (Batch) Statistical API results into a pandas.DataFrame

    This function has a dependency of the `pandas` library, which is not a requirement of sentinelhub-py and needs to be
    installed before using the function.

    :param result_data: An input representation of (Batch) Statistical API result returned from
        `AwsBatchStatisticalResults.get_data()`. Each JsonDict in the list is a Statistical API response of an input
        geometry.
    :param exclude_stats: The statistic names defined in this parameter will be excluded from the output DataFrame.

    :return: Statistical dataframe.
    """
    try:
        import pandas  # pylint: disable=import-outside-toplevel
    except ImportError as exception:
        raise ImportError(_PANDAS_IMPORT_MESSAGE) from exception

    exclude_stats = exclude_stats or []

    nresults = len(result_data)
    dfs = [None] * nresults
    for idx in range(nresults):
        identifier, response = result_data[idx]["identifier"], result_data[idx]["response"]
        if response:
            result_entries = _extract_response_data(response["data"], exclude_stats)
            result_df = pandas.DataFrame(result_entries)
            result_df["identifier"] = identifier
            dfs[idx] = result_df

    return pandas.concat(dfs)


def _get_failed_intervals(
    identifier: str, response_data: List[JsonDict]
) -> Optional[Dict[str, Union[str, List[Tuple[str, str]]]]]:
    """Collect failed intervals of a partially failed request

    :param identifier: The identifier of the geometry.
    :param response_data: An input representation of Statistical API response.
    :return: The identifier of a geometry that has a response status of PARTIAL and the failed intervals.
    """
    failed_intervals = []
    for interval in response_data:
        if "error" in interval:
            failed_intervals.append((interval["interval"]["from"], interval["interval"]["to"]))

    return {"identifier": identifier, "failed_intervals": failed_intervals} if failed_intervals else None


def get_failed_statistical_requests(result_data: List[JsonDict]) -> List[Dict[str, Union[str, List[Tuple[str, str]]]]]:
    """Collect failed requests of (Batch) Statistical Results

    :param result_data: An input representation of (Batch) Statistical API result.
    :return: Failed requests of (Batch) Statistical Results.
    """
    failed_requests = []
    for result in result_data:
        identifier, response = result["identifier"], result["response"]
        if not response:
            failed_requests.append({"identifier": identifier})
        else:
            failed_intervals = _get_failed_intervals(identifier, response["data"])
            if failed_intervals:
                failed_requests.append(failed_intervals)

    return failed_requests
