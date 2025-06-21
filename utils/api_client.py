import requests
import json
import logging
import pandas as pd


class APIDataParser():
    def __init__(self, logger=None):
        """
        Initializes the APIData client with optional custom logger.

        Parameters
        ----------
        logger : logging.Logger, optional
            Custom logger instance. If None, a default logger is created.
        """
        self.logger = logger or logging.getLogger(__name__)

    def clean_metadata(self, json_data):
        """
        Removes uncommon or inconsistent keys from a list of dictionaries,
        retaining only the keys common to all items. Useful for filtering metadata
        and preserving only the data structure suitable for tabular representation.

        Parameters
        ----------
        json_data : list of dict

        Returns
        -------
        list of dict or None
        """

        try:
            if not json_data:
                return None

            common_keys = set(json_data[0].keys())
            for item in json_data[1:]:
                common_keys &= set(item.keys())

            filtered_data = [{k: d[k]
                              for k in common_keys} for d in json_data]

            return filtered_data if filtered_data else None

        except (KeyError, TypeError, ValueError) as e:
            self.logger.error(
                f"Failed to extract common keys from JSON: {e}")
            return None

    def _validate_json(self, response, is_list, cols=None):
        """
        Validates the JSON structure of an HTTP response.

        Parameters
        ----------
        response : requests.Response
            The HTTP response object returned from a `requests.get()` or similar call.

        is_list : bool
            If True, expects the JSON response to be a list of dictionaries.
            If False, expects a nested object from which data will be extracted.

        Returns
        -------
        list of dict or None
            A list containing the parsed JSON content. If `is_list` is False,
            returns a single dictionary inside a list. Returns None if the structure
            is invalid or parsing fails.

        Raises
        ------
        Logs
            Errors and warnings are logged via self.logger; does not raise exceptions directly.
        """

        try:
            response_json = response.json()

            if is_list:
                if not isinstance(response_json, list):
                    self.logger.error(
                        "Expected a list, but JSON response is not a list.")
                    return None
                elif not all(isinstance(item, dict) for item in response_json):
                    self.logger.error(
                        "Not all response items are dictionaries.")
                    return None
                return response_json
            else:
                if isinstance(response_json, dict) and not cols is None:
                    first_value = list(response_json.values())[0]
                    if isinstance(first_value, dict):
                        if cols:
                            filtered = {k: first_value.get(k) for k in cols}
                            return [filtered]
                        else:
                            return [first_value]
                    self.logger.error(
                        "Unexpected JSON format for object case.")
                    return None

            if len(response_json) == 0:
                self.logger.warning("JSON returned an empty list")
                return None

            return response_json

        except json.JSONDecodeError as e:
            self.logger.error(f"Fail at JSON decoding: {e}")
            return None

    def _parse_json_to_df(
            self,
            json_data,
            cols=None,
            is_list: bool = False,
            convert_timestamp: bool = True,
            sanitize: bool = True,
            frequency: str = 'daily',
            col_freq="data",
            date_as_index: bool = False,
            date_format: str = "%d/%m/%Y") -> pd.DataFrame:
        """
        Converts a structured JSON in a pd.DataFrame, treating columns, data and Null Values.

        Parameters
        ----------
        json_data : list of dict
            List of records returned by the API in JSON format.

        cols : list of str, optional
            List of desired columns. If None, it will take all common columns across records.

        convert_timestamp : bool, default=True
            If True, converts the 'timestamp' column (if present) to a human-readable date column.

        sanitize : bool, default=True
            If True, removes columns that are completely Null and fills null values in numeric columns with zeros.

        frequency : str or None, default='daily'
            Desired frequency transformation for time series:
            'daily', 'monthly', 'quarterly', or 'auto' (infer from data).
            If None, no transformation is applied.

        col_freq : str, default='date'
            Temporal column to be used to obtain frequency.

        date_as_index : bool, default=False
            Sets this column as the DataFrame index.

        Returns
        -------
        pd.DataFrame or None
            DataFrame with applied preprocessing.
            Returns None if input is invalid or resulting DataFrame is empty.

        Raises
        ------
        Exception
            Internal exceptions are logged and None is returned.
        """
        try:
            if not json_data:
                return None

            if cols is None:
                clean_data = self.clean_metadata(json_data)
            else:
                clean_data = [{k: d.get(k, None) for k in cols}
                              for d in json_data]

            if is_list:
                df = pd.json_normalize(clean_data)
            else:
                df = pd.DataFrame(clean_data)

            if not df.empty:
                if sanitize:
                    df.dropna(axis=1, how="all", inplace=True)
                    num_cols = df.select_dtypes(include="number").columns
                    df[num_cols] = df[num_cols].fillna(0)

                if "timestamp" in df.columns and convert_timestamp:
                    df["data"] = pd.to_datetime(pd.to_numeric(
                        df["timestamp"]), unit="s").dt.date

                if frequency != 'daily':
                    if col_freq not in df.columns:
                        self.logger.warning(
                            f"Frequency column '{col_freq}' not found.")
                        return df

                    if frequency == "auto":

                        interval = df[col_freq].diff().dt.days.median()

                        if interval >= 80:
                            frequency = "quarterly"
                        elif interval >= 27:
                            frequency = "monthly"

                        self.logger.info(
                            f"Date frequency inferred: {frequency}")

                df[col_freq] = pd.to_datetime(df[col_freq], format=date_format)

                if frequency == "monthly":
                    df[col_freq] = df[col_freq].dt.to_period(
                        "M").dt.to_timestamp()
                elif frequency == "quarterly":
                    df[col_freq] = df[col_freq].dt.to_period(
                        "Q").dt.to_timestamp()
                elif frequency == "daily":
                    pass

            if date_as_index and col_freq in df.columns:
                df.set_index(col_freq, inplace=True)

            return df if not df.empty else None

        except Exception as e:
            self.logger.error(
                f"Error in parsing the JSON data: {e}")
            return None

    def get_from_api(self,
                     url,
                     variable_list,
                     is_list: bool = False,
                     convert_timestamp: bool = True,
                     sanitize: bool = True,
                     frequency=None,
                     col_freq="data",
                     date_as_index: bool = False,
                     http_get_timeout: int = 10,
                     date_format: str = "%d/%m/%Y"):
        """
        Sends an HTTP GET request to the specified URL and parses the JSON response into a DataFrame.

        Parameters
        ----------
        url : str
            The API endpoint to fetch data from.

        variable_list : list of str
            List of keys/columns to extract from the JSON payload.

        is_list : bool
            If True, the response JSON is expected to be a list of dicts.
            If False, expects a nested dictionary format.

        convert_timestamp : bool, default=True
            If True, converts a 'timestamp' column (if present) to human-readable dates.

        sanitize : bool, default=True
            If True, removes fully null columns and fills null values in numeric columns with zero.

        frequency : str or None, default=None
            Frequency of time series: 'daily', 'monthly', 'quarterly' or 'auto' for automatic detection.

        col_freq : str, default='data'
            Column name to use for frequency adjustment and date indexing.

        date_as_index : bool, default=False
            If True, sets the frequency column as the DataFrame index.

        Returns
        -------
        pd.DataFrame or None
            A processed DataFrame if successful, otherwise None.
        """

        try:
            response = self._get_api_data(
                url, http_get_timeout=http_get_timeout)

            json_data = self._validate_json(
                response, is_list, cols=variable_list)

            df = self._parse_json_to_df(
                json_data=json_data,
                cols=variable_list,
                is_list=is_list,
                convert_timestamp=convert_timestamp,
                sanitize=sanitize,
                frequency=frequency,
                col_freq=col_freq,
                date_as_index=date_as_index,
                date_format=date_format)

            return df

        except Exception as e:
            self.logger.error(
                f"Failed to get the API data: {e}")
            return None

    def _get_api_data(self, url, http_get_timeout):
        """
        Performs an HTTP GET request to the provided URL and validates the response.

        Parameters
        ----------
        url : str
            The API endpoint to request data from.

        Returns
        -------
        requests.Response or None
            The Response object if the request succeeds and returns status 200,
            otherwise None.

        Logs
        ----
        Errors and warnings are logged using the configured logger instance.
        """
        if url:
            try:
                response = requests.get(url, timeout=http_get_timeout)
                if response.status_code != 200:
                    erro_json = response.json()
                    erro_msg = erro_json.get("error") or \
                        (erro_json.get("erro") or {}).get("detail") or \
                        "Unknown error"
                    raise Exception(f"API error: {erro_msg}")

                return response

            except Exception as e:
                self.logger.error(
                    f"Failed to get the API data: {e}")
            return None

        else:
            self.logger.error(
                "No URL was provided.")
