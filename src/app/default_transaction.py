from pyspark.sql.types import DoubleType, DateType, StringType
from src.definitions.default_transaction_definitions import DefaultConfigDefinition, DescriptionTrimValuesDefinition
from src.app.spark_session import InitSpark

from src.helper.transaction_helper import \
    clean_description_column, \
    get_day_of_week, \
    merge_dataframes, \
    cast_to_datatype, \
    add_monotically_increasing_id, \
    drop_columns, \
    set_col_nullable, \
    udf_creator, \
    clean_dates

_header = DefaultConfigDefinition.HEADER
_csv_path = DefaultConfigDefinition.CSV_PATH
_trim_desc = DescriptionTrimValuesDefinition.TRIM_DESC_VALUES
_trim_replace = DescriptionTrimValuesDefinition.TRIM_REPLACE_VALUES
_col_desc = DefaultConfigDefinition.DESCRIPTION_COLUMN
_date_col = DefaultConfigDefinition.DATEFIELD_COLUMN
_desc_col = DefaultConfigDefinition.DESCRIPTION_COLUMN
_amount_col = DefaultConfigDefinition.AMOUNT_COLUMN
_final_default_mono_id_col = DefaultConfigDefinition.FINAL_DEFAULT_MONO_ID_COLUMN
_final_week_mono_id_col = DefaultConfigDefinition.FINAL_WEEK_MONO_ID_COLUMN
_final_drop_cols = DefaultConfigDefinition.FINAL_DROP_WEEK_DF_COLUMNS
_final_names_cols = DefaultConfigDefinition.FINAL_DEFAULT_NAMES_COLUMNS
_final_sort_cols = DefaultConfigDefinition.FINAL_DEFAULT_SORT_COLUMNS
_delimiter = DefaultConfigDefinition.DELIMITER


class Transactions(InitSpark):
    """Class will handle logic regarding parsing the CSV to dataframe
    """

    def __init__(self, _spark, app_name, warehouse_location):
        super().__init__(app_name, warehouse_location)

    def create_df(self):
        """Method transforms the raw parsed dataframe into the default dataframe
        being used to perform analytics in other apps

        dataframe columns are:

            date  | description | amount | day_number | week_day

        dataframe column datatypes and nullables:

                date: DateType, not nullable
                description: StringType, nullable
                amount: DoubleType, nullable
                day_number: StringType, not nullable
                weekday: StringType, not nullable

        Returns the transformed dataframe
        """
        # initial conversion from CSV to dataframe
        _df = self.read.option("delimiter", _delimiter) \
            .option('header', _header) \
            .csv(_csv_path) \
            .toDF(_date_col, _desc_col, _amount_col)

        # perform cleaning of description column
        _df = clean_description_column(_df, _trim_desc, _trim_replace, _col_desc)

        # perform cleaning of date column
        _df = udf_creator(_df, clean_dates, _date_col, _date_col, StringType())

        # typecast columns
        _df = cast_to_datatype(_df, _date_col, DateType)
        _df = cast_to_datatype(_df, _amount_col, DoubleType)

        # change if columns should be nullable
        _df = set_col_nullable(self, _df, [_date_col])

        # add monotically id to df, helps when merging
        _df = add_monotically_increasing_id(_df, _final_default_mono_id_col)

        # Extract day number with weekday
        _df_week_days = get_day_of_week(_df, _date_col)

        # add monotically id to df, helps when merging
        _df_week_days = add_monotically_increasing_id(_df_week_days, _final_week_mono_id_col)

        # drop not needed columns containing information about week days before merging
        _df_week_days = drop_columns(_df_week_days, _final_drop_cols)

        # Merge temp dataframe containing weekday information into default dataframe
        _df = merge_dataframes(
            _df,
            _df_week_days,
            _final_names_cols,
            _final_sort_cols
        )

        return _df
