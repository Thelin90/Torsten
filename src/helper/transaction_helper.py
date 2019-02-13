#!/usr/bin/env python
"""
This python script contains helper functions used to solve the
transactions task in the assignment. The reason why they are kept
here is to make it scale, these  functions should be able to be
implemented to other problems rather than what the specific apps
are trying to solve.

It should also show a clear structure in what needs to be unit
tested and what should be for integration testing
"""
import calendar
from pyspark.sql import Window
from pyspark.sql.types import DateType
from pyspark.sql.functions import \
    to_date, regexp_replace,\
    date_format, monotonically_increasing_id,\
    udf,\
    datediff,\
    lag


def clean_description_column(_df, _trim_desc, _trim_replace, _col_desc):
    """Function to clean description column
    Args:
        _df: spark dataframe
        _trim_desc: values to be replaced
        _trim_replace: the value to replace the old with
        _col_desc: the column to be cleaned

    Returns: cleaned description column within spark dataframe
    """
    # perform cleaning of description column
    for i in range(len(_trim_desc)):
        _change = _trim_desc[i]
        for _replace in _trim_replace:
            if _replace in _change:
                _df = trim_values(_df, _col_desc, _change, _replace)
    return _df


def get_last_friday_of_month(_df, _year=2018):
    """Function get available all available last fridays of every month
    for the given dates year
    # TODO: _year should be extracted in a better way
    Args:
        _df: spark dataframe
        _year: the specific year to get all last fridays from

    Returns:
    """
    global _month, _last_friday
    _last_fridays = []

    # loop through the months jan, feb, mars, apr, may, june, july, aug,
    # sep, oct, nov, dec
    for _month in range(1, 13):
        # get all last fridays of every month
        _last_friday = max(week[calendar.FRIDAY] for week in calendar.monthcalendar(_year, _month))
        # does the column date correspond to any of the last fridays of
        # the year
        _last_fridays.append('{:4d}-{:02d}-{:02d}'.format(_year, _month, _last_friday))

    # filter out the dates which corresponds to the dates that corresponds
    # to the last fridays of every month
    _df = _df.filter(_df['date'].isin(_last_fridays))

    return _df


def get_first_time_period_transaction(
        _df,
        _min_day_acceptance,
        _max_day_acceptance,
        _regularity,
        _days_passed_col,
        _desc_col
):
    """Function to find the starting transaction for the transactions during a
    certain regularity period, filtering based on the description, at this stage
    the amount and date will correspond. Filter out the transactions that is
    not the starting transaction point

    Args:
    _df:
    _min_day_acceptance: value representing the minimum days thas is accepted
    for a certain period
    _max_day_acceptance: value representing the maximum days thas is accepted
    for a certain period
    _regularity: string specifying the regularity period (week, fortnightly,
    month e.g.)
    _days_passed_col: column name for days_passed
    _desc_col: column name for description

    Returns:
    """
    global _df_tmp_two

    # create a temporary dataframe to filter out the transactions where it
    # has been marked to match the regularity
    _df_tmp = _df.filter(
        _df[_days_passed_col].between(_min_day_acceptance, _max_day_acceptance)).filter(
        _df[_regularity] == 1)

    # create a set of the description column representing the source of the
    # transaction to be able to filter out
    # when the transaction period started
    _description_list = _df_tmp.select(_desc_col).rdd.flatMap(lambda x: x).collect()
    _description_set = set(_description_list)

    # iterate through the description set, find the initial transactions for
    # the transactions for the regularity period
    # TODO: disgusting solution (yuck), should be able to use _df.filter.isin(list)
    for _ in range(len(_description_set)):
        if len(_description_set) > 0:
            # filter out the starting point of the transactions
            _df_tmp_two = _df.filter(_df[_regularity] == 0) \
                .filter(_df[_days_passed_col] == 0) \
                .filter(_df[_desc_col] == _description_set.pop())
        _df_tmp = _df_tmp.union(_df_tmp_two)

    return _df_tmp


def window_function_days_passed(
        _df,
        _col_one,
        _col_two,
        _col_date,
        _col_regularity,
        _min_day_acceptance,
        _max_day_acceptance,
        _col_days_passed='days_passed'
):
    """Window function to calculate the number of days passed between
    transactions
    Args:
        _df: spark dataframe
        _col_one: column used to partition by, and order by
        _col_two: column used to partition by, and order by
        _col_date: column used to calculate time diff and order by
        _col_regularity: column used for regularity period (weekly,
        fortnightly, monthly e.g.)
        _min_day_acceptance: min value for regularity period
        _max_day_acceptance: max value for regularity period
        _col_days_passed: column used for days passed

    Returns:
    """
    # initialise the window
    window = Window.partitionBy(_col_one, _col_two).orderBy(_col_date)

    # use the window function to calculate the number of days
    # that has passed
    _df = _df.withColumn(
        _col_days_passed,
        datediff(_df[_col_date],
                 lag(_df[_col_date], 1).over(window))) \
        .orderBy(_col_one, _col_two, _col_date, ascending=True)

    # mark the transactions that follows the regularity with a
    # flag (true)
    _df = _df.withColumn(_col_regularity, _df[_col_days_passed]
                         .between(
                            _min_day_acceptance,
                            _max_day_acceptance).astype('boolean')
                         )

    # Set days passed to 0 if it is not part of a accepted
    # regularity period, and false which means that the
    # period is either not a part the transaction period or
    # potentially the start of it
    _df = _df.fillna({'days_passed': '0'}).fillna({_col_regularity: '0'})

    return _df


def udf_creator(_df, _functioncall, _newcolname, _oldcolname, _datatype):
    """Function used to call udf functions generically
    Args:
        _df: Spark Dataframe
        _functioncall: The function to be called
        _newcolname: The name of the new column containing the
        extracted values
        _oldcolname: The column values from a specific column to
        be sent, example: ip addresses
        _datatype: Which datatype to work with, example: StringType()

    Returns: Spark Dataframe with one new additional column containing
    values from the UDF call
    """
    if isinstance(_newcolname, str) and isinstance(_oldcolname, str):
        _udf = udf(_functioncall, _datatype)
        return _df.withColumn(_newcolname, _udf(_df[_oldcolname]))
    else:
        raise Exception("Column names must be of str")


def set_col_nullable(_spark, _df, _col_list, _nullable=False):
    """Adds monotically increasing ID column to Spark Dataframe

    Args:
        _spark: Spark Dataframe
        _df:
        _col_list:
        _nullable:

    Returns: Spark Dataframe with a new definition for nullable columns
    """
    for _field in _df.schema:
        if _field.name in _col_list:
            _field.nullable = _nullable
    return _spark.createDataFrame(_df.rdd, _df.schema)


def add_monotically_increasing_id(_df, _colname):
    """Adds monotically increasing ID column to Spark Dataframe

    Args:
        _df: Spark Dataframe
        _colname: Name of the col containing the monotoically
        increasing ID

    Returns: Spark Dataframe with a column containing a monotoically
    increasing ID
    """
    if isinstance(_colname, str):
        return _df.withColumn(_colname, monotonically_increasing_id())
    else:
        raise ValueError


def trim_values(_df, _col, _value, _replace):
    """Function to clean up strings in given column column
    Args:
        _df: spark dataframe
        _col: the column value where the string needs to be trimmed
        _value:
        _replace:

    Returns: a cleaned column from given trim instructions
    """
    return _df.withColumn(_col, regexp_replace(_col, _value, _replace))


def get_day_of_week(_df, _col):
    """Function extracts the number equal to the the day it represents
    in the current week from the date

    Args:
        _df: spark dataframe
        _col:

    Returns dataframe with 2 new columns containing the actual day number
    and the given representing weekday
    """
    return _df.select(_col, date_format(_col, 'u').alias('day_number'), date_format(_col, 'E').alias('weekday'))


def drop_columns(_df, _colnames):
    """Drops N number of undesired columns

    Args:
        _df: Spark Dataframe
        _colnames: List containing column names to be dropped

    Returns: Spark Dataframe

    """
    if isinstance(_colnames, list):
        return _df.drop(*_colnames)
    else:
        raise ValueError("colnames is not a list")


def merge_dataframes(_df0, _df1, _colnames, _orderbycols):
    """Merges two Spark Dataframes to one

    Args:
        _df0: Spark Dataframe 0
        _df1: Spark Dataframe 1 to be joined with Spark Datafram 0
        _colnames: list of column names to be set
        _orderbycols: list containing id value to join by

    Returns: dataframe that has been merged from 2 different dataframes

    """
    ret_df = _df0.join(_df1, _df0[_orderbycols[0]] == _df1[_orderbycols[1]])
    ret_df = ret_df.orderBy(_orderbycols[0], ascending=True)
    ret_df = ret_df.select(*_colnames)

    return ret_df


def clean_dates(_date_str):
    """Function to make sure that date is correct by checking month
    value in date string, note this is done before
    parsing the values to pyspark.sql.types.DateType().
    Rather trivial approach but it works fine in this case.

    Args:
        _date_str: the raw date string value in dataframe

    Returns the correct date string value
    """
    _check_date = _date_str.split('/')
    _length_of_check_date = 3
    _max_month_value = 12

    if len(_check_date) == _length_of_check_date:
        if int(_check_date[1]) > _max_month_value:
            # switch month value: 01/23/2018 ->
            # 23/01/2018 because in the csv the format
            # should be day/month/year
            return _check_date[1] + "/" + _check_date[0] + "/" + _check_date[2]

        return _date_str
    else:
        raise ValueError('Date string must only include day/month/year total length of 3 elements')


def cast_to_datatype(_df, _colname, _datatype):
    """Function casts column values to given datatype, special case
    for timestamps (will delete row if invalid date).
    If the datatype is DateType it will be handled separately
    Args:
        _df: Spark Dataframe
        _colname: Name of column
        _datatype: pyspark datatype for given column

    Returns cleaned initial transaction dataframe
    """
    _coldatatype = _df.schema[_colname].dataType
    if isinstance(_coldatatype, _datatype):
        raise ValueError('Already this datatype')
    else:
        if _datatype == DateType:
            _df = _df.withColumn(_colname, to_date(_df[_colname], 'dd/MM/yy').cast(_datatype()))
            return _df.filter(_df[_colname].isNotNull())
        else:
            return _df.withColumn(_colname, _df[_colname].cast(_datatype()))
