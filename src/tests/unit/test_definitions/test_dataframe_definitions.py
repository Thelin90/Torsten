from src.tests.unit.app.fake_spark_session import PySparkTest
from src.definitions.default_transaction_definitions import DefaultConfigDefinition, DescriptionTrimValuesDefinition
from src.tests.unit.test_definitions.struct_definitions import FINAL_DEFAULT_STRUCT, WRONG_DEFAULT_STRUCT, \
    FINAL_MONTHLY_STRUCT, WRONG_MONTHLY_STRUCT, FINAL_FORTNIGHTLY_STRUCT, WRONG_FORTNIGHTLY_STRUCT
import os

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
_initial_names_cols = DefaultConfigDefinition.INITIAL_DEFAULT_NAMES_COLUMNS
_final_names_cols = DefaultConfigDefinition.FINAL_DEFAULT_NAMES_COLUMNS
_final_sort_cols = DefaultConfigDefinition.FINAL_DEFAULT_SORT_COLUMNS
_delimiter = DefaultConfigDefinition.DELIMITER


def get_records(_df, _col):
    return _df.select(_col).rdd.map(lambda row: row[0]).collect()


class TestDataFrames:
    # initialise a fake spark session to use in test
    FAKE_SPARK = PySparkTest.create_testing_pyspark_session()
    # initial conversion from test_data/default/CSV to dataframe
    CORRECT_DEFAULT_DF = FAKE_SPARK.read.option("delimiter", _delimiter) \
        .option('header', _header) \
        .schema(FINAL_DEFAULT_STRUCT) \
        .csv(
        os.getcwd() + '/src/tests/unit/test_data/default/correct.csv') \
        .toDF(_date_col, _desc_col, _amount_col, 'day_number', 'weekday')

    INCORRECT_DEFAULT_DF = FAKE_SPARK.read.option("delimiter", _delimiter) \
        .option('header', _header) \
        .schema(WRONG_DEFAULT_STRUCT) \
        .csv(
        os.getcwd() + '/src/tests/unit/test_data/default/incorrect.csv') \
        .toDF(_date_col, _desc_col, _amount_col, 'day_number', 'weekday')

    CORRECT_WEEKLY_DF = FAKE_SPARK.read.option("delimiter", _delimiter) \
        .option('header', _header) \
        .schema(FINAL_FORTNIGHTLY_STRUCT) \
        .csv(
        os.getcwd() + '/src/tests/unit/test_data/weekly_transactions/correct.csv') \
        .toDF(_date_col, _desc_col, _amount_col, 'day_number', 'weekday', 'days_passed', 'weekly')

    INCORRECT_WEEKLY_DF = FAKE_SPARK.read.option("delimiter", _delimiter) \
        .option('header', _header) \
        .schema(WRONG_FORTNIGHTLY_STRUCT) \
        .csv(
        os.getcwd() + '/src/tests/unit/test_data/weekly_transactions/incorrect.csv') \
        .toDF(_date_col, _desc_col, _amount_col, 'day_number', 'weekday', 'days_passed', 'weekly')

    CORRECT_FORTNIGHTLY_DF = FAKE_SPARK.read.option("delimiter", _delimiter) \
        .option('header', _header) \
        .schema(FINAL_FORTNIGHTLY_STRUCT) \
        .csv(
        os.getcwd() + '/src/tests/unit/test_data/fortnightly_transactions/correct.csv') \
        .toDF(_date_col, _desc_col, _amount_col, 'day_number', 'weekday', 'days_passed', 'fortnightly')

    INCORRECT_FORTNIGHTLY_DF = FAKE_SPARK.read.option("delimiter", _delimiter) \
        .option('header', _header) \
        .schema(WRONG_FORTNIGHTLY_STRUCT) \
        .csv(
        os.getcwd() + '/src/tests/unit/test_data/fortnightly_transactions/incorrect.csv') \
        .toDF(_date_col, _desc_col, _amount_col, 'day_number', 'weekday', 'days_passed', 'fortnightly')

    CORRECT_MONTHLY_DF = FAKE_SPARK.read.option("delimiter", _delimiter) \
        .option('header', _header) \
        .schema(FINAL_MONTHLY_STRUCT) \
        .csv(
        os.getcwd() + '/src/tests/unit/test_data/monthly_transactions/correct.csv') \
        .toDF(_date_col, _desc_col, _amount_col, 'day_number', 'weekday', 'days_passed', 'monthly')

    INCORRECT_MONTHLY_DF = FAKE_SPARK.read.option("delimiter", _delimiter) \
        .option('header', _header) \
        .schema(WRONG_MONTHLY_STRUCT) \
        .csv(
        os.getcwd() + '/src/tests/unit/test_data/monthly_transactions/incorrect.csv') \
        .toDF(_date_col, _desc_col, _amount_col, 'day_number', 'weekday', 'days_passed', 'monthly')

    CORRECT_LAST_FRIDAY_MONTLY_DF = FAKE_SPARK.read.option("delimiter", _delimiter) \
        .option('header', _header) \
        .schema(FINAL_MONTHLY_STRUCT) \
        .csv(
        os.getcwd() + '/src/tests/unit/test_data/last_friday_monthly_transactions/correct.csv') \
        .toDF(_date_col, _desc_col, _amount_col, 'day_number', 'weekday', 'days_passed', 'monthly')

    INCORRECT_LAST_FRIDAY_MONTLY_DF = FAKE_SPARK.read.option("delimiter", _delimiter) \
        .option('header', _header) \
        .schema(WRONG_MONTHLY_STRUCT) \
        .csv(
        os.getcwd() + '/src/tests/unit/test_data/last_friday_monthly_transactions/incorrect.csv') \
        .toDF(_date_col, _desc_col, _amount_col, 'day_number', 'weekday', 'days_passed', 'monthly')
