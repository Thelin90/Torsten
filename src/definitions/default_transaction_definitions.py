import os


class DefaultColumnDefinition:
    """Default column definitions transaction dataframe"""
    DATEFIELD_COLUMN = 'date'
    DESCRIPTION_COLUMN = 'description'
    AMOUNT_COLUMN = 'amount'
    INITIAL_DEFAULT_NAMES_COLUMNS = [DATEFIELD_COLUMN, DESCRIPTION_COLUMN, AMOUNT_COLUMN]
    FINAL_DEFAULT_NAMES_COLUMNS = [
        DATEFIELD_COLUMN, DESCRIPTION_COLUMN,
        AMOUNT_COLUMN,
        'day_number',
        'weekday'
    ]
    FINAL_WEEKLY_NAMES_COLUMNS = \
        [
            DATEFIELD_COLUMN,
            DESCRIPTION_COLUMN,
            AMOUNT_COLUMN,
            'day_number',
            'weekday',
            'days_passed',
            'weekly'
        ]
    FINAL_FORTNIGHTLY_NAMES_COLUMNS = \
        [
            DATEFIELD_COLUMN,
            DESCRIPTION_COLUMN,
            AMOUNT_COLUMN,
            'day_number',
            'weekday',
            'days_passed',
            'fortnightly'
        ]
    FINAL_MONTHLY_NAMES_COLUMNS = \
        [
            DATEFIELD_COLUMN,
            DESCRIPTION_COLUMN,
            AMOUNT_COLUMN,
            'day_number',
            'weekday',
            'days_passed',
            'monthly'
        ]
    FINAL_DEFAULT_MONO_ID_COLUMN = 'eventID'
    FINAL_WEEK_MONO_ID_COLUMN = 'id'
    FINAL_DEFAULT_SORT_COLUMNS = [FINAL_DEFAULT_MONO_ID_COLUMN, FINAL_WEEK_MONO_ID_COLUMN]
    FINAL_DROP_WEEK_DF_COLUMNS = [DATEFIELD_COLUMN, DESCRIPTION_COLUMN]


class DefaultConfigDefinition(DefaultColumnDefinition):
    """Default configuration for initial transaction dataframe"""
    HEADER = 'true'
    DELIMITER = ","
    CSV_PATH = os.getcwd() + '/src/data/initialdata.csv'


class DescriptionTrimValuesDefinition(DefaultColumnDefinition):
    """Default trim values for inital transaction dataframe"""
    TRIM_DESC_VALUES = ['UMBRELLA CORPORATION PAYMENT', 'UMBRELLA CORPORATION LTD', '/']
    TRIM_REPLACE_VALUES = ['UMBRELLA CORPORATION', '']
