from pyspark.sql.types import StructField, StructType, StringType, DateType, DoubleType, BooleanType, IntegerType

correct_default_types = [
    StructField('date', DateType(), True),
    StructField('description', StringType(), True),
    StructField('amount', DoubleType(), True),
    StructField('day_number', StringType(), True),
    StructField('weekday', StringType(), True),
]

FINAL_DEFAULT_STRUCT = StructType(fields=correct_default_types)

wrong_default_types = [
    StructField('date', StringType(), True),
    StructField('description', StringType(), True),
    StructField('amount', StringType(), True),
    StructField('day_number', StringType(), True),
    StructField('weekday', StringType(), True),
]

WRONG_DEFAULT_STRUCT = StructType(fields=wrong_default_types)

correct_fortnightly_regularity_types = [
    StructField('date', DateType(), True),
    StructField('description', StringType(), True),
    StructField('amount', DoubleType(), True),
    StructField('day_number', StringType(), True),
    StructField('weekday', StringType(), True),
    StructField('days_passed', IntegerType(), True),
    StructField('fortnightly', BooleanType(), True)
]

FINAL_FORTNIGHTLY_STRUCT = StructType(fields=correct_fortnightly_regularity_types)

wrong_fortnightly_regularity_types = [
    StructField('date', StringType(), True),
    StructField('description', StringType(), True),
    StructField('amount', DoubleType(), True),
    StructField('day_number', StringType(), True),
    StructField('weekday', StringType(), True),
    StructField('days_passed', StringType(), True),
    StructField('fortnightly', StringType(), True)
]

WRONG_FORTNIGHTLY_STRUCT = StructType(fields=wrong_fortnightly_regularity_types)

correct_monthly_regularity_types = [
    StructField('date', DateType(), True),
    StructField('description', StringType(), True),
    StructField('amount', DoubleType(), True),
    StructField('day_number', StringType(), True),
    StructField('weekday', StringType(), True),
    StructField('days_passed', IntegerType(), True),
    StructField('monthly', BooleanType(), True)
]

FINAL_MONTHLY_STRUCT = StructType(fields=correct_monthly_regularity_types)

wrong_monthly_regularity_types = [
    StructField('date', StringType(), True),
    StructField('description', StringType(), True),
    StructField('amount', DoubleType(), True),
    StructField('day_number', StringType(), True),
    StructField('weekday', StringType(), True),
    StructField('days_passed', StringType(), True),
    StructField('monthly', StringType(), True)
]

WRONG_MONTHLY_STRUCT = StructType(fields=wrong_monthly_regularity_types)
