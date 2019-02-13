#!/usr/bin/env python

"""
Python script to initiate and perform the ETL process
"""
from src.app.spark_session import InitSpark
from src.app.regularly_spaced_transactions import RegularSpacedTransactions
import warnings
import os


def write_to_csv(_df, _name):
    """Function to save the datafranes to CSV, with coalesce the whole file get written in on go,
    not nice in a real environment but ok for this setup
    Args:
        _df: spark dataframe
        _name: name of the folder containing the dataframe as CSV
    """
    _df.coalesce(1).write.format('com.databricks.spark.csv') \
        .option('header', 'true') \
        .mode('overwrite') \
        .save(os.getcwd() + '/src/data/' + _name)


def main():
    # Initialise SparkSession
    _init_spark_session = InitSpark("bankTransactions", "spark-warehouse")
    _default_spark_session = _init_spark_session.spark_init()
    _default_spark_session.sparkContext.setLogLevel("WARN")

    print("Creatingg the initial default" +
          "cleaned dataframe for transaction, ready to be used for analytics")
    _transactions = RegularSpacedTransactions(_default_spark_session)
    _df = RegularSpacedTransactions.run(_transactions)
    print(_df.printSchema())

    print("Printing Transformed weekly Transacations Dataframe Schema...")
    weekly_df = RegularSpacedTransactions.transform_reguarly_spaced_transactions(
        _df,
        'weekly',
        7,
        7,
    )
    weekly_df.printSchema()
    weekly_df.show()
    print("Saving to CSV...")
    write_to_csv(weekly_df, 'weekly_transactions')

    print("Printing Transformed fortnightly Transacations Dataframe Schema...")
    fortnightly_df = RegularSpacedTransactions.transform_reguarly_spaced_transactions(
        _df,
        'fortnightly',
        14,
        14,
    )
    fortnightly_df.printSchema()
    fortnightly_df.show()
    print("Saving to CSV...")
    write_to_csv(fortnightly_df, 'fortnightly_transactions')

    print("Printing Transformed monthly Transacations Dataframe Schema...")
    monthly_df = RegularSpacedTransactions.transform_reguarly_spaced_transactions(
        _df,
        'monthly',
        27,
        35,
    )
    monthly_df.printSchema()
    monthly_df.show()
    print("Saving to CSV...")
    write_to_csv(monthly_df, 'monthly_transactions')

    print("Printing Transformed monthly Transactions Occuring On The Last Friday Every Month...")
    last_friday_df = RegularSpacedTransactions.transform_last_friday_month_transactions(monthly_df)
    last_friday_df.printSchema()
    last_friday_df.show()
    print("Save to CSV...")
    write_to_csv(last_friday_df, 'last_friday_monthly_transactions')


if __name__ == "__main__":
    warnings.filterwarnings("ignore", message="numpy.dtype size changed")
    warnings.filterwarnings("ignore", message="numpy.ufunc size changed")
    main()
