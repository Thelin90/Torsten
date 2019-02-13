from src.app.default_transaction import Transactions
from src.helper.transaction_helper import \
    window_function_days_passed, \
    get_first_time_period_transaction, \
    get_last_friday_of_month


class RegularSpacedTransactions:
    """Class

    """

    def __init__(self, spark_session):
        super(RegularSpacedTransactions, self)
        self.app_name = "bankTransactions"
        self.warehouse_location = "spark-warehouse"
        self.spark_session = spark_session

    def run(self):
        """

        Returns:
        """
        return Transactions.create_df(self.spark_session)

    def transform_last_friday_month_transactions(self):
        """Transform method to calculate transactions that has the same description, amount, and a monthly regularity.
        the function expects a dataframe where the transactions has been sorted out after monthly regularity
        from transform_regularity_spaced_transactions

        :return:
        """

        # get the last friday of every month for a specific year
        _df = get_last_friday_of_month(self)

        # order by description and date
        _df = _df.orderBy('description', 'date')

        return _df

    def transform_reguarly_spaced_transactions(self, _regularity, _min_day_acceptance, _max_day_acceptance):
        """Transform method to calculate transactions with same description, amount, and regularly-spaced
        (weekly, fortnightly, monthly e.g.)
        Args:
            _regularity:
            _min_day_acceptance:
            _max_day_acceptance:

        Returns: Transformed spark dataframe for a given regularity period
        """
        # calculate how many days that has passed for the given period
        # (week, fortnightly, month e.g.)
        _df = window_function_days_passed(self, 'amount', 'description', 'date', _regularity, _min_day_acceptance,
                                          _max_day_acceptance)
        # add the transactions that started the given regularity period
        # these have 0 days passed therefor the initial transaction starting
        # the regularity period
        _df_tmp = get_first_time_period_transaction(_df, _min_day_acceptance, _max_day_acceptance, _regularity,
                                                    'days_passed', 'description')
        # order the table to be more readable
        _df_tmp = _df_tmp.orderBy('description', 'date', ascending=True)

        return _df_tmp
