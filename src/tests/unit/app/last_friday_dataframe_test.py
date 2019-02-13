#!/usr/bin/env python3
from src.app.default_transaction import Transactions
from src.app.regularly_spaced_transactions import RegularSpacedTransactions
from src.tests.unit.app.fake_spark_session import PySparkTest
from src.tests.unit.test_definitions.test_dataframe_definitions import TestDataFrames, get_records


class TestTransactions(PySparkTest):

    def test_monthly_last_friday_transactions_dataframe(self):
        """Test method verifies the process to create the last friday monthly transaction dataframe with a fake spark
        session. It is being created as the expected format. A snapshot of how the dataframe should be and is compared to.
        If any changes are made in transform_reguarly_spaced_transactions, or in any of the helper functions this
        test will fail.

        Args:
            self: containing spark session
        """
        _df = Transactions.create_df(self.spark)
        _correct_monthly_df = TestDataFrames.CORRECT_LAST_FRIDAY_MONTLY_DF
        _incorrect_monthly_df = TestDataFrames.INCORRECT_LAST_FRIDAY_MONTLY_DF

        _df = RegularSpacedTransactions.transform_reguarly_spaced_transactions(_df, 'monthly', 27, 35)
        _df = RegularSpacedTransactions.transform_last_friday_month_transactions(_df)

        print('Assert against a correctly defined last friday monthly schema named dataframe')
        self.assertEqual(_df.schema.names, _correct_monthly_df.schema.names)
        self.assertEqual(_df.dtypes, _correct_monthly_df.dtypes)

        print('Assert against a correctly defined last friday monthly schema names in dataframe')
        self.assertEqual(_df.schema.names, _incorrect_monthly_df.schema.names)

        print('Assert that it had correct last friday monthly schema names but wrong datatypes')
        self.assertNotEqual(_df.dtypes, _incorrect_monthly_df.dtypes)

        for col in _correct_monthly_df.schema.names:
            print('Assert that records in column: ' + col + ' is matching last friday monthly dataframe')
            _df_list = get_records(_df, col)
            _df_correct_list = get_records(_correct_monthly_df, col)
            self.assertEqual(_df_list, _df_correct_list)

            print('Assert that records in column: ' + col + ' is not matching last friday monthly dataframe')
            _df_incorrect_list = get_records(_incorrect_monthly_df, col)
            self.assertNotEqual(_df_list, _df_incorrect_list)
