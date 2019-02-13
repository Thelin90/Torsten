#!/usr/bin/env python3
from src.app.default_transaction import Transactions
from src.app.regularly_spaced_transactions import RegularSpacedTransactions
from src.tests.unit.app.fake_spark_session import PySparkTest
from src.tests.unit.test_definitions.test_dataframe_definitions import TestDataFrames, get_records


class TestTransactions(PySparkTest):

    def test_weekly_dataframe(self):
        """Test method verifies the process to create the weekly transaction dataframe with a fake spark session.
        It is being created as the expected format. A snapshot of how the dataframe should be and is compared to.
        If any changes are made in transform_reguarly_spaced_transactions, or in any of the helper functions this
        test will fail.


        Args:
            self: containing spark session
        """
        _df = Transactions.create_df(self.spark)
        _correct_weekly_df = TestDataFrames.CORRECT_WEEKLY_DF
        _incorrect_weekly_df = TestDataFrames.INCORRECT_WEEKLY_DF

        _df = RegularSpacedTransactions.transform_reguarly_spaced_transactions(_df, 'weekly', 7, 7)

        print('Assert against a correctly defined weekly schema named dataframe')
        self.assertEqual(_df.schema.names, _correct_weekly_df.schema.names)
        self.assertEqual(_df.dtypes, _correct_weekly_df.dtypes)

        print('Assert against a correctly defines weekly schema names in dataframe')
        self.assertEqual(_df.schema.names, _incorrect_weekly_df.schema.names)

        print('Assert that it had correct weekly schema names but wrong datatypes')
        self.assertNotEqual(_df.dtypes, _incorrect_weekly_df.dtypes)

        for col in _correct_weekly_df.schema.names:
            print('Assert that records in column: ' + col + ' is matching weekly dataframe')
            _df_list = get_records(_df, col)
            _df_correct_list = get_records(_correct_weekly_df, col)
            _df_incorrect_list = get_records(_incorrect_weekly_df, col)
            # with the given dataset they should be 0
            self.assertEqual(len(_df_list), 0)
            self.assertEqual(len(_df_correct_list), 0)
            self.assertEqual(len(_df_incorrect_list), 0)
            self.assertEqual(len(_df_list), len(_df_correct_list))
            self.assertEqual(len(_df_list), len(_df_incorrect_list))
