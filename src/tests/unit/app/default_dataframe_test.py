#!/usr/bin/env python3
from src.app.default_transaction import Transactions
from src.tests.unit.app.fake_spark_session import PySparkTest
from src.tests.unit.test_definitions.test_dataframe_definitions import TestDataFrames, get_records


class TestTransactions(PySparkTest):
    """Class will handle unittests regarding parsing the CSV to default dataframe, weekly, monthly, last friday
    transactions
    """
    def test_initial_default_dataframe(self):
        """Test method verifies the process to create the initial dataframe with a fake spark session.
        It is being created as the expected format. A snapshot of how the dataframe should be and is compared to.
        If any changes are made in create_df, or in any of the helper functions this test will fail.

        It checks that the column names, datatypes, and actual values are as expected. I rather do this approach
        than trying to unittest every single function/method, which can just make the test harder to read, this
        gives a nice overview, and if somebody tries to change the logic in the code when creating a transaction
        dataframe this test will crash and it will not be as expected.

        Args:
            self: containing spark session
        """
        _df = Transactions.create_df(self.spark)
        _correct_default_df = TestDataFrames.CORRECT_DEFAULT_DF
        _incorrect_default_df = TestDataFrames.INCORRECT_DEFAULT_DF

        print('Assert against a correctly defined default schema named dataframe')
        self.assertEqual(_df.schema.names, _correct_default_df.schema.names)
        self.assertEqual(_df.dtypes, _correct_default_df.dtypes)

        print('Assert against a correctly defines default schema names in dataframe')
        self.assertEqual(_df.schema.names, _incorrect_default_df.schema.names)

        print('Assert that it had correct schema default names but wrong datatypes')
        self.assertNotEqual(_df.dtypes, _incorrect_default_df.dtypes)

        for col in _correct_default_df.schema.names:
            print('Assert that records in column: ' + col + ' is matching default dataframe')
            _df_list = get_records(_df, col)
            _df_correct_list = get_records(_correct_default_df, col)
            self.assertEqual(_df_list, _df_correct_list)

            print('Assert that records in column: ' + col + ' is not matching default dataframe')
            _df_incorrect_list = get_records(_incorrect_default_df, col)
            self.assertNotEqual(_df_list, _df_incorrect_list)




