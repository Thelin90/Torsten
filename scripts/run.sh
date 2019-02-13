#!/usr/bin/env bash

# install requirements file
python3.6 -m pip install -r scripts/requirements.txt
# run tests
python3 -W ignore:ResourceWarning -m unittest src/tests/unit/app/default_dataframe_test.py
python3 -W ignore:ResourceWarning -m unittest src/tests/unit/app/fortnightly_dataframe_test.py
python3 -W ignore:ResourceWarning -m unittest src/tests/unit/app/weekly_dataframe_test.py
python3 -W ignore:ResourceWarning -m unittest src/tests/unit/app/monthly_dataframe_test.py
python3 -W ignore:ResourceWarning -m unittest src/tests/unit/app/last_friday_dataframe_test.py
# run application
spark-submit src/main.py
