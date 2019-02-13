# Overview

![Screenshot](/docs/images/tor.jpg)

Torsten is a hybrid between the god Thor and a stone, solid and reliable. Torsten uses PySpark to extract, clean and transform time frame sessions for bank transactions.
## Project Design
`Note: This project is for Linux environment`

The design of this project is that it should be fast and easy to deploy and run. Below is a description of how the application should be deployment ready.

![Screenshot](/docs/images/deployment.png)

Microservices is an architectural style that structures an application as a collection of loosely coupled services. Therefore enrich business capabilities, such as continuous delivery/deployment of scale.

## Data Processing

Apache Spark is the chosen tool used within this project. Spark is quick and very responsive tool to perform data processing with. It provides an analytic engine for large-scale data processing. It is a general distributed in-memory computing framework implemented in scala. Hence spark operates on distributed data collections. However it does not do distributed storage. Spark is proven to be much faster than the popular Apache Hadoop framework. Apache Spark and Hadoop have different purposes but works good together. A high level overview of Apache Spark below: 

![Screenshot](/docs/images/spark.png)

Hadoop is essentially a distributed data infrastructure. It provides distributes big data collections across multiple nodes within a cluster of commodity servers. A high level overview of Apache Hadoop below: 

![Screenshot](/docs/images/hadoop.png)

## Dataset
The dataset is comprising a person’s bank account

Docker needs to be installed on the machine. It can also run locally without docker.

## Setup

Configure:

* PYTHONPATH (Make sure the correct `VENV` is being used)
* SPARK_HOME 

#### Requirements

* Docker environment
* Python ^3
* Java ^8
* Spark ^2.3.2

Assuming that Python, Docker and Java is already setup.

### Testing

The project performs unittesting on each criteria for the created dataframes. It creates dataframe versions of the expected result, like a snapshot. To verify that the logic in the code works. If anything is changed in the code, this test will fail.

It specifically checks

* column names
* datatypes
* expected values in columns match

Run the following command to run tests:

```bash
python3 -m unittest src/tests/unit/app/<file-of-test-to-run>.py
```

The available files to test are

```bash
default_dataframe_test.py
weekly_dataframe_test.py
fortnightly_dataframe_test.py
last_friday_dataframe_test.py
monthly_dataframe_test.py
last_friday_monthly_dataframe.py
```

They are subclasses of the class `fake_spark_session.PySparkTest` which spins upp the `unittest.TestCase` with a fake `spark session`.

No mocking is currently taking place, further explained in the section of improvements.

### Manual Run

Run the script manually without using docker.

```bash
spark-submit src/main.py
```

### Run Project as Docker Container

When building and running the container, there is no need to configure anything to make the project run, this is all handled within the Dockerfile.

```bash
docker build -t tor-bank-transactions .
docker run tor-bank-transaction
```

Remember it will take som time the first time it is run. It will first run the unittests, and then the actual application.

## Result

* Transactions with same description, amount, and regularly-spaced dates
* Transactions with same description and amount, and dates that correspond to only the last
Friday of each month.
* Date regularity can be weekly, monthly, and fortnightly.

```bash
Creatingg the initial defaultcleaned dataframe for transaction, ready to be used for analytics
root
 |-- date: date (nullable = false)
 |-- description: string (nullable = true)
 |-- amount: double (nullable = true)
 |-- day_number: string (nullable = false)
 |-- weekday: string (nullable = false)

None
Printing Transformed weekly Transacations Dataframe Schema...
root
 |-- date: date (nullable = false)
 |-- description: string (nullable = true)
 |-- amount: double (nullable = true)
 |-- day_number: string (nullable = false)
 |-- weekday: string (nullable = false)
 |-- days_passed: integer (nullable = true)
 |-- weekly: boolean (nullable = true)

+----+-----------+------+----------+-------+-----------+------+
|date|description|amount|day_number|weekday|days_passed|weekly|
+----+-----------+------+----------+-------+-----------+------+
+----+-----------+------+----------+-------+-----------+------+

Saving to CSV...
Printing Transformed fortnightly Transacations Dataframe Schema...
root
 |-- date: date (nullable = false)
 |-- description: string (nullable = true)
 |-- amount: double (nullable = true)
 |-- day_number: string (nullable = false)
 |-- weekday: string (nullable = false)
 |-- days_passed: integer (nullable = true)
 |-- fortnightly: boolean (nullable = true)

+----------+-----------+------+----------+-------+-----------+-----------+
|      date|description|amount|day_number|weekday|days_passed|fortnightly|
+----------+-----------+------+----------+-------+-----------+-----------+
|2018-01-12|   COURSERA|  50.0|         5|    Fri|          0|      false|
|2018-01-26|   COURSERA|  50.0|         5|    Fri|         14|       true|
|2018-02-09|   COURSERA|  50.0|         5|    Fri|         14|       true|
|2018-02-23|   COURSERA|  50.0|         5|    Fri|         14|       true|
|2018-03-09|   COURSERA|  50.0|         5|    Fri|         14|       true|
|2018-03-23|   COURSERA|  50.0|         5|    Fri|         14|       true|
|2018-04-06|   COURSERA|  50.0|         5|    Fri|         14|       true|
|2018-04-20|   COURSERA|  50.0|         5|    Fri|         14|       true|
|2018-05-04|   COURSERA|  50.0|         5|    Fri|         14|       true|
|2018-05-18|   COURSERA|  50.0|         5|    Fri|         14|       true|
+----------+-----------+------+----------+-------+-----------+-----------+

Saving to CSV...
Printing Transformed monthly Transacations Dataframe Schema...
root
 |-- date: date (nullable = false)
 |-- description: string (nullable = true)
 |-- amount: double (nullable = true)
 |-- day_number: string (nullable = false)
 |-- weekday: string (nullable = false)
 |-- days_passed: integer (nullable = true)
 |-- monthly: boolean (nullable = true)

+----------+--------------------+--------+----------+-------+-----------+-------+
|      date|         description|  amount|day_number|weekday|days_passed|monthly|
+----------+--------------------+--------+----------+-------+-----------+-------+
|2018-01-26|            DATALABS| 2822.17|         5|    Fri|          0|  false|
|2018-02-23|            DATALABS| 2822.17|         5|    Fri|         28|   true|
|2018-03-30|            DATALABS| 2822.17|         5|    Fri|         35|   true|
|2018-04-27|            DATALABS| 2822.17|         5|    Fri|         28|   true|
|2018-05-25|            DATALABS| 2822.17|         5|    Fri|         28|   true|
|2018-06-29|            DATALABS| 2822.17|         5|    Fri|         35|   true|
|2018-01-17|       LANDLORD RENT|  1075.0|         3|    Wed|          0|  false|
|2018-02-18|       LANDLORD RENT|  1075.0|         7|    Sun|         32|   true|
|2018-03-17|       LANDLORD RENT|  1075.0|         6|    Sat|         27|   true|
|2018-04-17|       LANDLORD RENT|  1075.0|         2|    Tue|         31|   true|
|2018-05-16|       LANDLORD RENT|  1075.0|         3|    Wed|         29|   true|
|2018-06-17|       LANDLORD RENT|  1075.0|         7|    Sun|         32|   true|
|2018-01-27|    SYSTEMBOLAGET AB|200000.0|         6|    Sat|          0|  false|
|2018-02-27|    SYSTEMBOLAGET AB|200000.0|         2|    Tue|         31|   true|
|2018-01-18|UMBRELLA CORPORATION|  1075.0|         4|    Thu|          0|  false|
|2018-02-18|UMBRELLA CORPORATION|  1075.0|         7|    Sun|         31|   true|
|2018-03-17|UMBRELLA CORPORATION|  1075.0|         6|    Sat|         27|   true|
|2018-04-17|UMBRELLA CORPORATION|  1075.0|         2|    Tue|         31|   true|
|2018-05-19|UMBRELLA CORPORATION|  1075.0|         6|    Sat|         32|   true|
|2018-06-19|UMBRELLA CORPORATION|  1075.0|         2|    Tue|         31|   true|
+----------+--------------------+--------+----------+-------+-----------+-------+

Saving to CSV...
Printing Transformed monthly Transactions Occuring On The Last Friday Every Month...
root
 |-- date: date (nullable = false)
 |-- description: string (nullable = true)
 |-- amount: double (nullable = true)
 |-- day_number: string (nullable = false)
 |-- weekday: string (nullable = false)
 |-- days_passed: integer (nullable = true)
 |-- monthly: boolean (nullable = true)

+----------+-----------+-------+----------+-------+-----------+-------+
|      date|description| amount|day_number|weekday|days_passed|monthly|
+----------+-----------+-------+----------+-------+-----------+-------+
|2018-01-26|   DATALABS|2822.17|         5|    Fri|          0|  false|
|2018-02-23|   DATALABS|2822.17|         5|    Fri|         28|   true|
|2018-03-30|   DATALABS|2822.17|         5|    Fri|         35|   true|
|2018-04-27|   DATALABS|2822.17|         5|    Fri|         28|   true|
|2018-05-25|   DATALABS|2822.17|         5|    Fri|         28|   true|
|2018-06-29|   DATALABS|2822.17|         5|    Fri|         35|   true|
+----------+-----------+-------+----------+-------+-----------+-------+

Save to CSV...

Process finished with exit code 0
```

### Results

* No transactions with date regularity, same amount and description for weekly transactions

* Transactions with date regularity, same amount and description fortnightly transactions was found

* Transactions with date regularity, same amount and description monthly transactions was found

* Transactions with date regularity, same amount and description last friday of every month transactions was found

### Spark UI

To see the spark jobs process do the following while running the docker image:

```bash
docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' <container id>
```

To get container id:
```bash
docker ps
```

This will give the IP of the docker container, type following in the web browser:

`container-ip:4040`

This will give the following output:

![Screenshot](/docs/images/unittest.png)

---



## Author Info

* [Simon Thelin](https://www.linkedin.com/in/simon-thelin-3ba96986/)