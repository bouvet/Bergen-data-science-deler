# Databricks notebook source
# MAGIC %md
# MAGIC # Using Lambda in Databricks - Case 1
# MAGIC
# MAGIC This notebooks gives a simple example of using lambda in Databricks. For more information see this article (add link).
# MAGIC
# MAGIC Author: Morten Mjelde <morten.mjelde@bouvet.no>

# COMMAND ----------

# Define DataSet1
from pyspark.sql import Row

dataSet1 = spark.createDataFrame(
    [
        Row(location="Oslo", firstName="Harald", lastName="Haraldson"),
        Row(location="Stockholm", firstName="Ingrid", lastName="Hansen"),
        Row(location="Oslo", firstName="Mark", lastName="Hunter"),
    ]
)

# COMMAND ----------

# Define DataSet2
from pyspark.sql import Row

dataSet2 = spark.createDataFrame(
    [
        Row(location="Oslo", firstName="Roger", nobleName="of house Henriksen"),
        Row(location="Oslo", firstName="Hans", nobleName="of house Dale"),
    ]
)

# COMMAND ----------

# Define transformations
from pyspark.sql.functions import DataFrame, col, concat, lit


def addInitials(df: DataFrame) -> DataFrame:
    """Add initials column."""
    df_new = df.withColumn(
        "intials", concat(col("firstName").substr(0, 1), col("lastName").substr(0, 1))
    )
    return df_new


def addfullName(df: DataFrame) -> DataFrame:
    """Add full name column."""
    return df.withColumn(
        "fullName", concat(col("firstName"), lit(" "), col("lastName"))
    )


def nameTransformation(
    data: DataFrame, correctNameColumns: callable = lambda df: df
) -> DataFrame:
    """Perform all transformations."""
    data = correctNameColumns(data)
    data = addInitials(data)
    data = addfullName(data)
    return data


# COMMAND ----------

# Execute transformations on DataSet1
result1 = nameTransformation(data=dataSet1)

display(result1)

# COMMAND ----------

# Execute transformations on DataSet2, which includes the lambda.
from pyspark.sql.functions import element_at, split

result2 = nameTransformation(
    data=dataSet2,
    correctNameColumns=lambda df: df.withColumn(
        "lastName", element_at(split(col("nobleName"), " "), -1)
    ),
)

display(result2)

# COMMAND ----------
