# Databricks notebook source
# MAGIC %md
# MAGIC # Using Lambda in Databricks - Case 2
# MAGIC
# MAGIC This notebooks gives a slightly complex example of using lambda in Databricks. For more information see this article (add link).
# MAGIC
# MAGIC Author: Morten Mjelde <morten.mjelde@bouvet.no>

# COMMAND ----------

# Define DataSet3
from pyspark.sql import Row

DataSet3 = spark.createDataFrame(
    [
        Row(
            location="Oslo",
            people=[
                Row(firstName="Harald", lastName="Haraldsen"),
                Row(firstName="Mark", lastName="Hunter"),
            ],
        ),
        Row(location="Stockholm", people=[Row(firstName="Ingrid", lastName="Hansen")]),
    ]
)

# COMMAND ----------

# Define DataSet4
from pyspark.sql import Row

DataSet4 = spark.createDataFrame(
    [
        Row(
            location="Copenhagen",
            people=[
                Row(firstName="Roger", nobleName="of house Henriksen"),
                Row(firstName="Hans", nobleName="of house Dale"),
            ],
        ),
    ]
)

# COMMAND ----------

# Define transformations
from pyspark.sql.functions import DataFrame, col, concat, explode, lit


def explodePeople(df: DataFrame) -> DataFrame:
    """Explode people column."""
    return df.withColumn("expl_people", explode("people")).select(
        "location", "expl_people.*"
    )


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
    data = explodePeople(data)
    data = correctNameColumns(data)
    data = addInitials(data)
    data = addfullName(data)
    return data


# COMMAND ----------

# Execute transformations on DataSet3
result3 = nameTransformation(data=DataSet3)

display(result3)

# COMMAND ----------

# Execute transformations on DataSet4, which includes the lambda.
from pyspark.sql.functions import element_at, split

result4 = nameTransformation(
    data=DataSet4,
    correctNameColumns=lambda df: df.withColumn(
        "lastName", element_at(split(col("nobleName"), " "), -1)
    ),
)

display(result4)

# COMMAND ----------
