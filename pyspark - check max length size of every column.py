
# CHECK MAX LENGTH SIZE OF EVERY COLUMN

df1 = UNION_DF.select([length(col).alias(col) for col in UNION_DF.columns])
display(df1.groupby().max())


# CHECK MAX LENGTH SIZE OF EVERY COLUMN in SINGLE STEP

from pyspark.sql.functions import col, length, max

df=df.select([max(length(col(name))).alias(name) for name in df.schema.names])

# CHECK MAX LENGTH SIZE OF EVERY COLUMN in SINGLE STEP SHOW AS ROWS

df = df.select([max(length(col(name))).alias(name) for name in df.schema.names])
row=df.first().asDict()
df2 = spark.createDataFrame([Row(col=name, length=row[name]) for name in df.schema.names], ['col', 'length'])
