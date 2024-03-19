# Add new column and cast to type in select keeping the current columns

from pyspark.sql.functions import col

string_missing_columns = []
numeric_missing_columns = []

df = df.select(*(col(c).cast("double").alias(c) for c in subset),*[x for x in df.columns if x not in subset])

df = df.select(['*'] + [ lit(' ').alias(col) if col not in df.schema.names else None for col in subset_of_columns_to_add ] )

df = df.select(['*'] +  [ lit(" ").alias(col) for col in string_missing_columns if col not in df.schema.names ] + [lit('0').cast(DecimalType(38,18)).alias(col)  for col in numeric_missing_columns  if col not in df.schema.names ])
