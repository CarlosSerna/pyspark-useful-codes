# Add columns to a dataframe based on other dataframe column schema
## Add the missing columns to the dataframe df_2 based on the schema of df_1

### Example 1

df_2 = df_2.select([df_2[col] if col in df_2.schema.names else lit(None).alias(col) for col in df_1.schema.names])

### Example 2
for columnas in df_1.columns:
  if columnas not in df_2.columns:
    df_2 =  df_2.withColumn(columnas, lit(None))

### Example 3

df_2 = df_2.select( [lit(None).alias(col) if col not in df_2.columns else df_2[col] for col in df_1.columns ] )
