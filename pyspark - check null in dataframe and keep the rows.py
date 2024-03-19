# Function to check all the dataframe and keep the rows with nulls in

from pyspark.sql.functions import col
from functools import reduce

def select_rows_with_nulls(from_df):
	return from_df.where(
		reduce(
			lambda col1, col2: col1 | col2, 
			[col(col_name).isNull() for col_name in from_df.columns]
		)
	)
	
df_with_nulls = select_rows_with_nulls(from_df = df)
