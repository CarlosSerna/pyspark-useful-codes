
# Sort columns alphabetically
## Sort the columns alphabetically for df_1
## Create a key list
## Filter sorted elements un df_1 not in key
## Combine Key list and sorted elements

### Example 1

sort = sorted(df_1.columns,reverse=False)
print(sort)
key = ['DATE','MSPN_NBR','COUNTRY_CODE','BT_CUST_CATEG_CODE','BT_PROGRAM_2']
sorted_elements = filter(lambda i: i not in key, sort)
key.extend(sorted_elements)
print(key)

df = df.select(*key)

### Example 2

def reorder_columns(on_df, first_cols: List[str]):
	remaining_columns = [column for column in on_df.columns if column not in first_cols]
	new_column_order = first_cols + remaining_columns 
	return on_df.select(*new_column_order)

    # Example use:

df = reorder_columns(on_df=df, first_cols=["first", "second", "third"])

