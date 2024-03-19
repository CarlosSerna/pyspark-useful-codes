--------------------------------------------------------------------
--------------------------------------------------------------------
#RENAME DATAFRAME COLUMNS BASED ON DICTIONARY


rename_dict = {
  'FName':'FirstName',
  'LName':'LastName',
  'DOB':'BirthDate'
}

df_renamed = df_initial \
.select([col(c).alias(rename_dict.get(c, c)) for c in df_initial.columns])

----------------------------------------------------------------------
----------------------------------------------------------------------

from pyspark.sql import DataFrame

def transform(self, f):
    return f(self)

DataFrame.transform = transform


def renameColumns(df):

    
     rename_dict = {
       'FName':'FirstName',
       'LName':'LastName',
       'DOB':'BirthDate'
        }

     return df.select([col(c).alias(rename_dict.get(c, c)) for c in df.columns])


df_renamed = spark.read.load('/mnt/datalake/bronze/testData') \
.transform(renameColumns)
