# function to write and rename files in CDL

def write_parquet_file(output_sp_df,TEMP_OUTPUT_PATH,OUTPUT_PATH,OUTPUT_FILE):
    TempFilePath = TEMP_OUTPUT_PATH
    output_sp_df.coalesce(1).write\
           .mode("overwrite")\
           .option("header", "true")\
           .parquet(TempFilePath)\
    
    # Now read file from temp location write it to new location with new name and delete temp directory
    readPath = TEMP_OUTPUT_PATH
    writePath = OUTPUT_PATH
    read_name = None
    file_list = dbutils.fs.ls(readPath) # List out all files in temp directory
    
    for i in file_list:
      if (i[1].startswith("part-00000")) and (i[1].endswith('.parquet')):read_name = i[1] #### find your temp file name
      
    # Move it outside to the new_dir folder and rename
    dbutils.fs.mv(readPath+"/"+read_name, writePath+"/"+OUTPUT_FILE)
  
    # Remove the temp folder
    dbutils.fs.rm(readPath, recurse = True)
