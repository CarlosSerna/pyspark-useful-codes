
#View file properties
##Returns file properties including file name, file path, file size, file modification time, and whether it is a directory and a file.

files = mssparkutils.fs.ls('Your directory path')
for file in files:
    print(file.name, file.isDir, file.isFile, file.path, file.size, file.modifyTime)

