#! /usr/local/bin/python3
'''
mblue:
This is a demo for myself on reading a text file, converting and writing in
different formats dynamicly creating a schema.
Original Personal Exercise is to take a text file with key points and a description of 
a mini CV for fun and create different formats. 
'''
import json
import argparse
import sys
import pandas as pd



# Passing in the file
parser = argparse.ArgumentParser()

parser.add_argument('--upload', '-u', help='Upload text file',\
     nargs='*', dest='file_name')

args = (parser.parse_args())


# Usage if script is ran without an argument

if args.file_name is None:
    print(" Usage is --upload textfile.tx")
    sys.exit()

else:
    if args.file_name:
        FILE_NAME = ' '.join(args.file_name)
        PNAME = FILE_NAME.replace('.txt', '')
        J_FILEN = PNAME + ('.json')
        J2_FILEN = PNAME + ('_load.json')  # To loadable. 
        P_FILEN = PNAME + ('.parquet')
        SQL_FILEN = PNAME + ('.sql')

# creating dictionary
mini_data = {}

with open(FILE_NAME, encoding="utf-8") as jd:

    for line in jd:
        command, description = line.strip().split(None, 1)
        mini_data[command] = description.strip()

out_file = open(J_FILEN, "w", encoding="utf-8")
json.dump(mini_data, out_file, sort_keys=False)
out_file.close()

out_file = open(J2_FILEN, "w", encoding="utf-8")
json.dump(mini_data, out_file, sort_keys=False)
out_file.close()


# This seems to wrong but it works and needed for other
# Programs to read the json file.

with open(J_FILEN, 'r', encoding="utf-8") as json_file:
    mini_cv = json.load(json_file)
    jfile = json.dumps(mini_cv, indent=2)

print(f"\nConverting {PNAME} from Text to JSON File: {J_FILEN} \n\n", jfile, "\n")

with open(J_FILEN, 'w', encoding="utf-8") as new_json:
    new_json.write("[\n")
    new_json.write(jfile)

with open(J_FILEN, 'a', encoding="utf-8") as new_json:
    new_json.write("\n]")

with open(J_FILEN, 'r', encoding="utf-8") as json_file:
    mini_cv = json.load(json_file)
    jfile = json.dumps(mini_cv, indent=2)



# Pandas

def print_full(dis_set):
    '''Pandas Setting Display'''
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', 4)
    pd.set_option('display.width', 2000)
    pd.set_option('display.float_format', '{:20,.2f}'.format)
    pd.set_option('display.max_colwidth', None)
    print(dis_set)
    pd.reset_option('display.max_rows')
    pd.reset_option('display.max_columns')
    pd.reset_option('display.width')
    pd.reset_option('display.float_format')
    pd.reset_option('display.max_colwidth')

# Converting File to parq


print("Convert JSON to Parquet:\nCreated File: '",P_FILEN,"' ...")

convert_data = pd.read_json(J_FILEN)
convert_data.to_parquet(P_FILEN)

# Reading Parquet file as a DataFrame
print("Reading Parquet '",P_FILEN,"' as a Pandas DataFrame Summmary:\n")
df = pd.read_parquet(P_FILEN, engine='pyarrow')
print_full(df)


# SQL portion prints and writes sql equilivant.
# Yes most modern RDBs have JSON support but thats
# not the purpose of this exercise.

TABLE_NAME = PNAME
SQL_STATEMENT = ''
SQL_SCHEMA = ''
with open(J_FILEN, 'r',  encoding="utf-8") as f:
    jsondata = json.loads(f.read())

UNICODE = "\u00f8"

for json in jsondata:
    KEY_LIST = "("
    KEY_LIST2 = ""
    VALUE_LIST = "("
    FIRST_PAIR = True
    for key, value in json.items():
        if not FIRST_PAIR:
            KEY_LIST += ", \n    "
            KEY_LIST2 += " varchar(250) DEFAULT NULL,\n   "
            VALUE_LIST += ", \n    "
        FIRST_PAIR = False
        KEY_LIST += key
        KEY_LIST2 += key
        if type(value) in (str, UNICODE):
            VALUE_LIST += "'" + value + "'"
        else:
            VALUE_LIST += str(value)
    KEY_LIST += ")"
    VALUE_LIST += ");"

    SQL_STATEMENT += "\nINSERT INTO " + TABLE_NAME + "\n  \
" + KEY_LIST + "\n VALUES " + VALUE_LIST + "\n"

    SQL_SCHEMA += "CREATE TABLE IF NOT EXISTS " + "`" + TABLE_NAME + "` (\n" +\
"   `Id` MEDIUMINT NOT NULL AUTO_INCREMENT," + KEY_LIST2 + " \
varchar(250) DEFAULT NULL,   \n   PRIMARY KEY (id)\n" + ");\n"

with open(SQL_FILEN, 'w', encoding="utf-8") as sql_file:
    sql_file.write(SQL_SCHEMA)
    sql_file.write(SQL_STATEMENT)

print("\nConvert Json to SQL:\nCreated File:", SQL_FILEN,"\n")
print(SQL_SCHEMA)
print(SQL_STATEMENT)
