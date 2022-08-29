import os
import shutil
import sys
import re
from datetime import datetime
import datetime
from random import randint
import MySQLdb
import pandas as pd
import distutils.dir_util
from csv import writer
from csv import reader

# ----------------------------------------------------------------------------
def pev(s, num=1):
    ele = (s.split("\\\\"))
    res = ""
    for i in ele[:-num]:
        val = i.replace('\'', "")
        res += val + "\\\\"
    return res

# Path
argument_path = repr(sys.argv[0])
file_path = repr(__file__)
folder_path = pev(file_path)
call_path = repr(os.getcwd())
secret_path = pev(file_path, 3) + "cache\\secret.txt"
path_path = pev(file_path, 3) + "cache\\path.txt"


f = open(str(secret_path))
data = f.read()
user = data.split('\n')[0]
password = data.split('\n')[1]
host = data.split('\n')[2]
database = data.split('\n')[3]


mydb = MySQLdb.connect(host=host,
    user=user,
    passwd=password,
    db=database)
cursor = mydb.cursor()


f2 = open(str(path_path))
data2 = f2.read()
path = str(data2.split('\n')[0])
store_path = str(data2.split('\n')[1])

# ----------------------------------------------------------------------------

current_path = os.getcwd() + '\\\\'

def remove(path):
    """ param <path> could either be relative or absolute. """
    if os.path.isfile(path) or os.path.islink(path):
        os.remove(path)  # remove the file
    elif os.path.isdir(path):
        shutil.rmtree(path)  # remove dir and all contains
    else:
        raise ValueError("file {} is not a file or dir.".format(path))

def copy(src, dest):
    return distutils.dir_util.copy_tree(src, dest)

def all_files(directory: str):
    try:
        string = ""
        # create a list of file and subdirectories
        # names in the given directory
        listOfFile = os.listdir(directory)
        allFiles = list()
        # Iterate over all the entries
        for entry in listOfFile:
            # Create full path
            fullPath = os.path.join(directory, entry)
            # If entry is a directory then get the list of files in this directory
            if os.path.isdir(fullPath):
                allFiles = allFiles + all_files(fullPath)
            else:
                allFiles.append(fullPath)

        return allFiles
    except:
        return ""

def slugify(strng):
    '''create a slug from a free form string'''
    if strng:
        strng = strng.strip()
        strng = re.sub('..', '', strng)
        strng = re.sub('\s+', '_', strng)
        strng = re.sub('[^\w.-]', '', strng)
        return strng.strip('_.- ').lower()

def load_DataTrain(filename, num):
    try:
        with open(filename, newline='') as iris:
            # returning from 2nd row
            return list(reader(iris, delimiter=','))[num]
    except FileNotFoundError as e:
        raise e

def create_table(file_name, tbl_name=None):
    header = load_DataTrain(file_name, 0)
    try:
        first_row = load_DataTrain(file_name, 1)
        command = '''CREATE TABLE IF NOT EXISTS {} ( '''.format(tbl_name)
        if (not ("id" in header or "ID" in header or "Id" in header or "iD" in header)):
            # command += "ID INT AUTO_INCREMENT PRIMARY KEY, "
            count = 0
            # Open the input_file in read mode and output_file in write mode
            with open(file_name, 'r') as read_obj, \
                    open(file_name + '_1.csv', 'w', newline='') as write_obj:
                csv_reader = reader(read_obj)
                csv_writer = writer(write_obj)
                for row in csv_reader:
                    if count == 0:
                        row.append("ID")
                        csv_writer.writerow(row)
                    else:
                        row.append(str(count))
                        csv_writer.writerow(row)
                    count += 1
            remove(file_name)
            os.rename(file_name + '_1.csv', file_name)


        i = 0
        for part in first_row:
            width = len(part)
            typ = 'TEXT'
            if (header[i] == 'id' or header[i] == 'ID' or header[i] == 'Id' or header[i] == 'iD'):
                typ = 'INT AUTO_INCREMENT PRIMARY KEY'
            else:
                try:
                    datetime.datetime.strptime(str(part), '%Y-%m-%d')
                    typ = 'DATE'
                except:
                    try:
                        int(part)
                        typ = 'INT'
                    except:
                        try:
                            float(part)
                            typ = 'REAL'
                        except:
                            pass
            command += str(header[i]) + " " + str(typ) + ", "
            i += 1

        command = command[:-2] + ");"
        cursor.execute(command)
        print(command)
    except:
        None

def populate_table(file_name, tbl_name):
    data = pd.read_csv(file_name)
    pattern = "([0-3]?[0-9]/[0-1]?[0-9]/([0-9][0-9])?[0-9][1-9])"

    # creating column list for insertion
    cols = "`,`".join([str(i) for i in data.columns.tolist()])

    # Insert DataFrame records one by one.
    for i, row in data.iterrows():
        try:
            sql = "INSERT INTO `" + tbl_name + "` (`" + cols + "`) VALUES " + str(tuple(row)) + ";"
            sql = sql.replace('nan', "NULL")
            dates = [i[0] for i in re.findall(pattern, sql)]
            for i in range(0, len(dates)):
                sql = sql.replace(dates[i], str(datetime.datetime.strptime(dates[i], "%m/%d/%Y").strftime("%Y-%m-%d")))
            cursor.execute(sql)
            print(sql)
        except:
            None

def main():
    # copy(store_path, path)
    if len(sys.argv) > 1:
        file_name = path + '\\\\' + str(sys.argv[1]) + ".csv"
        tbl_name = file_name.split("\\")[-1].split('.')[0].strip()

        create_table(file_name, tbl_name)
        populate_table(file_name, tbl_name)

    else:
        file_names = all_files(path + '\\\\')
        for file_name in file_names:
            tbl_name = file_name.split("\\")[-1].split('.')[0].strip()
            print(tbl_name)
            create_table(file_name, tbl_name)
            populate_table(file_name, tbl_name)

main()