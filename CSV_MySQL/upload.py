import os
import sys
import re
from datetime import datetime
import datetime
from random import randint
from csv import reader
import MySQLdb
import pandas as pd
import distutils.dir_util

# ----------------------------------------------------------------------------
f = open(str(os.getcwd()) + '\\secret.txt')
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


f2 = open(str(os.getcwd()) + '\\path.txt')
data2 = f2.read()
path = str(data2.split('\n')[0])
store_path = str(data2.split('\n')[1])

# ----------------------------------------------------------------------------

current_path = os.getcwd() + '\\\\'
TEMPLATE = '''CREATE TABLE IF NOT EXISTS %(tbl_name)s (%(colnames)s);'''

def copy(src, dest):
    return distutils.dir_util.copy_tree(src, dest)

def all_files(directory: str):
    try:
        string = ""
        # create a list of file and sub directories
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

        types = []
        i = 0

        for part in first_row:
            # print(str(i), part)
            width = len(part)
            typ = 'TEXT'
            if (header[i] == 'id' or header[i] == 'ID' or header[i] == 'Id' or header[i] == 'iD'):
                typ = 'INT AUTO_INCREMENT PRIMARY KEY'
            else:
                try:
                    datetime.strptime(str(part), '%Y-%m-%d')
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

            types.append(' `%s` %s\n' % (slugify(header[i]), typ))
            i += 1

        colnames = ','.join(types)
        tbl_name = '`' + slugify(tbl_name) + '`'

        if not tbl_name:
            try:
                fpart = file_name.split('/')[-1].split('.')[0]
                tbl_name = '%s_%s' % (fpart, randint(1, 100))
            except:
                tbl_name = 'tbl_%s' % randint(1, 100)

        cursor.execute(str(TEMPLATE % locals()))
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
            # print(sql)
            cursor.execute(sql)
        except:
            None

def main():
    if len(sys.argv) > 1:
        file_name = path + '\\\\' + str(sys.argv[1]) + ".csv"
        tbl_name = file_name.split("\\")[-1].split('.')[0].strip()

        create_table(file_name, tbl_name)
        populate_table(file_name, tbl_name)

    else:
        file_names = all_files(path + '\\\\')
        for file_name in file_names:
            tbl_name = file_name.split("\\")[-1].split('.')[0].strip()

            create_table(file_name, tbl_name)
            populate_table(file_name, tbl_name)

    copy(path, store_path)

main()