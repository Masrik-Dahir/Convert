import csv
import os
import mysql.connector
import sys
import distutils.dir_util

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
stock_path = pev(file_path, 3) + "cache\\stock.txt"
csv_path = pev(file_path, 3) + "Excel"

f = open(str(secret_path))
data = f.read()
user = data.split('\n')[0]
password = data.split('\n')[1]
host = data.split('\n')[2]
database = data.split('\n')[3]
db = mysql.connector.connect(
    user=user, password=password, host=host, database=database)


f2 = open(str(path_path))
data2 = f2.read()
path = str(data2.split('\n')[0])
store_path = str(data2.split('\n')[1])

current_path = os.getcwd() + '\\\\'
desired_path = path + '\\\\'

def copy(src, dest):
    return distutils.dir_util.copy_tree(src, dest)

def main():
    if len(sys.argv) == 1:
        cursor = db.cursor()
        cursor.execute("Show tables;")
        myresult = cursor.fetchall()

        tables = []
        for x in myresult:
            tables.append(x[0])

        cursor2 = db.cursor(buffered=True)
        for i in tables:
            try:
                table_name = str(i)
                sql = "select * from `{}`;".format(table_name)
                cursor2.execute(sql)
                with open(desired_path + str(i)+ ".csv", "w", newline='') as csv_file:
                    csv_writer = csv.writer(csv_file)
                    csv_writer.writerow([i[0] for i in cursor2.description])
                    csv_writer.writerows(cursor2)
                print("Downloaded --> " + desired_path + str(i) + ".csv")
            except:
                print("Can't Downloaded --> " + desired_path + str(i) + ".csv")
                None
    else:
        cursor = db.cursor()
        cursor.execute("Show tables;")
        myresult = cursor.fetchall()

        tables = []
        for x in myresult:
            tables.append(x[0])
        cursor2 = db.cursor(buffered=True)
        try:
            table_name = str(sys.argv[1])
            cursor2.execute("select * from `{}`;".format(table_name))
            with open(desired_path + table_name + ".csv", "w", newline='') as csv_file:
                csv_writer = csv.writer(csv_file)
                csv_writer.writerow([i[0] for i in cursor2.description])
                csv_writer.writerows(cursor2)
            print("Downloaded --> " + desired_path + str(table_name) + ".csv")
        except:
            print("Can't Downloaded --> " + desired_path + str(table_name) + ".csv")
            None

    copy(path, store_path)
main()