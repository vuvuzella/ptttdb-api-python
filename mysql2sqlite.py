"""
converts a table to a single database in sqlite3
Arguments:
  -d <database.sql> - the name of the database (should be in the current
  working directory)
  [-t <table>] - The name of the table to convert/create/recreate

1. Open ptttdb.sql file
2. Read each line until 'Table structure for tbl_calapan_mst' string is found
3. Set sql to empty string
3. Read line until 'Table structure for 'x' is found.'
  3.1 Append the line to sql
  3.2 if ';' is found in the line, then:
    3.2.1. Execute the statement in sql
    3.2.2. Set sql to empty string
"""
from pymysql import cursors

import re
import os
import sys
import getopt
import sqlite3
import pymysql

help_string = """Unrecognized arguments.
-d <database filename> - to set the databse filename
-t <table name> - to set the table to be converted
-h - to print this help file"""

def openDb(fileDir):
  """
  Returns the file descriptor if fileDir is found.
  else, exit the function
  """
  dbFileDesc = None
  if os.path.isfile(fileDir):
    # Open the file
    dbFileDesc = open(fileDir, "rb")
  else:
    # File does not exist
    # exit with error
    print 'File open error'
    print fileDir
    # sys.exit(-1)

  if dbFileDesc is not None:
    print "File opened successfully"
    # dbFileDesc.close()
  else:
    print "dbFileDesc is Empty"
    print "This code should not be reached"
  return dbFileDesc

def seekToStartLine(dbFileDesc, tableName):
  """
  Seek to the starting line of the given tableName
  Returns 1 and sets the dbFileDesc set to the starting line if tableName is found
  else Resets dbFileDesc and returns -1
  """
  # readLine = dbFileDesc.readline()
  line = 1
  regExSearch = re.compile(r"^.*" + tableName + r".$", re.I)
  print "Searching for", tableName

  for readLine in dbFileDesc:
    print line
    if regExSearch.search("-- Table structure 'tbl_calapan_mst'\n") != None:
      print readLine
      print line
      return 1
    line += 1

  # while readLine is not "":
  #   print line
  #   # if readLine.find(tableName) >= 0:
  #   if regExSearch.search(readLine, re.I) != None:
  #     print readLine
  #     print line
  #     return 1
  #   readLine = dbFileDesc.readline()
  #   line += 1
  print tableName, "is not Found"
  dbFileDesc.seek(0)
  return -1

def main(args):
  # Initial set-ups
  cwd = os.getcwd()
  fileDir = cwd + "/"
  tableName = ""

  # Get current working directory, without end forward-slash
  try:
    options = getopt.getopt(args, "hd:t:", ["database=","table="])
  except GetoptError:
    print help_string
    sys.exit(-1)

  # Process arguments provided
  for opt, arg in options[0]:
    if opt in ("-d", "--database",):
      # Set filedir
      fileDir += arg
      pass
    elif opt in ("-t", "--table",):
      # Set table name
      tableName = arg
      pass
    else:
      # unrecognized options. print h.
      print help_string
      print "opt:", opt
      print "arg:", arg
      # sys.exit(-1)

  # Open File if it exists
  dbFileDesc = openDb(fileDir)
  if seekToStartLine(dbFileDesc, tableName) > 0:
    # tableName is found
    print "table is found"
    # print dbFileDesc.readline()
  else:
    print "table is not found"
    # print dbFileDesc.readline()

  dbFileDesc.close()


if __name__ == '__main__':
  main(sys.argv[1:])
