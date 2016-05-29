"""
A RESTful flask api for PTTT database
"""

from flask import Flask, make_response
from flask_restful import Resource, Api
from pymysql import cursors
from decimal import Decimal

import re
import sys
import json
import pymysql
import sqlite3

app = Flask(__name__)
api = Api(app)

api_version_1 = '/pttt/v1.1/'

class HelloWorldResource(Resource):
  def get(self):
    return { 'hello': 'world' }

class DrugResource(Resource):
  def get(self, drugName):
    drugInfo = self.getDrugInfo(drugName)
    # Retrieve the keyword argument
    print len(drugInfo)
    strResult = ""
    for index in range(len(drugInfo)):
      row = drugInfo[index]
      # strResult += row['field1'] + '|' + \
      # row['drugstore'] + '|' + \
      # str(row['selling_price']) + '||'
      strResult += row[0] + " " +  \
          row[2] + "\n"
          # row[1] + "|" + \  # This is the pharmacist data
    # To be able to set response header Content-Type to text/plain
    response = make_response(strResult)
    response.headers["Content-Type"] = "text/plain"
    return response

  def regexp(self, expr, item):
    try:
      reg = re.compile(expr, re.I)
    except:
      print "Regexp Error occurred:"
    return reg.search(item) is not None

  def getDrugInfo(self, drugName):
    # Database connection
    # connection = pymysql.connect(host='localhost',
    #                              user='root',
    #                              password='0jonats',
    #                              db='ptttdb',
    #                              charset='utf8',
    #                              cursorclass=pymysql.cursors.DictCursor)
    connection = sqlite3.connect('data/ptttdb.sqlite3')
    result = []

    try:
      # from
      # http://stackoverflow.com/questions/5365451/problem-with-regexp-python-and-sqlite
      connection.create_function("REGEXP", 2, self.regexp)
      # 'with' -
      # http://preshing.com/20110920/the-python-with-statement-by-example/
      # with connection.cursor() as cursor:
      cursor = connection.cursor()
      print 'searching for', drugName
      # sql = 'SELECT `field1`, `drugstore`, `selling_price` FROM `tbl_calapan_mst` WHERE `field1` REGEXP `%s`'
      sql = 'SELECT field1, drugstore, selling_price FROM tbl_calapan_mst WHERE field1 REGEXP (?)'
      cursor.execute(sql, (drugName,))
      result = cursor.fetchall()
      print "Resut:", result
      print type(result)
    except sqlite3.Error as e:
      # do nothing
      print 'getDrugInfo Error occurred:', e.args[0]
      pass
    finally:
      connection.close()

    return result

# helper class to decode decimal from mySQL
class DecimalEncoder(json.JSONEncoder):
  def default(self, o):
    if isinstance(o, Decimal):
      return float(o)
    return super(DecimalEncoder, self).default(o)


api.add_resource(HelloWorldResource, api_version_1)
api.add_resource(DrugResource, api_version_1 + '<string:drugName>')

if __name__ == '__main__':
  app.run(debug=True)
