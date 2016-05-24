"""
A RESTful flask api for PTTT database
"""

from flask import Flask
from flask_restful import Resource, Api
from pymysql import cursors
from decimal import Decimal

import json
import pymysql

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
    strResult = ''
    for index in range(len(drugInfo)):
      row = drugInfo[index]
      strResult += row['field1'] + '|' + \
      row['drugstore'] + '|' + \
      str(row['selling_price']) + '||'
      # for k, v in drugInfo[index]:
      #   row += v + ' '
      #   row += '\n'
      #   strResult += row
    return strResult
    # return {}

  def getDrugInfo(self, drugName):
    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 password='0jonats',
                                 db='ptttdb',
                                 charset='utf8',
                                 cursorclass=pymysql.cursors.DictCursor)
    result = []

    try:
      # 'with' -
      # http://preshing.com/20110920/the-python-with-statement-by-example/
      with connection.cursor() as cursor:
        print 'searching for', drugName
        # sql = 'SELECT `field1`, `drugstore`, `selling_price` FROM `tbl_calapan_mst` WHERE `field1` REGEXP `%s`'
        sql = 'SELECT field1, drugstore, selling_price FROM tbl_calapan_mst WHERE field1 REGEXP %s'
        cursor.execute(sql, (drugName))
        result = cursor.fetchall()
        print result
        print type(result)
    except:
      # do nothing
      print 'Error occurred'
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
