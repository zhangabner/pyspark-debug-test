from pyspark import SparkFiles
from pyspark.sql import Row, SparkSession, DataFrame
from pyspark.sql.types import StringType
from pyspark import SQLContext
from pyspark import SparkContext
from pyspark.sql import SQLContext
import pandas as pd

sc = SparkContext('local','example')  # if using locally
sql_sc = SQLContext(sc)

  
csvfile = sc.textFile("people.csv")
splitfile= csvfile.map(lambda d: d.split(','))
upfile=splitfile.map(lambda d: d[0].upper())
filterfile=upfile.filter(lambda d: d == 'ab')

data = (sc.textFile('/path/to/data')
        .map(lambda d: d.split(','))
        .map(lambda d: d[0].upper())
        .filter(lambda d: d == 'THING'))

df = filterfile.toDF(['first_name', 'last_name', 'birth_date'])

df.show()


def parse_line(line):
    parts = line.split(',')
    return parts[0].upper()

def is_valid_thing(thing):
    return thing == 'ab'

data = (sc.textFile("people.csv")
        .map(parse_line)
        .filter(is_valid_thing))

Employee_df = data.toDF(['first_name', 'last_name', 'birth_date'])

Employee_df.show()

class Person(object):

    __slots__ = ('first_name', 'last_name', 'birth_date')

    def __init__(first_name, last_name, birth_date):
        self.first_name = first_name
        self.last_name = last_name
        self.birth_date = birth_date

    @classmethod
    def from_csv_line(cls, line):
        parts = line.split(',')
        if len(parts) != 3:
            raise Exception('Bad line')

        return cls(*parts)


def is_valid_person(person):
    return person.first_name is not None and person.last_name is not None


data = (sc.textFile("people.csv")
        .map(Person.from_csv_line)
        .filter(is_valid_person))
Employee_df = data.toDF(['first_name', 'last_name', 'birth_date'])

Employee_df.show()
