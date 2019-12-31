import pyodbc
from django.conf import settings

DATABASE = settings.PHG_DATABASE

DATABASE['ENGINE'] = pyodbc