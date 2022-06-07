import pymssql
from django.conf import settings

DATABASE = settings.PHG_DATABASE

DATABASE['ENGINE'] = pymssql