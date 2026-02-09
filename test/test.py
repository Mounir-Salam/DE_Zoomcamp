import datetime

year = datetime.datetime.now().year - 1
print(year)
print(isinstance(year, int))

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{}-{}.parquet"

url = BASE_URL.format(2021, 1)
print(url)

url = BASE_URL.format(year, 1)
print(url)