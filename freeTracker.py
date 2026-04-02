import os
import matplotlib.pyplot as plt
import matplotlib.animation as animation

from pyspark.sql import SparkSession
from flightPlot import update
from flightData import get_flight_data

os.environ["PYSPARK_PYTHON"] = r"C:\Users\Thoma\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\Thoma\AppData\Local\Programs\Python\Python311\python.exe"

fig, ax = plt.subplots(figsize=(14, 7))

spark = SparkSession.builder.appName("FlightData").getOrCreate()

# Retrieve flight data from API. We need to use lambda here as by default you only pass in the "Frame" but we need to pass in flight data too.
# By default the function won't do any spark transformations.
flight_data = get_flight_data()
flightMap = animation.FuncAnimation(fig, lambda x:update(fig, ax, flight_data), interval=10000)
plt.show()