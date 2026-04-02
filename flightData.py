from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from pyspark.sql.functions import col, when

import requests, json

schema = StructType([
    StructField("icao24",      StringType(),    True),
    StructField("callsign",       StringType(),    True),
    StructField("origin_country",     StringType(),    True),
    StructField("time_position",          StringType(),    True),
    StructField("last_contact",          StringType(),    True),
    StructField("longitude",        StringType(),   True),
    StructField("latitude",          StringType(),   True),
    StructField("baro_altitude",       StringType(),   True),
    StructField("on_ground",       BooleanType(),   True),
    StructField("velocity",       StringType(),   True),
    StructField("true_track",    StringType(), True),
    StructField("vertical_rate",       StringType(),    True),
    StructField("sensors",          StringType(),    True),
    StructField("geo_altitude",         StringType(),    True),
    StructField("squawk",          StringType(),    True),
    StructField("spi",   BooleanType(),    True),
    StructField("position_source", StringType(),    True),
    StructField("category",        StringType(),    True)
])

def get_flight_data(createDF=False):
    data = requests.get("https://opensky-network.org/api/states/all?extended=1")
    data = json.loads(data.text).get("states")#.split(",")
    flight_data = [r for r in data]

    if not createDF:
        return(flight_data)

    if createDF:
        df = spark.createDataFrame(data, schema=schema)
        df = (
            df.withColumn("category_name",
                            when(col("category") == "0", "No information at all")
                            .when(col("category") == "1", "No ADS-B Emitter Category Information")
                            .when(col("category") == "2", "Light (<15500 lbs)")
                            .when(col("category") == "3", "Small (15500 to 75000 lbs)")
                            .when(col("category") == "4", "Large (75000 to 300000 lbs)")
                            .when(col("category") == "5", "High Vortex Large (aircraft such as B-757)")
                            .when(col("category") == "6", "Heavy (>300000 lbs)")
                            .when(col("category") == "7", "High Performance (>5 Mach) and High Speed (> 400 kts)")
                            .when(col("category") == "8", "Rotorcraft")
                            .when(col("category") == "9", "Glider / sailplane")
                            .when(col("category") == "10", "Lighter-than-air")
                            .when(col("category") == "11", "Parachutist / Skydiver")
                            .when(col("category") == "12", "Ultralight / hang-glider / paraglider")
                            .when(col("category") == "13", "Reserved")
                            .when(col("category") == "14", "Unmanned Aerial Vehicle")
                            .when(col("category") == "15", "Space / Trans-atmospheric vehicle")
                            .when(col("category") == "16", "Surface Vehicle - Emergency Vehicle")
                            .when(col("category") == "17", "Surface Vehicle - Service Vehicle")
                            .when(col("category") == "18", "Point Obstacle (includes tethered balloons)")
                            .when(col("category") == "19", "Cluster Obstacle")
                            .when(col("category") == "20", "Line Obstacle")
                        )
        )
        return(df)