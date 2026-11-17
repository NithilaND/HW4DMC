import os
from flask import Flask, request, jsonify
from neo4j import GraphDatabase
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, round, desc

app = Flask(__name__)

driver = GraphDatabase.driver(
    "bolt://localhost:7687",
    auth=("neo4j", os.environ["NEO4J_PASSWORD"])
)

spark = SparkSession.builder.appName("TaxiAPI").master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv("taxi_trips_clean.csv", header=True, inferSchema=True)


def run_query(query, **params):
    records, _, _ = driver.execute_query(query, database_="neo4j", **params)
    return [r.data() for r in records]


@app.route("/graph-summary")
def graph_summary():
    results = run_query("""
        MATCH (d:Driver)
        WITH count(d) AS drivers
        MATCH (c:Company)
        WITH drivers, count(c) AS companies
        MATCH (a:Area)
        WITH drivers, companies, count(a) AS areas
        MATCH ()-[t:TRIP]->()
        RETURN drivers AS driver_count, companies AS company_count,
               areas AS area_count, count(t) AS trip_count
    """)
    return jsonify(results[0])


@app.route("/top-companies")
def top_companies():
    n = request.args.get("n", 5, type=int)

    results = run_query("""
        MATCH (c:Company)<-[:WORKS_FOR]-(d:Driver)-[t:TRIP]->(a:Area)
        RETURN c.name AS name, count(t) AS trip_count
        ORDER BY trip_count DESC, name
        LIMIT $n
    """, n=max(n, 0))

    return jsonify({"companies": results})


@app.route("/high-fare-trips")
def high_fare_trips():
    area = request.args.get("area_id", type=int)
    fare = request.args.get("min_fare", 0, type=float)

    results = run_query("""
        MATCH (d:Driver)-[t:TRIP]->(a:Area)
        WHERE a.area_id = $area AND t.fare > $fare
        RETURN t.trip_id AS trip_id, t.fare AS fare,
               d.driver_id AS driver_id
        ORDER BY fare DESC, trip_id ASC
    """, area=area, fare=fare)

    return jsonify({"trips": results})


@app.route("/co-area-drivers")
def co_area_drivers():
    driver_id = request.args.get("driver_id", "")

    results = run_query("""
        MATCH (d1:Driver {driver_id: $driver_id})-[:TRIP]->(a:Area)
              <-[:TRIP]-(d2:Driver)
        WHERE d1 <> d2
        RETURN d2.driver_id AS driver_id,
               count(DISTINCT a) AS shared_areas
        ORDER BY shared_areas DESC, driver_id
    """, driver_id=driver_id)

    return jsonify({"co_area_drivers": results})


@app.route("/avg-fare-by-company")
def avg_fare_by_company():
    results = run_query("""
        MATCH (c:Company)<-[:WORKS_FOR]-(d:Driver)-[t:TRIP]->(a:Area)
        RETURN c.name AS name,
               round(avg(t.fare) * 100) / 100.0 AS avg_fare
        ORDER BY avg_fare DESC, name
    """)

    return jsonify({"companies": results})


@app.route("/area-stats")
def area_stats():
    area = request.args.get("area_id", type=int)

    rows = (df.filter(col("dropoff_area") == area)
              .groupBy("dropoff_area")
              .agg(
                  count("*").alias("trip_count"),
                  round(avg("fare"), 2).alias("avg_fare"),
                  round(avg("trip_seconds"), 2).alias("avg_trip_seconds")
              ).collect())

    if not rows:
        return jsonify({"error": "area not found"}), 404

    row = rows[0]

    return jsonify({
        "area_id": area,
        "trip_count": row["trip_count"],
        "avg_fare": row["avg_fare"],
        "avg_trip_seconds": row["avg_trip_seconds"]
    })


@app.route("/top-pickup-areas")
def top_pickup_areas():
    n = request.args.get("n", 5, type=int)

    rows = (df.groupBy("pickup_area")
              .agg(count("*").alias("trip_count"))
              .orderBy(desc("trip_count"), "pickup_area")
              .limit(max(n, 0))
              .collect())

    areas = []
    for row in rows:
        areas.append({
            "pickup_area": row["pickup_area"],
            "trip_count": row["trip_count"]
        })

    return jsonify({"areas": areas})


@app.route("/company-compare")
def company_compare():
    c1 = request.args.get("company1", "")
    c2 = request.args.get("company2", "")

    trips = df.withColumn(
        "fare_per_minute",
        col("fare") / (col("trip_seconds") / 60.0)
    )
    trips.createOrReplaceTempView("trips")

    results = spark.sql("""
        SELECT company, COUNT(*) AS trip_count,
               ROUND(AVG(fare), 2) AS avg_fare,
               ROUND(AVG(fare_per_minute), 2) AS avg_fare_per_minute,
               ROUND(AVG(trip_seconds), 2) AS avg_trip_seconds
        FROM trips
        WHERE company IN (:c1, :c2)
        GROUP BY company
    """, args={"c1": c1, "c2": c2})

    companies = [row.asDict() for row in results.collect()]
    companies.sort(key=lambda x: 0 if x["company"] == c1 else 1)

    if len(companies) != len({c1, c2}):
        return jsonify({"error": "one or more companies not found"})

    for company in companies:
        company["avg_trip_seconds"] = float(company["avg_trip_seconds"])

    return jsonify({"comparison": companies})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
