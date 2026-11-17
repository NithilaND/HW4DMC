import csv
import getpass
from neo4j import GraphDatabase

password = getpass.getpass("Neo4j password: ")
driver = GraphDatabase.driver(
    "bolt://127.0.0.1:7687",
    auth=("neo4j", password))
with open("taxi_trips_clean.csv", newline="") as f:
    rows = []
    for r in csv.DictReader(f):
        rows.append({
            "trip_id": r["trip_id"],
            "driver_id": r["driver_id"],
            "company": r["company"],
            "dropoff_area": int(r["dropoff_area"]),
            "fare": float(r["fare"]),
            "trip_seconds": int(r["trip_seconds"]),
        })

with driver.session(database="neo4j") as session:
    session.run(
        "CREATE CONSTRAINT driver_id_unique IF NOT EXISTS "
        "FOR (d:Driver) REQUIRE d.driver_id IS UNIQUE"
    ).consume()
    session.run(
        "CREATE CONSTRAINT company_name_unique IF NOT EXISTS "
        "FOR (c:Company) REQUIRE c.name IS UNIQUE"
    ).consume()
    session.run(
        "CREATE CONSTRAINT area_id_unique IF NOT EXISTS "
        "FOR (a:Area) REQUIRE a.area_id IS UNIQUE"
    ).consume()
    query = """
    UNWIND $rows AS r
    MERGE (d:Driver {driver_id: r.driver_id})
    MERGE (c:Company {name: r.company})
    MERGE (d)-[:WORKS_FOR]->(c)
    MERGE (a:Area {area_id: r.dropoff_area})
    MERGE (d)-[t:TRIP {trip_id: r.trip_id}]->(a)
    SET t.fare = r.fare,
        t.trip_seconds = r.trip_seconds
    """
    for i in range(0, len(rows), 500):
        session.run(query, rows=rows[i:i + 500]).consume()
        print(f"Loaded {min(i + 500, len(rows))}/{len(rows)} trips")
    for label, expression in [
        ("Drivers", "MATCH (n:Driver) RETURN count(n) AS total"),
        ("Companies", "MATCH (n:Company) RETURN count(n) AS total"),
        ("Areas", "MATCH (n:Area) RETURN count(n) AS total"),
        ("Trips", "MATCH ()-[r:TRIP]->() RETURN count(r) AS total"),
    ]:
        result = session.run(expression).single()
        print(f"{label}: {result['total']}")

driver.close()
