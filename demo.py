import time  
import random  
import string  
from threading import Thread  
from pymongo import MongoClient, InsertOne, UpdateOne  
from pymongo.errors import BulkWriteError  
from tabulate import tabulate  
  
# -------------------------------  
# Configuration Parameters  
# -------------------------------  
  
MONGO_URI = 'mongodb://localhost:27017/'  # Replace with your MongoDB URI  
DATABASE_NAME = 'flightdata'  
FLIGHT_COLLECTION = 'flightcollection'  
POSITION_COLLECTION = 'positioncollection'  
SURFACE_COLLECTION = 'surfacecollection'  
  
NUM_RECORDS = 30000  # Total number of synthetic records to generate  
BATCH_SIZE = 5000    # Number of records per batch  
USE_MULTITHREADING = True  # Used in optimized approach  
  
# -------------------------------  
# Synthetic Data Generation  
# -------------------------------  
  
def generate_flight_id():  
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))  
  
def generate_position_report(flight_id):  
    return {  
        "flightid": flight_id,  
        "posmsgtype": random.choice(["position", "surface"]),  
        "time_added": time.time(),  
        "latitude": random.uniform(-90, 90),  
        "longitude": random.uniform(-180, 180),  
        "altitude": random.uniform(0, 40000),  
        "speed": random.uniform(200, 600),  
        # Additional fields as needed  
    }  
  
def generate_synthetic_data(num_records):  
    data = {}  
    for _ in range(num_records):  
        flight_id = generate_flight_id()  
        data[flight_id] = generate_position_report(flight_id)  
    return data  
  
# -------------------------------  
# MongoDB Initialization  
# -------------------------------  
  
def init_mongo_database(mongo_uri, database_name):  
    client = MongoClient(mongo_uri)  
    db = client[database_name]  
    return db  
  
# -------------------------------  
# Original Approach  
# -------------------------------  
  
def original_write_position_mongo_database(db, data):  
    operations = {  
        FLIGHT_COLLECTION: [],  
        POSITION_COLLECTION: [],  
        SURFACE_COLLECTION: []  
    }  
  
    for row in data:  
        try:  
            writedata = data[row]  
        except Exception as e:  
            raise Exception(f"Exception during translation of position columns: {e}")  
  
        operations[FLIGHT_COLLECTION].append(UpdateOne({"flightid": row}, {"$set": writedata}, upsert=True))  
        if writedata["posmsgtype"] == "position":  
            operations[POSITION_COLLECTION].append(InsertOne(writedata))  
        else:  
            operations[SURFACE_COLLECTION].append(InsertOne(writedata))  
  
    write_stats = {}  
  
    for collection_name, ops in operations.items():  
        if ops:  
            collection = db[collection_name]  
            start_write = time.monotonic()  
            try:  
                collection.bulk_write(ops, ordered=False)  
            except Exception as e:  
                raise Exception(f"Exception during position bulk_write operation to {collection_name}: {e}")  
            end_write = time.monotonic()  
            duration = end_write - start_write  
            write_stats[collection_name] = {  
                'num_operations': len(ops),  
                'duration': duration  
            }  
            print(f"[Original] Wrote {len(ops)} docs to {collection_name} in {duration:.3f} seconds.")  
  
    return write_stats  
  
# -------------------------------  
# Optimized Approach  
# -------------------------------  
  
def optimized_write_position_mongo_database(db, data, batch_size, use_multithreading):  
    data_items = list(data.items())  
    total_batches = (len(data_items) + batch_size - 1) // batch_size  
  
    write_stats = {}  
  
    for batch_num in range(total_batches):  
        batch_start = batch_num * batch_size  
        batch_end = min(batch_start + batch_size, len(data_items))  
        data_batch = dict(data_items[batch_start:batch_end])  
  
        positions_to_insert = []  
        surfaces_to_insert = []  
        updates_to_perform = []  
  
        for flight_id, report in data_batch.items():  
            # Prepare update for flightcollection  
            updates_to_perform.append(UpdateOne(  
                {"flightid": flight_id},  
                {"$set": report},  
                upsert=True  
            ))  
  
            # Prepare inserts for positioncollection or surfacecollection  
            if report["posmsgtype"] == "position":  
                positions_to_insert.append(report)  
            else:  
                surfaces_to_insert.append(report)  
  
        def perform_write(collection_name, operations_list, is_update=False):  
            if operations_list:  
                collection = db[collection_name]  
                start_time = time.monotonic()  
                try:  
                    if is_update:  
                        collection.bulk_write(operations_list, ordered=False)  
                    else:  
                        collection.insert_many(operations_list, ordered=False)  
                    end_time = time.monotonic()  
                    duration = end_time - start_time  
                    write_stats_key = f"{collection_name}_batch_{batch_num}"  
                    write_stats[write_stats_key] = {  
                        'num_operations': len(operations_list),  
                        'duration': duration  
                    }  
                    print(f"[Optimized] Wrote {len(operations_list)} docs to {collection_name} in {duration:.3f} seconds.")  
                except BulkWriteError as bwe:  
                    print(f"Bulk write error: {bwe.details}")  
                except Exception as e:  
                    print(f"Exception during write operation to {collection.name}: {e}")  
  
        threads = []  
  
        # Flightcollection updates  
        if updates_to_perform:  
            if use_multithreading:  
                t = Thread(target=perform_write, args=(FLIGHT_COLLECTION, updates_to_perform, True))  
                threads.append(t)  
            else:  
                perform_write(FLIGHT_COLLECTION, updates_to_perform, True)  
  
        # Positioncollection inserts  
        if positions_to_insert:  
            if use_multithreading:  
                t = Thread(target=perform_write, args=(POSITION_COLLECTION, positions_to_insert))  
                threads.append(t)  
            else:  
                perform_write(POSITION_COLLECTION, positions_to_insert)  
  
        # Surfacecollection inserts  
        if surfaces_to_insert:  
            if use_multithreading:  
                t = Thread(target=perform_write, args=(SURFACE_COLLECTION, surfaces_to_insert))  
                threads.append(t)  
            else:  
                perform_write(SURFACE_COLLECTION, surfaces_to_insert)  
  
        if use_multithreading:  
            # Start all threads  
            for t in threads:  
                t.start()  
            # Wait for all threads to complete  
            for t in threads:  
                t.join()  
  
    return write_stats  
  
# -------------------------------  
# Performance Measurement  
# -------------------------------  
  
def compare_results(original_stats, optimized_stats, total_time_original, total_time_optimized):  
    print("\n--- Performance Comparison ---\n")  
    data = []  
    collections = set(list(original_stats.keys()) + list(optimized_stats.keys()))  
  
    for collection in collections:  
        orig_ops = original_stats.get(collection, {}).get('num_operations', 0)  
        orig_time = original_stats.get(collection, {}).get('duration', 0)  
        opt_ops = sum([v['num_operations'] for k, v in optimized_stats.items() if k.startswith(collection)])  
        opt_time = sum([v['duration'] for k, v in optimized_stats.items() if k.startswith(collection)])  
  
        data.append([  
            collection,  
            orig_ops,  
            orig_time,  
            opt_ops,  
            opt_time  
        ])  
  
    print(tabulate(data, headers=["Collection", "Original Ops", "Original Time (s)", "Optimized Ops", "Optimized Time (s)"]))  
  
    print(f"\nTotal time for original approach: {total_time_original:.3f} seconds")  
    print(f"Total time for optimized approach: {total_time_optimized:.3f} seconds")  
  
# -------------------------------  
# Main Execution  
# -------------------------------  
  
if __name__ == "__main__":  
    # Clear collections before starting  
    db = init_mongo_database(MONGO_URI, DATABASE_NAME)  
    db[FLIGHT_COLLECTION].drop()  
    db[POSITION_COLLECTION].drop()  
    db[SURFACE_COLLECTION].drop()  
  
    print("Generating synthetic data...")  
    synthetic_data = generate_synthetic_data(NUM_RECORDS)  
    print(f"Generated {NUM_RECORDS} records.")  
  
    # --- Run Original Approach ---  
    print("\n--- Running Original Approach ---\n")  
    start_time_original = time.monotonic()  
    original_stats = original_write_position_mongo_database(db, synthetic_data)  
    end_time_original = time.monotonic()  
    total_time_original = end_time_original - start_time_original  
    print(f"\nTotal time for original approach: {total_time_original:.3f} seconds")  
  
    # Clear collections before running optimized approach  
    db[FLIGHT_COLLECTION].drop()  
    db[POSITION_COLLECTION].drop()  
    db[SURFACE_COLLECTION].drop()  
  
    # --- Run Optimized Approach ---  
    print("\n--- Running Optimized Approach ---\n")  
    start_time_optimized = time.monotonic()  
    optimized_stats = optimized_write_position_mongo_database(db, synthetic_data, BATCH_SIZE, USE_MULTITHREADING)  
    end_time_optimized = time.monotonic()  
    total_time_optimized = end_time_optimized - start_time_optimized  
    print(f"\nTotal time for optimized approach: {total_time_optimized:.3f} seconds")  
  
    # --- Compare Results ---  
    compare_results(original_stats, optimized_stats, total_time_original, total_time_optimized)  


"""
Generating synthetic data...
Generated 30000 records.

--- Running Original Approach ---

[Original] Wrote 30000 docs to flightcollection in 410.452 seconds.
[Original] Wrote 15023 docs to positioncollection in 0.557 seconds.
[Original] Wrote 14977 docs to surfacecollection in 0.724 seconds.

Total time for original approach: 411.773 seconds

--- Running Optimized Approach ---

[Optimized] Wrote 2542 docs to positioncollection in 0.143 seconds.
[Optimized] Wrote 2458 docs to surfacecollection in 0.742 seconds.
[Optimized] Wrote 5000 docs to flightcollection in 16.287 seconds.
[Optimized] Wrote 2471 docs to surfacecollection in 0.167 seconds.
[Optimized] Wrote 2529 docs to positioncollection in 0.203 seconds.
[Optimized] Wrote 5000 docs to flightcollection in 37.161 seconds.
[Optimized] Wrote 2478 docs to positioncollection in 0.153 seconds.
[Optimized] Wrote 2522 docs to surfacecollection in 0.156 seconds.
[Optimized] Wrote 5000 docs to flightcollection in 60.042 seconds.
[Optimized] Wrote 2484 docs to positioncollection in 0.205 seconds.
[Optimized] Wrote 2516 docs to surfacecollection in 0.195 seconds.
[Optimized] Wrote 5000 docs to flightcollection in 57.086 seconds.
[Optimized] Wrote 2532 docs to positioncollection in 0.131 seconds.
[Optimized] Wrote 2468 docs to surfacecollection in 0.611 seconds.
[Optimized] Wrote 5000 docs to flightcollection in 104.391 seconds.
[Optimized] Wrote 2542 docs to surfacecollection in 0.211 seconds.
[Optimized] Wrote 2458 docs to positioncollection in 0.227 seconds.
[Optimized] Wrote 5000 docs to flightcollection in 75.096 seconds.

Total time for optimized approach: 350.145 seconds

--- Performance Comparison ---

Collection                    Original Ops    Original Time (s)    Optimized Ops    Optimized Time (s)
--------------------------  --------------  -------------------  ---------------  --------------------
flightcollection_batch_5                 0             0                    5000             75.0964
positioncollection_batch_3               0             0                    2484              0.205223
positioncollection_batch_2               0             0                    2478              0.15335
flightcollection_batch_0                 0             0                    5000             16.2866
flightcollection_batch_3                 0             0                    5000             57.0865
surfacecollection_batch_1                0             0                    2471              0.167176
surfacecollection_batch_2                0             0                    2522              0.155536
surfacecollection_batch_0                0             0                    2458              0.742014
positioncollection_batch_5               0             0                    2458              0.227237
surfacecollection_batch_4                0             0                    2468              0.611027
positioncollection                   15023             0.557372            15023              1.06253
flightcollection                     30000           410.452               30000            350.064
surfacecollection                    14977             0.723529            14977              2.0822
flightcollection_batch_1                 0             0                    5000             37.1615
surfacecollection_batch_5                0             0                    2542              0.211409
positioncollection_batch_0               0             0                    2542              0.142616
positioncollection_batch_4               0             0                    2532              0.131348
surfacecollection_batch_3                0             0                    2516              0.195042
positioncollection_batch_1               0             0                    2529              0.202755
flightcollection_batch_4                 0             0                    5000            104.391
flightcollection_batch_2                 0             0                    5000             60.0418

Total time for original approach: 411.773 seconds
Total time for optimized approach: 350.145 seconds
"""
