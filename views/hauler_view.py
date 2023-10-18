import sqlite3
import json


def update_hauler(id, hauler_data):
    with sqlite3.connect("./shipping.db") as conn:
        db_cursor = conn.cursor()

        db_cursor.execute(
            """
            UPDATE Hauler
                SET
                    name = ?,
                    dock_id = ?
            WHERE id = ?
            """,
            (hauler_data['name'], hauler_data['dock_id'], id)
        )

        rows_affected = db_cursor.rowcount

    return True if rows_affected > 0 else False


def delete_hauler(pk):
    with sqlite3.connect("./shipping.db") as conn:
        conn.row_factory = sqlite3.Row
        db_cursor = conn.cursor()

        # Write the SQL query to get the information you want
        db_cursor.execute("""
        DELETE FROM Hauler WHERE id = ?
        """, (pk,)
        )
        number_of_rows_deleted = db_cursor.rowcount

    return True if number_of_rows_deleted > 0 else False


def list_haulers(url):
    with sqlite3.connect("./shipping.db") as conn:
        conn.row_factory = sqlite3.Row
        db_cursor = conn.cursor()

        if "_expand" in url['query_params']:
            db_cursor.execute("""
            SELECT
                h.id,
                h.name,
                h.dock_id,
                d.id as dockId,
                d.location as dockLocation,
                d.capacity as dockCapacity
            FROM Hauler h
            JOIN Dock d
                ON d.id = h.dock_id
            """)
        elif "_embed" in url["query_params"]:

            db_cursor.execute("""
            SELECT
                h.id,
                h.name,
                h.dock_id,
                s.id as shipId,
                s.name as shipName,
                s.hauler_id as shipHaulerId
            FROM Hauler h
            LEFT JOIN Ship s
                ON s.hauler_id = h.id
            """)
        else:
            db_cursor.execute("""
            SELECT
                h.id,
                h.name,
                h.dock_id
            FROM Hauler h
            """)

        query_results = db_cursor.fetchall()
        if "_embed" in url["query_params"]:
            haulers = {}
            for row in query_results:
                hauler_id = row["hauler_id"]
                if hauler_id not in haulers:
                    haulers[hauler_id] = {
                        "id": hauler_id,
                        "name": row["hauler_name"],
                        "ships": []
                    }

                if row["ship_id"] is not None:
                    ship = {
                        "id": row["ship_id"],
                        "name": row["ship_name"]
                    }
                    haulers[hauler_id]["ships"].append(ship)

            serialized_haulers = json.dumps(list(haulers.values()))

        elif "_expand" in url['query_params']:
            for row in query_results:

                dock = {
                    "id": row["dockId"],
                    "location": row["dockLocation"],
                    "capacity": row["dockCapacity"]
                }
                hauler = {
                    "id": row['id'],
                    "name": row['name'],
                    "dock_id": row["dock_id"],
                    "dock": dock
                }

                haulers.append(hauler)
        # elif "_embed" in url["query_params"]:
        #     for row in query_results:
        #         ships = {
        #             "id": row["shipId"],
        #             "name": row["shipName"],
        #             "hauler_id": row["shipHaulerId"]
        #         }
        #         hauler = {
        #             "id": row['id'],
        #             "name": row['name'],
        #             "dock_id": row["dock_id"],
        #             "ships": ships
        #         }
        #         haulers.append(hauler)

        else:
            for row in query_results:
                hauler = {
                    "id": row['id'],
                    "name": row['name'],
                    "dock_id": row["dock_id"],
                }
                haulers.append(dict(row))

        serialized_haulers = json.dumps(haulers)

    return serialized_haulers


def retrieve_hauler(pk, url):
    # Open a connection to the database
    with sqlite3.connect("./shipping.db") as conn:
        conn.row_factory = sqlite3.Row
        db_cursor = conn.cursor()

        # Check if the '_embed' parameter exists in the URL dictionary
        if "_embed" in url['query_params']:
            # Handle the case when _embed exists in the URL
            db_cursor.execute("""
            SELECT
                h.id,
                h.name,
                h.dock_id,
                s.id as ship_id,
                s.name as ship_name,
                s.hauler_id as ship_hauler_id
            FROM Hauler h
            LEFT JOIN Ship s
                ON h.id = s.hauler_id
            WHERE h.id = ?
            """, (pk,))

            # Initialize a dictionary to store the hauler's information and embedded ships
            hauler_info = None
            query_results = db_cursor.fetchall()
            # Iterate over query results
            for row in query_results:
                if hauler_info is None:
                    hauler_info = {
                        "id": row['id'],
                        "name": row['name'],
                        "dock_id": row['dock_id'],
                        "ships": []
                    }

                # Add the ship information to the hauler's embedded ships list
                if row['ship_id']:
                    ship_info = {
                        "id": row['ship_id'],
                        "name": row['ship_name'],
                        "hauler_id": row['ship_hauler_id']
                    }
                    hauler_info["ships"].append(ship_info)

            # Serialize the hauler information (including embedded ships) to JSON encoded string
            serialized_hauler = json.dumps(hauler_info)
        else:
            # If _embed is not in the URL, retrieve only the hauler's information
            db_cursor.execute("""
            SELECT
                h.id,
                h.name,
                h.dock_id
            FROM Hauler h
            WHERE h.id = ?
            """, (pk,))
            query_results = db_cursor.fetchone()

            # Serialize the hauler's information to JSON encoded string
            serialized_hauler = json.dumps(dict(query_results))

    return serialized_hauler


# haulers = {}

#             # Iterate over query results
#             for row in db_cursor.fetchall():
#                 hauler_id = row['id']

#                 # If the hauler is not in the dictionary, add it with its information
#                 if hauler_id not in haulers:
#                     haulers[hauler_id] = {
#                         "id": hauler_id,
#                         "name": row['name'],
#                         "dock_id": row['dock_id'],
#                         "ships": []
#                     }

#                 # Add the ship information to the hauler's embedded ships list
#                 if row['ship_id']:
#                     ship_info = {
#                         "id": row['ship_id'],
#                         "name": row['ship_name'],
#                         "hauler_id": row['ship_hauler_id']
#                     }
#                     haulers[hauler_id]["ships"].append(ship_info)

#             # Serialize the haulers dictionary to JSON encoded string
#             serialized_haulers = json.dumps(list(haulers.values()))
#         else:
#             # Write the default SQL query to get hauler information
#             db_cursor.execute("""
#             SELECT
#                 h.id,
#                 h.name,
#                 h.dock_id
#             FROM Hauler h
#             """)

#             # Fetch all haulers
#             query_results = db_cursor.fetchall()

#             # Initialize an empty list and then add each dictionary to it
#             haulers = []
#             for row in query_results:
#                 hauler = {
#                     "id": row['id'],
#                     "name": row['name'],
#                     "dock_id": row['dock_id']
#                 }
#                 haulers.append(hauler)

#             # Serialize the list of haulers to JSON encoded string
#             serialized_haulers = json.dumps(haulers)

#     return serialized_haulers
