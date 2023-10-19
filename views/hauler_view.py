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
            """)

            # Initialize a dictionary to store haulers and their embedded ships
            haulers = {}

            # Iterate over query results
            for row in db_cursor.fetchall():
                hauler_id = row['id']

                # If the hauler is not in the dictionary, add it with its information
                if hauler_id not in haulers:
                    haulers[hauler_id] = {
                        "id": hauler_id,
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
                    haulers[hauler_id]["ships"].append(ship_info)

            # Serialize the haulers dictionary to JSON encoded string
            serialized_haulers = json.dumps(list(haulers.values()))
        else:
            # Write the default SQL query to get hauler information
            db_cursor.execute("""
            SELECT
                h.id,
                h.name,
                h.dock_id
            FROM Hauler h
            """)

            # Fetch all haulers
            query_results = db_cursor.fetchall()

            # Initialize an empty list and then add each dictionary to it
            haulers = []
            for row in query_results:
                hauler = {
                    "id": row['id'],
                    "name": row['name'],
                    "dock_id": row['dock_id']
                }
                haulers.append(hauler)

            # Serialize the list of haulers to JSON encoded string
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

def create_hauler(hauler_data):
    # Extract hauler data
    name = hauler_data.get('name', '')
    dock_id = hauler_data.get('dock_id', 0)

    # Connect to the database
    with sqlite3.connect("./shipping.db") as conn:
        db_cursor = conn.cursor()

        # Insert a new hauler record
        db_cursor.execute(
            """
            INSERT INTO hauler (name, dock_id)
            VALUES (?, ?)
            """,
            (name, dock_id)
        )

        # Get the ID of the newly created hauler
        new_hauler_id = db_cursor.lastrowid

    return new_hauler_id