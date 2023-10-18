import sqlite3
import json


def update_ship(id, ship_data):
    with sqlite3.connect("./shipping.db") as conn:
        db_cursor = conn.cursor()

        db_cursor.execute(
            """
            UPDATE Ship
                SET
                    name = ?,
                    hauler_id = ?
            WHERE id = ?
            """,
            (ship_data['name'], ship_data['hauler_id'], id)
        )

        rows_affected = db_cursor.rowcount

    return True if rows_affected > 0 else False


def delete_ship(pk):
    with sqlite3.connect("./shipping.db") as conn:
        conn.row_factory = sqlite3.Row
        db_cursor = conn.cursor()

        # Write the SQL query to get the information you want
        db_cursor.execute("""
        DELETE FROM Ship WHERE id = ?
        """, (pk,)
        )
        number_of_rows_deleted = db_cursor.rowcount

    return True if number_of_rows_deleted > 0 else False


def list_ships(url):
    # Open a connection to the database
    with sqlite3.connect("./shipping.db") as conn:
        conn.row_factory = sqlite3.Row
        db_cursor = conn.cursor()

        # Check if the '_expand' parameter exists in the URL dictionary
        if "expand" in url['query_params']:
            # Handle the case when _expand exists in the URL
            db_cursor.execute("""
            SELECT
                s.id,
                s.name,
                s.hauler_id,
                h.id haulerId,
                h.name haulerName,
                h.dock_id
            FROM Ship s
            JOIN Hauler h 
                ON h.id = s.hauler_id
            """)
        else:
            # Write the default SQL query to get the information you want
            db_cursor.execute("""
            SELECT
                s.id,
                s.name,
                s.hauler_id
            FROM Ship s
            """)

        query_results = db_cursor.fetchall()

        # Initialize an empty list and then add each dictionary to it
        ships = []
        if "expand" in url['query_params']:
            for row in query_results:
                # if '_expand' in url:
                # Build a hauler dictionary with the correct keys and values
                hauler = {
                    "id": row['haulerId'],
                    "name": row['haulerName'],
                    "dock_id": row["dock_id"]
                }
                # Build a ship dictionary that includes the hauler dictionary as a nested dictionary
                ship = {
                    "id": row['id'],
                    "name": row['name'],
                    "hauler_id": row["hauler_id"],
                    "hauler": hauler
                }
            # Append the new ship dictionary to the list of ships
                ships.append(ship)
        else:
            for row in query_results:
                ship = {
                    "id": row['id'],
                    "name": row['name'],
                    "hauler_id": row["hauler_id"],
                }
                # If not expanding, append the row as is
                ships.append(dict(row))

        # Serialize Python list to JSON encoded string
        serialized_ships = json.dumps(ships)

    return serialized_ships


def retrieve_ship(pk, url):
    # Open a connection to the database
    with sqlite3.connect("./shipping.db") as conn:
        conn.row_factory = sqlite3.Row
        db_cursor = conn.cursor()

        # Check if the '_expand' parameter exists in the URL dictionary
        if "expand" in url['query_params']:
            # Handle the case when _expand exists in the URL
            db_cursor.execute("""
            SELECT
                s.id,
                s.name,
                s.hauler_id,
                h.id haulerId,
                h.name haulerName,
                h.dock_id
            FROM Ship s
            JOIN Hauler h ON s.hauler_id = h.id
            WHERE s.id = ?
            """, (pk,))

            # method that is used to retrieve a single row of data from the result set of a database query.
            query_results = db_cursor.fetchone()

            hauler = {
                "id": query_results['haulerId'],
                "name": query_results['haulerName'],
                "dock_id": query_results["dock_id"]
            }

            # Build a ship dictionary that includes the hauler dictionary as a nested dictionary
            ship = {
                "id": query_results['id'],
                "name": query_results['name'],
                "hauler_id": query_results["hauler_id"],
                "hauler": hauler
            }

            # Serialize the ship dictionary to JSON encoded string
            serialized_ship = json.dumps(ship)

        else:
            # Write the default SQL query to get the information you want
            db_cursor.execute("""
            SELECT
                s.id,
                s.name,
                s.hauler_id
            FROM Ship s
            WHERE s.id = ?
            """, (pk,))

            query_results = db_cursor.fetchone()
            serialized_ship = json.dumps(dict(query_results))

    return serialized_ship
