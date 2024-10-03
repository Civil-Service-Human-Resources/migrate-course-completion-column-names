from typing import Dict
import mysql.connector
import psycopg2
from psycopg2.extras import execute_values
import dotenv
import os

dotenv.load_dotenv()

mysql_connection = mysql.connector.connect(
    database='csrs',
    host=os.environ['MYSQL_HOST'],
    user=os.environ['MYSQL_USER'],
    password=os.environ['MYSQL_PASSWORD']
)

pg_connection = psycopg2.connect(
    dbname='reporting',
    host=os.environ['PG_HOST'],
    password=os.environ['PG_PASSWORD'],
    port=5432,
    user=os.environ['PG_USER']
)


def get_all_organisation_ids_and_names():
    print("Fetching organisations from the csrs database")
    with mysql_connection.cursor() as cursor:
        orgs = {}
        cursor.execute("SELECT id, name, parent_id FROM organisational_unit")
        for row in cursor.fetchall():
            orgs[row[0]] = {"_id": row[0], "name": row[1], "parent_id": row[2]}
        return orgs


def get_formatted_names_for_orgs():
    formatted_org_names: Dict[int, str] = {}
    orgs = get_all_organisation_ids_and_names()
    print("Formatting organisation names")
    for org in orgs.values():
        current_name = org["name"]
        current_parent_id = org["parent_id"]
        while (current_parent_id is not None):
            parent = orgs.get(current_parent_id, None)
            if (parent is not None):
                current_name = f"{parent['name']} | {current_name}"
                current_parent_id = parent['parent_id']
            else:
                current_parent_id = None
        formatted_org_names[org['_id']] = current_name
    return formatted_org_names


def get_all_grade_ids_and_names():
    print("Fetching grades and their names from the csrs database")
    grades: Dict[int, str] = {}
    with mysql_connection.cursor() as cursor:
        cursor.execute("SELECT id, name FROM grade")
        for row in cursor.fetchall():
            grades[row[0]] = row[1]
    return grades


def update_organisation_names(formatted_org_names: Dict[int, str]):
    formatted_names_tuples = [(item[0], item[1]) for item in formatted_org_names.items()]
    joined_update_sql = """UPDATE course_completion_events
                            SET organisation_name = payload.organisation_name
                            FROM (VALUES %s) AS payload (id, organisation_name)
                            WHERE course_completion_events.organisation_id = payload.id"""
    print(f"Updating organisation data with SQL: {joined_update_sql}")
    with pg_connection.cursor() as cursor:
        execute_values(cursor, joined_update_sql, formatted_names_tuples)
    pg_connection.commit()


def update_grade_names(grades: Dict[int, str]):
    grades_tuples = [(item[0], item[1]) for item in grades.items()]
    joined_update_sql = """UPDATE course_completion_events
                            SET grade_name = payload.grade_name
                            FROM (VALUES %s) AS payload (id, grade_name)
                            WHERE course_completion_events.grade_id = payload.id"""
    print(f"Updating grade data with SQL: {joined_update_sql}")
    with pg_connection.cursor() as cursor:
        execute_values(cursor, joined_update_sql, grades_tuples)
    pg_connection.commit()


def run():
    print("Loading grade data")
    grades = get_all_grade_ids_and_names()
    print(f"{len(grades)} grades left to process")
    if (len(grades) > 0):
        update_grade_names(grades)

    print("Loading organisation data")
    formatted_org_names = get_formatted_names_for_orgs()
    print(f"{len(formatted_org_names)} organisations left to process")
    if (len(grades) > 0):
        update_organisation_names(formatted_org_names)


run()
