import mysql.connector

def get_connection():
    return mysql.connector.connect(
        host="localhost",
        user="app_user",
        password="password",
        database="banking_project"
    )