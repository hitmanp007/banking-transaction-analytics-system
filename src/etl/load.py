import pandas as pd
from src.utils.db_connection import get_connection

# Load cleaned data
df = pd.read_csv("data/processed/cleaned_transactions.csv")

conn = get_connection()
cursor = conn.cursor()

# ------------------ INSERT CUSTOMERS ------------------
customers = df[['Customer_ID', 'Gender', 'Age', 'City', 'State']].drop_duplicates()

for _, row in customers.iterrows():
    cursor.execute("""
        INSERT INTO customers (customer_id, gender, age, city, state)
        VALUES (%s, %s, %s, %s, %s)
    """, tuple(row))

# ------------------ INSERT ACCOUNTS ------------------
accounts = df[['Customer_ID', 'Account_Type', 'Bank_Branch']].drop_duplicates()

for _, row in accounts.iterrows():
    cursor.execute("""
        INSERT INTO accounts (customer_id, account_type, bank_branch)
        VALUES (%s, %s, %s)
    """, tuple(row))

# ------------------ INSERT TRANSACTIONS ------------------
transactions = df[['Transaction_ID', 'Transaction_DateTime', 'Transaction_Amount',
                'Transaction_Type', 'Transaction_Currency', 'Transaction_Description']]

for _, row in transactions.iterrows():
    cursor.execute("""
        INSERT INTO transactions 
        (transaction_id, account_id, transaction_datetime, amount, transaction_type, currency, description)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (row['Transaction_ID'], 1, row['Transaction_DateTime'],
        row['Transaction_Amount'], row['Transaction_Type'],
        row['Transaction_Currency'], row['Transaction_Description']))

# ------------------ INSERT FRAUD ------------------
fraud = df[['Transaction_ID', 'Is_Fraud']]

for _, row in fraud.iterrows():
    cursor.execute("""
        INSERT INTO fraud_detection (transaction_id, is_fraud)
        VALUES (%s, %s)
    """, tuple(row))

conn.commit()
cursor.close()
conn.close()

print("Data inserted successfully 🚀")