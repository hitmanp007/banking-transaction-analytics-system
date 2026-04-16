import pandas as pd
from src.utils.db_connection import get_connection

# Load cleaned data
df = pd.read_csv("data/processed/cleaned_transactions.csv")
df['Account_ID'] = df['Customer_ID']
df['Transaction_ID'] = df['Transaction_ID'].str.extract(r'(\d+)').astype(int)


df['Transaction_DateTime'] = pd.to_datetime(
df['Transaction_Date'].astype(str) + ' ' + df['Transaction_Time']

)

conn = get_connection()
cursor = conn.cursor()

# ------------------ INSERT CUSTOMERS ------------------
print("Connecting to DB...")
conn = get_connection()
cursor = conn.cursor()

print("Preparing customer data...")
customers = df[['Customer_ID', 'Gender', 'Age', 'City', 'State']] \
            .drop_duplicates(subset=['Customer_ID'])

data = [tuple(row) for row in customers.values]

print(f"Inserting {len(data)} customers...")

cursor.executemany("""
    INSERT IGNORE INTO customers (customer_id, gender, age, city, state)
    VALUES (%s, %s, %s, %s, %s)
""", data)

print("Committing...")
conn.commit()

print("Customers inserted successfully ✅")
# ------------------ INSERT ACCOUNTS ------------------
accounts = df[['Customer_ID', 'Account_Type', 'Bank_Branch']].drop_duplicates()

for _, row in accounts.iterrows():
    cursor.execute("""
        INSERT INTO accounts (customer_id, account_type, bank_branch)
        VALUES (%s, %s, %s)
    """, tuple(row))

# ------------------ INSERT TRANSACTIONS ------------------
print("Preparing transactions data...")

transactions = df[['Transaction_ID', 'Account_ID', 'Transaction_DateTime', 'Transaction_Amount',
                'Transaction_Type', 'Transaction_Currency', 'Transaction_Description']] \
                .drop_duplicates(subset=['Transaction_ID'])

for _, row in transactions.iterrows():
    cursor.execute("""
        INSERT IGNORE INTO transactions
        (transaction_id, account_id, transaction_datetime, amount, transaction_type, currency, description)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (
        row['Transaction_ID'],
        row['Account_ID'],
        row['Transaction_DateTime'],
        row['Transaction_Amount'],
        row['Transaction_Type'],
        row['Transaction_Currency'],
        row['Transaction_Description']
    ))

print("Transactions inserted ✅")
conn.commit()
# ------------------ INSERT FRAUD ------------------
fraud = df[['Transaction_ID', 'Is_Fraud']]
data = [(int(row[0]), int(row[1])) for row in fraud.values]

cursor.executemany("""
    INSERT IGNORE INTO fraud_detection (transaction_id, is_fraud)
    VALUES (%s, %s)
""", data)