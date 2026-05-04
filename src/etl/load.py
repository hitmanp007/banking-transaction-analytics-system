import pandas as pd
from src.utils.db_connection import get_connection

# Load cleaned data
df = pd.read_csv("data/processed/cleaned_transactions.csv")
# ✅ Create realistic customers (grouping)
df['Customer_ID'] = (df.index // 5) + 1

# ✅ Create multiple accounts per customer
df['Account_ID'] = df['Customer_ID'] * 10 + (df.index % 2)

# ✅ Unique transaction IDs
df['Transaction_ID'] = range(1, len(df)+1)

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
accounts = df[['Account_ID', 'Customer_ID', 'Account_Type', 'Bank_Branch']] \
            .drop_duplicates(subset=['Account_ID'])

data = [tuple(row) for row in accounts.values]

cursor.executemany("""
    INSERT INTO accounts (account_id, customer_id, account_type, bank_branch)
    VALUES (%s, %s, %s, %s)
""", data)

conn.commit()
# ------------------ INSERT TRANSACTIONS ------------------
print("Preparing transactions data...")

transactions = df[['Transaction_ID', 'Account_ID', 'Transaction_DateTime', 'Transaction_Amount',
                'Transaction_Type', 'Transaction_Currency', 'Transaction_Description']] 
                

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
print("Preparing fraud data...")

fraud = df[['Transaction_ID', 'Is_Fraud']]

# ✅ convert to pure Python int
data = [(int(row[0]), int(row[1])) for row in fraud.values]

print("Fraud rows:", len(data))

cursor.executemany("""
    INSERT INTO fraud_detection (transaction_id, is_fraud)
    VALUES (%s, %s)
""", data)

conn.commit()

print("Fraud inserted ✅")