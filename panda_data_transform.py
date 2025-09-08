import pandas as pd
from datetime import datetime

# Sample data as a list of dictionaries
data = [
    [2025, 1, 15, 15, 59, 'AA', '1a'],
    [2025, 2, 14, 23, 55, 'B', '2'],
    [2021, 10, 13, 22, 55, 'AA', '1'],
    [2023, 11, 12, 8, 50, 'AA', '2'],
    [2025, 3, 11, 7, 45, 'AA', '1'],
    [1990, 4, 10, 2, 45, 'B', '2'],
    [2001, 4, 9, 11, 45, 'B', '1'],
    [1990, 4, 10, 2, 45, 'B', '2'],
    [2021, 10, 13, 22, 55, 'AA', '1']
]

# Define column names
columns = ['year', 'month', 'day', 'hour', 'seconds', 'transaction_id', 'amount']

# Load into DataFrame
df = pd.DataFrame(data, columns=columns)

# Clean 'amount' column - remove non-digit characters, convert to numeric
df['amount'] = pd.to_numeric(df['amount'].astype(str).str.extract(r'(\d+)')[0], errors='coerce').fillna(0)

# Create datetime column
df['transaction_datetime'] = pd.to_datetime(df[['year', 'month', 'day', 'hour', 'seconds']])

# Format datetime as required: [mm-YYYY-dd hour:minute:second]
df['formatted_date'] = df['transaction_datetime'].dt.strftime('%m-%Y-%d %H:%M:%S')

# Group by transaction_id
grouped = df.groupby('transaction_id').agg(
    total_amount=('amount', 'sum'),
    most_recent_transaction_date=('transaction_datetime', 'max')
).reset_index()

# Reformat most recent transaction date
grouped['most_recent_transaction_date'] = grouped['most_recent_transaction_date'].dt.strftime('%m-%Y-%d %H:%M:%S')

# Display the final DataFrame
print("\nOutput Data Frame\n" + "-"*50)
print(grouped.to_string(index=False))
