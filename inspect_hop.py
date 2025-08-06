import pandas as pd
import numpy as np

# Load generated file
df = pd.read_excel('transactions_hop_02_08_2025.xlsx', sheet_name='Transactions')  # Assuming sheet name

# Find empty Partenaire transaction
empty_rows = df[df['Partenaire transaction'].isna() | (df['Partenaire transaction'] == '') | (df['Partenaire transaction'].str.strip() == '')]

print('Empty Partenaire transaction rows:')
print(empty_rows[['Date transaction', 'Heure transaction', 'Reference transaction', 'Type transaction', 'Nom portefeuille', 'Partenaire transaction']])

# Get references
references = empty_rows['Reference transaction'].tolist()

# Load raw files
agent_df = pd.read_excel('AGENT_02_08_2025.xlsx', header=1)
customer_df = pd.read_excel('CUSTOMER_02_08_2025.xlsx', header=1)
combined_raw = pd.concat([agent_df, customer_df])

# For each reference, find in raw and print relevant columns
for ref in references:
    raw_row = combined_raw[combined_raw['Reference transaction'] == ref]
    if not raw_row.empty:
        print(f'\nRaw data for ref {ref}:')
        print(raw_row[['Date heure', 'Type transaction', 'Nom portefeuille expediteur', 'Nom portefeuille destinataire', 'Nom destinataire', 'Compte bancaire destinataire']])
    else:
        print(f'\nNo raw data found for ref {ref}')

# After the loop, collect all matching raw rows into suspect_df
suspect_rows = []
for ref in references:
    raw_row = combined_raw[combined_raw['Reference transaction'] == ref]
    if not raw_row.empty:
        suspect_rows.append(raw_row)

suspect_df = pd.concat(suspect_rows) if suspect_rows else pd.DataFrame()

print('\nSuspect DataFrame (raw rows with empty Partenaire transaction):')
print(suspect_df[['Date heure', 'Type transaction', 'Nom portefeuille expediteur', 'Nom portefeuille destinataire', 'Nom destinataire', 'Compte bancaire destinataire', 'Montant transaction', 'Type canal', 'Statut transaction']])
print(f'\nShape of suspect_df: {suspect_df.shape}') 