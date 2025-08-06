import pandas as pd
import numpy as np
import sys

def load_and_analyze():
    """Load and analyze which AGENT transaction is not matched"""
    agent_file = 'AGENT_05_08_2025.xlsx'
    customer_file = 'CUSTOMER_05_08_2025.xlsx'
    
    # Load data with correct header
    agent_df = pd.read_excel(agent_file, header=1, engine='openpyxl')
    customer_df = pd.read_excel(customer_file, header=1, engine='openpyxl')
    
    print(f"Agent data shape: {agent_df.shape}")
    print(f"Customer data shape: {customer_df.shape}")
    
    # Expected columns (from the pipeline)
    expected_columns = [
        'Date heure', 'Reference transaction', 'Type transaction', 
        'Type utilisateur transaction', 'Nom portefeuille expediteur',
        'Numero porte feuille expediteur', 'Solde expediteur avant transaction',
        'Montant transaction', 'Solde expediteur apres transaction',
        'Compte bancaire destinataire', 'Nom destinataire',
        'Numero porte feuille destinataire', 'Nom portefeuille destinataire', 
        'Solde destinataire avant transaction', 'Solde destinataire apres transaction',
        'Type canal', 'Statut transaction'
    ]
    
    # Filter to expected columns (removing unnamed columns)
    agent_df = agent_df[expected_columns]
    customer_df = customer_df[expected_columns]
    
    # Filter for completed transactions
    agent_df = agent_df[agent_df['Statut transaction'] == 'COMPLETED']
    customer_df = customer_df[customer_df['Statut transaction'] == 'COMPLETED']
    
    print(f"After filtering completed: Agent: {agent_df.shape}, Customer: {customer_df.shape}")
    
    # Combine the data
    combined_df = pd.concat([agent_df, customer_df], ignore_index=True)
    print(f"Combined shape: {combined_df.shape}")
    
    # Get only AGENT transactions
    agent_transactions = combined_df[combined_df['Type utilisateur transaction'] == 'AGENT'].copy()
    print(f"AGENT transactions: {len(agent_transactions)}")
    
    # Define agency patterns (exact same as pipeline)
    def is_hop_agency(row):
        hop_patterns = ['HOP', 'Hop', 'hop', 'HOp', 'hOP', 'hOp', 'hoP', 'HoP']
        return any(pattern in str(row['Nom portefeuille expediteur']) for pattern in hop_patterns)
    
    def is_emi_money_agency(row):
        emi_patterns = ['Emi Money', 'EMI Money', 'emi money', 'EMI MONEY', 'Emi money', 'emi Money', 'EMI money', 'eMI Money']
        return any(pattern in str(row['Nom portefeuille expediteur']) for pattern in emi_patterns)
    
    def is_express_union_agency(row):
        express_patterns = ['Express Union', 'EXPRESS UNION', 'express union', 'Express union', 'express Union', 'EXPRESS Union']
        return any(pattern in str(row['Nom portefeuille expediteur']) for pattern in express_patterns)
    
    def is_instant_transfer_agency(row):
        instant_patterns = ['Instant Transfer', 'INSTANT TRANSFER', 'instant transfer', 'Instant transfer', 'instant Transfer', 'INSTANT Transfer']
        return any(pattern in str(row['Nom portefeuille expediteur']) for pattern in instant_patterns)
    
    def is_multiservice_agency(row):
        multi_patterns = ['Multi Service', 'MULTI SERVICE', 'multi service', 'Multi service', 'multi Service', 'MULTI Service', 'MultiService', 'MULTISERVICE', 'multiservice']
        return any(pattern in str(row['Nom portefeuille expediteur']) for pattern in multi_patterns)
    
    def is_muffa_agency(row):
        muffa_patterns = ['Muffa', 'MUFFA', 'muffa', 'MUffa', 'muFfa', 'mufFa', 'muffA']
        return any(pattern in str(row['Nom portefeuille expediteur']) for pattern in muffa_patterns)
    
    def is_call_box_agency(row):
        call_box_patterns = ['Call Box', 'CALL BOX', 'call box', 'Call box', 'call Box', 'CALL Box']
        return any(pattern in str(row['Nom portefeuille expediteur']) for pattern in call_box_patterns)
    
    # Check each agency
    agencies = {
        'HOP': is_hop_agency,
        'EMI Money': is_emi_money_agency,
        'Express Union': is_express_union_agency,
        'Instant Transfer': is_instant_transfer_agency,
        'Multi-Service': is_multiservice_agency,
        'Muffa': is_muffa_agency,
        'Call Box': is_call_box_agency
    }
    
    matched_indices = set()
    agency_counts = {}
    
    for agency_name, check_func in agencies.items():
        matches = agent_transactions[agent_transactions.apply(check_func, axis=1)]
        agency_counts[agency_name] = len(matches)
        print(f"{agency_name}: {len(matches)} transactions")
        matched_indices.update(matches.index)
        
        if len(matches) > 0:
            print(f"  Sample wallets: {list(matches['Nom portefeuille expediteur'].unique()[:3])}")
    
    # Find unmatched transactions
    unmatched = agent_transactions[~agent_transactions.index.isin(matched_indices)]
    print(f"\nUnmatched AGENT transactions: {len(unmatched)}")
    print(f"Total unique matched: {len(matched_indices)}")
    print(f"Total agency transactions: {sum(agency_counts.values())}")
    
    if len(unmatched) > 0:
        print("\n=== UNMATCHED TRANSACTIONS ===")
        for idx, row in unmatched.iterrows():
            print(f"Index {idx}:")
            print(f"  Wallet: '{row['Nom portefeuille expediteur']}'")
            print(f"  Transaction Type: '{row['Type transaction']}'")
            print(f"  Reference: '{row['Reference transaction']}'")
            print(f"  Date: {row['Date heure']}")
            print(f"  Amount: {row['Montant transaction']}")
            print()
    
    # Show all unique wallet names for reference
    print("=== ALL UNIQUE AGENT WALLET NAMES ===")
    unique_wallets = agent_transactions['Nom portefeuille expediteur'].unique()
    for i, wallet in enumerate(sorted(unique_wallets)):
        print(f"{i+1:2d}: '{wallet}'")

if __name__ == "__main__":
    load_and_analyze() 