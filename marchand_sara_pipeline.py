#!/usr/bin/env python3
"""
Marchand SARA Transaction Analysis Pipeline
==========================================

This module processes agent and customer transaction data from SAP BI,
categorizes transactions by agency type, and generates detailed Excel reports
for each agency with balance progression tracking.

Author: SARA Team
Version: 1.0.0
"""

import logging
import warnings
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Union
import openpyxl

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sara_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class AgencyClassifier:
    """
    Handles classification of different agency types based on naming patterns.
    """
    
    def __init__(self):
        self.super_agents = {
            'hop': 'HOP SERVICESARL',
            'express_union': 'EXPRESS UNIONSA',
            'emi_money': 'EMI MONEY SARL',
            'multiservice': 'MULTI-SERVICE SARL',
            'instant_transfer': 'INSTANTTRANSFER SARL'
        }
    
    @staticmethod
    def is_hop_agency(name: str) -> bool:
        """Check if agency name matches HOP patterns."""
        if not isinstance(name, str):
            return False
        name = name.strip().upper()
        return (name.startswith('HOP') or 
                name == 'HOP SERVICESARL' or
                name == 'ALBINEHOP' or 
                name == 'DTN NANGA NANGA JUNIORHOP SIEGE DCM')
    
    @staticmethod
    def is_emi_money_agency(name: str) -> bool:
        """Check if agency name matches EMI Money patterns."""
        if not isinstance(name, str):
            return False
        name = name.strip()
        patterns = ['Emi money', 'EMI MONEY', 'Sarl Emi', 'Sarl Emi money', 'EMI MONEY SARL']
        return any(pattern in name for pattern in patterns)
    
    @staticmethod
    def is_express_union_agency(name: str) -> bool:
        """Check if agency name matches Express Union patterns."""
        if not isinstance(name, str):
            return False
        name = name.strip()
        return name.startswith('EU ') or name.startswith('EUF ') or name == 'EXPRESS UNIONSA'
    
    @staticmethod
    def is_instant_transfer_agency(name: str) -> bool:
        """Check if agency name matches Instant Transfer patterns."""
        if not isinstance(name, str):
            return False
        name = name.strip()
        return name.startswith('IT ') or name == 'INSTANTTRANSFER SARL'
    
    @staticmethod
    def is_multiservice_agency(name: str) -> bool:
        """Check if agency name matches Multi-Service patterns."""
        if not isinstance(name, str):
            return False
        name = name.strip()
        # Check if name starts with 'MS ' or contains 'MULTI-SERVICE'
        if name.startswith('MS ') or 'MULTI-SERVICE' in name.upper():
            return True
        # Check for isolated 'MS' in the name
        words = name.split()
        if 'MS' in words:
            return True
        return False
    
    @staticmethod
    def is_muffa_agency(name: str) -> bool:
        """Check if agency name matches Muffa patterns."""
        if not isinstance(name, str):
            return False
        name = name.strip().upper()
        return name.startswith('MUFFA')
    
    @staticmethod
    def is_call_box_agency(name: str) -> bool:
        """Check if agency name matches Call Box patterns."""
        if not isinstance(name, str):
            return False
        name = name.strip().upper()
        return name.startswith('CB')
    
    def is_any_agency(self, name: str) -> bool:
        """Check if name belongs to any agency type."""
        return (self.is_hop_agency(name) or self.is_emi_money_agency(name) or 
                self.is_express_union_agency(name) or self.is_instant_transfer_agency(name) or
                self.is_multiservice_agency(name) or self.is_muffa_agency(name) or 
                self.is_call_box_agency(name))


class TransactionProcessor:
    """
    Main class for processing transaction data and generating reports.
    """
    
    def __init__(self, data_path: str = "."):
        self.data_path = Path(data_path)
        self.classifier = AgencyClassifier()
        self.transactions_df = None
        self.agency_dataframes = {}
        self.annuaire_dataframes = {}
        self.bank_annuaire_dataframes = {}
        
    def load_data(self) -> None:
        """Load transaction and point of service data."""
        try:
            logger.info("Loading transaction data...")
            
            # Load transaction data
            transactions_agent_df = pd.read_excel(
                self.data_path / 'test_agent.xlsx', 
                header=1, 
                engine='openpyxl'
            )
            transactions_customer_df = pd.read_excel(
                self.data_path / 'test_customer.xlsx', 
                header=3, 
                engine='openpyxl'
            )
            
            # Clean data
            transactions_agent_df.drop('Unnamed: 0', axis=1, inplace=True)
            transactions_customer_df.drop('Unnamed: 0', axis=1, inplace=True)
            
            # Filter successful transactions only
            transactions_agent_df = transactions_agent_df[
                transactions_agent_df['Statut transaction'] == "COMPLETED"
            ]
            transactions_customer_df = transactions_customer_df[
                transactions_customer_df['Statut transaction'] == "COMPLETED"
            ]
            
            # Combine dataframes
            self.transactions_df = pd.concat([
                transactions_agent_df, 
                transactions_customer_df
            ], ignore_index=True)
            
            logger.info(f"Loaded {len(self.transactions_df)} transactions")
            
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise
    
    def preprocess_data(self) -> None:
        """Preprocess transaction data."""
        try:
            logger.info("Preprocessing transaction data...")
            
            # Create date and time columns
            self.transactions_df['Heure transaction'] = pd.to_datetime(
                self.transactions_df['Date heure']
            ).dt.time
            self.transactions_df['Date transaction'] = pd.to_datetime(
                self.transactions_df['Date heure']
            ).dt.date
            self.transactions_df.drop(['Date heure'], axis=1, inplace=True)
            
            # Clean portfolio numbers
            self.transactions_df['Numero porte feuille expediteur'] = (
                self.transactions_df['Numero porte feuille expediteur']
                .fillna(0).astype(int).astype(str)
            )
            self.transactions_df['Numero porte feuille destinataire'] = (
                self.transactions_df['Numero porte feuille destinataire']
                .fillna(0).astype(int).astype(str)
            )
            
            # Remove trailing spaces from all text columns
            for column in self.transactions_df.columns:
                if self.transactions_df[column].dtype == 'object':
                    self.transactions_df[column] = (
                        self.transactions_df[column]
                        .astype(str)
                        .replace('nan', '')
                        .str.strip()
                    )
            
            logger.info("Data preprocessing completed")
            
        except Exception as e:
            logger.error(f"Error preprocessing data: {e}")
            raise
    
    def load_point_of_service_data(self) -> Dict[str, np.ndarray]:
        """Load and process point of service data for all agency types."""
        try:
            logger.info("Loading point of service data...")
            
            # Define file configurations
            pos_files = {
                'emi_money': {'file': 'point_de_service_emi_money.xlsx', 'header': 1, 'drop_cols': ['Unnamed: 0']},
                'express_union': {'file': 'point_de_service_EU.xlsx', 'header': 3, 'drop_cols': ['Unnamed: 0', 'Unnamed: 2', 'Unnamed: 3']},
                'hop': {'file': 'point_de_service_hop.xlsx', 'header': 1, 'drop_cols': ['Unnamed: 0']},
                'instant_transfer': {'file': 'point_de_service_IT.xlsx', 'header': 3, 'drop_cols': ['Unnamed: 0', 'Unnamed: 2', 'Unnamed: 3']},
                'multiservice': {'file': 'point_de_service_MS.xlsx', 'header': 1, 'drop_cols': ['Unnamed: 0']},
                'muffa': {'file': 'point_de_service_muffa.xlsx', 'header': 3, 'drop_cols': ['Unnamed: 0', 'Unnamed: 2', 'Unnamed: 3']},
                'call_box': {'file': 'call_box.xlsx', 'header': 3, 'drop_cols': ['Unnamed: 0', 'Unnamed: 2', 'Unnamed: 3']}
            }
            
            agencies = {}
            
            for agency_type, config in pos_files.items():
                try:
                    df = pd.read_excel(
                        self.data_path / config['file'], 
                        header=config['header'], 
                        engine='openpyxl'
                    )
                    df.drop(config['drop_cols'], axis=1, inplace=True)
                    df['Nom  porteur'] = df['Nom  porteur'].str.strip()
                    agencies[agency_type] = df['Nom  porteur'].unique()
                    logger.info(f"Loaded {len(agencies[agency_type])} {agency_type} agencies")
                except Exception as e:
                    logger.warning(f"Could not load {agency_type} data: {e}")
                    agencies[agency_type] = np.array([])
            
            # Handle CB entries from multiservice
            if 'multiservice' in agencies:
                cb_entries = np.array([x for x in agencies['multiservice'] if x.startswith('CB ')])
                agencies['call_box'] = np.unique(np.concatenate([agencies['call_box'], cb_entries]))
                agencies['multiservice'] = np.array([x for x in agencies['multiservice'] if not x.startswith('CB ')])
            
            return agencies
            
        except Exception as e:
            logger.error(f"Error loading point of service data: {e}")
            raise
    
    def extract_agency_transactions(self) -> None:
        """Extract transactions for each agency type."""
        try:
            logger.info("Extracting agency transactions...")
            
            agency_functions = {
                'hop': self.classifier.is_hop_agency,
                'emi_money': self.classifier.is_emi_money_agency,
                'express_union': self.classifier.is_express_union_agency,
                'instant_transfer': self.classifier.is_instant_transfer_agency,
                'multiservice': self.classifier.is_multiservice_agency,
                'muffa': self.classifier.is_muffa_agency,
                'call_box': self.classifier.is_call_box_agency
            }
            
            for agency_type, is_agency_func in agency_functions.items():
                transactions = self.transactions_df[
                    (self.transactions_df['Nom portefeuille expediteur'].apply(is_agency_func)) |
                    (self.transactions_df['Nom portefeuille destinataire'].apply(is_agency_func))
                ].copy()
                
                self.agency_dataframes[f'{agency_type}_transactions'] = transactions
                logger.info(f"Extracted {len(transactions)} {agency_type} transactions")
            
        except Exception as e:
            logger.error(f"Error extracting agency transactions: {e}")
            raise
    
    def categorize_transactions(self, df: pd.DataFrame, is_agency_func, super_agent: Optional[str] = None) -> pd.DataFrame:
        """Categorize transactions based on type and participants."""
        df = df.copy()
        
        # Replace CASH_IN and CASH_OUT
        df['Type transaction'] = df['Type transaction'].replace({
            'CASH_IN': 'Approvisionement',
            'CASH_OUT': 'Versement bancaire'
        })
        
        # Handle WALLET_TO_WALLET cases
        wallet_mask = df['Type transaction'] == 'WALLET_TO_WALLET'
        
        # Case 1: From agency or super agent to non-agency (Dépot)
        depot_mask = (
            wallet_mask & 
            ((df['Nom portefeuille expediteur'].apply(is_agency_func)) | 
             (df['Nom portefeuille expediteur'] == super_agent)) &
            ~df['Nom portefeuille destinataire'].apply(is_agency_func)
        )
        df.loc[depot_mask, 'Type transaction'] = 'Dépot'
        
        # Case 2: From non-agency and non-super agent to agency (Retrait)
        retrait_mask = (
            wallet_mask &
            ~df['Nom portefeuille expediteur'].apply(is_agency_func) &
            (df['Nom portefeuille expediteur'] != super_agent) &
            df['Nom portefeuille destinataire'].apply(is_agency_func)
        )
        df.loc[retrait_mask, 'Type transaction'] = 'Retrait'
        
        # Super agent logic if provided
        if super_agent:
            # Case 3: From super agent to its own agency type (Approvisionement)
            appro_mask = (
                wallet_mask &
                (df['Nom portefeuille expediteur'] == super_agent) &
                df['Nom portefeuille destinataire'].apply(is_agency_func)
            )
            df.loc[appro_mask, 'Type transaction'] = 'Approvisionement'
            
            # Case 4: From agency to its own super agent type (Décharge)
            decharge_mask = (
                wallet_mask &
                df['Nom portefeuille expediteur'].apply(is_agency_func) &
                (df['Nom portefeuille destinataire'] == super_agent)
            )
            df.loc[decharge_mask, 'Type transaction'] = 'Décharge'
        
        # Handle inter-agency transactions
        inter_agency_mask = wallet_mask & (
            df['Nom portefeuille expediteur'].apply(self.classifier.is_any_agency) &
            df['Nom portefeuille destinataire'].apply(self.classifier.is_any_agency)
        )
        
        # Classify based on balance changes
        retrait_by_balance = (
            inter_agency_mask &
            (df['Solde expediteur avant transaction'] > df['Solde expediteur apres transaction'])
        )
        df.loc[retrait_by_balance, 'Type transaction'] = 'Dépot'
        
        depot_by_balance = (
            inter_agency_mask &
            (df['Solde expediteur avant transaction'] < df['Solde expediteur apres transaction'])
        )
        df.loc[depot_by_balance, 'Type transaction'] = 'Retrait'
        
        return df
    
    def create_solde_columns(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Create unified agent perspective columns with balance progression."""
        df = df.copy()
        
        # Create annuaire_df with unique Nom/Numero pairs
        expediteur_pairs = df[['Nom portefeuille expediteur', 'Numero porte feuille expediteur']].rename(
            columns={'Nom portefeuille expediteur': 'Nom portefeuille', 
                    'Numero porte feuille expediteur': 'Numero portefeuille'}
        )
        destinataire_pairs = df[['Nom portefeuille destinataire', 'Numero porte feuille destinataire']].rename(
            columns={'Nom portefeuille destinataire': 'Nom portefeuille',
                    'Numero porte feuille destinataire': 'Numero portefeuille'}
        )
        annuaire_df = pd.concat([expediteur_pairs, destinataire_pairs]).drop_duplicates().dropna()
        
        # Create bank account annuaire
        bank_annuaire = df[['Nom destinataire', 'Compte bancaire destinataire']].dropna().drop_duplicates()
        
        # Create new columns
        new_columns = [
            'Nom portefeuille', 'Numero portefeuille', 'Solde avant transaction', 
            'Solde après transaction', 'Partenaire transaction', 
            'Numero portefeuille partenaire transaction', 'Compte bancaire partenaire'
        ]
        for col in new_columns:
            df[col] = None
        
        # When agent is expediteur
        expediteur_mask = df['Nom portefeuille expediteur'].apply(self.classifier.is_any_agency)
        df.loc[expediteur_mask, 'Nom portefeuille'] = df.loc[expediteur_mask, 'Nom portefeuille expediteur']
        df.loc[expediteur_mask, 'Numero portefeuille'] = df.loc[expediteur_mask, 'Numero porte feuille expediteur']
        df.loc[expediteur_mask, 'Solde avant transaction'] = df.loc[expediteur_mask, 'Solde expediteur avant transaction']
        df.loc[expediteur_mask, 'Solde après transaction'] = df.loc[expediteur_mask, 'Solde expediteur apres transaction']
        df.loc[expediteur_mask, 'Partenaire transaction'] = df.loc[expediteur_mask, 'Nom portefeuille destinataire']
        df.loc[expediteur_mask, 'Numero portefeuille partenaire transaction'] = df.loc[expediteur_mask, 'Numero porte feuille destinataire']
        
        # Add bank account info when available
        bank_mask = expediteur_mask & df['Compte bancaire destinataire'].notna()
        df.loc[bank_mask, 'Compte bancaire partenaire'] = df.loc[bank_mask, 'Compte bancaire destinataire']
        df.loc[bank_mask, 'Partenaire transaction'] = df.loc[bank_mask, 'Nom destinataire']
        
        # When agent is destinataire
        destinataire_mask = df['Nom portefeuille destinataire'].apply(self.classifier.is_any_agency)
        df.loc[destinataire_mask, 'Nom portefeuille'] = df.loc[destinataire_mask, 'Nom portefeuille destinataire']
        df.loc[destinataire_mask, 'Numero portefeuille'] = df.loc[destinataire_mask, 'Numero porte feuille destinataire']
        df.loc[destinataire_mask, 'Solde avant transaction'] = df.loc[destinataire_mask, 'Solde destinataire avant transaction']
        df.loc[destinataire_mask, 'Solde après transaction'] = df.loc[destinataire_mask, 'Solde destinataire apres transaction']
        df.loc[destinataire_mask, 'Partenaire transaction'] = df.loc[destinataire_mask, 'Nom portefeuille expediteur']
        df.loc[destinataire_mask, 'Numero portefeuille partenaire transaction'] = df.loc[destinataire_mask, 'Numero porte feuille expediteur']
        
        # Sort by Nom portefeuille and Date/Heure
        df = df.sort_values(['Nom portefeuille', 'Date transaction', 'Heure transaction'])
        
        # Drop old columns
        columns_to_drop = [
            'Nom portefeuille expediteur', 'Nom portefeuille destinataire',
            'Solde expediteur avant transaction', 'Solde expediteur apres transaction', 
            'Solde destinataire avant transaction', 'Solde destinataire apres transaction',
            'Numero porte feuille expediteur', 'Numero porte feuille destinataire',
            'Compte bancaire destinataire', 'Nom destinataire'
        ]
        df = df.drop(columns=columns_to_drop)
        
        return df, annuaire_df, bank_annuaire
    
    def process_all_agencies(self) -> None:
        """Process all agency transactions with categorization and solde columns."""
        try:
            logger.info("Processing all agency transactions...")
            
            agency_configs = {
                'hop': {'func': self.classifier.is_hop_agency, 'super_agent': self.classifier.super_agents['hop']},
                'emi_money': {'func': self.classifier.is_emi_money_agency, 'super_agent': None},
                'express_union': {'func': self.classifier.is_express_union_agency, 'super_agent': self.classifier.super_agents['express_union']},
                'instant_transfer': {'func': self.classifier.is_instant_transfer_agency, 'super_agent': self.classifier.super_agents['instant_transfer']},
                'multiservice': {'func': self.classifier.is_multiservice_agency, 'super_agent': self.classifier.super_agents['multiservice']},
                'muffa': {'func': self.classifier.is_muffa_agency, 'super_agent': None},
                'call_box': {'func': self.classifier.is_call_box_agency, 'super_agent': None}
            }
            
            for agency_type, config in agency_configs.items():
                transactions_key = f'{agency_type}_transactions'
                if transactions_key in self.agency_dataframes:
                    # Categorize transactions
                    categorized = self.categorize_transactions(
                        self.agency_dataframes[transactions_key],
                        config['func'],
                        config['super_agent']
                    )
                    
                    # Create solde columns
                    processed_df, annuaire_df, bank_annuaire_df = self.create_solde_columns(categorized)
                    
                    # Store results
                    self.agency_dataframes[agency_type] = processed_df
                    self.annuaire_dataframes[agency_type] = annuaire_df
                    self.bank_annuaire_dataframes[agency_type] = bank_annuaire_df
                    
                    # Reorder columns
                    columns_order = [
                        'Date transaction', 'Heure transaction', 'Reference transaction', 
                        'Type transaction', 'Type utilisateur transaction', 'Nom portefeuille', 
                        'Numero portefeuille', 'Solde avant transaction', 'Montant transaction',
                        'Solde après transaction', 'Partenaire transaction',
                        'Numero portefeuille partenaire transaction', 'Compte bancaire partenaire',
                        'Type canal', 'Statut transaction'
                    ]
                    
                    # Ensure all columns exist before reordering
                    available_columns = [col for col in columns_order if col in processed_df.columns]
                    self.agency_dataframes[agency_type] = processed_df[available_columns]
                    
                    # Final sort
                    self.agency_dataframes[agency_type] = self.agency_dataframes[agency_type].sort_values([
                        'Nom portefeuille', 'Date transaction', 'Heure transaction'
                    ])
                    
                    logger.info(f"Processed {len(processed_df)} {agency_type} transactions")
            
        except Exception as e:
            logger.error(f"Error processing agency transactions: {e}")
            raise
    
    def generate_excel_reports(self, output_path: str = ".") -> None:
        """Generate Excel reports for all processed agencies."""
        try:
            logger.info("Generating Excel reports...")
            output_dir = Path(output_path)
            output_dir.mkdir(exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            for agency_type, df in self.agency_dataframes.items():
                if agency_type.endswith('_transactions'):
                    continue  # Skip raw transaction data
                
                if df is not None and not df.empty:
                    filename = f'transactions_{agency_type}_{timestamp}.xlsx'
                    filepath = output_dir / filename
                    
                    try:
                        df.to_excel(filepath, index=False)
                        logger.info(f"Generated {filename} with {len(df)} records")
                    except Exception as e:
                        logger.error(f"Error generating {filename}: {e}")
            
            logger.info("Excel report generation completed")
            
        except Exception as e:
            logger.error(f"Error generating Excel reports: {e}")
            raise
    
    def get_summary_statistics(self) -> Dict:
        """Generate summary statistics for all agencies."""
        stats = {}
        
        for agency_type, df in self.agency_dataframes.items():
            if agency_type.endswith('_transactions'):
                continue
            
            if df is not None and not df.empty:
                stats[agency_type] = {
                    'total_transactions': len(df),
                    'unique_agents': df['Nom portefeuille'].nunique(),
                    'transaction_types': df['Type transaction'].value_counts().to_dict(),
                    'date_range': {
                        'start': str(df['Date transaction'].min()),
                        'end': str(df['Date transaction'].max())
                    }
                }
        
        return stats
    
    def run_pipeline(self, data_path: str = ".", output_path: str = ".") -> Dict:
        """Run the complete pipeline."""
        try:
            logger.info("Starting SARA transaction analysis pipeline...")
            
            # Update data path
            self.data_path = Path(data_path)
            
            # Execute pipeline steps
            self.load_data()
            self.preprocess_data()
            self.extract_agency_transactions()
            self.process_all_agencies()
            self.generate_excel_reports(output_path)
            
            # Generate summary
            summary = self.get_summary_statistics()
            
            logger.info("Pipeline completed successfully!")
            logger.info(f"Processed {len(summary)} agency types")
            
            return summary
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise


def main():
    """Main execution function."""
    try:
        # Initialize processor
        processor = TransactionProcessor()
        
        # Run pipeline
        summary = processor.run_pipeline(
            data_path=".",  # Current directory
            output_path="./outputs"  # Output directory
        )
        
        # Print summary
        print("\n" + "="*50)
        print("PIPELINE EXECUTION SUMMARY")
        print("="*50)
        
        for agency_type, stats in summary.items():
            print(f"\n{agency_type.upper()} AGENCY:")
            print(f"  Total Transactions: {stats['total_transactions']:,}")
            print(f"  Unique Agents: {stats['unique_agents']:,}")
            print(f"  Date Range: {stats['date_range']['start']} to {stats['date_range']['end']}")
            print(f"  Transaction Types:")
            for trans_type, count in stats['transaction_types'].items():
                print(f"    {trans_type}: {count:,}")
        
        print(f"\nExcel reports generated in: ./outputs/")
        print("Pipeline completed successfully! 🚀")
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        print(f"❌ Pipeline failed: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main()) 