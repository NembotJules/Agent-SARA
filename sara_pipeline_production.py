"""
SARA Transaction Processing Pipeline - Production Version
======================================================

This module processes transaction data from various agencies and categorizes them
for analysis. It handles data loading, cleaning, agency identification, and
transaction categorization.

Author: Generated from agent_sara.ipynb
"""

import numpy as np
import pandas as pd
import openpyxl
import logging
from typing import Dict, List, Tuple, Optional, Union
from pathlib import Path
import warnings

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Suppress openpyxl warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")


class DataSchema:
    """
    Defines expected data schemas for validation
    """
    
    # Expected columns for transaction data (complete list from original data)
    TRANSACTION_COLUMNS = [
        'Date heure', 
        'Reference transaction', 
        'Type transaction', 
        'Type utilisateur transaction', 
        'Nom portefeuille expediteur',
        'Numero porte feuille expediteur', 
        'Solde expediteur avant transaction',
        'Montant transaction', 
        'Solde expediteur apres transaction',
        'Compte bancaire destinataire',
        'Numero porte feuille destinataire',
        'Nom portefeuille destinataire', 
        'Solde destinataire avant transaction', 
        'Solde destinataire apres transaction',
        'Type canal', 
        'Statut transaction', 
        'Nom destinataire'
    ]
    
    # Essential columns that must be present (minimal required columns)
    ESSENTIAL_COLUMNS = [
        'Date heure', 
        'Reference transaction', 
        'Type transaction',
        'Type utilisateur transaction', 
        'Statut transaction',
        'Montant transaction'
    ]
    
    # Expected columns for point of service data
    POINT_SERVICE_COLUMNS = [
        'Nom  porteur'  # Note: double space is intentional as per original data
    ]
    
    # Final output columns order
    FINAL_COLUMNS_ORDER = [
        'Date transaction', 'Heure transaction', 'Reference transaction', 
        'Type transaction', 'Type utilisateur transaction', 'Nom portefeuille', 
        'Numero portefeuille', 'Solde avant transaction', 'Montant transaction',
        'Solde après transaction', 'Partenaire transaction',
        'Numero portefeuille partenaire transaction', 'Compte bancaire partenaire',
        'Type canal', 'Statut transaction'
    ]


def load_transaction_data(agent_file: str, customer_file: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load and validate transaction data from Excel files.
    
    Args:
        agent_file: Path to agent transactions Excel file
        customer_file: Path to customer transactions Excel file
    
    Returns:
        Tuple of (agent_df, customer_df)
    
    Raises:
        FileNotFoundError: If files don't exist
        ValueError: If required columns are missing
    """
    logger.info(f"Loading transaction data from {agent_file} and {customer_file}")
    
    # Check if files exist
    if not Path(agent_file).exists():
        raise FileNotFoundError(f"Agent file not found: {agent_file}")
    if not Path(customer_file).exists():
        raise FileNotFoundError(f"Customer file not found: {customer_file}")
    
    try:
        # Load agent transactions (header at row 3, 0-indexed)
        agent_df = pd.read_excel(agent_file, header=3, engine='openpyxl')
        logger.info(f"Loaded agent data: {agent_df.shape}")
        
        # Load customer transactions (header at row 1, 0-indexed)  
        customer_df = pd.read_excel(customer_file, header=1, engine='openpyxl')
        logger.info(f"Loaded customer data: {customer_df.shape}")
        
        # Validate and filter schemas
        agent_df = _validate_and_filter_transaction_schema(agent_df, "agent")
        customer_df = _validate_and_filter_transaction_schema(customer_df, "customer")
        
        return agent_df, customer_df
        
    except Exception as e:
        logger.error(f"Error loading transaction data: {str(e)}")
        raise


def _validate_and_filter_transaction_schema(df: pd.DataFrame, data_type: str) -> pd.DataFrame:
    """
    Validate that the dataframe has expected columns and filter to keep only expected ones.
    
    Args:
        df: DataFrame to validate and filter
        data_type: Type of data for logging ("agent" or "customer")
    
    Returns:
        DataFrame with only expected columns
    
    Raises:
        ValueError: If essential columns are missing
    """
    logger.info(f"Validating and filtering schema for {data_type} data")
    
    # Get all current columns
    current_columns = list(df.columns)
    
    # Check for essential columns
    missing_essential = [col for col in DataSchema.ESSENTIAL_COLUMNS if col not in current_columns]
    
    if missing_essential:
        raise ValueError(f"Missing essential columns in {data_type} data: {missing_essential}")
    
    # Find expected columns that exist in the dataframe
    available_expected_columns = [col for col in DataSchema.TRANSACTION_COLUMNS if col in current_columns]
    
    # Find unexpected columns (excluding Unnamed columns which we expect to remove)
    unexpected_columns = [col for col in current_columns 
                         if col not in DataSchema.TRANSACTION_COLUMNS 
                         and not col.startswith('Unnamed')]
    
    # Find Unnamed columns
    unnamed_columns = [col for col in current_columns if col.startswith('Unnamed')]
    
    # Log what we're removing
    if unexpected_columns:
        logger.warning(f"Removing unexpected columns from {data_type} data: {unexpected_columns}")
    
    if unnamed_columns:
        logger.debug(f"Removing unnamed columns from {data_type} data: {unnamed_columns}")
    
    # Filter dataframe to keep only expected columns
    filtered_df = df[available_expected_columns].copy()
    
    # Check if we're missing any expected columns
    missing_expected = [col for col in DataSchema.TRANSACTION_COLUMNS if col not in available_expected_columns]
    if missing_expected:
        logger.warning(f"Expected columns not found in {data_type} data: {missing_expected}")
    
    logger.info(f"Schema validation passed for {data_type} data")
    logger.info(f"Kept {len(available_expected_columns)} expected columns, removed {len(current_columns) - len(available_expected_columns)} columns")
    
    return filtered_df


def clean_transaction_data(agent_df: pd.DataFrame, customer_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Clean and preprocess transaction data.
    
    Args:
        agent_df: Agent transactions dataframe (already schema-filtered)
        customer_df: Customer transactions dataframe (already schema-filtered)
    
    Returns:
        Tuple of cleaned dataframes
    """
    logger.info("Cleaning transaction data")
    
    # Create copies to avoid modifying originals
    agent_clean = agent_df.copy()
    customer_clean = customer_df.copy()
    
    # Filter for successful transactions only
    agent_clean = agent_clean[agent_clean['Statut transaction'] == "COMPLETED"]
    customer_clean = customer_clean[customer_clean['Statut transaction'] == "COMPLETED"]
    
    logger.info(f"After filtering for COMPLETED transactions - Agent: {agent_clean.shape}, Customer: {customer_clean.shape}")
    
    # Remove trailing spaces from string columns
    agent_clean = _remove_trailing_spaces(agent_clean)
    customer_clean = _remove_trailing_spaces(customer_clean)
    
    return agent_clean, customer_clean


def _remove_trailing_spaces(df: pd.DataFrame) -> pd.DataFrame:
    """Remove trailing spaces from all string columns"""
    df_clean = df.copy()
    
    for column in df_clean.columns:
        if df_clean[column].dtype == 'object':
            # Convert to string and handle NaN values
            df_clean[column] = df_clean[column].astype(str).replace('nan', '')
            df_clean[column] = df_clean[column].str.strip()
    
    return df_clean


def combine_transactions(agent_df: pd.DataFrame, customer_df: pd.DataFrame) -> pd.DataFrame:
    """
    Combine agent and customer transactions, filter for relevant transaction types.
    
    Args:
        agent_df: Cleaned agent transactions
        customer_df: Cleaned customer transactions
    
    Returns:
        Combined and filtered transactions dataframe
    """
    logger.info("Combining transaction dataframes")
    
    # Concatenate dataframes
    combined_df = pd.concat([agent_df, customer_df], ignore_index=True)
    logger.info(f"Combined dataframe shape: {combined_df.shape}")
    
    # Filter for relevant transaction types
    relevant_types = ['CASH_IN', 'CASH_OUT', 'WALLET_TO_WALLET']
    combined_df = combined_df[combined_df['Type transaction'].isin(relevant_types)]
    logger.info(f"After filtering for relevant types: {combined_df.shape}")
    
    return combined_df


def create_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create separate date and time columns from datetime.
    
    Args:
        df: DataFrame with 'Date heure' column
    
    Returns:
        DataFrame with 'Date transaction' and 'Heure transaction' columns
    """
    logger.info("Creating date and time columns")
    
    df_processed = df.copy()
    
    # Create date and time columns
    df_processed['Heure transaction'] = pd.to_datetime(df_processed['Date heure']).dt.time
    df_processed['Date transaction'] = pd.to_datetime(df_processed['Date heure']).dt.date
    
    # Drop original datetime column
    df_processed = df_processed.drop(['Date heure'], axis=1)
    
    # Clean portfolio number columns
    df_processed['Numero porte feuille expediteur'] = (
        df_processed['Numero porte feuille expediteur']
        .fillna(0).astype(int).astype(str)
    )
    df_processed['Numero porte feuille destinataire'] = (
        df_processed['Numero porte feuille destinataire']
        .fillna(0).astype(int).astype(str)
    )
    
    logger.info("Date/time columns created successfully")
    return df_processed


if __name__ == "__main__":
    # Example usage
    try:
        # Load data
        agent_data, customer_data = load_transaction_data(
            'AGENT_28_07_2025.xlsx', 
            'CUSTOMER_28_07_2025.xlsx'
        )
        
        # Clean data
        agent_clean, customer_clean = clean_transaction_data(agent_data, customer_data)
        
        # Combine transactions
        transactions = combine_transactions(agent_clean, customer_clean)
        
        # Create datetime columns
        transactions = create_datetime_columns(transactions)
        
        print(f"Processing complete. Final dataset shape: {transactions.shape}")
        print(f"Transaction types: {transactions['Type transaction'].value_counts()}")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise 