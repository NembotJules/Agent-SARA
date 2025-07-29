"""
SARA Transaction Processing Pipeline - Production Version
======================================================

This module processes transaction data from various agencies and categorizes them
for analysis. It handles data loading, cleaning, agency identification, and
transaction categorization.

Author: Nembot Jules, DRI 
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

# Suppress logging...
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
        'Nom destinataire',
        'Numero porte feuille destinataire',
        'Nom portefeuille destinataire', 
                'Solde destinataire avant transaction', 
        'Solde destinataire apres transaction',
        'Type canal', 
        'Statut transaction'
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
        ValueError: If expected columns are missing
    """
    logger.info(f"Validating and filtering schema for {data_type} data")
    
    # Get all current columns
    current_columns = list(df.columns)
    
    # Find expected columns that exist in the dataframe
    available_expected_columns = [col for col in DataSchema.TRANSACTION_COLUMNS if col in current_columns]
    
    # Check if we're missing any expected columns
    missing_expected = [col for col in DataSchema.TRANSACTION_COLUMNS if col not in available_expected_columns]
    if missing_expected:
        raise ValueError(f"Missing expected columns in {data_type} data: {missing_expected}")
    
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
    
    # Filter dataframe to keep only expected columns (in the order they appear in TRANSACTION_COLUMNS)
    filtered_df = df[DataSchema.TRANSACTION_COLUMNS].copy()
    
    logger.info(f"Schema validation passed for {data_type} data")
    logger.info(f"Kept {len(DataSchema.TRANSACTION_COLUMNS)} expected columns, removed {len(current_columns) - len(DataSchema.TRANSACTION_COLUMNS)} columns")
    
    return filtered_df


def clean_transaction_data(agent_df: pd.DataFrame, customer_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Clean and preprocess transaction data.
    
    Args:
        agent_df: Agent transactions dataframe (already schema-filtered)
        customer_df: Customer transactions dataframe (already schema-filtered)
    
    Returns:
        Tuple of cleaned dataframes
    
    Raises:
        ValueError: If dataframes don't have expected number of columns
    """
    logger.info("Cleaning transaction data")
    
    # Validate input dataframes have expected number of columns
    expected_cols = len(DataSchema.TRANSACTION_COLUMNS)
    if len(agent_df.columns) != expected_cols:
        raise ValueError(f"Agent dataframe has {len(agent_df.columns)} columns, expected {expected_cols}")
    if len(customer_df.columns) != expected_cols:
        raise ValueError(f"Customer dataframe has {len(customer_df.columns)} columns, expected {expected_cols}")
    
    logger.info(f"Input validation passed - Both dataframes have {expected_cols} columns")
    
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
    
    # Final validation - ensure cleaned dataframes still have correct number of columns
    if len(agent_clean.columns) != expected_cols:
        raise ValueError(f"Cleaned agent dataframe has {len(agent_clean.columns)} columns, expected {expected_cols}")
    if len(customer_clean.columns) != expected_cols:
        raise ValueError(f"Cleaned customer dataframe has {len(customer_clean.columns)} columns, expected {expected_cols}")
    
    logger.info(f"Cleaning validation passed - Both cleaned dataframes have {expected_cols} columns")
    
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
    
    Raises:
        ValueError: If combination validation fails
    """
    logger.info("Combining transaction dataframes")
    
    # Store original counts for validation
    original_agent_rows = len(agent_df)
    original_customer_rows = len(customer_df)
    expected_total_rows = original_agent_rows + original_customer_rows
    expected_cols = len(DataSchema.TRANSACTION_COLUMNS)
    
    logger.info(f"Input dataframes - Agent: {original_agent_rows} rows, Customer: {original_customer_rows} rows")
    
    # Concatenate dataframes
    combined_df = pd.concat([agent_df, customer_df], ignore_index=True)
    logger.info(f"Combined dataframe shape: {combined_df.shape}")
    
    # Validation checks
    if len(combined_df.columns) != expected_cols:
        raise ValueError(f"Combined dataframe has {len(combined_df.columns)} columns, expected {expected_cols}")
    
    if len(combined_df) != expected_total_rows:
        raise ValueError(f"Combined dataframe has {len(combined_df)} rows, expected {expected_total_rows} (sum of input dataframes)")
    
    logger.info(f"Combination validation passed - {len(combined_df)} rows = {original_agent_rows} + {original_customer_rows}")
    
    # Filter for relevant transaction types
    relevant_types = ['CASH_IN', 'CASH_OUT', 'WALLET_TO_WALLET']
    rows_before_filter = len(combined_df)
    combined_df = combined_df[combined_df['Type transaction'].isin(relevant_types)]
    rows_after_filter = len(combined_df)
    
    logger.info(f"After filtering for relevant types: {combined_df.shape}")
    logger.info(f"Filtered out {rows_before_filter - rows_after_filter} rows with irrelevant transaction types")
    
    # Final validation - ensure filtered dataframe still has correct number of columns
    if len(combined_df.columns) != expected_cols:
        raise ValueError(f"Filtered combined dataframe has {len(combined_df.columns)} columns, expected {expected_cols}")
    
    return combined_df


def create_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create separate date and time columns from datetime.
    
    Args:
        df: DataFrame with 'Date heure' column
    
    Returns:
        DataFrame with 'Date transaction' and 'Heure transaction' columns
    
    Raises:
        ValueError: If datetime processing affects expected column count
    """
    logger.info("Creating date and time columns")
    
    # Validate input has expected columns
    expected_input_cols = len(DataSchema.TRANSACTION_COLUMNS)
    if len(df.columns) != expected_input_cols:
        raise ValueError(f"Input dataframe has {len(df.columns)} columns, expected {expected_input_cols}")
    
    original_rows = len(df)
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
    
    # Validation checks
    # After dropping 'Date heure' and adding 'Date transaction' and 'Heure transaction', 
    # we should have expected_input_cols - 1 + 2 = expected_input_cols + 1 columns
    expected_output_cols = expected_input_cols + 1
    if len(df_processed.columns) != expected_output_cols:
        raise ValueError(f"Output dataframe has {len(df_processed.columns)} columns, expected {expected_output_cols}")
    
    if len(df_processed) != original_rows:
        raise ValueError(f"Row count changed during datetime processing: {len(df_processed)} vs {original_rows}")
    
    logger.info(f"Date/time columns created successfully - {expected_output_cols} columns, {len(df_processed)} rows")
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