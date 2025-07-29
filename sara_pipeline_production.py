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
import smtplib
import os
import re
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

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


class EmailConfig:
    """
    Email configuration for sending generated reports
    """
    
    # Default SMTP settings (can be overridden via environment variables)
    DEFAULT_SMTP_SERVER = "smtp.gmail.com"
    DEFAULT_SMTP_PORT = 587
    
    @classmethod
    def get_config(cls) -> Dict[str, str]:
        """
        Get email configuration from environment variables with fallbacks.
        
        Required environment variables:
        - SARA_EMAIL_SENDER: Sender email address
        - SARA_EMAIL_PASSWORD: Email password or app password  
        - SARA_EMAIL_RECIPIENTS: Comma-separated list of recipient emails
        
        Optional environment variables:
        - SARA_SMTP_SERVER: SMTP server (default: smtp.gmail.com)
        - SARA_SMTP_PORT: SMTP port (default: 587)
        
        Returns:
            Dictionary with email configuration
            
        Raises:
            ValueError: If required configuration is missing
        """
        config = {
            'smtp_server': os.getenv('SARA_SMTP_SERVER', cls.DEFAULT_SMTP_SERVER),
            'smtp_port': int(os.getenv('SARA_SMTP_PORT', cls.DEFAULT_SMTP_PORT)),
            'sender_email': os.getenv('SARA_EMAIL_SENDER'),
            'sender_password': os.getenv('SARA_EMAIL_PASSWORD'),
            'recipients': os.getenv('SARA_EMAIL_RECIPIENTS', '').split(',') if os.getenv('SARA_EMAIL_RECIPIENTS') else []
        }
        
        # Validate required fields
        missing_fields = []
        if not config['sender_email']:
            missing_fields.append('SARA_EMAIL_SENDER')
        if not config['sender_password']:
            missing_fields.append('SARA_EMAIL_PASSWORD')
        if not config['recipients']:
            missing_fields.append('SARA_EMAIL_RECIPIENTS')
            
        if missing_fields:
            raise ValueError(
                f"Missing required email configuration: {', '.join(missing_fields)}. "
                f"Please set these environment variables."
            )
        
        # Clean up recipients list
        config['recipients'] = [email.strip() for email in config['recipients'] if email.strip()]
        
        return config


# ============================================================================
# DATA LOADING AND VALIDATION FUNCTIONS
# ============================================================================

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


# ============================================================================
# AGENCY IDENTIFICATION FUNCTIONS
# ============================================================================

class AgencyIdentifier:
    """
    Contains all agency identification functions and constants
    """
    
    # Super agent names
    HOP_SUPER_AGENT = 'HOP SERVICESARL'
    EXPRESS_UNION_SUPER_AGENT = 'EXPRESS UNIONSA'
    EMI_MONEY_SUPER_AGENT = 'EMI MONEY SARL'
    MULTISERVICE_SUPER_AGENT = 'MULTI-SERVICE SARL'
    INSTANT_TRANSFER_SUPER_AGENT = 'INSTANTTRANSFER SARL'
    
    @staticmethod
    def is_hop_agency(name: str) -> bool:
        """
        Check if a name matches HOP agency patterns.
        
        Args:
            name: Agency name to check
            
        Returns:
            True if name matches HOP patterns...
        """
        if not isinstance(name, str):
            return False
        name = name.strip().upper()
        return (name.startswith('HOP') or 
                name == 'HOP SERVICESARL' or
                name == 'ALBINEHOP' or 
                name == 'DTN NANGA NANGA JUNIORHOP SIEGE DCM')
    
    @staticmethod
    def is_emi_money_agency(name: str) -> bool:
        """
        Check if a name matches EMI Money agency patterns.
        
        Args:
            name: Agency name to check
            
        Returns:
            True if name matches EMI Money patterns
        """
        if not isinstance(name, str):
            return False
        name = name.strip()
        patterns = ['Emi money', 'EMI MONEY', 'Sarl Emi', 'Sarl Emi money', 'EMI MONEY SARL']
        return any(pattern in name for pattern in patterns)
    
    @staticmethod
    def is_express_union_agency(name: str) -> bool:
        """
        Check if a name matches Express Union agency patterns.
        
        Args:
            name: Agency name to check
            
        Returns:
            True if name matches Express Union patterns
        """
        if not isinstance(name, str):
            return False
        name = name.strip()
        return name.startswith('EU ') or name.startswith('EUF ') or name == 'EXPRESS UNIONSA'
    
    @staticmethod
    def is_instant_transfer_agency(name: str) -> bool:
        """
        Check if a name matches Instant Transfer agency patterns.
        
        Args:
            name: Agency name to check
            
        Returns:
            True if name matches Instant Transfer patterns
        """
        if not isinstance(name, str):
            return False
        name = name.strip()
        return name.startswith('IT ') or name == 'INSTANTTRANSFER SARL'
    
    @staticmethod
    def is_multiservice_agency(name: str) -> bool:
        """
        Check if a name matches Multi-Service agency patterns.
        
        Args:
            name: Agency name to check
            
        Returns:
            True if name matches Multi-Service patterns
        """
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
        """
        Check if a name matches Muffa agency patterns.
        
        Args:
            name: Agency name to check
            
        Returns:
            True if name matches Muffa patterns
        """
        if not isinstance(name, str):
            return False
        name = name.strip().upper()
        return name.startswith('MUFFA')
    
    @staticmethod
    def is_call_box_agency(name: str) -> bool:
        """
        Check if a name matches Call Box agency patterns.
        
        Args:
            name: Agency name to check
            
        Returns:
            True if name matches Call Box patterns
        """
        if not isinstance(name, str):
            return False
        name = name.strip().upper()
        return name.startswith('CB')
    
    @staticmethod
    def is_any_agency(name: str) -> bool:
        """
        Check if a name matches any agency pattern.
        
        Args:
            name: Agency name to check
            
        Returns:
            True if name matches any agency pattern
        """
        return (AgencyIdentifier.is_hop_agency(name) or 
                AgencyIdentifier.is_emi_money_agency(name) or 
                AgencyIdentifier.is_express_union_agency(name) or 
                AgencyIdentifier.is_instant_transfer_agency(name) or
                AgencyIdentifier.is_multiservice_agency(name) or 
                AgencyIdentifier.is_muffa_agency(name) or 
                AgencyIdentifier.is_call_box_agency(name))


def filter_transactions_by_agency(df: pd.DataFrame, agency_check_func, agency_name: str) -> pd.DataFrame:
    """
    Filter transactions where either sender or receiver matches agency pattern.
    Only considers AGENT transactions to avoid false positives from CUSTOMER wallet names.
    
    Args:
        df: Transactions dataframe
        agency_check_func: Function to check if name belongs to agency
        agency_name: Name of agency for logging
    
    Returns:
        Filtered dataframe containing only AGENT transactions involving the agency
    """
    logger.info(f"Filtering transactions for {agency_name} agency")
    
    # First filter for AGENT transactions only
    agent_transactions = df[df['Type utilisateur transaction'] == 'AGENT']
    
    # Then apply agency pattern matching
    agency_transactions = agent_transactions[
        (agent_transactions['Nom portefeuille expediteur'].apply(agency_check_func)) |
        (agent_transactions['Nom portefeuille destinataire'].apply(agency_check_func))
    ].copy()
    
    logger.info(f"{agency_name} transactions found: {len(agency_transactions)} out of {len(df)} total ({len(agency_transactions)} out of {len(agent_transactions)} AGENT transactions)")
    
    # Validate filtered dataframe maintains structure
    if len(agency_transactions) > 0:
        expected_cols = len(df.columns)
        if len(agency_transactions.columns) != expected_cols:
            raise ValueError(f"{agency_name} filtered dataframe has {len(agency_transactions.columns)} columns, expected {expected_cols}")
        
        # Validate all transactions are AGENT transactions
        user_types = agency_transactions['Type utilisateur transaction'].unique()
        if len(user_types) != 1 or user_types[0] != 'AGENT':
            raise ValueError(f"{agency_name} filtered dataframe contains non-AGENT transactions: {user_types}")
    
    return agency_transactions


def identify_all_agency_transactions(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Identify and separate transactions for all agency types.
    
    Args:
        df: Combined transactions dataframe
    
    Returns:
        Dictionary of agency dataframes
    
    Raises:
        ValueError: If data integrity checks fail
    """
    logger.info("Identifying transactions for all agency types")
    
    # Count AGENT and CUSTOMER transactions for validation
    agent_transactions_count = len(df[df['Type utilisateur transaction'] == 'AGENT'])
    customer_transactions_count = len(df[df['Type utilisateur transaction'] == 'CUSTOMER'])
    total_expected = agent_transactions_count + customer_transactions_count
    
    logger.info(f"Input validation - AGENT: {agent_transactions_count}, CUSTOMER: {customer_transactions_count}, Total: {total_expected}")
    
    if len(df) != total_expected:
        raise ValueError(f"Unexpected user transaction types found. Expected {total_expected}, got {len(df)}")
    
    agency_dataframes = {}
    
    # Filter transactions for each agency type
    agency_dataframes['hop'] = filter_transactions_by_agency(
        df, AgencyIdentifier.is_hop_agency, "HOP"
    )
    
    agency_dataframes['emi_money'] = filter_transactions_by_agency(
        df, AgencyIdentifier.is_emi_money_agency, "EMI Money"
    )
    
    agency_dataframes['express_union'] = filter_transactions_by_agency(
        df, AgencyIdentifier.is_express_union_agency, "Express Union"
    )
    
    agency_dataframes['instant_transfer'] = filter_transactions_by_agency(
        df, AgencyIdentifier.is_instant_transfer_agency, "Instant Transfer"
    )
    
    agency_dataframes['multiservice'] = filter_transactions_by_agency(
        df, AgencyIdentifier.is_multiservice_agency, "Multi-Service"
    )
    
    agency_dataframes['muffa'] = filter_transactions_by_agency(
        df, AgencyIdentifier.is_muffa_agency, "Muffa"
    )
    
    agency_dataframes['call_box'] = filter_transactions_by_agency(
        df, AgencyIdentifier.is_call_box_agency, "Call Box"
    )
    
    # Data integrity validation
    total_agency_transactions = sum(len(agency_df) for agency_df in agency_dataframes.values())
    logger.info(f"Total agency transactions identified: {total_agency_transactions}")
    
    # Check for transactions not captured by any agency
    all_agency_indices = set()
    overlapping_indices = set()
    
    # Track overlaps
    for agency_name, agency_df in agency_dataframes.items():
        agency_indices = set(agency_df.index)
        # Check for overlaps with previously seen indices
        overlap_with_existing = agency_indices.intersection(all_agency_indices)
        if overlap_with_existing:
            overlapping_indices.update(overlap_with_existing)
            logger.warning(f"{agency_name} has {len(overlap_with_existing)} overlapping transactions")
        
        all_agency_indices.update(agency_indices)
    
    # Count unique transactions
    unique_agency_transactions = len(all_agency_indices)
    if overlapping_indices:
        logger.warning(f"Found {len(overlapping_indices)} transactions counted in multiple agencies")
        logger.warning(f"Unique agency transactions: {unique_agency_transactions} (vs {total_agency_transactions} total)")
    
    non_agency_transactions = df[~df.index.isin(all_agency_indices)]
    non_agency_count = len(non_agency_transactions)
    logger.info(f"Transactions not captured by any agency: {non_agency_count}")
    
    # Update validation to use unique count
    total_agency_transactions = unique_agency_transactions
    
    # CRITICAL VALIDATION 1: Sum of agency transactions should equal number of AGENT transactions
    if total_agency_transactions != agent_transactions_count:
        raise ValueError(f"Agency transaction count mismatch! "
                        f"Sum of agency transactions: {total_agency_transactions}, "
                        f"Expected AGENT transactions: {agent_transactions_count}")
    
    logger.info(f"✅ Agency count validation passed: {total_agency_transactions} = {agent_transactions_count}")
    
    # CRITICAL VALIDATION 2: All non-agency transactions should be CUSTOMER transactions
    non_agency_user_types = non_agency_transactions['Type utilisateur transaction'].unique()
    if len(non_agency_user_types) != 1 or non_agency_user_types[0] != 'CUSTOMER':
        raise ValueError(f"Non-agency transactions contain unexpected user types: {non_agency_user_types}. "
                        f"Expected only 'CUSTOMER'")
    
    # Additional validation: Count should match
    if non_agency_count != customer_transactions_count:
        raise ValueError(f"Non-agency transaction count mismatch! "
                        f"Non-agency transactions: {non_agency_count}, "
                        f"Expected CUSTOMER transactions: {customer_transactions_count}")
    
    logger.info(f"✅ Customer count validation passed: {non_agency_count} = {customer_transactions_count}")
    
    # Final comprehensive validation
    total_validated = total_agency_transactions + non_agency_count
    if total_validated != len(df):
        raise ValueError(f"Total validation failed! "
                        f"Agency + Non-agency: {total_validated}, "
                        f"Total input: {len(df)}")
    
    logger.info(f"✅ Complete validation passed: {total_agency_transactions} (agency) + {non_agency_count} (customer) = {total_validated} total")
    
    return agency_dataframes


# ============================================================================
# TRANSACTION CATEGORIZATION FUNCTIONS
# ============================================================================

def categorize_transactions(df: pd.DataFrame, is_agency_func, super_agent: str = None) -> pd.DataFrame:
    """
    Categorizes transactions based on type and participants.
    
    Args:
        df: DataFrame containing transactions
        is_agency_func: Function that takes a name and returns True if it belongs to the agency
        super_agent: Name of the super agent (optional)
        
    Returns:
        DataFrame with categorized transaction types
    
    Raises:
        ValueError: If input validation fails
    """
    logger.info(f"Categorizing transactions for agency (super_agent: {super_agent})")
    
    # Validate input
    if len(df) == 0:
        logger.warning("Empty dataframe provided for categorization")
        return df.copy()
    
    original_shape = df.shape
    df_categorized = df.copy()
    
    # Replace basic transaction types
    df_categorized['Type transaction'] = df_categorized['Type transaction'].replace({
        'CASH_IN': 'Approvisionement',
        'CASH_OUT': 'Versement bancaire'
    })
    
    # Handle WALLET_TO_WALLET cases
    wallet_mask = df_categorized['Type transaction'] == 'WALLET_TO_WALLET'
    wallet_transactions = wallet_mask.sum()
    logger.info(f"Processing {wallet_transactions} WALLET_TO_WALLET transactions")
    
    if wallet_transactions > 0:
        # Case 1: From agency or super agent to non-agency (Dépot)
        depot_mask = (
            wallet_mask & 
            ((df_categorized['Nom portefeuille expediteur'].apply(is_agency_func)) | 
             (df_categorized['Nom portefeuille expediteur'] == super_agent if super_agent else False)) &
            ~df_categorized['Nom portefeuille destinataire'].apply(is_agency_func)
        )
        depot_count = depot_mask.sum()
        df_categorized.loc[depot_mask, 'Type transaction'] = 'Dépot'
        
        # Case 2: From non-agency and non-super agent to agency (Retrait)
        retrait_mask = (
            wallet_mask &
            ~df_categorized['Nom portefeuille expediteur'].apply(is_agency_func) &
            (df_categorized['Nom portefeuille expediteur'] != super_agent if super_agent else True) &
            df_categorized['Nom portefeuille destinataire'].apply(is_agency_func)
        )
        retrait_count = retrait_mask.sum()
        df_categorized.loc[retrait_mask, 'Type transaction'] = 'Retrait'
        
        # Super agent specific logic (only if super_agent is provided)
        appro_count = 0
        decharge_count = 0
        if super_agent:
            # Case 3: From super agent to its own agency type (Approvisionement)
            appro_mask = (
                wallet_mask &
                (df_categorized['Nom portefeuille expediteur'] == super_agent) &
                df_categorized['Nom portefeuille destinataire'].apply(is_agency_func)
            )
            appro_count = appro_mask.sum()
            df_categorized.loc[appro_mask, 'Type transaction'] = 'Approvisionement'

            # Case 4: From agency to its own super agent type (Décharge)
            decharge_mask = (
                wallet_mask &
                df_categorized['Nom portefeuille expediteur'].apply(is_agency_func) &
                (df_categorized['Nom portefeuille destinataire'] == super_agent)
            )
            decharge_count = decharge_mask.sum()
            df_categorized.loc[decharge_mask, 'Type transaction'] = 'Décharge'

        # Handle transactions between different types of agents (inter-agency)
        inter_agency_mask = wallet_mask & (
            df_categorized['Nom portefeuille expediteur'].apply(AgencyIdentifier.is_any_agency) &
            df_categorized['Nom portefeuille destinataire'].apply(AgencyIdentifier.is_any_agency)
        )
        
        # For inter-agency transactions, classify based on balance changes
        # Money going out (balance decreases) = Dépot from perspective of sender
        inter_depot_mask = (
            inter_agency_mask &
            (df_categorized['Solde expediteur avant transaction'] > df_categorized['Solde expediteur apres transaction'])
        )
        inter_depot_count = inter_depot_mask.sum()
        df_categorized.loc[inter_depot_mask, 'Type transaction'] = 'Dépot'
        
        # Money coming in (balance increases) = Retrait from perspective of sender  
        inter_retrait_mask = (
            inter_agency_mask &
            (df_categorized['Solde expediteur avant transaction'] < df_categorized['Solde expediteur apres transaction'])
        )
        inter_retrait_count = inter_retrait_mask.sum()
        df_categorized.loc[inter_retrait_mask, 'Type transaction'] = 'Retrait'
        
        # Log categorization summary
        logger.info(f"WALLET_TO_WALLET categorization summary:")
        logger.info(f"  - Dépot: {depot_count + inter_depot_count}")
        logger.info(f"  - Retrait: {retrait_count + inter_retrait_count}")
        if super_agent:
            logger.info(f"  - Approvisionement (super agent): {appro_count}")
            logger.info(f"  - Décharge (super agent): {decharge_count}")
    
    # Final validation
    if df_categorized.shape != original_shape:
        raise ValueError(f"Shape changed during categorization: {original_shape} -> {df_categorized.shape}")
    
    # Check for remaining WALLET_TO_WALLET transactions
    remaining_wallet = (df_categorized['Type transaction'] == 'WALLET_TO_WALLET').sum()
    if remaining_wallet > 0:
        logger.warning(f"{remaining_wallet} WALLET_TO_WALLET transactions could not be categorized")
    
    # Log final transaction type distribution
    final_types = df_categorized['Type transaction'].value_counts()
    logger.info(f"Final transaction types: {dict(final_types)}")
    
    return df_categorized


def categorize_all_agency_transactions(agency_dataframes: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """
    Apply transaction categorization to all agency dataframes.
    
    Args:
        agency_dataframes: Dictionary of agency dataframes
    
    Returns:
        Dictionary of categorized agency dataframes
        
    Raises:
        ValueError: If categorization fails for any agency
    """
    logger.info("Categorizing transactions for all agencies")
    
    categorized_dataframes = {}
    
    # Define agency functions and super agents
    agency_configs = {
        'hop': (AgencyIdentifier.is_hop_agency, AgencyIdentifier.HOP_SUPER_AGENT),
        'emi_money': (AgencyIdentifier.is_emi_money_agency, None),  # No super agent
        'express_union': (AgencyIdentifier.is_express_union_agency, AgencyIdentifier.EXPRESS_UNION_SUPER_AGENT),
        'instant_transfer': (AgencyIdentifier.is_instant_transfer_agency, AgencyIdentifier.INSTANT_TRANSFER_SUPER_AGENT),
        'multiservice': (AgencyIdentifier.is_multiservice_agency, AgencyIdentifier.MULTISERVICE_SUPER_AGENT),
        'muffa': (AgencyIdentifier.is_muffa_agency, None),  # No super agent
        'call_box': (AgencyIdentifier.is_call_box_agency, None)  # No super agent
    }
    
    # Apply categorization to each agency
    for agency_name, agency_df in agency_dataframes.items():
        if agency_name not in agency_configs:
            logger.warning(f"No configuration found for agency: {agency_name}")
            categorized_dataframes[agency_name] = agency_df.copy()
            continue
            
        is_agency_func, super_agent = agency_configs[agency_name]
        
        try:
            categorized_df = categorize_transactions(agency_df, is_agency_func, super_agent)
            categorized_dataframes[agency_name] = categorized_df
            logger.info(f"✅ Categorization completed for {agency_name}: {len(categorized_df)} transactions")
            
        except Exception as e:
            logger.error(f"Failed to categorize transactions for {agency_name}: {str(e)}")
            raise ValueError(f"Categorization failed for {agency_name}: {str(e)}")
    
    # Validation: ensure total transaction count is preserved
    original_total = sum(len(df) for df in agency_dataframes.values())
    categorized_total = sum(len(df) for df in categorized_dataframes.values())
    
    if original_total != categorized_total:
        raise ValueError(f"Transaction count mismatch after categorization: {original_total} -> {categorized_total}")
    
    logger.info(f"✅ All agency categorization completed. Total transactions: {categorized_total}")
    
    return categorized_dataframes


# ============================================================================
# SOLDE COLUMNS CREATION FUNCTIONS
# ============================================================================

def create_solde_columns(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Creates unified agent perspective columns for all agency types.
    Transforms transaction data to show everything from the agency's perspective.
    
    Args:
        df: DataFrame containing categorized agency transactions
    
    Returns:
        Tuple of (processed_df, annuaire_df, bank_annuaire_df)
        - processed_df: DataFrame with agency-centric columns
        - annuaire_df: Directory of wallet name/number mappings
        - bank_annuaire_df: Directory of bank account mappings
    
    Raises:
        ValueError: If input validation fails
    """
    logger.info("Creating agency-centric solde columns")
    
    # Validate input
    if len(df) == 0:
        logger.warning("Empty dataframe provided for solde column creation")
        return df.copy(), pd.DataFrame(), pd.DataFrame()
    
    original_shape = df.shape
    
    # Helper function to check if a name belongs to any agency type
    def is_any_agency(name):
        return (AgencyIdentifier.is_hop_agency(name) or 
                AgencyIdentifier.is_emi_money_agency(name) or 
                AgencyIdentifier.is_express_union_agency(name) or 
                AgencyIdentifier.is_instant_transfer_agency(name) or
                AgencyIdentifier.is_multiservice_agency(name) or 
                AgencyIdentifier.is_muffa_agency(name) or 
                AgencyIdentifier.is_call_box_agency(name))
    
    # Create annuaire (directory) with unique Nom/Numero pairs
    expediteur_pairs = df[['Nom portefeuille expediteur', 'Numero porte feuille expediteur']].rename(
        columns={'Nom portefeuille expediteur': 'Nom portefeuille', 
                'Numero porte feuille expediteur': 'Numero portefeuille'}
    )
    destinataire_pairs = df[['Nom portefeuille destinataire', 'Numero porte feuille destinataire']].rename(
        columns={'Nom portefeuille destinataire': 'Nom portefeuille',
                'Numero porte feuille destinataire': 'Numero portefeuille'}
    )
    annuaire_df = pd.concat([expediteur_pairs, destinataire_pairs]).drop_duplicates().dropna()
    logger.info(f"Created wallet directory with {len(annuaire_df)} unique wallet mappings")

    # Create bank annuaire for bank accounts
    bank_annuaire = df[['Nom destinataire', 'Compte bancaire destinataire']].dropna().drop_duplicates()
    logger.info(f"Created bank directory with {len(bank_annuaire)} unique bank account mappings")

    # Create new agency-centric columns
    df_processed = df.copy()
    df_processed['Nom portefeuille'] = None
    df_processed['Numero portefeuille'] = None
    df_processed['Solde avant transaction'] = None
    df_processed['Solde après transaction'] = None
    df_processed['Partenaire transaction'] = None
    df_processed['Numero portefeuille partenaire transaction'] = None
    df_processed['Compte bancaire partenaire'] = None

    # Case 1: When agency is expediteur (sender)
    expediteur_mask = df_processed['Nom portefeuille expediteur'].apply(is_any_agency)
    expediteur_count = expediteur_mask.sum()
    
    df_processed.loc[expediteur_mask, 'Nom portefeuille'] = df_processed.loc[expediteur_mask, 'Nom portefeuille expediteur']
    df_processed.loc[expediteur_mask, 'Numero portefeuille'] = df_processed.loc[expediteur_mask, 'Numero porte feuille expediteur']
    df_processed.loc[expediteur_mask, 'Solde avant transaction'] = df_processed.loc[expediteur_mask, 'Solde expediteur avant transaction']
    df_processed.loc[expediteur_mask, 'Solde après transaction'] = df_processed.loc[expediteur_mask, 'Solde expediteur apres transaction']
    df_processed.loc[expediteur_mask, 'Partenaire transaction'] = df_processed.loc[expediteur_mask, 'Nom portefeuille destinataire']
    df_processed.loc[expediteur_mask, 'Numero portefeuille partenaire transaction'] = df_processed.loc[expediteur_mask, 'Numero porte feuille destinataire']
    
    # Add bank account info when available (for expediteur case)
    bank_mask_exp = expediteur_mask & df_processed['Compte bancaire destinataire'].notna()
    df_processed.loc[bank_mask_exp, 'Compte bancaire partenaire'] = df_processed.loc[bank_mask_exp, 'Compte bancaire destinataire']
    df_processed.loc[bank_mask_exp, 'Partenaire transaction'] = df_processed.loc[bank_mask_exp, 'Nom destinataire']

    # Case 2: When agency is destinataire (receiver)
    destinataire_mask = df_processed['Nom portefeuille destinataire'].apply(is_any_agency)
    destinataire_count = destinataire_mask.sum()
    
    df_processed.loc[destinataire_mask, 'Nom portefeuille'] = df_processed.loc[destinataire_mask, 'Nom portefeuille destinataire']
    df_processed.loc[destinataire_mask, 'Numero portefeuille'] = df_processed.loc[destinataire_mask, 'Numero porte feuille destinataire']
    df_processed.loc[destinataire_mask, 'Solde avant transaction'] = df_processed.loc[destinataire_mask, 'Solde destinataire avant transaction']
    df_processed.loc[destinataire_mask, 'Solde après transaction'] = df_processed.loc[destinataire_mask, 'Solde destinataire apres transaction']
    df_processed.loc[destinataire_mask, 'Partenaire transaction'] = df_processed.loc[destinataire_mask, 'Nom portefeuille expediteur']
    df_processed.loc[destinataire_mask, 'Numero portefeuille partenaire transaction'] = df_processed.loc[destinataire_mask, 'Numero porte feuille expediteur']

    logger.info(f"Agency perspective applied: {expediteur_count} as sender, {destinataire_count} as receiver")

    # Sort by wallet name and date/time to see progression
    df_processed = df_processed.sort_values(['Nom portefeuille', 'Date transaction', 'Heure transaction'])

    # Drop old columns that are no longer needed
    columns_to_drop = [
        'Nom portefeuille expediteur', 'Nom portefeuille destinataire',
        'Solde expediteur avant transaction', 'Solde expediteur apres transaction', 
        'Solde destinataire avant transaction', 'Solde destinataire apres transaction',
        'Numero porte feuille expediteur', 'Numero porte feuille destinataire',
        'Compte bancaire destinataire', 'Nom destinataire'
    ]
    df_processed = df_processed.drop(columns=columns_to_drop)

    # Final validation
    expected_cols = original_shape[1] - len(columns_to_drop) + 7  # Dropped 10, added 7
    if len(df_processed.columns) != expected_cols:
        raise ValueError(f"Column count mismatch after solde creation: expected {expected_cols}, got {len(df_processed.columns)}")
    
    if len(df_processed) != original_shape[0]:
        raise ValueError(f"Row count changed during solde creation: {original_shape[0]} -> {len(df_processed)}")

    logger.info(f"✅ Solde columns created successfully: {len(df_processed)} rows, {len(df_processed.columns)} columns")

    return df_processed, annuaire_df, bank_annuaire


def create_solde_columns_for_all_agencies(categorized_dataframes: Dict[str, pd.DataFrame]) -> Dict[str, Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]]:
    """
    Apply solde column creation to all agency dataframes.
    
    Args:
        categorized_dataframes: Dictionary of categorized agency dataframes
    
    Returns:
        Dictionary with agency names as keys and (processed_df, annuaire_df, bank_annuaire_df) tuples as values
        
    Raises:
        ValueError: If solde creation fails for any agency
    """
    logger.info("Creating solde columns for all agencies")
    
    processed_agencies = {}
    
    for agency_name, agency_df in categorized_dataframes.items():
        try:
            processed_df, annuaire_df, bank_annuaire_df = create_solde_columns(agency_df)
            processed_agencies[agency_name] = (processed_df, annuaire_df, bank_annuaire_df)
            logger.info(f"✅ Solde columns created for {agency_name}: {len(processed_df)} transactions")
            
        except Exception as e:
            logger.error(f"Failed to create solde columns for {agency_name}: {str(e)}")
            raise ValueError(f"Solde creation failed for {agency_name}: {str(e)}")
    
    # Validation: ensure total transaction count is preserved
    original_total = sum(len(df) for df in categorized_dataframes.values())
    processed_total = sum(len(result[0]) for result in processed_agencies.values())
    
    if original_total != processed_total:
        raise ValueError(f"Transaction count mismatch after solde creation: {original_total} -> {processed_total}")
    
    logger.info(f"✅ All agency solde columns created. Total transactions: {processed_total}")
    
    return processed_agencies


def reorder_final_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reorder columns to match the final expected output format.
    
    Args:
        df: DataFrame with agency-centric columns
    
    Returns:
        DataFrame with columns in final order
        
    Raises:
        ValueError: If expected columns are missing
    """
    # Get available columns that match our expected final columns
    available_final_columns = [col for col in DataSchema.FINAL_COLUMNS_ORDER if col in df.columns]
    
    # Check for missing expected columns
    missing_columns = [col for col in DataSchema.FINAL_COLUMNS_ORDER if col not in df.columns]
    if missing_columns:
        logger.warning(f"Missing expected final columns: {missing_columns}")
    
    # Reorder to final format
    df_final = df[available_final_columns].copy()
    
    logger.info(f"Reordered to final format: {len(available_final_columns)} columns")
    
    return df_final


def finalize_all_agency_data(processed_agencies: Dict[str, Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]]) -> Dict[str, Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]]:
    """
    Apply final column ordering and sorting to all agency dataframes.
    
    Args:
        processed_agencies: Dictionary of processed agency data
    
    Returns:
        Dictionary with finalized agency dataframes
    """
    logger.info("Finalizing all agency dataframes")
    
    finalized_agencies = {}
    
    for agency_name, (processed_df, annuaire_df, bank_annuaire_df) in processed_agencies.items():
        try:
            # Reorder columns
            final_df = reorder_final_columns(processed_df)
            
            # Sort by agency name, date, and time
            final_df = final_df.sort_values(['Nom portefeuille', 'Date transaction', 'Heure transaction'])
            
            finalized_agencies[agency_name] = (final_df, annuaire_df, bank_annuaire_df)
            logger.info(f"✅ Finalized {agency_name}: {len(final_df)} transactions with {len(final_df.columns)} columns")
            
        except Exception as e:
            logger.error(f"Failed to finalize {agency_name}: {str(e)}")
            raise ValueError(f"Finalization failed for {agency_name}: {str(e)}")
    
    logger.info("✅ All agency dataframes finalized")
    
    return finalized_agencies


# ============================================================================
# DATA EXPORT FUNCTIONS
# ============================================================================

def export_agency_data_to_excel(
    agency_name: str, 
    final_df: pd.DataFrame, 
    annuaire_df: pd.DataFrame, 
    bank_annuaire_df: pd.DataFrame,
    output_dir: str = "."
) -> str:
    """
    Export a single agency's data to Excel file.
    
    Args:
        agency_name: Name of the agency
        final_df: Finalized agency transactions dataframe
        annuaire_df: Wallet directory dataframe
        bank_annuaire_df: Bank directory dataframe
        output_dir: Directory to save the file (default: current directory)
    
    Returns:
        Path to the created Excel file
        
    Raises:
        ValueError: If export fails
    """
    # Create filename following the original naming convention
    filename = f"transactions_{agency_name}_28_07_2025.xlsx"
    filepath = Path(output_dir) / filename
    
    logger.info(f"Exporting {agency_name} data to {filepath}")
    
    try:
        # Validate data before export
        if len(final_df) == 0:
            logger.warning(f"No transactions to export for {agency_name}")
            return str(filepath)
        
        # Create Excel writer with multiple sheets
        with pd.ExcelWriter(filepath, engine='xlsxwriter') as writer:
            # Main transactions sheet
            final_df.to_excel(writer, sheet_name='Transactions', index=False)
            
            # Wallet directory sheet (if not empty)
            if len(annuaire_df) > 0:
                annuaire_df.to_excel(writer, sheet_name='Annuaire_Portefeuilles', index=False)
            
            # Bank directory sheet (if not empty)
            if len(bank_annuaire_df) > 0:
                bank_annuaire_df.to_excel(writer, sheet_name='Annuaire_Banques', index=False)
            
            # Summary sheet
            summary_data = {
                'Metric': [
                    'Total Transactions',
                    'Transaction Types',
                    'Date Range Start',
                    'Date Range End', 
                    'Unique Wallets',
                    'Unique Bank Accounts'
                ],
                'Value': [
                    len(final_df),
                    ', '.join(final_df['Type transaction'].value_counts().index.tolist()),
                    final_df['Date transaction'].min() if len(final_df) > 0 else 'N/A',
                    final_df['Date transaction'].max() if len(final_df) > 0 else 'N/A',
                    len(annuaire_df),
                    len(bank_annuaire_df)
                ]
            }
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        logger.info(f"✅ Successfully exported {agency_name}: {len(final_df)} transactions to {filepath}")
        return str(filepath)
        
    except Exception as e:
        logger.error(f"Failed to export {agency_name} data: {str(e)}")
        raise ValueError(f"Export failed for {agency_name}: {str(e)}")


def export_all_agency_data(
    final_agency_data: Dict[str, Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]],
    output_dir: str = "."
) -> Dict[str, str]:
    """
    Export all agency data to Excel files.
    
    Args:
        final_agency_data: Dictionary of finalized agency data
        output_dir: Directory to save files (default: current directory)
    
    Returns:
        Dictionary mapping agency names to their exported file paths
        
    Raises:
        ValueError: If any export fails
    """
    logger.info(f"Exporting all agency data to directory: {output_dir}")
    
    # Ensure output directory exists
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    exported_files = {}
    export_summary = {
        'total_agencies': len(final_agency_data),
        'total_transactions': 0,
        'successful_exports': 0,
        'failed_exports': []
    }
    
    for agency_name, (final_df, annuaire_df, bank_annuaire_df) in final_agency_data.items():
        try:
            # Export agency data
            filepath = export_agency_data_to_excel(
                agency_name, final_df, annuaire_df, bank_annuaire_df, output_dir
            )
            
            exported_files[agency_name] = filepath
            export_summary['total_transactions'] += len(final_df)
            export_summary['successful_exports'] += 1
            
        except Exception as e:
            logger.error(f"Failed to export {agency_name}: {str(e)}")
            export_summary['failed_exports'].append(agency_name)
            # Don't raise immediately, try to export other agencies first
    
    # Final validation
    if export_summary['failed_exports']:
        failed_agencies = ', '.join(export_summary['failed_exports'])
        raise ValueError(f"Failed to export agencies: {failed_agencies}")
    
    logger.info(f"✅ All agency exports completed successfully:")
    logger.info(f"  - {export_summary['successful_exports']} agencies exported")
    logger.info(f"  - {export_summary['total_transactions']} total transactions")
    logger.info(f"  - Files saved to: {output_dir}")
    
    return exported_files


def create_master_summary_file(
    final_agency_data: Dict[str, Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]],
    exported_files: Dict[str, str],
    output_dir: str = "."
) -> str:
    """
    Create a master summary Excel file with overview of all agencies.
    
    Args:
        final_agency_data: Dictionary of finalized agency data
        exported_files: Dictionary of exported file paths
        output_dir: Directory to save the file
    
    Returns:
        Path to the master summary file
    """
    master_filepath = Path(output_dir) / "SARA_Master_Summary_28_07_2025.xlsx"
    
    logger.info(f"Creating master summary file: {master_filepath}")
    
    try:
        with pd.ExcelWriter(master_filepath, engine='xlsxwriter') as writer:
            # Agency overview sheet
            overview_data = []
            for agency_name, (final_df, annuaire_df, bank_annuaire_df) in final_agency_data.items():
                if len(final_df) > 0:
                    transaction_types = final_df['Type transaction'].value_counts()
                    overview_data.append({
                        'Agency': agency_name.upper(),
                        'Total_Transactions': len(final_df),
                        'Versement_bancaire': transaction_types.get('Versement bancaire', 0),
                        'Approvisionement': transaction_types.get('Approvisionement', 0),
                        'Depot': transaction_types.get('Dépot', 0),
                        'Retrait': transaction_types.get('Retrait', 0),
                        'Decharge': transaction_types.get('Décharge', 0),
                        'Unique_Wallets': len(annuaire_df),
                        'Bank_Accounts': len(bank_annuaire_df),
                        'Date_Range_Start': final_df['Date transaction'].min(),
                        'Date_Range_End': final_df['Date transaction'].max(),
                        'Excel_File': exported_files.get(agency_name, 'N/A')
                    })
            
            overview_df = pd.DataFrame(overview_data)
            overview_df.to_excel(writer, sheet_name='Agency_Overview', index=False)
            
            # Combined transaction types summary
            all_transaction_types = {}
            total_transactions = 0
            
            for agency_name, (final_df, _, _) in final_agency_data.items():
                if len(final_df) > 0:
                    transaction_types = final_df['Type transaction'].value_counts()
                    for trans_type, count in transaction_types.items():
                        all_transaction_types[trans_type] = all_transaction_types.get(trans_type, 0) + count
                    total_transactions += len(final_df)
            
            summary_data = {
                'Transaction_Type': list(all_transaction_types.keys()),
                'Total_Count': list(all_transaction_types.values()),
                'Percentage': [round(count/total_transactions*100, 2) for count in all_transaction_types.values()]
            }
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Transaction_Summary', index=False)
            
            # Export info sheet
            export_info = pd.DataFrame({
                'Export_Date': [pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')],
                'Total_Agencies': [len(final_agency_data)],
                'Total_Transactions': [total_transactions],
                'Output_Directory': [output_dir],
                'Pipeline_Version': ['Production v1.0']
            })
            export_info.to_excel(writer, sheet_name='Export_Info', index=False)
        
        logger.info(f"✅ Master summary file created: {master_filepath}")
        return str(master_filepath)
        
    except Exception as e:
        logger.error(f"Failed to create master summary: {str(e)}")
        raise ValueError(f"Master summary creation failed: {str(e)}")


# ============================================================================
# EMAIL FUNCTIONS
# ============================================================================

def send_email_with_attachments(
    subject: str,
    body: str, 
    attachment_paths: List[str],
    email_config: Optional[Dict[str, Union[str, int, List[str]]]] = None
) -> bool:
    """
    Send email with Excel file attachments.
    
    Args:
        subject: Email subject line
        body: Email body text (HTML supported)
        attachment_paths: List of file paths to attach
        email_config: Email configuration dict (if None, uses EmailConfig.get_config())
    
    Returns:
        True if successful, False otherwise
        
    Raises:
        ValueError: If email configuration is invalid
        Exception: If email sending fails
    """
    try:
        # Get email configuration
        if email_config is None:
            email_config = EmailConfig.get_config()
        
        logger.info(f"Preparing to send email to {len(email_config['recipients'])} recipients")
        
        # Create message container
        msg = MIMEMultipart()
        msg['From'] = email_config['sender_email']
        msg['To'] = ', '.join(email_config['recipients'])
        msg['Subject'] = subject
        
        # Add body to email
        msg.attach(MIMEText(body, 'html'))
        
        # Add attachments
        total_size = 0
        max_size = 25 * 1024 * 1024  # 25MB limit for most email providers
        
        for attachment_path in attachment_paths:
            if not os.path.exists(attachment_path):
                logger.warning(f"Attachment file not found: {attachment_path}")
                continue
            
            file_size = os.path.getsize(attachment_path)
            total_size += file_size
            
            if total_size > max_size:
                logger.warning(f"Attachment size limit exceeded. Skipping: {attachment_path}")
                continue
            
            filename = os.path.basename(attachment_path)
            logger.info(f"Adding attachment: {filename} ({file_size/1024/1024:.2f}MB)")
            
            with open(attachment_path, "rb") as attachment:
                # Instance of MIMEBase and named as part
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(attachment.read())
            
            # Encode file in ASCII characters to send by email    
            encoders.encode_base64(part)
            
            # Add header as key/value pair to attachment part
            part.add_header(
                'Content-Disposition',
                f'attachment; filename= {filename}',
            )
            
            # Attach the part to message
            msg.attach(part)
        
        # Create SMTP session
        server = smtplib.SMTP(email_config['smtp_server'], email_config['smtp_port'])
        server.starttls()  # Enable security
        server.login(email_config['sender_email'], email_config['sender_password'])
        
        # Send email
        text = msg.as_string()
        server.sendmail(email_config['sender_email'], email_config['recipients'], text)
        server.quit()
        
        logger.info(f"✅ Email sent successfully to {len(email_config['recipients'])} recipients")
        logger.info(f"  - Attachments: {len([p for p in attachment_paths if os.path.exists(p)])}")
        logger.info(f"  - Total size: {total_size/1024/1024:.2f}MB")
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to send email: {str(e)}")
        return False


def send_sara_report_email(
    exported_files: Dict[str, str],
    master_summary_file: str,
    processing_stats: Optional[Dict[str, Union[str, int]]] = None
) -> bool:
    """
    Send SARA transaction processing report via email.
    
    Args:
        exported_files: Dictionary mapping agency names to their Excel file paths
        master_summary_file: Path to the master summary Excel file
        processing_stats: Optional processing statistics for email body
    
    Returns:
        True if email sent successfully, False otherwise
    """
    try:
        # Check if email configuration is available
        try:
            email_config = EmailConfig.get_config()
        except ValueError as e:
            logger.warning(f"Email configuration not available: {str(e)}")
            logger.info("Skipping email send. To enable email, set the required environment variables:")
            logger.info("  - SARA_EMAIL_SENDER: Your sender email address")
            logger.info("  - SARA_EMAIL_PASSWORD: Your email password/app password")
            logger.info("  - SARA_EMAIL_RECIPIENTS: Comma-separated recipient emails")
            return False
        
        # Extract date from filename (format: transactions_hop_28_07_2025.xlsx)
        # Get the first file to extract the date
        first_file = list(exported_files.values())[0] if exported_files else master_summary_file
        filename = os.path.basename(first_file)
        
        # Extract date from filename pattern: transactions_xxx_DD_MM_YYYY.xlsx
        date_match = re.search(r'(\d{2}_\d{2}_\d{4})', filename)
        if date_match:
            date_str = date_match.group(1)
            # Convert from DD_MM_YYYY to DD/MM/YYYY format
            report_date = date_str.replace('_', '/')
        else:
            # Fallback to current date if pattern not found
            report_date = pd.Timestamp.now().strftime('%d/%m/%Y')
        
        # Create email subject with extracted date
        subject = f"Reporting SARA - {report_date}"
        
        # Prepare attachment list
        attachment_paths = [master_summary_file]
        attachment_paths.extend(exported_files.values())
        
        # Filter existing files only
        existing_attachments = [path for path in attachment_paths if os.path.exists(path)]
        
        # Create professional French email body
        current_time = pd.Timestamp.now().strftime('%d/%m/%Y à %H:%M')
        total_transactions = processing_stats.get('total_transactions', 0) if processing_stats else 0
        total_agencies = len(exported_files)
        
        body = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.8; color: #333; margin: 20px; }}
                .greeting {{ font-size: 16px; margin-bottom: 20px; }}
                .content {{ margin: 20px 0; }}
                .summary {{ background-color: #f8f9fa; padding: 15px; border-radius: 5px; margin: 20px 0; }}
                .files {{ background-color: #e8f4fd; padding: 15px; border-radius: 5px; margin: 15px 0; }}
                .footer {{ margin-top: 30px; font-size: 12px; color: #666; }}
                ul {{ padding-left: 20px; }}
                li {{ margin: 5px 0; }}
            </style>
        </head>
        <body>
            <div class="greeting">
                <p>Bonsoir cher partenaire,</p>
                <p>Trouvez ci-joint le reporting de vos transactions pour la journée indiquée en objet.</p>
            </div>
            
            <div class="content">
                <div class="summary">
                    <h4>📊 Résumé du traitement :</h4>
                    <ul>
                        <li><strong>Date de traitement :</strong> {report_date}</li>
                        <li><strong>Nombre d'agences :</strong> {total_agencies}</li>
                        <li><strong>Total des transactions :</strong> {total_transactions:,}</li>
                        <li><strong>Fichiers générés :</strong> {len(existing_attachments)}</li>
                        <li><strong>Heure de génération :</strong> {current_time}</li>
                    </ul>
                </div>
                
                <div class="files">
                    <h4>📎 Fichiers joints :</h4>
                    <ul>
                        <li><strong>Résumé général :</strong> {os.path.basename(master_summary_file)}</li>"""
        
        # Add individual agency files
        for agency_name, filepath in exported_files.items():
            filename = os.path.basename(filepath)
            file_size = os.path.getsize(filepath) / 1024  # KB
            body += f"<li><strong>{agency_name.upper()} :</strong> {filename} ({file_size:.1f} Ko)</li>"
        
        total_size_mb = sum(os.path.getsize(f) for f in existing_attachments)/1024/1024
        
        body += f"""
                    </ul>
                    <p><strong>Taille totale :</strong> {total_size_mb:.2f} Mo</p>
                </div>
                
                <p>Tous les fichiers sont au format Excel avec plusieurs feuilles incluant les données de transaction, les annuaires de portefeuilles et les annuaires bancaires.</p>
            </div>
            
            <div class="footer">
                <p>Cordialement,<br>
                L'équipe SARA<br>
                <em>Rapport généré automatiquement le {current_time}</em></p>
            </div>
        </body>
        </html>
        """
        
        # Send email
        success = send_email_with_attachments(subject, body, existing_attachments, email_config)
        
        if success:
            logger.info("📧 SARA report email sent successfully")
        else:
            logger.error("📧 Failed to send SARA report email")
        
        return success
        
    except Exception as e:
        logger.error(f"Error sending SARA report email: {str(e)}")
        return False


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
        
        # Identify agency transactions
        agency_transactions = identify_all_agency_transactions(transactions)
        
        # Categorize agency transactions
        categorized_agency_transactions = categorize_all_agency_transactions(agency_transactions)
        
        # Create solde columns (agency-centric perspective)
        agency_solde_data = create_solde_columns_for_all_agencies(categorized_agency_transactions)
        
        # Finalize data (column ordering and sorting)
        final_agency_data = finalize_all_agency_data(agency_solde_data)
        
        # Export data to Excel files
        exported_files = export_all_agency_data(final_agency_data)
        
        # Create master summary file
        master_summary_file = create_master_summary_file(final_agency_data, exported_files)
        
        logger.info("✅ All files generated successfully. Proceeding to email notification...")
        
        # Prepare processing statistics for email
        total_transactions = sum(len(final_df) for final_df, _, _ in final_agency_data.values())
        processing_stats = {
            'total_transactions': total_transactions,
            'total_agencies': len(final_agency_data),
            'processing_date': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Send email with generated files (only after successful file generation)
        email_sent = send_sara_report_email(exported_files, master_summary_file, processing_stats)
        
        print(f"Processing complete. Final dataset shape: {transactions.shape}")
        print(f"Original transaction types: {transactions['Type transaction'].value_counts()}")
        print("\nFinal agency transaction breakdown:")
        for agency_name, (final_df, annuaire_df, bank_annuaire_df) in final_agency_data.items():
            print(f"- {agency_name.upper()}: {len(final_df)} transactions")
            if len(final_df) > 0:
                transaction_types = final_df['Type transaction'].value_counts()
                print(f"  Transaction types: {dict(transaction_types)}")
                print(f"  Final columns: {len(final_df.columns)} (standardized format)")
                print(f"  Wallet directory: {len(annuaire_df)} entries")
                print(f"  Bank directory: {len(bank_annuaire_df)} entries")
        
        print(f"\n📁 Export Summary:")
        print(f"✅ {len(exported_files)} agency files exported successfully")
        print(f"✅ Master summary file: {master_summary_file}")
        print("\nExported files:")
        for agency_name, filepath in exported_files.items():
            print(f"  - {agency_name.upper()}: {filepath}")
        
        print(f"\n📧 Email Summary:")
        if email_sent:
            print("✅ Email sent successfully with all generated files")
        else:
            print("ℹ️  Email not sent (configuration not available or failed)")
            print("   To enable email notifications, set these environment variables:")
            print("   export SARA_EMAIL_SENDER='your_email@gmail.com'")
            print("   export SARA_EMAIL_PASSWORD='your_app_password'")
            print("   export SARA_EMAIL_RECIPIENTS='recipient1@email.com,recipient2@email.com'")
            print("   Optional: SARA_SMTP_SERVER and SARA_SMTP_PORT")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise 