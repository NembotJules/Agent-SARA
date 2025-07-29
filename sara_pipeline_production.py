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
        
        print(f"Processing complete. Final dataset shape: {transactions.shape}")
        print(f"Original transaction types: {transactions['Type transaction'].value_counts()}")
        print("\nAgency transaction breakdown:")
        for agency_name, agency_df in categorized_agency_transactions.items():
            print(f"- {agency_name.upper()}: {len(agency_df)} transactions")
            if len(agency_df) > 0:
                transaction_types = agency_df['Type transaction'].value_counts()
                print(f"  Transaction types: {dict(transaction_types)}")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise 