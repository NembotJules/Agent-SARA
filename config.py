"""
Configuration settings for the SARA Transaction Analysis Pipeline
"""

import os
from pathlib import Path

# File paths and directories
DATA_DIR = Path(".")
OUTPUT_DIR = Path("./outputs")
LOG_DIR = Path("./logs")

# Input file configurations
INPUT_FILES = {
    'transactions_agent': 'test_agent.xlsx',
    'transactions_customer': 'test_customer.xlsx',
    'point_of_service_emi_money': 'point_de_service_emi_money.xlsx',
    'point_of_service_express_union': 'point_de_service_EU.xlsx',
    'point_of_service_hop': 'point_de_service_hop.xlsx',
    'point_of_service_instant_transfer': 'point_de_service_IT.xlsx',
    'point_of_service_multiservice': 'point_de_service_MS.xlsx',
    'point_of_service_muffa': 'point_de_service_muffa.xlsx',
    'call_box': 'call_box.xlsx'
}

# Agency configurations
AGENCY_CONFIGS = {
    'hop': {
        'super_agent': 'HOP SERVICESARL',
        'pos_file': 'point_de_service_hop.xlsx',
        'header': 1,
        'drop_columns': ['Unnamed: 0']
    },
    'express_union': {
        'super_agent': 'EXPRESS UNIONSA',
        'pos_file': 'point_de_service_EU.xlsx',
        'header': 3,
        'drop_columns': ['Unnamed: 0', 'Unnamed: 2', 'Unnamed: 3']
    },
    'emi_money': {
        'super_agent': 'EMI MONEY SARL',
        'pos_file': 'point_de_service_emi_money.xlsx',
        'header': 1,
        'drop_columns': ['Unnamed: 0']
    },
    'instant_transfer': {
        'super_agent': 'INSTANTTRANSFER SARL',
        'pos_file': 'point_de_service_IT.xlsx',
        'header': 3,
        'drop_columns': ['Unnamed: 0', 'Unnamed: 2', 'Unnamed: 3']
    },
    'multiservice': {
        'super_agent': 'MULTI-SERVICE SARL',
        'pos_file': 'point_de_service_MS.xlsx',
        'header': 1,
        'drop_columns': ['Unnamed: 0']
    },
    'muffa': {
        'super_agent': None,
        'pos_file': 'point_de_service_muffa.xlsx',
        'header': 3,
        'drop_columns': ['Unnamed: 0', 'Unnamed: 2', 'Unnamed: 3']
    },
    'call_box': {
        'super_agent': None,
        'pos_file': 'call_box.xlsx',
        'header': 3,
        'drop_columns': ['Unnamed: 0', 'Unnamed: 2', 'Unnamed: 3']
    }
}

# Output column order
OUTPUT_COLUMNS = [
    'Date transaction', 'Heure transaction', 'Reference transaction', 
    'Type transaction', 'Type utilisateur transaction', 'Nom portefeuille', 
    'Numero portefeuille', 'Solde avant transaction', 'Montant transaction',
    'Solde après transaction', 'Partenaire transaction',
    'Numero portefeuille partenaire transaction', 'Compte bancaire partenaire',
    'Type canal', 'Statut transaction'
]

# Logging configuration
LOGGING_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(levelname)s - %(message)s',
    'file': 'sara_pipeline.log'
}

# Transaction type mappings
TRANSACTION_TYPE_MAPPING = {
    'CASH_IN': 'Approvisionement',
    'CASH_OUT': 'Versement bancaire'
}

# Performance settings
CHUNK_SIZE = 10000  # For processing large datasets
MAX_MEMORY_USAGE = '1GB'  # Memory limit for pandas operations

# Create directories if they don't exist
def create_directories():
    """Create necessary directories for the pipeline."""
    OUTPUT_DIR.mkdir(exist_ok=True)
    LOG_DIR.mkdir(exist_ok=True)

# Environment-specific settings
ENVIRONMENT = os.getenv('SARA_ENV', 'development')

if ENVIRONMENT == 'production':
    # Production settings
    LOG_LEVEL = 'INFO'
    DEBUG = False
    PARALLEL_PROCESSING = True
elif ENVIRONMENT == 'development':
    # Development settings
    LOG_LEVEL = 'DEBUG'
    DEBUG = True
    PARALLEL_PROCESSING = False
else:
    # Default settings
    LOG_LEVEL = 'INFO'
    DEBUG = False
    PARALLEL_PROCESSING = False 