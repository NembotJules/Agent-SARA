#!/usr/bin/env python3
"""
Test Script for SARA Transaction Analysis Pipeline
=================================================

This script provides basic tests to validate the pipeline functionality
and help with debugging and development.

Usage:
    python test_pipeline.py
"""

import sys
import pandas as pd
from pathlib import Path
from marchand_sara_pipeline import TransactionProcessor, AgencyClassifier

def test_agency_classifier():
    """Test the agency classification functions."""
    print("Testing Agency Classifier...")
    classifier = AgencyClassifier()
    
    # Test cases for each agency type
    test_cases = {
        'hop': [
            ('HOP DOUALA CENTRE', True),
            ('HOP SERVICESARL', True),
            ('ALBINEHOP', True),
            ('EU YAOUNDE', False),
            ('EMI MONEY SARL', False)
        ],
        'express_union': [
            ('EU YAOUNDE CENTRALE', True),
            ('EUF DOUALA', True),
            ('EXPRESS UNIONSA', True),
            ('HOP DOUALA', False),
            ('MS BAFOUSSAM', False)
        ],
        'emi_money': [
            ('EMI MONEY SARL', True),
            ('Emi money Douala', True),
            ('Sarl Emi money', True),
            ('HOP DOUALA', False),
            ('EU YAOUNDE', False)
        ],
        'instant_transfer': [
            ('IT YAOUNDE CENTRE', True),
            ('INSTANTTRANSFER SARL', True),
            ('HOP DOUALA', False),
            ('EU YAOUNDE', False)
        ],
        'multiservice': [
            ('MS BAFOUSSAM', True),
            ('MULTI-SERVICE SARL', True),
            ('MULTI-SERVICE Douala', True),
            ('HOP DOUALA', False),
            ('EU YAOUNDE', False)
        ],
        'muffa': [
            ('MUFFA DOUALA', True),
            ('MUFFA YAOUNDE CENTRE', True),
            ('HOP DOUALA', False),
            ('EU YAOUNDE', False)
        ],
        'call_box': [
            ('CB DOUALA CENTRE', True),
            ('CB YAOUNDE', True),
            ('HOP DOUALA', False),
            ('EU YAOUNDE', False)
        ]
    }
    
    # Get classifier functions
    classifier_functions = {
        'hop': classifier.is_hop_agency,
        'express_union': classifier.is_express_union_agency,
        'emi_money': classifier.is_emi_money_agency,
        'instant_transfer': classifier.is_instant_transfer_agency,
        'multiservice': classifier.is_multiservice_agency,
        'muffa': classifier.is_muffa_agency,
        'call_box': classifier.is_call_box_agency
    }
    
    all_passed = True
    
    for agency_type, cases in test_cases.items():
        func = classifier_functions[agency_type]
        print(f"\n  Testing {agency_type} classifier:")
        
        for test_name, expected in cases:
            result = func(test_name)
            status = "✓" if result == expected else "✗"
            print(f"    {status} {test_name}: {result} (expected {expected})")
            
            if result != expected:
                all_passed = False
    
    print(f"\nAgency Classifier Tests: {'PASSED' if all_passed else 'FAILED'}")
    return all_passed

def test_data_files():
    """Test if required data files are present."""
    print("\nTesting Data Files...")
    
    required_files = [
        'test_agent.xlsx',
        'test_customer.xlsx',
        'point_de_service_emi_money.xlsx',
        'point_de_service_EU.xlsx',
        'point_de_service_hop.xlsx',
        'point_de_service_IT.xlsx',
        'point_de_service_MS.xlsx',
        'point_de_service_muffa.xlsx',
        'call_box.xlsx'
    ]
    
    missing_files = []
    
    for file in required_files:
        if Path(file).exists():
            print(f"  ✓ {file}")
        else:
            print(f"  ✗ {file} (missing)")
            missing_files.append(file)
    
    if missing_files:
        print(f"\nMissing files: {missing_files}")
        print("Please ensure all required data files are in the current directory.")
        return False
    
    print("\nData Files Test: PASSED")
    return True

def test_pipeline_initialization():
    """Test pipeline initialization."""
    print("\nTesting Pipeline Initialization...")
    
    try:
        processor = TransactionProcessor()
        print("  ✓ TransactionProcessor initialized successfully")
        
        classifier = AgencyClassifier()
        print("  ✓ AgencyClassifier initialized successfully")
        
        print("\nPipeline Initialization Test: PASSED")
        return True
        
    except Exception as e:
        print(f"  ✗ Error initializing pipeline: {e}")
        print("\nPipeline Initialization Test: FAILED")
        return False

def test_sample_data_loading():
    """Test loading sample data (if files exist)."""
    print("\nTesting Sample Data Loading...")
    
    if not Path('test_agent.xlsx').exists():
        print("  ! Skipping data loading test - test_agent.xlsx not found")
        return True
    
    try:
        processor = TransactionProcessor()
        
        # Try to load just the agent data file
        agent_df = pd.read_excel('test_agent.xlsx', header=1, engine='openpyxl')
        print(f"  ✓ Loaded test_agent.xlsx: {len(agent_df)} rows")
        
        # Basic data validation
        if 'Statut transaction' in agent_df.columns:
            completed_count = len(agent_df[agent_df['Statut transaction'] == 'COMPLETED'])
            print(f"  ✓ Found {completed_count} completed transactions")
        
        print("\nSample Data Loading Test: PASSED")
        return True
        
    except Exception as e:
        print(f"  ✗ Error loading sample data: {e}")
        print("\nSample Data Loading Test: FAILED")
        return False

def run_basic_pipeline_test():
    """Run a basic pipeline test if data files are available."""
    print("\nTesting Basic Pipeline Run...")
    
    # Check if minimum required files exist
    if not (Path('test_agent.xlsx').exists() and Path('test_customer.xlsx').exists()):
        print("  ! Skipping pipeline test - transaction files not found")
        return True
    
    try:
        processor = TransactionProcessor()
        
        # Test just the data loading and preprocessing steps
        processor.load_data()
        print("  ✓ Data loading successful")
        
        processor.preprocess_data()
        print("  ✓ Data preprocessing successful")
        
        print(f"  ✓ Total transactions loaded: {len(processor.transactions_df)}")
        
        print("\nBasic Pipeline Test: PASSED")
        return True
        
    except Exception as e:
        print(f"  ✗ Error in basic pipeline test: {e}")
        print("\nBasic Pipeline Test: FAILED")
        return False

def main():
    """Run all tests."""
    print("SARA Pipeline Test Suite")
    print("=" * 50)
    
    tests = [
        test_agency_classifier,
        test_data_files,
        test_pipeline_initialization,
        test_sample_data_loading,
        run_basic_pipeline_test
    ]
    
    results = []
    
    for test in tests:
        try:
            result = test()
            results.append(result)
        except Exception as e:
            print(f"\nUnexpected error in test: {e}")
            results.append(False)
    
    # Summary
    print("\n" + "=" * 50)
    print("TEST SUMMARY")
    print("=" * 50)
    
    passed = sum(results)
    total = len(results)
    
    print(f"Tests passed: {passed}/{total}")
    
    if passed == total:
        print("🎉 All tests passed! Pipeline is ready to use.")
        return 0
    else:
        print("⚠️  Some tests failed. Please check the issues above.")
        return 1

if __name__ == "__main__":
    exit(main()) 