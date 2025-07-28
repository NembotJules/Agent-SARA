# SARA Transaction Analysis Pipeline

A production-ready Python pipeline for processing agent and customer transaction data from SAP BI, categorizing transactions by agency type, and generating detailed Excel reports with balance progression tracking.

## 🚀 Features

- **Multi-Agency Support**: Processes 7 different agency types (HOP, Express Union, EMI Money, Instant Transfer, Multi-Service, Muffa, Call Box)
- **Balance Progression Tracking**: Unified view of agent transactions with before/after balance tracking
- **Transaction Categorization**: Intelligent classification of transaction types (Approvisionement, Dépot, Retrait, Décharge, Versement bancaire)
- **Partner Information**: Tracks transaction partners and their portfolio numbers
- **Excel Report Generation**: Automated creation of detailed Excel reports for each agency
- **Comprehensive Logging**: Detailed logs for monitoring and debugging
- **Error Handling**: Robust error handling and recovery mechanisms

## 📋 Requirements

- Python 3.8+
- pandas >= 2.0.0
- numpy >= 1.24.0
- openpyxl >= 3.1.0

## 🛠️ Installation

1. **Clone or download the pipeline files:**
   ```bash
   # Make sure you have all the required files:
   # - marchand_sara_pipeline.py
   # - config.py
   # - requirements.txt
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Prepare your data files:**
   Place the following Excel files in the same directory as the pipeline:
   - `test_agent.xlsx` - Agent transaction data
   - `test_customer.xlsx` - Customer transaction data
   - `point_de_service_emi_money.xlsx` - EMI Money point of service data
   - `point_de_service_EU.xlsx` - Express Union point of service data
   - `point_de_service_hop.xlsx` - HOP point of service data
   - `point_de_service_IT.xlsx` - Instant Transfer point of service data
   - `point_de_service_MS.xlsx` - Multi-Service point of service data
   - `point_de_service_muffa.xlsx` - Muffa point of service data
   - `call_box.xlsx` - Call Box point of service data

## 🏃‍♂️ Usage

### Basic Usage

Run the pipeline with default settings:

```bash
python marchand_sara_pipeline.py
```

### Advanced Usage

```python
from marchand_sara_pipeline import TransactionProcessor

# Initialize processor
processor = TransactionProcessor()

# Run pipeline with custom paths
summary = processor.run_pipeline(
    data_path="./input_data",      # Path to input files
    output_path="./reports"        # Path for output Excel files
)

# Print summary statistics
for agency_type, stats in summary.items():
    print(f"{agency_type}: {stats['total_transactions']} transactions")
```

### Using as a Module

```python
from marchand_sara_pipeline import TransactionProcessor, AgencyClassifier

# Use agency classifier independently
classifier = AgencyClassifier()

# Check if a name is a HOP agency
is_hop = classifier.is_hop_agency("HOP DOUALA CENTRE")

# Check if a name is any agency type
is_agency = classifier.is_any_agency("EU YAOUNDE CENTRALE")
```

## 📊 Output

The pipeline generates:

1. **Excel Reports**: One file per agency type with columns:
   - Date and time information
   - Transaction details
   - Agent portfolio information
   - Balance before/after transaction
   - Partner transaction details
   - Bank account information (when applicable)

2. **Log Files**: Detailed execution logs in `sara_pipeline.log`

3. **Summary Statistics**: Console output showing:
   - Total transactions per agency
   - Unique agents count
   - Transaction type breakdown
   - Date range processed

## 🏗️ Pipeline Architecture

### Key Components

1. **AgencyClassifier**: Identifies different agency types using pattern matching
2. **TransactionProcessor**: Main processing engine that handles:
   - Data loading and preprocessing
   - Transaction extraction by agency type
   - Transaction categorization
   - Balance progression calculation
   - Excel report generation

### Processing Flow

```
Input Data → Data Cleaning → Agency Classification → Transaction Categorization → 
Balance Calculation → Report Generation → Excel Output
```

## 🔧 Configuration

Modify `config.py` to customize:

- File paths and directories
- Agency configurations
- Output column order
- Logging settings
- Performance parameters

## 📝 Transaction Types

The pipeline categorizes transactions into:

- **Approvisionement**: Cash-in operations or super agent to agency transfers
- **Versement bancaire**: Cash-out operations
- **Dépot**: Agency to customer transfers
- **Retrait**: Customer to agency transfers
- **Décharge**: Agency to super agent transfers

## 🐛 Troubleshooting

### Common Issues

1. **File Not Found Error**:
   - Ensure all required Excel files are in the correct directory
   - Check file names match exactly (case-sensitive)

2. **Memory Issues**:
   - Adjust `CHUNK_SIZE` in config.py for large datasets
   - Monitor memory usage during processing

3. **Excel Generation Errors**:
   - Ensure output directory exists and has write permissions
   - Check for special characters in data that might cause Excel issues

### Logging

Check `sara_pipeline.log` for detailed error information and processing steps.

## 📈 Performance

The pipeline is optimized for:
- Processing large datasets (tested with 10,000+ transactions)
- Memory-efficient operations
- Fast Excel generation
- Comprehensive error recovery

## 🔒 Data Security

- No data is transmitted outside the local environment
- All processing happens locally
- Input files are read-only (never modified)
- Output files are timestamped to prevent overwrites

## 📞 Support

For issues or questions:
1. Check the log files for error details
2. Verify input data format matches expected structure
3. Ensure all dependencies are properly installed

## 🔄 Future Enhancements

Planned improvements:
- Database integration for real-time data fetching
- Web dashboard for interactive analysis
- Automated scheduling capabilities
- Performance monitoring and alerting
- Multi-format output support (CSV, JSON, etc.)

## 📄 License

This project is proprietary software developed for SARA transaction analysis.

---

**Version**: 1.0.0  
**Last Updated**: December 2024  
**Author**: SARA Development Team 