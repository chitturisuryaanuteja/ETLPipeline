# Banks ETL Pipeline

This project implements an ETL (Extract, Transform, Load) pipeline to process data on the largest banks by market capitalization.

## Features
- Data Extraction: Scrapes data from a webpage.
- Data Transformation: Converts market cap values from USD to other currencies.
- Data Storage: Saves the data as a CSV and in a SQLite database.

## Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/username/repository.git
   cd repository
   ```
2. Install dependencies using:
   ```bash
   pip install -r requirements.txt
   ```

## Usage
Run the pipeline with the following command:
   ```bash
   python3 Banks_project.py
   ```

## Project Structure
```plaintext
├── Banks_project.py         # Main ETL script
├── exchange_rate.csv        # Exchange rates for currency conversion
├── Largest_banks_data.csv   # Output CSV file for transformed data
├── Banks.db                 # SQLite database storing the final data
├── code_log.txt             # Log file for ETL process
├── requirements.txt         # Python dependencies
└── README.md                # Project documentation
```
