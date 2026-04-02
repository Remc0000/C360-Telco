# Telco Customer Data Generator

A configurable Python application that generates realistic synthetic data for a telecommunications customer data model. Designed specifically for **Netherlands (Dutch)** customers with accurate names, addresses, and geographic coordinates.

## Features

- **Realistic Dutch Data**: Dutch names, addresses, cities with accurate lat/long coordinates
- **Complete Schema Coverage**: Generates data for all 26 tables in the Telco customer schema
- **Configurable Scale**: From small test datasets to enterprise-scale generation
- **Multiple Output Formats**: CSV, JSON, and Parquet support
- **Referential Integrity**: Maintains proper relationships between tables
- **Reproducible Results**: Optional random seed for consistent data generation

## Installation

```bash
# Clone or navigate to the project directory
cd customerModels

# Install dependencies (optional - only needed for Parquet output)
pip install -r requirements.txt
```

**Note**: The generator works with Python standard library only for CSV/JSON output. Install pandas and pyarrow only if you need Parquet format.

## Quick Start

### Basic Usage

```bash
# Generate small dataset (100 parties, 80 accounts, 120 subscribers)
python generate.py --preset small

# Generate medium dataset (1000 parties, 800 accounts, 1200 subscribers)
python generate.py --preset medium

# Generate large dataset (10000 parties, 8000 accounts, 12000 subscribers)
python generate.py --preset large
```

### Custom Configuration

```bash
# Specify exact counts
python generate.py --parties 5000 --accounts 4000 --subscribers 6000

# Change output format and directory
python generate.py --preset medium --format parquet --output data/telco_data

# Use a seed for reproducibility
python generate.py --preset small --seed 12345
```

### Using a Configuration File

```bash
# Save config to file
python -c "from config import MEDIUM_CONFIG; MEDIUM_CONFIG.to_json_file('my_config.json')"

# Use config file
python generate.py --config my_config.json
```

## Configuration Options

| Parameter | Default | Description |
|-----------|---------|-------------|
| `num_parties` | 1000 | Number of person/organization records |
| `num_accounts` | 800 | Number of customer accounts |
| `num_subscribers` | 1200 | Number of subscribers (phone lines, etc.) |
| `num_products` | 50 | Number of products in catalog |
| `output_format` | csv | Output format: csv, json, or parquet |
| `output_dir` | output | Directory for generated files |
| `random_seed` | 42 | Seed for reproducibility (None for random) |
| `prepaid_ratio` | 0.3 | Ratio of prepaid vs postpaid subscribers |
| `data_start_year` | 2020 | Start year for generated dates |
| `data_end_year` | 2026 | End year for generated dates |

### Secondary Entity Ratios

These control the volume of related records:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `subscriptions_per_subscriber` | 1.5 | Average subscriptions per subscriber |
| `invoices_per_account` | 12.0 | Average invoices per account |
| `charges_per_account` | 36.0 | Average charges per account |
| `payments_per_account` | 10.0 | Average payments per account |
| `tickets_per_account` | 2.0 | Average support tickets per account |

## Generated Tables

The generator creates data for all tables in the Telco customer schema:

### Party & Account
- `party` - Customer/organization records
- `customer_account` - Account information
- `account_party_role` - Account-party relationships
- `address` - Physical addresses with coordinates
- `party_address` - Party-address relationships

### Subscriptions
- `subscriber` - Mobile/fixed/IoT subscribers
- `subscriber_status_history` - Status change history
- `subscription` - Product subscriptions
- `entitlement` - Subscription entitlements

### MSISDN, SIM & Devices
- `msisdn` - Phone numbers (E.164 format)
- `subscriber_msisdn` - Subscriber-MSISDN links
- `sim` - SIM card records
- `subscriber_sim` - Subscriber-SIM links
- `device` - Device information
- `subscriber_device` - Subscriber-device links

### Catalog
- `product_catalog` - Products, plans, add-ons

### Services & Provisioning
- `service` - Active services
- `service_order` - Service orders
- `porting_request` - Number porting requests

### Billing & Payments
- `charge` - Individual charges
- `invoice` - Invoices
- `invoice_line` - Invoice line items
- `payment` - Payment records
- `prepaid_balance_snapshot` - Prepaid balances
- `topup` - Prepaid top-ups

### Customer Care
- `case_ticket` - Support tickets

## Dutch Data Features

### Cities with Accurate Coordinates
The generator includes 50+ major Dutch cities with accurate latitude/longitude:
- Amsterdam (52.3676, 4.9041)
- Rotterdam (51.9244, 4.4777)
- Den Haag (52.0705, 4.3007)
- Utrecht (52.0907, 5.1214)
- And many more across all provinces

### Realistic Dutch Names
- Common Dutch first names (male and female)
- Authentic Dutch surnames (de Jong, van den Berg, etc.)
- Dutch organization naming patterns (B.V., N.V., etc.)

### Dutch Addresses
- Proper street naming conventions
- Dutch postal code format (1234 AB)
- Provincial assignments
- Small random offsets from city centers for realistic distribution

### Phone Numbers
- E.164 format (+31...)
- Mobile numbers (06-XXXXXXXX pattern)
- Dutch landline area codes

### Email Domains
- Dutch email providers (ziggo.nl, kpnmail.nl, xs4all.nl, etc.)
- Common patterns based on names

## Programmatic Usage

```python
from config import GeneratorConfig
from generate import TelcoDataGenerator

# Create custom configuration
config = GeneratorConfig(
    num_parties=2000,
    num_accounts=1500,
    num_subscribers=2500,
    output_format="json",
    output_dir="my_data",
    random_seed=42
)

# Generate data
generator = TelcoDataGenerator(config)
data = generator.generate_all()

# Access generated data
parties = data["party"]
accounts = data["customer_account"]
subscribers = data["subscriber"]

# Save to files
generator.save()

# Or save in a specific format
generator.save_to_csv("csv_output")
generator.save_to_json("json_output")
generator.save_to_parquet("parquet_output")
```

## Output Examples

### Party Record (CSV)
```csv
party_id,party_type,display_name,legal_name,given_name,family_name,birth_date,email,created_ts,updated_ts
abc123,PERSON,Jan de Jong,Jan de Jong,Jan,de Jong,1985-03-15,jan.dejong@ziggo.nl,2022-05-10T14:30:00,2026-04-01T10:00:00
```

### Address Record (CSV)
```csv
address_id,line1,line2,city,state_region,postal_code,country_code,latitude,longitude,created_ts,updated_ts
xyz789,Kalverstraat 45,,Amsterdam,Noord-Holland,1012 PA,NL,52.369421,4.891532,2022-05-10T14:30:00,2026-04-01T10:00:00
```

## Scale Guidelines

| Preset | Parties | Accounts | Subscribers | Est. Total Records | Est. Time |
|--------|---------|----------|-------------|-------------------|-----------|
| small | 100 | 80 | 120 | ~3,000 | <5s |
| medium | 1,000 | 800 | 1,200 | ~30,000 | ~10s |
| large | 10,000 | 8,000 | 12,000 | ~300,000 | ~2min |
| enterprise | 100,000 | 80,000 | 150,000 | ~3,000,000 | ~20min |

## File Structure

```
customerModels/
├── generate.py          # Main generator script
├── config.py            # Configuration classes and presets
├── dutch_data.py        # Dutch data utilities (names, cities, etc.)
├── requirements.txt     # Python dependencies
├── README.md            # This file
├── Telco_customer_schema.txt  # Original schema definition
└── output/              # Generated data (created on first run)
    ├── party.csv
    ├── customer_account.csv
    ├── subscriber.csv
    └── ... (26 CSV files total)
```

## License

This project is provided as-is for data generation purposes.

## Contributing

Feel free to extend the Dutch data utilities or add support for additional output formats.
