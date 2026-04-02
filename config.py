"""
Configuration module for Telco Customer Data Generator
Adjust these settings to control the amount and type of data generated.
"""

from dataclasses import dataclass, field
from typing import Dict, List
import json
import os


@dataclass
class GeneratorConfig:
    """Configuration for synthetic data generation"""
    
    # Output settings
    output_dir: str = "output"
    output_format: str = "csv"  # Options: csv, json, parquet
    
    # Core entity counts
    num_parties: int = 1000
    num_accounts: int = 800
    num_subscribers: int = 1200
    num_products: int = 50
    
    # Secondary entity ratios (relative to core entities)
    subscriptions_per_subscriber: float = 1.5
    devices_per_subscriber: float = 1.2
    sims_per_subscriber: float = 1.1
    msisdns_per_subscriber: float = 1.0
    
    # Billing and payments
    invoices_per_account: float = 12.0  # ~1 year of monthly invoices
    invoice_lines_per_invoice: float = 4.0
    charges_per_account: float = 36.0
    payments_per_account: float = 10.0
    
    # Support tickets
    tickets_per_account: float = 2.0
    
    # Service orders
    orders_per_subscriber: float = 3.0
    
    # Porting requests ratio (lower as not all customers port)
    porting_ratio: float = 0.15
    
    # Prepaid ratios
    prepaid_ratio: float = 0.3  # 30% prepaid subscribers
    topups_per_prepaid: float = 6.0
    balance_snapshots_per_prepaid: float = 12.0
    
    # History tracking
    status_history_per_subscriber: float = 2.5
    
    # Entitlements
    entitlements_per_subscription: float = 2.0
    
    # Services
    services_per_subscriber: float = 2.0
    
    # Date ranges
    data_start_year: int = 2020
    data_end_year: int = 2026
    
    # Seed for reproducibility (None for random)
    random_seed: int = 42
    
    # Dutch-specific settings
    country_code: str = "NL"
    currency_code: str = "EUR"
    phone_country_code: str = "+31"
    
    # Operator/brand settings
    operator_name: str = "BrightTelco"
    brands: List[str] = field(default_factory=lambda: ["BrightTelco", "npkNL", "BrightMobile"])
    
    def to_dict(self) -> Dict:
        """Convert config to dictionary"""
        return {
            "output_dir": self.output_dir,
            "output_format": self.output_format,
            "num_parties": self.num_parties,
            "num_accounts": self.num_accounts,
            "num_subscribers": self.num_subscribers,
            "num_products": self.num_products,
            "subscriptions_per_subscriber": self.subscriptions_per_subscriber,
            "devices_per_subscriber": self.devices_per_subscriber,
            "sims_per_subscriber": self.sims_per_subscriber,
            "msisdns_per_subscriber": self.msisdns_per_subscriber,
            "invoices_per_account": self.invoices_per_account,
            "invoice_lines_per_invoice": self.invoice_lines_per_invoice,
            "charges_per_account": self.charges_per_account,
            "payments_per_account": self.payments_per_account,
            "tickets_per_account": self.tickets_per_account,
            "orders_per_subscriber": self.orders_per_subscriber,
            "porting_ratio": self.porting_ratio,
            "prepaid_ratio": self.prepaid_ratio,
            "topups_per_prepaid": self.topups_per_prepaid,
            "balance_snapshots_per_prepaid": self.balance_snapshots_per_prepaid,
            "status_history_per_subscriber": self.status_history_per_subscriber,
            "entitlements_per_subscription": self.entitlements_per_subscription,
            "services_per_subscriber": self.services_per_subscriber,
            "data_start_year": self.data_start_year,
            "data_end_year": self.data_end_year,
            "random_seed": self.random_seed,
            "country_code": self.country_code,
            "currency_code": self.currency_code,
            "phone_country_code": self.phone_country_code,
            "operator_name": self.operator_name,
            "brands": self.brands
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> "GeneratorConfig":
        """Create config from dictionary"""
        return cls(**data)
    
    @classmethod
    def from_json_file(cls, filepath: str) -> "GeneratorConfig":
        """Load config from JSON file"""
        with open(filepath, 'r') as f:
            data = json.load(f)
        return cls.from_dict(data)
    
    def to_json_file(self, filepath: str) -> None:
        """Save config to JSON file"""
        with open(filepath, 'w') as f:
            json.dump(self.to_dict(), f, indent=2)
    
    def validate(self) -> bool:
        """Validate configuration values"""
        assert self.num_parties > 0, "num_parties must be positive"
        assert self.num_accounts > 0, "num_accounts must be positive"
        assert self.num_subscribers > 0, "num_subscribers must be positive"
        assert self.output_format in ["csv", "json", "parquet"], "Invalid output format"
        return True


# Preset configurations for different use cases
SMALL_CONFIG = GeneratorConfig(
    num_parties=100,
    num_accounts=80,
    num_subscribers=120,
    num_products=20
)

MEDIUM_CONFIG = GeneratorConfig(
    num_parties=1000,
    num_accounts=800,
    num_subscribers=1200,
    num_products=50
)

LARGE_CONFIG = GeneratorConfig(
    num_parties=10000,
    num_accounts=8000,
    num_subscribers=12000,
    num_products=100
)

ENTERPRISE_CONFIG = GeneratorConfig(
    num_parties=100000,
    num_accounts=80000,
    num_subscribers=150000,
    num_products=200
)
