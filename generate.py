"""
Telco Customer Data Generator
Generates realistic synthetic data for Dutch telecommunications customers.
"""

import uuid
import random
import os
import json
import csv
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, asdict
from config import GeneratorConfig, SMALL_CONFIG, MEDIUM_CONFIG, LARGE_CONFIG
from dutch_data import get_dutch_data_generator, DutchDataGenerator


class TelcoDataGenerator:
    """Main generator for Telco customer schema data"""
    
    def __init__(self, config: GeneratorConfig):
        self.config = config
        if config.random_seed:
            random.seed(config.random_seed)
        self.dutch = get_dutch_data_generator(config.random_seed)
        
        # Initialize data stores
        self.data = {
            "party": [],
            "customer_account": [],
            "account_party_role": [],
            "subscriber": [],
            "subscriber_status_history": [],
            "msisdn": [],
            "subscriber_msisdn": [],
            "sim": [],
            "subscriber_sim": [],
            "device": [],
            "subscriber_device": [],
            "product_catalog": [],
            "subscription": [],
            "entitlement": [],
            "service": [],
            "service_order": [],
            "porting_request": [],
            "charge": [],
            "invoice": [],
            "invoice_line": [],
            "payment": [],
            "prepaid_balance_snapshot": [],
            "topup": [],
            "case_ticket": [],
            "address": [],
            "party_address": [],
        }
        
        # Track IDs for referential integrity
        self.party_ids = []
        self.account_ids = []
        self.subscriber_ids = []
        self.prepaid_subscriber_ids = []
        self.product_ids = []
        self.subscription_ids = []
        self.address_ids = []
        self.msisdn_ids = []
        self.sim_ids = []
        self.device_ids = []
        self.invoice_ids = []
    
    def _generate_uuid(self) -> str:
        """Generate a UUID string"""
        return str(uuid.uuid4())
    
    def _generate_timestamp(self, start_date: Optional[date] = None, 
                           end_date: Optional[date] = None) -> str:
        """Generate a random timestamp within range"""
        if start_date is None:
            start_date = date(self.config.data_start_year, 1, 1)
        if end_date is None:
            end_date = date(self.config.data_end_year, 12, 31)
        
        days_between = (end_date - start_date).days
        if days_between <= 0:
            days_between = 1
        random_days = random.randint(0, days_between)
        random_date = start_date + timedelta(days=random_days)
        
        # Add random time
        random_time = timedelta(
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )
        
        dt = datetime.combine(random_date, datetime.min.time()) + random_time
        return dt.isoformat()
    
    def _now_timestamp(self) -> str:
        """Get current timestamp"""
        return datetime.now().isoformat()
    
    # =========================================================================
    # PARTY & ACCOUNT GENERATION
    # =========================================================================
    
    def generate_parties(self) -> None:
        """Generate party records"""
        print(f"Generating {self.config.num_parties} parties...")
        
        for i in range(self.config.num_parties):
            party_id = self._generate_uuid()
            self.party_ids.append(party_id)
            
            # 85% individuals, 15% organizations
            is_org = random.random() < 0.15
            
            if is_org:
                org_name = self.dutch.generate_organization_name()
                party = {
                    "party_id": party_id,
                    "party_type": "ORG",
                    "display_name": org_name,
                    "legal_name": org_name,
                    "given_name": None,
                    "family_name": None,
                    "birth_date": None,
                    "email": f"info@{org_name.lower().replace(' ', '').replace('.', '')[:20]}.nl",
                    "created_ts": self._generate_timestamp(),
                    "updated_ts": self._now_timestamp()
                }
            else:
                given_name, family_name, gender = self.dutch.generate_full_name()
                display_name = f"{given_name} {family_name}"
                email = self.dutch.generate_email(given_name, family_name)
                birth_date = self.dutch.generate_birthdate()
                
                party = {
                    "party_id": party_id,
                    "party_type": "PERSON",
                    "display_name": display_name,
                    "legal_name": display_name,
                    "given_name": given_name,
                    "family_name": family_name,
                    "birth_date": birth_date,
                    "email": email,
                    "created_ts": self._generate_timestamp(),
                    "updated_ts": self._now_timestamp()
                }
            
            self.data["party"].append(party)
            
            # Generate address for party
            self._generate_party_address(party_id)
    
    def _generate_party_address(self, party_id: str) -> None:
        """Generate address for a party"""
        address_id = self._generate_uuid()
        self.address_ids.append(address_id)
        
        addr_data = self.dutch.generate_address()
        created_ts = self._generate_timestamp()
        
        address = {
            "address_id": address_id,
            "line1": addr_data["line1"],
            "line2": addr_data["line2"],
            "city": addr_data["city"],
            "state_region": addr_data["state_region"],
            "postal_code": addr_data["postal_code"],
            "country_code": addr_data["country_code"],
            "latitude": addr_data["latitude"],
            "longitude": addr_data["longitude"],
            "created_ts": created_ts,
            "updated_ts": self._now_timestamp()
        }
        self.data["address"].append(address)
        
        # Link party to address
        party_address = {
            "party_id": party_id,
            "address_id": address_id,
            "address_type": random.choice(["HOME", "BILLING", "SERVICE"]),
            "primary_flag": True,
            "valid_from_ts": created_ts,
            "valid_to_ts": None
        }
        self.data["party_address"].append(party_address)
    
    def generate_accounts(self) -> None:
        """Generate customer account records"""
        print(f"Generating {self.config.num_accounts} accounts...")
        
        for i in range(self.config.num_accounts):
            account_id = self._generate_uuid()
            self.account_ids.append(account_id)
            
            created_ts = self._generate_timestamp()
            
            account = {
                "account_id": account_id,
                "account_type": random.choices(
                    ["CONSUMER", "SMB", "ENTERPRISE"],
                    weights=[0.75, 0.20, 0.05]
                )[0],
                "account_status": random.choices(
                    ["ACTIVE", "SUSPENDED", "CLOSED"],
                    weights=[0.85, 0.10, 0.05]
                )[0],
                "billing_cycle_day": random.randint(1, 28),
                "currency_code": self.config.currency_code,
                "credit_class": random.choice(["A", "B", "C", "D"]),
                "payment_terms": random.choice(["NET_14", "NET_30", "NET_45", "PREPAID"]),
                "created_ts": created_ts,
                "updated_ts": self._now_timestamp()
            }
            self.data["customer_account"].append(account)
            
            # Link account to party
            party_id = random.choice(self.party_ids)
            account_party_role = {
                "account_id": account_id,
                "party_id": party_id,
                "role_type": "OWNER",
                "effective_from_ts": created_ts,
                "effective_to_ts": None,
                "is_current": True,
                "created_ts": created_ts,
                "updated_ts": self._now_timestamp()
            }
            self.data["account_party_role"].append(account_party_role)
            
            # Sometimes add additional party roles
            if random.random() < 0.2:
                payer_party = random.choice(self.party_ids)
                payer_role = {
                    "account_id": account_id,
                    "party_id": payer_party,
                    "role_type": "PAYER",
                    "effective_from_ts": created_ts,
                    "effective_to_ts": None,
                    "is_current": True,
                    "created_ts": created_ts,
                    "updated_ts": self._now_timestamp()
                }
                self.data["account_party_role"].append(payer_role)
    
    # =========================================================================
    # PRODUCT CATALOG GENERATION
    # =========================================================================
    
    def generate_products(self) -> None:
        """Generate product catalog"""
        print(f"Generating {self.config.num_products} products...")
        
        product_templates = [
            # Mobile Postpaid Plans
            ("Unlimited Plus", "PLAN", "MOBILE_POSTPAID", 49.99),
            ("Unlimited Basic", "PLAN", "MOBILE_POSTPAID", 34.99),
            ("Family Share 50GB", "PLAN", "MOBILE_POSTPAID", 59.99),
            ("Business Mobile Pro", "PLAN", "MOBILE_POSTPAID", 44.99),
            ("Student Mobile", "PLAN", "MOBILE_POSTPAID", 19.99),
            ("Senior Mobile", "PLAN", "MOBILE_POSTPAID", 14.99),
            
            # Mobile Prepaid Plans
            ("Prepaid Basic", "PLAN", "PREPAID", 10.00),
            ("Prepaid Plus", "PLAN", "PREPAID", 20.00),
            ("Prepaid Unlimited Day", "PLAN", "PREPAID", 5.00),
            
            # Fixed/Broadband Plans
            ("Fiber 100 Mbps", "PLAN", "FTTH", 39.99),
            ("Fiber 500 Mbps", "PLAN", "FTTH", 54.99),
            ("Fiber 1 Gbps", "PLAN", "FTTH", 69.99),
            ("DSL Basic", "PLAN", "FWA", 29.99),
            
            # IoT Plans
            ("IoT Starter 500MB", "PLAN", "IOT", 4.99),
            ("IoT Business 2GB", "PLAN", "IOT", 9.99),
            ("IoT Enterprise Unlimited", "PLAN", "IOT", 24.99),
            
            # Add-ons
            ("Extra 10GB Data", "ADDON", "MOBILE_POSTPAID", 9.99),
            ("Extra 25GB Data", "ADDON", "MOBILE_POSTPAID", 19.99),
            ("EU Roaming Pack", "ADDON", "MOBILE_POSTPAID", 7.99),
            ("World Roaming Pack", "ADDON", "MOBILE_POSTPAID", 14.99),
            ("Insurance Basic", "ADDON", "MOBILE_POSTPAID", 5.99),
            ("Insurance Premium", "ADDON", "MOBILE_POSTPAID", 9.99),
            ("Cloud Storage 100GB", "ADDON", "MOBILE_POSTPAID", 2.99),
            ("Music Streaming", "ADDON", "MOBILE_POSTPAID", 9.99),
            ("Video Streaming", "ADDON", "MOBILE_POSTPAID", 12.99),
            
            # Devices
            ("iPhone 15 Pro", "DEVICE", "MOBILE_POSTPAID", 1199.00),
            ("iPhone 15", "DEVICE", "MOBILE_POSTPAID", 899.00),
            ("Samsung Galaxy S24", "DEVICE", "MOBILE_POSTPAID", 999.00),
            ("Samsung Galaxy A54", "DEVICE", "MOBILE_POSTPAID", 449.00),
            ("Google Pixel 8", "DEVICE", "MOBILE_POSTPAID", 699.00),
            ("Xiaomi 14", "DEVICE", "MOBILE_POSTPAID", 599.00),
            ("4G Router Home", "DEVICE", "FWA", 149.00),
            ("5G Router Pro", "DEVICE", "FWA", 299.00),
            
            # Bundles
            ("Complete Home Bundle", "BUNDLE", "FTTH", 89.99),
            ("Family Digital Bundle", "BUNDLE", "MOBILE_POSTPAID", 119.99),
            ("Business Starter Bundle", "BUNDLE", "MOBILE_POSTPAID", 79.99),
        ]
        
        for i, template in enumerate(product_templates[:self.config.num_products]):
            product_id = self._generate_uuid()
            self.product_ids.append(product_id)
            
            name, ptype, family, price = template
            
            product = {
                "product_id": product_id,
                "product_type": ptype,
                "product_name": name,
                "product_family": family,
                "created_ts": self._generate_timestamp(),
                "updated_ts": self._now_timestamp()
            }
            self.data["product_catalog"].append(product)
        
        # Generate additional products if needed
        while len(self.product_ids) < self.config.num_products:
            product_id = self._generate_uuid()
            self.product_ids.append(product_id)
            
            ptype = random.choice(["PLAN", "ADDON", "DEVICE", "BUNDLE"])
            family = random.choice(["MOBILE_POSTPAID", "PREPAID", "FTTH", "FWA", "IOT"])
            
            product = {
                "product_id": product_id,
                "product_type": ptype,
                "product_name": f"{ptype.title()} {family} #{len(self.product_ids)}",
                "product_family": family,
                "created_ts": self._generate_timestamp(),
                "updated_ts": self._now_timestamp()
            }
            self.data["product_catalog"].append(product)
    
    # =========================================================================
    # SUBSCRIBER & MSISDN/SIM/DEVICE GENERATION
    # =========================================================================
    
    def generate_subscribers(self) -> None:
        """Generate subscriber records"""
        print(f"Generating {self.config.num_subscribers} subscribers...")
        
        for i in range(self.config.num_subscribers):
            subscriber_id = self._generate_uuid()
            self.subscriber_ids.append(subscriber_id)
            
            account_id = random.choice(self.account_ids)
            subscriber_type = random.choices(
                ["MOBILE", "FIXED", "IOT"],
                weights=[0.75, 0.20, 0.05]
            )[0]
            
            activation_date = self._generate_timestamp()
            is_prepaid = random.random() < self.config.prepaid_ratio
            
            status = random.choices(
                ["ACTIVE", "SUSPENDED", "BARRED", "TERMINATED"],
                weights=[0.80, 0.10, 0.05, 0.05]
            )[0]
            
            subscriber = {
                "subscriber_id": subscriber_id,
                "account_id": account_id,
                "subscriber_type": subscriber_type,
                "brand": random.choice(self.config.brands),
                "status": status,
                "activation_date": activation_date[:10],  # Date only
                "termination_date": None if status != "TERMINATED" else self._generate_timestamp()[:10],
                "created_ts": activation_date,
                "updated_ts": self._now_timestamp()
            }
            self.data["subscriber"].append(subscriber)
            
            if is_prepaid:
                self.prepaid_subscriber_ids.append(subscriber_id)
            
            # Generate related entities
            self._generate_subscriber_status_history(subscriber_id, activation_date)
            
            if subscriber_type == "MOBILE":
                self._generate_msisdn_for_subscriber(subscriber_id)
                self._generate_sim_for_subscriber(subscriber_id)
                self._generate_device_for_subscriber(subscriber_id)
    
    def _generate_subscriber_status_history(self, subscriber_id: str, activation_ts: str) -> None:
        """Generate status history for subscriber"""
        num_history = int(random.gauss(self.config.status_history_per_subscriber, 1))
        num_history = max(1, num_history)
        
        statuses = ["ACTIVE", "SUSPENDED", "ACTIVE", "BARRED", "ACTIVE"]
        prev_ts = activation_ts
        
        for j in range(num_history):
            effective_from = prev_ts
            effective_to = self._generate_timestamp() if j < num_history - 1 else None
            
            history = {
                "subscriber_id": subscriber_id,
                "status": statuses[j % len(statuses)],
                "reason_code": random.choice(["INITIAL", "PAYMENT_ISSUE", "CUSTOMER_REQUEST", "FRAUD_CHECK", "RESOLVED"]),
                "effective_from_ts": effective_from,
                "effective_to_ts": effective_to,
                "is_current": j == num_history - 1
            }
            self.data["subscriber_status_history"].append(history)
            prev_ts = effective_to or self._now_timestamp()
    
    def _generate_msisdn_for_subscriber(self, subscriber_id: str) -> None:
        """Generate MSISDN and link to subscriber"""
        msisdn_id = self._generate_uuid()
        self.msisdn_ids.append(msisdn_id)
        
        phone = self.dutch.generate_phone_number(mobile=True)
        created_ts = self._generate_timestamp()
        
        msisdn = {
            "msisdn_id": msisdn_id,
            "e164_number": phone,
            "number_status": "ASSIGNED",
            "country_code": "NL",
            "created_ts": created_ts,
            "updated_ts": self._now_timestamp()
        }
        self.data["msisdn"].append(msisdn)
        
        # Link to subscriber
        subscriber_msisdn = {
            "subscriber_id": subscriber_id,
            "msisdn_id": msisdn_id,
            "effective_from_ts": created_ts,
            "effective_to_ts": None,
            "is_current": True
        }
        self.data["subscriber_msisdn"].append(subscriber_msisdn)
    
    def _generate_sim_for_subscriber(self, subscriber_id: str) -> None:
        """Generate SIM and link to subscriber"""
        sim_id = self._generate_uuid()
        self.sim_ids.append(sim_id)
        
        # Generate realistic ICCID (19-20 digits)
        iccid = f"8931{random.randint(10, 99)}" + ''.join([str(random.randint(0, 9)) for _ in range(13)])
        imsi = f"204{random.randint(10, 99)}" + ''.join([str(random.randint(0, 9)) for _ in range(10)])
        created_ts = self._generate_timestamp()
        
        sim = {
            "sim_id": sim_id,
            "iccid": iccid,
            "imsi": imsi,
            "sim_type": random.choices(["ESIM", "PHYSICAL"], weights=[0.3, 0.7])[0],
            "sim_status": "ASSIGNED",
            "created_ts": created_ts,
            "updated_ts": self._now_timestamp()
        }
        self.data["sim"].append(sim)
        
        # Link to subscriber
        subscriber_sim = {
            "subscriber_id": subscriber_id,
            "sim_id": sim_id,
            "effective_from_ts": created_ts,
            "effective_to_ts": None,
            "is_current": True,
            "swap_reason": None
        }
        self.data["subscriber_sim"].append(subscriber_sim)
    
    def _generate_device_for_subscriber(self, subscriber_id: str) -> None:
        """Generate device and link to subscriber"""
        device_id = self._generate_uuid()
        self.device_ids.append(device_id)
        
        # Device templates
        devices = [
            ("Apple", "iPhone 15 Pro", "iOS", "17.0", "PHONE"),
            ("Apple", "iPhone 15", "iOS", "17.0", "PHONE"),
            ("Apple", "iPhone 14", "iOS", "16.0", "PHONE"),
            ("Apple", "iPhone 13", "iOS", "15.0", "PHONE"),
            ("Samsung", "Galaxy S24", "Android", "14", "PHONE"),
            ("Samsung", "Galaxy S23", "Android", "13", "PHONE"),
            ("Samsung", "Galaxy A54", "Android", "13", "PHONE"),
            ("Google", "Pixel 8", "Android", "14", "PHONE"),
            ("Google", "Pixel 7", "Android", "13", "PHONE"),
            ("Xiaomi", "14", "Android", "14", "PHONE"),
            ("Xiaomi", "13", "Android", "13", "PHONE"),
            ("OnePlus", "12", "Android", "14", "PHONE"),
            ("Huawei", "P60", "HarmonyOS", "4.0", "PHONE"),
            ("Nokia", "G42", "Android", "13", "PHONE"),
        ]
        
        manufacturer, model, os_name, os_version, category = random.choice(devices)
        
        # Generate IMEI (15 digits)
        imei = ''.join([str(random.randint(0, 9)) for _ in range(15)])
        tac = imei[:8]
        created_ts = self._generate_timestamp()
        
        device = {
            "device_id": device_id,
            "imei": imei,
            "tac": tac,
            "manufacturer": manufacturer,
            "model": model,
            "os_name": os_name,
            "os_version": os_version,
            "device_category": category,
            "created_ts": created_ts,
            "updated_ts": self._now_timestamp()
        }
        self.data["device"].append(device)
        
        # Link to subscriber
        subscriber_device = {
            "subscriber_id": subscriber_id,
            "device_id": device_id,
            "effective_from_ts": created_ts,
            "effective_to_ts": None,
            "is_current": True,
            "ownership_type": random.choice(["BYOD", "FINANCED", "LEASED"])
        }
        self.data["subscriber_device"].append(subscriber_device)
    
    # =========================================================================
    # SUBSCRIPTIONS & ENTITLEMENTS
    # =========================================================================
    
    def generate_subscriptions(self) -> None:
        """Generate subscriptions linking subscribers to products"""
        print("Generating subscriptions...")
        
        plan_products = [p for p in self.data["product_catalog"] if p["product_type"] == "PLAN"]
        addon_products = [p for p in self.data["product_catalog"] if p["product_type"] == "ADDON"]
        
        for subscriber_id in self.subscriber_ids:
            # Each subscriber has at least one plan
            num_subscriptions = max(1, int(random.gauss(self.config.subscriptions_per_subscriber, 0.5)))
            
            # Add a plan subscription
            plan = random.choice(plan_products) if plan_products else random.choice(self.data["product_catalog"])
            self._create_subscription(subscriber_id, plan["product_id"])
            
            # Potentially add add-ons
            for _ in range(num_subscriptions - 1):
                if addon_products:
                    addon = random.choice(addon_products)
                    self._create_subscription(subscriber_id, addon["product_id"])
    
    def _create_subscription(self, subscriber_id: str, product_id: str) -> None:
        """Create a subscription record"""
        subscription_id = self._generate_uuid()
        self.subscription_ids.append(subscription_id)
        
        start_date = self._generate_timestamp()
        status = random.choices(["ACTIVE", "SUSPENDED", "CANCELLED"], weights=[0.85, 0.10, 0.05])[0]
        
        subscription = {
            "subscription_id": subscription_id,
            "subscriber_id": subscriber_id,
            "product_id": product_id,
            "start_date": start_date[:10],
            "end_date": None if status != "CANCELLED" else self._generate_timestamp()[:10],
            "status": status,
            "recurring_fee": round(random.uniform(5, 100), 2),
            "currency_code": self.config.currency_code,
            "created_ts": start_date,
            "updated_ts": self._now_timestamp()
        }
        self.data["subscription"].append(subscription)
        
        # Generate entitlements
        self._generate_entitlements(subscription_id)
    
    def _generate_entitlements(self, subscription_id: str) -> None:
        """Generate entitlements for a subscription"""
        num_entitlements = max(1, int(random.gauss(self.config.entitlements_per_subscription, 0.5)))
        
        entitlement_types = [
            ("DATA_CAP", 10, "GB"),
            ("DATA_CAP", 25, "GB"),
            ("DATA_CAP", 50, "GB"),
            ("DATA_CAP", 100, "GB"),
            ("SPEED_TIER", 100, "MBPS"),
            ("SPEED_TIER", 500, "MBPS"),
            ("SPEED_TIER", 1000, "MBPS"),
            ("ROAMING_PACK", 5, "GB"),
            ("ROAMING_PACK", 10, "GB"),
            ("CONTENT", 1, "DAYS"),
        ]
        
        for _ in range(num_entitlements):
            ent_type, value, unit = random.choice(entitlement_types)
            created_ts = self._generate_timestamp()
            
            entitlement = {
                "entitlement_id": self._generate_uuid(),
                "subscription_id": subscription_id,
                "entitlement_type": ent_type,
                "value": value,
                "unit": unit,
                "valid_from_ts": created_ts,
                "valid_to_ts": None,
                "created_ts": created_ts,
                "updated_ts": self._now_timestamp()
            }
            self.data["entitlement"].append(entitlement)
    
    # =========================================================================
    # SERVICES & ORDERS
    # =========================================================================
    
    def generate_services(self) -> None:
        """Generate service records"""
        print("Generating services and orders...")
        
        for subscriber_id in self.subscriber_ids:
            num_services = max(1, int(random.gauss(self.config.services_per_subscriber, 0.5)))
            
            for _ in range(num_services):
                service_id = self._generate_uuid()
                created_ts = self._generate_timestamp()
                
                service = {
                    "service_id": service_id,
                    "subscriber_id": subscriber_id,
                    "service_type": random.choice(["VOICE", "DATA", "SMS", "BROADBAND", "TV", "IOT"]),
                    "service_status": random.choices(["ACTIVE", "SUSPENDED", "TERMINATED"], weights=[0.85, 0.10, 0.05])[0],
                    "created_ts": created_ts,
                    "updated_ts": self._now_timestamp()
                }
                self.data["service"].append(service)
            
            # Generate service orders
            num_orders = max(1, int(random.gauss(self.config.orders_per_subscriber, 1)))
            for _ in range(num_orders):
                self._generate_service_order(subscriber_id)
            
            # Generate porting requests
            if random.random() < self.config.porting_ratio:
                self._generate_porting_request(subscriber_id)
    
    def _generate_service_order(self, subscriber_id: str) -> None:
        """Generate a service order"""
        order_id = self._generate_uuid()
        requested_ts = self._generate_timestamp()
        
        status = random.choices(
            ["SUBMITTED", "IN_PROGRESS", "COMPLETED", "FAILED"],
            weights=[0.10, 0.15, 0.70, 0.05]
        )[0]
        
        order = {
            "order_id": order_id,
            "subscriber_id": subscriber_id,
            "order_type": random.choice(["ACTIVATE", "PLAN_CHANGE", "SIM_SWAP", "PORT_IN", "TERMINATE"]),
            "order_status": status,
            "requested_ts": requested_ts,
            "completed_ts": self._generate_timestamp() if status in ["COMPLETED", "FAILED"] else None,
            "failure_reason": "SYSTEM_ERROR" if status == "FAILED" else None,
            "created_ts": requested_ts,
            "updated_ts": self._now_timestamp()
        }
        self.data["service_order"].append(order)
    
    def _generate_porting_request(self, subscriber_id: str) -> None:
        """Generate a porting request"""
        operators = ["KPN", "T-Mobile", "Vodafone", "Tele2", "Simpel", "Ben", "Lebara"]
        
        port_id = self._generate_uuid()
        direction = random.choice(["PORT_IN", "PORT_OUT"])
        created_ts = self._generate_timestamp()
        
        status = random.choices(
            ["REQUESTED", "APPROVED", "SCHEDULED", "COMPLETED", "REJECTED"],
            weights=[0.10, 0.15, 0.15, 0.55, 0.05]
        )[0]
        
        request = {
            "port_request_id": port_id,
            "subscriber_id": subscriber_id,
            "direction": direction,
            "donor_operator": random.choice(operators) if direction == "PORT_IN" else self.config.operator_name,
            "recipient_operator": self.config.operator_name if direction == "PORT_IN" else random.choice(operators),
            "requested_msisdn": self.dutch.generate_phone_number(),
            "status": status,
            "requested_date": created_ts[:10],
            "completed_date": self._generate_timestamp()[:10] if status == "COMPLETED" else None,
            "created_ts": created_ts,
            "updated_ts": self._now_timestamp()
        }
        self.data["porting_request"].append(request)
    
    # =========================================================================
    # BILLING & PAYMENTS
    # =========================================================================
    
    def generate_billing(self) -> None:
        """Generate charges, invoices, and payments"""
        print("Generating billing records...")
        
        for account_id in self.account_ids:
            # Generate invoices
            num_invoices = max(1, int(random.gauss(self.config.invoices_per_account, 2)))
            for _ in range(num_invoices):
                self._generate_invoice(account_id)
            
            # Generate charges
            num_charges = max(1, int(random.gauss(self.config.charges_per_account, 5)))
            for _ in range(num_charges):
                self._generate_charge(account_id)
            
            # Generate payments
            num_payments = max(1, int(random.gauss(self.config.payments_per_account, 2)))
            for _ in range(num_payments):
                self._generate_payment(account_id)
        
        # Generate prepaid-specific records
        for subscriber_id in self.prepaid_subscriber_ids:
            num_topups = max(1, int(random.gauss(self.config.topups_per_prepaid, 2)))
            for _ in range(num_topups):
                self._generate_topup(subscriber_id)
            
            num_snapshots = max(1, int(random.gauss(self.config.balance_snapshots_per_prepaid, 2)))
            for _ in range(num_snapshots):
                self._generate_balance_snapshot(subscriber_id)
    
    def _generate_invoice(self, account_id: str) -> None:
        """Generate an invoice with line items"""
        invoice_id = self._generate_uuid()
        self.invoice_ids.append(invoice_id)
        
        billing_start = self._generate_timestamp()[:10]
        billing_start_date = datetime.fromisoformat(billing_start)
        billing_end_date = billing_start_date + timedelta(days=30)
        invoice_date = billing_end_date + timedelta(days=3)
        due_date = invoice_date + timedelta(days=14)
        
        total_amount = round(random.uniform(20, 200), 2)
        
        status = random.choices(
            ["ISSUED", "PAID", "OVERDUE", "CANCELLED"],
            weights=[0.15, 0.75, 0.08, 0.02]
        )[0]
        
        invoice = {
            "invoice_id": invoice_id,
            "account_id": account_id,
            "billing_period_start": billing_start,
            "billing_period_end": billing_end_date.date().isoformat(),
            "invoice_date": invoice_date.date().isoformat(),
            "due_date": due_date.date().isoformat(),
            "total_amount": total_amount,
            "currency_code": self.config.currency_code,
            "status": status,
            "created_ts": self._generate_timestamp(),
            "updated_ts": self._now_timestamp()
        }
        self.data["invoice"].append(invoice)
        
        # Generate invoice lines
        num_lines = max(1, int(random.gauss(self.config.invoice_lines_per_invoice, 1)))
        remaining_amount = total_amount
        
        for i in range(num_lines):
            if i == num_lines - 1:
                line_amount = remaining_amount
            else:
                line_amount = round(random.uniform(5, remaining_amount * 0.5), 2)
                remaining_amount -= line_amount
            
            line_type = random.choice(["PLAN", "ADDON", "USAGE", "ROAMING", "DEVICE_FINANCE", "DISCOUNT"])
            
            invoice_line = {
                "invoice_line_id": self._generate_uuid(),
                "invoice_id": invoice_id,
                "subscriber_id": random.choice(self.subscriber_ids) if self.subscriber_ids else None,
                "line_type": line_type,
                "description": f"{line_type} charges",
                "amount": line_amount if line_type != "DISCOUNT" else -abs(line_amount),
                "tax_amount": round(line_amount * 0.21, 2),  # 21% Dutch VAT
                "created_ts": self._generate_timestamp()
            }
            self.data["invoice_line"].append(invoice_line)
    
    def _generate_charge(self, account_id: str) -> None:
        """Generate a charge record"""
        charge = {
            "charge_id": self._generate_uuid(),
            "account_id": account_id,
            "subscriber_id": random.choice(self.subscriber_ids) if self.subscriber_ids else None,
            "charge_type": random.choice(["RECURRING", "USAGE", "ONE_TIME", "ADJUSTMENT"]),
            "source_usage_id": None,
            "amount": round(random.uniform(1, 100), 2),
            "currency_code": self.config.currency_code,
            "charge_ts": self._generate_timestamp(),
            "created_ts": self._generate_timestamp()
        }
        self.data["charge"].append(charge)
    
    def _generate_payment(self, account_id: str) -> None:
        """Generate a payment record"""
        payment = {
            "payment_id": self._generate_uuid(),
            "account_id": account_id,
            "payment_ts": self._generate_timestamp(),
            "amount": round(random.uniform(20, 200), 2),
            "currency_code": self.config.currency_code,
            "method": random.choice(["CARD", "DD", "BANK_TRANSFER", "IDEAL", "WALLET"]),
            "status": random.choices(["AUTHORIZED", "SETTLED", "FAILED", "REFUNDED"], weights=[0.10, 0.80, 0.05, 0.05])[0],
            "created_ts": self._generate_timestamp()
        }
        self.data["payment"].append(payment)
    
    def _generate_topup(self, subscriber_id: str) -> None:
        """Generate a prepaid top-up"""
        topup = {
            "topup_id": self._generate_uuid(),
            "subscriber_id": subscriber_id,
            "topup_ts": self._generate_timestamp(),
            "amount": random.choice([5, 10, 15, 20, 25, 30, 50]),
            "currency_code": self.config.currency_code,
            "channel": random.choice(["APP", "VOUCHER", "RETAIL", "BANK"]),
            "created_ts": self._generate_timestamp()
        }
        self.data["topup"].append(topup)
    
    def _generate_balance_snapshot(self, subscriber_id: str) -> None:
        """Generate a prepaid balance snapshot"""
        snapshot = {
            "snapshot_id": self._generate_uuid(),
            "subscriber_id": subscriber_id,
            "snapshot_ts": self._generate_timestamp(),
            "balance_amount": round(random.uniform(0, 50), 2),
            "currency_code": self.config.currency_code,
            "created_ts": self._generate_timestamp()
        }
        self.data["prepaid_balance_snapshot"].append(snapshot)
    
    # =========================================================================
    # CUSTOMER CARE
    # =========================================================================
    
    def generate_support_tickets(self) -> None:
        """Generate case/support tickets"""
        print("Generating support tickets...")
        
        subjects = [
            "Cannot make calls", "Slow data speed", "Billing inquiry", "Plan change request",
            "SIM not working", "App login issue", "Roaming charges question", "Device issue",
            "Port number request", "Cancel service", "Payment not reflecting", "Network coverage issue",
            "VoLTE activation", "International calling", "Invoice clarification", "Upgrade request"
        ]
        
        for account_id in self.account_ids:
            num_tickets = max(0, int(random.gauss(self.config.tickets_per_account, 1)))
            
            for _ in range(num_tickets):
                case = {
                    "case_id": self._generate_uuid(),
                    "account_id": account_id,
                    "subscriber_id": random.choice(self.subscriber_ids) if self.subscriber_ids else None,
                    "opened_ts": self._generate_timestamp(),
                    "channel": random.choice(["CALL", "CHAT", "STORE", "APP", "EMAIL", "SOCIAL"]),
                    "category": random.choice(["BILLING", "NETWORK", "DEVICE", "PORTING", "PLAN", "GENERAL"]),
                    "priority": random.choices(["LOW", "MED", "HIGH", "CRITICAL"], weights=[0.30, 0.45, 0.20, 0.05])[0],
                    "status": random.choices(["OPEN", "IN_PROGRESS", "RESOLVED", "CLOSED"], weights=[0.15, 0.20, 0.30, 0.35])[0],
                    "subject": random.choice(subjects),
                    "created_ts": self._generate_timestamp(),
                    "updated_ts": self._now_timestamp()
                }
                self.data["case_ticket"].append(case)
    
    # =========================================================================
    # MAIN GENERATION FLOW
    # =========================================================================
    
    def generate_all(self) -> Dict[str, List[Dict]]:
        """Generate all data according to schema"""
        print("=" * 60)
        print("Starting Telco Customer Data Generation")
        print(f"Configuration: {self.config.num_parties} parties, {self.config.num_accounts} accounts, {self.config.num_subscribers} subscribers")
        print("=" * 60)
        
        # Generate in dependency order
        self.generate_products()
        self.generate_parties()
        self.generate_accounts()
        self.generate_subscribers()
        self.generate_subscriptions()
        self.generate_services()
        self.generate_billing()
        self.generate_support_tickets()
        
        print("=" * 60)
        print("Data generation complete!")
        self._print_summary()
        print("=" * 60)
        
        return self.data
    
    def _print_summary(self) -> None:
        """Print summary of generated data"""
        print("\nGenerated records by table:")
        for table, records in sorted(self.data.items()):
            print(f"  {table}: {len(records):,}")
    
    # =========================================================================
    # DATA EXPORT
    # =========================================================================
    
    def save_to_csv(self, output_dir: str = None) -> None:
        """Save all data to CSV files"""
        output_dir = output_dir or self.config.output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        print(f"\nSaving to CSV in {output_dir}/...")
        
        for table_name, records in self.data.items():
            if not records:
                continue
            
            filepath = os.path.join(output_dir, f"{table_name}.csv")
            fieldnames = records[0].keys()
            
            with open(filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(records)
            
            print(f"  Saved {table_name}.csv ({len(records):,} records)")
    
    def save_to_json(self, output_dir: str = None) -> None:
        """Save all data to JSON files"""
        output_dir = output_dir or self.config.output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        print(f"\nSaving to JSON in {output_dir}/...")
        
        for table_name, records in self.data.items():
            if not records:
                continue
            
            filepath = os.path.join(output_dir, f"{table_name}.json")
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(records, f, indent=2, default=str)
            
            print(f"  Saved {table_name}.json ({len(records):,} records)")
    
    def save_to_parquet(self, output_dir: str = None) -> None:
        """Save all data to Parquet files (requires pandas and pyarrow)"""
        try:
            import pandas as pd
        except ImportError:
            print("Error: pandas required for Parquet export. Install with: pip install pandas pyarrow")
            return
        
        output_dir = output_dir or self.config.output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        print(f"\nSaving to Parquet in {output_dir}/...")
        
        for table_name, records in self.data.items():
            if not records:
                continue
            
            filepath = os.path.join(output_dir, f"{table_name}.parquet")
            df = pd.DataFrame(records)
            df.to_parquet(filepath, index=False)
            
            print(f"  Saved {table_name}.parquet ({len(records):,} records)")
    
    def save(self, output_dir: str = None, format: str = None) -> None:
        """Save data in the configured format"""
        format = format or self.config.output_format
        
        if format == "csv":
            self.save_to_csv(output_dir)
        elif format == "json":
            self.save_to_json(output_dir)
        elif format == "parquet":
            self.save_to_parquet(output_dir)
        else:
            print(f"Unknown format: {format}. Defaulting to CSV.")
            self.save_to_csv(output_dir)


def main():
    """Main entry point with CLI interface"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Generate realistic Dutch Telco customer data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python generate.py --preset small
  python generate.py --parties 5000 --accounts 4000 --subscribers 6000
  python generate.py --preset large --format parquet --output data/output
  python generate.py --config my_config.json
        """
    )
    
    parser.add_argument("--preset", choices=["small", "medium", "large", "enterprise"],
                       help="Use a preset configuration")
    parser.add_argument("--config", type=str, help="Path to JSON configuration file")
    parser.add_argument("--parties", type=int, help="Number of party records to generate")
    parser.add_argument("--accounts", type=int, help="Number of account records to generate")
    parser.add_argument("--subscribers", type=int, help="Number of subscriber records to generate")
    parser.add_argument("--products", type=int, help="Number of products in catalog")
    parser.add_argument("--format", choices=["csv", "json", "parquet"], default="csv",
                       help="Output format (default: csv)")
    parser.add_argument("--output", type=str, default="output",
                       help="Output directory (default: output)")
    parser.add_argument("--seed", type=int, help="Random seed for reproducibility")
    
    args = parser.parse_args()
    
    # Load configuration
    if args.config:
        config = GeneratorConfig.from_json_file(args.config)
    elif args.preset:
        presets = {
            "small": SMALL_CONFIG,
            "medium": MEDIUM_CONFIG,
            "large": LARGE_CONFIG,
            "enterprise": GeneratorConfig(
                num_parties=100000,
                num_accounts=80000,
                num_subscribers=150000,
                num_products=200
            )
        }
        config = presets[args.preset]
    else:
        config = GeneratorConfig()
    
    # Override with CLI arguments
    if args.parties:
        config.num_parties = args.parties
    if args.accounts:
        config.num_accounts = args.accounts
    if args.subscribers:
        config.num_subscribers = args.subscribers
    if args.products:
        config.num_products = args.products
    if args.format:
        config.output_format = args.format
    if args.output:
        config.output_dir = args.output
    if args.seed:
        config.random_seed = args.seed
    
    # Generate data
    generator = TelcoDataGenerator(config)
    generator.generate_all()
    generator.save()
    
    print(f"\nData saved to: {config.output_dir}/")


if __name__ == "__main__":
    main()
