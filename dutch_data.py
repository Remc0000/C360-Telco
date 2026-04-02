"""
Dutch Data Utilities
Contains realistic Dutch names, addresses, cities with accurate lat/long coordinates.
"""

import random
from dataclasses import dataclass
from typing import List, Tuple, Optional


@dataclass
class DutchCity:
    """Dutch city with accurate coordinates"""
    name: str
    province: str
    postal_code_prefix: str  # First 2 digits of postal code
    latitude: float
    longitude: float


# Major Dutch cities with accurate coordinates
DUTCH_CITIES: List[DutchCity] = [
    # North Holland
    DutchCity("Amsterdam", "Noord-Holland", "10", 52.3676, 4.9041),
    DutchCity("Haarlem", "Noord-Holland", "20", 52.3873, 4.6462),
    DutchCity("Zaandam", "Noord-Holland", "15", 52.4387, 4.8266),
    DutchCity("Alkmaar", "Noord-Holland", "18", 52.6324, 4.7534),
    DutchCity("Hilversum", "Noord-Holland", "12", 52.2292, 5.1669),
    DutchCity("Amstelveen", "Noord-Holland", "11", 52.3114, 4.8643),
    DutchCity("Purmerend", "Noord-Holland", "14", 52.5053, 4.9596),
    DutchCity("Hoorn", "Noord-Holland", "16", 52.6427, 5.0590),
    
    # South Holland
    DutchCity("Rotterdam", "Zuid-Holland", "30", 51.9244, 4.4777),
    DutchCity("Den Haag", "Zuid-Holland", "25", 52.0705, 4.3007),
    DutchCity("Leiden", "Zuid-Holland", "23", 52.1601, 4.4970),
    DutchCity("Dordrecht", "Zuid-Holland", "33", 51.8133, 4.6901),
    DutchCity("Delft", "Zuid-Holland", "26", 52.0116, 4.3571),
    DutchCity("Zoetermeer", "Zuid-Holland", "27", 52.0570, 4.4931),
    DutchCity("Gouda", "Zuid-Holland", "28", 52.0115, 4.7104),
    DutchCity("Schiedam", "Zuid-Holland", "31", 51.9217, 4.3893),
    DutchCity("Alphen aan den Rijn", "Zuid-Holland", "24", 52.1293, 4.6578),
    DutchCity("Vlaardingen", "Zuid-Holland", "31", 51.9125, 4.3419),
    
    # Utrecht
    DutchCity("Utrecht", "Utrecht", "35", 52.0907, 5.1214),
    DutchCity("Amersfoort", "Utrecht", "38", 52.1561, 5.3878),
    DutchCity("Nieuwegein", "Utrecht", "34", 52.0287, 5.0814),
    DutchCity("Veenendaal", "Utrecht", "39", 52.0283, 5.5539),
    DutchCity("Zeist", "Utrecht", "37", 52.0907, 5.2336),
    
    # North Brabant
    DutchCity("Eindhoven", "Noord-Brabant", "56", 51.4416, 5.4697),
    DutchCity("Tilburg", "Noord-Brabant", "50", 51.5555, 5.0913),
    DutchCity("Breda", "Noord-Brabant", "48", 51.5719, 4.7683),
    DutchCity("'s-Hertogenbosch", "Noord-Brabant", "52", 51.6978, 5.3037),
    DutchCity("Helmond", "Noord-Brabant", "57", 51.4818, 5.6611),
    DutchCity("Oss", "Noord-Brabant", "53", 51.7651, 5.5190),
    DutchCity("Roosendaal", "Noord-Brabant", "47", 51.5307, 4.4655),
    
    # Gelderland
    DutchCity("Nijmegen", "Gelderland", "65", 51.8126, 5.8372),
    DutchCity("Arnhem", "Gelderland", "68", 51.9851, 5.8987),
    DutchCity("Apeldoorn", "Gelderland", "73", 52.2112, 5.9699),
    DutchCity("Ede", "Gelderland", "67", 52.0383, 5.6678),
    DutchCity("Doetinchem", "Gelderland", "70", 51.9654, 6.2882),
    
    # Overijssel
    DutchCity("Enschede", "Overijssel", "75", 52.2215, 6.8937),
    DutchCity("Zwolle", "Overijssel", "80", 52.5168, 6.0830),
    DutchCity("Deventer", "Overijssel", "74", 52.2548, 6.1630),
    DutchCity("Hengelo", "Overijssel", "75", 52.2657, 6.7933),
    DutchCity("Almelo", "Overijssel", "76", 52.3567, 6.6628),
    
    # Limburg
    DutchCity("Maastricht", "Limburg", "62", 50.8514, 5.6910),
    DutchCity("Venlo", "Limburg", "59", 51.3704, 6.1724),
    DutchCity("Heerlen", "Limburg", "64", 50.8880, 5.9812),
    DutchCity("Sittard", "Limburg", "61", 51.0023, 5.8698),
    DutchCity("Roermond", "Limburg", "60", 51.1940, 5.9877),
    
    # Groningen
    DutchCity("Groningen", "Groningen", "97", 53.2194, 6.5665),
    DutchCity("Hoogezand", "Groningen", "96", 53.1614, 6.7620),
    
    # Friesland
    DutchCity("Leeuwarden", "Friesland", "89", 53.2014, 5.7992),
    DutchCity("Drachten", "Friesland", "92", 53.1123, 6.0918),
    
    # Drenthe
    DutchCity("Assen", "Drenthe", "94", 52.9929, 6.5642),
    DutchCity("Emmen", "Drenthe", "78", 52.7860, 6.8973),
    
    # Flevoland
    DutchCity("Almere", "Flevoland", "13", 52.3508, 5.2647),
    DutchCity("Lelystad", "Flevoland", "82", 52.5185, 5.4714),
    
    # Zeeland
    DutchCity("Middelburg", "Zeeland", "43", 51.4988, 3.6136),
    DutchCity("Vlissingen", "Zeeland", "44", 51.4427, 3.5709),
    DutchCity("Goes", "Zeeland", "44", 51.5035, 3.8893),
]

# Common Dutch first names (male)
DUTCH_MALE_FIRST_NAMES = [
    "Jan", "Pieter", "Willem", "Johan", "Hendrik", "Cornelis", "Johannes", "Gerrit",
    "Dirk", "Adrianus", "Jacobus", "Martinus", "Petrus", "Theodorus", "Antonius",
    "Thomas", "Daan", "Sem", "Lucas", "Levi", "Finn", "Noah", "Milan", "Jesse",
    "Lars", "Luuk", "Bram", "Max", "Julian", "Jayden", "Tim", "Thijs", "Ruben",
    "Sven", "Jeroen", "Bas", "Joost", "Mark", "Erik", "Wouter", "Frank", "Arjan",
    "Dennis", "Martijn", "Rick", "Stefan", "Ramon", "Kevin", "Michiel", "Roel",
    "Jasper", "Timo", "Koen", "Stijn", "Joris", "Maarten", "Vincent", "Gijs",
    "Olivier", "Benjamin", "Sam", "Adam", "Oscar", "Floris", "Mees", "Hugo",
    "Sander", "Patrick", "Robert", "Paul", "Marco", "Bart", "Niels", "David"
]

# Common Dutch first names (female)
DUTCH_FEMALE_FIRST_NAMES = [
    "Maria", "Anna", "Elisabeth", "Johanna", "Cornelia", "Hendrika", "Wilhelmina",
    "Margaretha", "Geertruida", "Adriana", "Emma", "Sophie", "Julia", "Lotte",
    "Eva", "Sara", "Lisa", "Sanne", "Fleur", "Anne", "Mila", "Lieke", "Noa",
    "Iris", "Isa", "Tessa", "Femke", "Lynn", "Laura", "Kim", "Linda", "Nicole",
    "Sandra", "Marieke", "Anouk", "Marloes", "Esther", "Monique", "Ellen",
    "Nienke", "Daphne", "Rianne", "Eline", "Charlotte", "Amber", "Zoë", "Nina",
    "Roos", "Floor", "Britt", "Merel", "Loes", "Kirsten", "Manon", "Renate",
    "Danielle", "Michelle", "Chantal", "Wendy", "Susan", "Patricia", "Judith",
    "Denise", "Marleen", "Ingrid", "Ilse", "Bianca", "Heleen", "Petra", "Wilma"
]

# Common Dutch surnames
DUTCH_SURNAMES = [
    "de Jong", "Jansen", "de Vries", "van den Berg", "van Dijk", "Bakker",
    "Janssen", "Visser", "Smit", "Meijer", "de Boer", "Mulder", "de Groot",
    "Bos", "Vos", "Peters", "Hendriks", "van Leeuwen", "Dekker", "Brouwer",
    "de Wit", "Dijkstra", "Smits", "de Graaf", "van der Meer", "van der Linden",
    "Kok", "Jacobs", "de Haan", "Vermeer", "van den Heuvel", "van der Veen",
    "van den Broek", "de Bruijn", "van der Heijden", "Schouten", "van Beek",
    "Willems", "van Vliet", "van de Ven", "Hoekstra", "Maas", "Verhoeven",
    "Koster", "van Dam", "van der Wal", "Prins", "Blom", "Huisman", "Peeters",
    "de Jonge", "Kuijpers", "van Loon", "Kuiper", "van den Brink", "Molenaar",
    "van Wijk", "Groen", "van der Berg", "Franssen", "Postma", "Hermans",
    "van der Zee", "Jonker", "van Veen", "Klein", "van der Horst", "Driessen",
    "Gerritsen", "van Schaik", "van der Pol", "Evers", "van der Steen", "Schipper",
    "de Koning", "van der Spek", "Verwey", "van Houten", "Mens", "Cuperus",
    "Bosch", "Koopman", "Vrolijk", "van Haaren", "Vink", "Stam", "Spijker"
]

# Common Dutch street name components
DUTCH_STREET_PREFIXES = [
    "Kerk", "Hoofd", "Markt", "Station", "School", "Park", "Molen", "Dorps",
    "Water", "Haven", "Bos", "Berg", "Dijk", "Singel", "Gracht", "Laan",
    "Zand", "Nieuwe", "Oude", "Lange", "Korte", "Breede", "Smalle", "Noord",
    "Zuid", "Oost", "West", "Groen", "Rood", "Witte", "Zwarte", "Hoge", "Lage"
]

DUTCH_STREET_MAIN = [
    "straat", "weg", "laan", "plein", "kade", "gracht", "singel", "dreef",
    "pad", "steeg", "hof", "ring", "dijk", "baan", "boulevard"
]

DUTCH_STREET_NAMES = [
    "Kalverstraat", "Damrak", "Rokin", "Prinsengracht", "Herengracht",
    "Keizersgracht", "Amstelstraat", "Leidsestraat", "Spuistraat",
    "Utrechtsestraat", "Vijzelstraat", "Reguliersbreestraat", "Koningsplein",
    "Rembrandtplein", "Leidseplein", "Museumplein", "Vondelstraat",
    "Nassaukade", "Bilderdijkstraat", "Kinkerstraat", "Jan Pieter Heijestraat",
    "Overtoom", "Eerste Constantijn Huygensstraat", "Ferdinand Bolstraat",
    "Albert Cuypstraat", "Van Woustraat", "Ceintuurbaan", "Jodenbreestraat",
    "Nieuwendijk", "Haarlemmerstraat", "Haarlemmerdijk", "Westerstraat",
    "Brouwersgracht", "Lindengracht", "Egelantiersgracht", "Bloemgracht",
    "Lauriergracht", "Looiersgracht", "Passeerdersgracht", "Leidsegracht",
    "Kerkstraat", "Nieuwe Spiegelstraat", "Spiegelgracht", "Stadhouderskade",
    "Westeinde", "Buitenhof", "Lange Voorhout", "Plein", "Parkstraat",
    "Laan van Meerdervoort", "Scheveningseweg", "Badhuisweg", "Gevers Deynootweg",
    "Strandweg", "Erasmusweg", "Loosduinseweg", "Zuiderparklaan", "Hobbemastraat",
    "Javastraat", "Sumatrastraat", "Borneostraat", "Molukkenstraat", "Celebesstraat"
]

# Dutch email domains
DUTCH_EMAIL_DOMAINS = [
    "gmail.com", "hotmail.nl", "outlook.nl", "live.nl", "kpnmail.nl",
    "ziggo.nl", "xs4all.nl", "planet.nl", "hetnet.nl", "home.nl",
    "tele2.nl", "quicknet.nl", "upcmail.nl", "casema.nl", "chello.nl",
    "yahoo.nl", "msn.nl", "zonnet.nl", "wxs.nl", "freeler.nl"
]


class DutchDataGenerator:
    """Generator for realistic Dutch personal and address data"""
    
    def __init__(self, seed: Optional[int] = None):
        if seed is not None:
            random.seed(seed)
        self._address_offsets = {}
    
    def generate_first_name(self, gender: Optional[str] = None) -> Tuple[str, str]:
        """Generate a Dutch first name. Returns (name, gender)"""
        if gender is None:
            gender = random.choice(["M", "F"])
        
        if gender == "M":
            name = random.choice(DUTCH_MALE_FIRST_NAMES)
        else:
            name = random.choice(DUTCH_FEMALE_FIRST_NAMES)
        
        return name, gender
    
    def generate_surname(self) -> str:
        """Generate a Dutch surname"""
        return random.choice(DUTCH_SURNAMES)
    
    def generate_full_name(self, gender: Optional[str] = None) -> Tuple[str, str, str]:
        """Generate full Dutch name. Returns (given_name, family_name, gender)"""
        given_name, gen = self.generate_first_name(gender)
        family_name = self.generate_surname()
        return given_name, family_name, gen
    
    def generate_email(self, given_name: str, family_name: str) -> str:
        """Generate email based on name"""
        domain = random.choice(DUTCH_EMAIL_DOMAINS)
        
        # Clean names for email
        given_clean = given_name.lower().replace(" ", "").replace("'", "")
        family_clean = family_name.lower().replace(" ", "").replace("'", "")
        family_clean = family_clean.replace("de", "").replace("van", "").replace("den", "").strip()
        
        patterns = [
            f"{given_clean}.{family_clean}",
            f"{given_clean}{family_clean}",
            f"{given_clean[0]}.{family_clean}",
            f"{given_clean}_{family_clean}",
            f"{given_clean}{random.randint(1, 99)}",
            f"{family_clean}.{given_clean}",
        ]
        
        username = random.choice(patterns)
        return f"{username}@{domain}"
    
    def generate_city(self) -> DutchCity:
        """Select a random Dutch city"""
        # Weight larger cities more heavily
        weights = []
        for city in DUTCH_CITIES:
            if city.name in ["Amsterdam", "Rotterdam", "Den Haag", "Utrecht"]:
                weights.append(5)
            elif city.name in ["Eindhoven", "Tilburg", "Groningen", "Almere", "Breda", "Nijmegen"]:
                weights.append(3)
            else:
                weights.append(1)
        
        return random.choices(DUTCH_CITIES, weights=weights, k=1)[0]
    
    def generate_street_name(self) -> str:
        """Generate a Dutch street name"""
        if random.random() < 0.6:
            # Use predefined street names
            return random.choice(DUTCH_STREET_NAMES)
        else:
            # Generate composite street name
            prefix = random.choice(DUTCH_STREET_PREFIXES)
            suffix = random.choice(DUTCH_STREET_MAIN)
            return f"{prefix}{suffix}"
    
    def generate_house_number(self) -> str:
        """Generate a Dutch house number (sometimes with letter suffix)"""
        number = random.randint(1, 200)
        if random.random() < 0.1:
            letter = random.choice("ABCDEF")
            return f"{number}{letter}"
        return str(number)
    
    def generate_postal_code(self, city: DutchCity) -> str:
        """Generate a Dutch postal code (format: 1234 AB)"""
        # Use city prefix for first two digits
        prefix = city.postal_code_prefix
        suffix_digits = str(random.randint(0, 99)).zfill(2)
        letters = ''.join(random.choices("ABCDEFGHJKLMNPRSTUVWXYZ", k=2))
        return f"{prefix}{suffix_digits} {letters}"
    
    def generate_coordinates(self, city: DutchCity, street: str = "", house_number: str = "") -> Tuple[float, float]:
        """Generate street-level coordinates based on city, street name and house number.
        
        Uses deterministic hashing to generate consistent coordinates for the same
        street/house combination, while ensuring geographic distribution within the city.
        """
        # Base city coordinates
        base_lat = city.latitude
        base_lon = city.longitude
        
        # Use street name to determine the "street" location within city
        # This creates a grid-like street pattern
        if street:
            street_hash = hash(street + city.name) % 10000
            # Map to a position within ~3km of city center
            street_lat_offset = ((street_hash % 100) - 50) * 0.0006  # ~60m per unit
            street_lon_offset = ((street_hash // 100) - 50) * 0.0008  # ~60m per unit
        else:
            street_lat_offset = random.uniform(-0.03, 0.03)
            street_lon_offset = random.uniform(-0.04, 0.04)
        
        # Use house number to place along the street
        # Even/odd numbers on opposite sides, increasing numbers along street
        if house_number:
            try:
                num = int(''.join(filter(str.isdigit, house_number)) or '1')
            except ValueError:
                num = random.randint(1, 200)
            
            # Position along street based on house number
            position_offset = (num % 200) * 0.00002  # ~2m per house
            
            # Even/odd on opposite sides of street
            side_offset = 0.00005 if num % 2 == 0 else -0.00005
            
            # Add small random variation for realism (~5m)
            lat_jitter = random.uniform(-0.00004, 0.00004)
            lon_jitter = random.uniform(-0.00006, 0.00006)
        else:
            position_offset = random.uniform(0, 0.004)
            side_offset = random.choice([-0.00005, 0.00005])
            lat_jitter = random.uniform(-0.0001, 0.0001)
            lon_jitter = random.uniform(-0.0001, 0.0001)
        
        # Calculate final coordinates
        lat = round(base_lat + street_lat_offset + side_offset + lat_jitter, 6)
        lon = round(base_lon + street_lon_offset + position_offset + lon_jitter, 6)
        
        return lat, lon
    
    def generate_address(self) -> dict:
        """Generate a complete Dutch address with street-level coordinates"""
        city = self.generate_city()
        street = self.generate_street_name()
        house_number = self.generate_house_number()
        postal_code = self.generate_postal_code(city)
        lat, lon = self.generate_coordinates(city, street, house_number)
        
        return {
            "line1": f"{street} {house_number}",
            "line2": None,
            "city": city.name,
            "state_region": city.province,
            "postal_code": postal_code,
            "country_code": "NL",
            "latitude": lat,
            "longitude": lon
        }
    
    def generate_phone_number(self, mobile: bool = True) -> str:
        """Generate a Dutch phone number in E.164 format"""
        if mobile:
            # Dutch mobile numbers: 06-XXXXXXXX
            prefix = "6"
            number = ''.join(random.choices("0123456789", k=8))
        else:
            # Dutch landline: area code + number
            area_codes = ["10", "13", "14", "15", "20", "23", "24", "26", "30", "33", 
                         "35", "36", "38", "40", "43", "45", "46", "50", "53", "55", 
                         "58", "70", "71", "72", "73", "74", "75", "76", "77", "78", "79"]
            prefix = random.choice(area_codes)
            number = ''.join(random.choices("0123456789", k=7))
        
        return f"+31{prefix}{number}"
    
    def generate_birthdate(self, min_age: int = 18, max_age: int = 80) -> str:
        """Generate a random birthdate"""
        from datetime import date, timedelta
        
        today = date.today()
        min_date = today - timedelta(days=max_age * 365)
        max_date = today - timedelta(days=min_age * 365)
        
        days_between = (max_date - min_date).days
        random_days = random.randint(0, days_between)
        birth_date = min_date + timedelta(days=random_days)
        
        return birth_date.isoformat()
    
    def generate_organization_name(self) -> str:
        """Generate a Dutch organization/company name"""
        prefixes = [
            "Nederlandse", "Holland", "Amsterdam", "Rotterdam", "Utrecht",
            "Noord", "Zuid", "West", "Oost", "Centraal", "Eerste", "Nieuwe"
        ]
        
        industries = [
            "Technologie", "Consulting", "Services", "Solutions", "Digital",
            "Logistics", "Transport", "Retail", "Finance", "Healthcare",
            "Media", "Telecom", "Energy", "Food", "Construction"
        ]
        
        suffixes = ["B.V.", "N.V.", "VOF", ""]
        
        pattern = random.choice([
            f"{random.choice(prefixes)} {random.choice(industries)} {random.choice(suffixes)}",
            f"{self.generate_surname()} {random.choice(industries)} {random.choice(suffixes)}",
            f"{random.choice(industries)} {random.choice(prefixes).lower()} {random.choice(suffixes)}"
        ])
        
        return pattern.strip()


# Singleton instance for easy access
_generator = None

def get_dutch_data_generator(seed: Optional[int] = None) -> DutchDataGenerator:
    """Get or create Dutch data generator instance"""
    global _generator
    if _generator is None or seed is not None:
        _generator = DutchDataGenerator(seed)
    return _generator
