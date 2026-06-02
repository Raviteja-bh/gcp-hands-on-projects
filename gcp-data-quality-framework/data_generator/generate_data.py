import random       # For generating random values
import uuid         # For generating unique IDs like "POL-A1B2C3D4"
from datetime import datetime, timedelta  # For date calculations
import json         # For saving data as JSON files
import os           # For file path operations
from schemas import REGION_CODES, POLICY_TYPES, CURRENCY_CODES, PERIL_TYPES

random.seed(42)  # Makes random numbers reproducible
                 # Same seed = same "random" data every time
                 # Why? So your results are consistent

def random_date(start_year=2023, end_year=2025):
    """Generate a random date between two years"""
    start = datetime(start_year, 1, 1)   # Jan 1 of start year
    end = datetime(end_year, 12, 31)     # Dec 31 of end year
    delta = end - start                  # Total days between them
    random_days = random.randint(0, delta.days)  # Pick random day count
    return (start + timedelta(days=random_days)).strftime('%Y-%m-%d')
                                         # Return as "YYYY-MM-DD" string

def generate_policies(num_records=1000):
    """Generate fake insurance policies with some bad data"""
    
    policies = []  # Empty list - we'll add records one by one

    for i in range(num_records):  # Loop 1000 times
        
        # Generate a unique ID like "POL-A1B2C3D4"
        policy_id = f"POL-{str(uuid.uuid4())[:8].upper()}"

        # Generate random dates
        inception = random_date(2023, 2024)  # Start date
        expiry = random_date(2025, 2026)     # End date

        # Generate premium - normally between 10,000 and 5,000,000
        premium = random.uniform(10000, 5000000)

        # ⚠️ INTENTIONAL BAD DATA: 2% of records get negative premium
        if random.random() < 0.02:  # random() gives 0.0 to 1.0
            premium = -premium      # Flip to negative

        # Coverage must always be bigger than premium (business rule)
        coverage_limit = random.uniform(premium * 5, premium * 20)

        # ⚠️ INTENTIONAL BAD DATA: 1% get coverage LESS than premium
        if random.random() < 0.01:
            coverage_limit = premium * 0.5  # Half of premium = invalid!

        # Region code - 3% will be missing (NULL)
        region = random.choice(REGION_CODES)
        if random.random() < 0.03:
            region = None  # None in Python = NULL in BigQuery

        # Currency - 1% get invalid currency "XXX"
        currency = random.choice(CURRENCY_CODES)
        if random.random() < 0.01:
            currency = "XXX"  # Not in our valid list!

        # Build the record as a dictionary
        policies.append({
            "policy_id": policy_id,
            "policy_number": f"PN-{2023000 + i}",
            "insured_name": f"Insured Corp {i}",
            "inception_date": inception,
            "expiry_date": expiry,
            "premium_amount": round(premium, 2),
            "currency_code": currency,
            "coverage_limit": round(coverage_limit, 2),
            "region_code": region,
            "policy_type": random.choice(POLICY_TYPES),
            "created_at": datetime.now().isoformat(),
        })

    return policies  # Return the full list of 1000 records

def save_to_json(data, filename):
    """Save generated data to a JSON file"""
    
    with open(filename, 'w') as f:  # Open file for writing
        json.dump(data, f, indent=2) # Write data as formatted JSON
    
    print(f"✓ Generated {len(data)} records → {filename}")

if __name__ == "__main__":
    # This block only runs when you directly run this file
    # NOT when another file imports it
    
    print("Generating synthetic reinsurance data...\n")
    
    # Generate data
    policies = generate_policies(1000)
    
    # Save to JSON
    save_to_json(policies, "data_generator/policies.json")
    
    print("\nDone!")