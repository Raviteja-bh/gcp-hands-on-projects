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


def generate_claims(policies, num_claims=500):
    """Generate fake claims - some referencing invalid policies"""
    
    claims = []
    
    # Get list of valid policy IDs from generated policies
    valid_policy_ids = [p["policy_id"] for p in policies]
    
    for i in range(num_claims):
        claim_id = f"CLM-{str(uuid.uuid4())[:8].upper()}"
        
        # 95% get valid policy_id, 5% get fake one (orphaned claim)
        if random.random() < 0.95:
            policy_id = random.choice(valid_policy_ids)  # Real policy
        else:
            policy_id = f"POL-INVALID{i}"               # Fake policy!

        loss_amount = random.uniform(5000, 1000000)
        
        # ⚠️ 2% get negative loss amount
        if random.random() < 0.02:
            loss_amount = -loss_amount

        paid_amount = random.uniform(0, abs(loss_amount) * 0.8)
        
        # ⚠️ 3% get paid > loss (business rule violation)
        if random.random() < 0.03:
            paid_amount = abs(loss_amount) * 1.5

        claims.append({
            "claim_id": claim_id,
            "policy_id": policy_id,
            "claim_number": f"CN-{2023000 + i}",
            "loss_date": random_date(2023, 2025),
            "reported_date": random_date(2023, 2025),
            "loss_amount": round(loss_amount, 2),
            "paid_amount": round(paid_amount, 2),
            "reserve_amount": round(abs(loss_amount) - paid_amount, 2),
            "claim_status": random.choice(["Open", "Closed", "Pending", "Settled"]),
            "peril_type": random.choice(PERIL_TYPES),
            "created_at": datetime.now().isoformat(),
        })

    return claims

def generate_treaties(num_records=50):
    """Generate reinsurance treaties with some business rule violations"""
    
    treaties = []

    for i in range(num_records):
        treaty_id = f"TRT-{str(uuid.uuid4())[:8].upper()}"

        retention = random.uniform(100000, 5000000)
        limit = random.uniform(10000000, 50000000)

        # ⚠️ 5% get retention > limit (business rule violation)
        # retention should ALWAYS be less than limit
        if random.random() < 0.05:
            retention, limit = limit, retention  # Swap them!

        commission = random.uniform(0.05, 0.35)

        # ⚠️ 2% get commission > 1 (over 100% - impossible!)
        if random.random() < 0.02:
            commission = random.uniform(1.1, 2.0)

        treaties.append({
            "treaty_id": treaty_id,
            "treaty_name": f"Treaty-{2023}-{i:03d}",
            "treaty_type": random.choice(["Quota Share", "Surplus", 
                                          "Excess of Loss", "Stop Loss", 
                                          "Catastrophe XL"]),
            "effective_date": random_date(2023, 2024),
            "expiration_date": random_date(2025, 2026),
            "retention_amount": round(retention, 2),
            "limit_amount": round(limit, 2),
            "ceding_commission": round(commission, 4),
            "territory": random.choice(["North America", "Europe", 
                                        "Asia Pacific", "Global"]),
            "created_at": datetime.now().isoformat(),
        })

    return treaties

def generate_exposures(num_records=100):
    """Generate geographic exposure data"""
    
    exposures = []

    for i in range(num_records):
        exposure_id = f"EXP-{str(uuid.uuid4())[:8].upper()}"
        
        tiv = random.uniform(10000000, 500000000)  # Total Insured Value
        
        # ⚠️ 3% get negative TIV
        if random.random() < 0.03:
            tiv = -tiv

        num_policies = random.randint(10, 1000)
        avg_premium = abs(tiv) / num_policies * random.uniform(0.01, 0.05)

        exposures.append({
            "exposure_id": exposure_id,
            "region_code": random.choice(REGION_CODES),
            "peril_type": random.choice(PERIL_TYPES),
            "total_insured_value": round(tiv, 2),
            "number_of_policies": num_policies,
            "average_premium": round(avg_premium, 2),
            "as_of_date": random_date(2024, 2025),
            "created_at": datetime.now().isoformat(),
        })

    return exposures

if __name__ == "__main__":
    print("Generating synthetic reinsurance data...\n")

    # Generate all datasets
    policies = generate_policies(1000)
    claims = generate_claims(policies, 500)   # Needs policies for valid IDs
    treaties = generate_treaties(50)
    exposures = generate_exposures(100)

    # Save all to JSON
    save_to_json(policies, "data_generator/policies.json")
    save_to_json(claims, "data_generator/claims.json")
    save_to_json(treaties, "data_generator/treaties.json")
    save_to_json(exposures, "data_generator/exposures.json")

    print("\nDone!")