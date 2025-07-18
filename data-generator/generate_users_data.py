from faker import Faker
import uuid
import pandas as pd
import random
import time

def generate_users(n=500, output_path="../data/users.csv"):
    start = time.time()
    fake = Faker()
    random.seed(42)

    tiers = ["Starter", "Bronze", "Silver", "Gold", "Platinum"]

    users = []
    for _ in range(n):
        users.append({
            "user_id": str(uuid.uuid4()),
            "signup_date": fake.date_between(start_date="-5y", end_date="today"),
            "loyalty_tier": random.choices(tiers, weights=[0.4, 0.3, 0.15, 0.10, 0.05])[0]
        })

    df = pd.DataFrame(users)
    df.to_csv(output_path, index=False)
    
    end = time.time()
    elapsed = end - start
    print(f"Users generated. Elapsed time: {elapsed:.2f} seconds")

if __name__ == "__main__":
    generate_users()
