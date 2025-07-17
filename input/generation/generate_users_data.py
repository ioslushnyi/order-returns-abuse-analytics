from faker import Faker
import uuid
import pandas as pd
import random

fake = Faker()
random.seed(42)

tiers = ["Starter", "Bronze", "Silver", "Gold", "Platinum"]

users = []
for _ in range(5000):
    users.append({
        "user_id": str(uuid.uuid4()),
        "signup_date": fake.date_between(start_date="-5y", end_date="today"),
        "loyalty_tier": random.choices(tiers, weights=[0.4, 0.3, 0.15, 0.10, 0.05])[0]
    })

df = pd.DataFrame(users)
df.to_csv("../data/users.csv", index=False)
