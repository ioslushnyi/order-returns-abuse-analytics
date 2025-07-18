import pandas as pd
import random
from faker import Faker
import time

def generate_products(output_path="../data/products.csv"):
    start = time.time()
    fake = Faker()
    random.seed(42)

    categories = ["apparel", "electronics", "home", "music", "toys", "beauty", "sports", "kitchen", "books"]
    brands = ["ACME", "Globex", "Initech", "Januszex", "Umbrella", "Wonka", "Soylent", "WayneTech", "StarkIndustries"]

    products = []
    for i in range(1000):  # Increase to 10,000 for full scale
        sku = f"SKU-{i:05}"
        category = random.choice(categories)
        brand = random.choice(brands)
        price = round(random.uniform(10, 999), 2)
        launch_date = fake.date_between(start_date='-5y', end_date='today')
        returnable = random.choices([True, False], weights=[0.9, 0.1])[0]  # Most products are returnable

        products.append({
            "sku": sku,
            "category": category,
            "brand": brand,
            "price": price,
            "launch_date": launch_date,
            "returnable": returnable
        })

    df = pd.DataFrame(products)
    df.to_csv("../data/products.csv", index=False)
    
    end = time.time()
    elapsed = end - start
    print(f"Products generated. Elapsed time: {elapsed:.2f} seconds")