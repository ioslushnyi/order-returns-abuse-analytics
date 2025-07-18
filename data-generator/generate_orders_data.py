import pandas as pd
import random
import uuid
from datetime import datetime, timedelta
import time

def generate_orders(users, products, n=10000, output_path="../data/orders.csv"):
    start = time.time()
    random.seed(42)

    users = pd.read_csv("../data/users.csv")
    products = pd.read_csv("../data/products.csv")

    # Preprocess: convert signup_date once
    users["signup_date"] = pd.to_datetime(users["signup_date"]).dt.date

    # Convert to lists for faster access
    user_list = users.to_dict("records")
    product_list = products.to_dict("records")

    orders = []

    for _ in range(n):  # Increase to 2–5M for full scale
        user = random.choice(user_list)
        product = random.choice(product_list)
        quantity = random.randint(1, 5)

        # Faster random date generation
        signup_date = user["signup_date"]
        days_since_signup = (datetime.today().date() - signup_date).days
        order_date = signup_date + timedelta(days=random.randint(0, max(days_since_signup, 1)))

        orders.append({
            "order_id": str(uuid.uuid4()),
            "user_id": user["user_id"],
            "sku": product["sku"],
            "unit_price": product["price"],
            "quantity": quantity,
            "order_date": order_date
        })

    df = pd.DataFrame(orders)
    df.to_csv(output_path, index=False)
    
    end = time.time()
    elapsed = end - start
    print(f"Orders generated. Elapsed time: {elapsed:.2f} seconds")

if __name__ == "__main__":
    generate_orders()
