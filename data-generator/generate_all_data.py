from generate_users_data import generate_users
from generate_products_data import generate_products
from generate_orders_data import generate_orders
from generate_returns_data import generate_returns

import pandas as pd
import time

start = time.time()

# Step 1: Generate users
generate_users()
users = pd.read_csv("../data/users.csv")

# Step 2: Generate products
generate_products()
products = pd.read_csv("../data/products.csv")

# Step 3: Generate orders
generate_orders(users, products)
orders = pd.read_csv("../data/orders.csv")

# Step 4: Generate returns
generate_returns(orders, users, products)

end = time.time()
elapsed = end - start
print(f"All data generated. Elapsed time: {elapsed:.2f} seconds")
