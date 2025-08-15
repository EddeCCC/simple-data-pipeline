import random
import pandas as pd
from faker import Faker
from logger import logging as log

fake = Faker()
Faker.seed(42)

def generate_users(n=100):
    """Generate fake user data. Some data may contain invalid fields."""
    data = []
    for _ in range(n):
        user = {
            "id": fake.uuid4(),
            "name": fake.name(),
            "email": fake.email(),
            "signup_date": fake.date_between(start_date='-1y', end_date='today'),
            "is_active": random.choice(["yes", "no", True, False, "TRUE", "", None]),
            "age": fake.random_int(18, 100)
        }

        if random.random() < 0.3:
            user["age"] = random.choice([None, -1, 999])

        data.append(user)
    return pd.DataFrame(data)

if __name__ == "__main__":
    log.info("Generating data...")

    generate_users()
    users = generate_users(100)

    output = "data/raw/users.csv"
    users.to_csv(output, index=False)

    log.info(f"Generated data saved to {output}")
