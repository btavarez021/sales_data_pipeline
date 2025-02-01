from faker import Faker
import csv
import random

fake = Faker()

def generate_orders(num_orders):
    with open('orders.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['OrderID', 'CustomerID', 'ProductID', 'OrderDate', 'Quantity', 'Price'])
        for _ in range(num_orders):
            writer.writerow([
                fake.uuid4(),
                random.randint(1, 100),
                random.randint(1, 50),
                fake.date_this_year(),
                random.randint(1, 5),
                round(random.uniform(10.0, 100.0), 2)
            ])

def generate_customers(num_customers):
    with open('customers.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['CustomerID', 'Name', 'Email', 'Phone', 'Address'])
        for i in range(1, num_customers + 1):
            writer.writerow([
                i,
                fake.name(),
                fake.email(),
                fake.phone_number(),
                fake.address()
            ])

def generate_products(num_products):
    with open('products.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['ProductID', 'Name', 'Category', 'Price'])
        for i in range(1, num_products + 1):
            writer.writerow([
                i,
                fake.word(),
                fake.word(),
                round(random.uniform(5.0, 50.0), 2)
            ])

generate_orders(100)
generate_customers(100)
generate_products(50)