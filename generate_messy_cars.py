import csv
import random
import math

random.seed(42)

# Messy column names
HEADERS = [
    "Car Make",
    "model_NAME",
    "YEAR ",
    "price($)",
    "Mileage(km) ",
    "colour/Color",
    " fuel_Type",
    "No. of Owners"
]

MAKES = ["Toyota", "toyota", "TOYOTA", "Ford", "ford", "BMW", "bmw", "B.M.W",
         "Honda", "HONDA", "Chevrolet", "chevy", "Nissan", "nissan", "Hyundai",
         "VW", "Volkswagen", "volkswagen", "Audi", "AUDI"]

MODELS = ["Camry", "camry", "Corolla", "F-150", "f150", "F 150", "Civic",
          "civic", "Mustang", "mustang", "Accord", "3 Series", "3series",
          "Elantra", "Golf", "A4", "Silverado", "silverado", "Altima", "Sentra"]

COLORS = ["Red", "red", "RED", "Blue", "blue", "BLUE", "White", "white",
          "Black", "black", "Silver", "silver", "SILVER", "Grey", "Gray",
          "gray", "Green", "green", "N/A", "unknown", "?", ""]

FUEL_TYPES = ["Petrol", "petrol", "PETROL", "Gasoline", "gasoline", "Gas",
              "Diesel", "diesel", "DIESEL", "Electric", "electric", "EV",
              "Hybrid", "hybrid", "HYBRID", "plug-in hybrid", "Unknown", ""]

YEAR_FORMATS = [
    lambda y: str(y),
    lambda y: str(y)[2:],
    lambda y: str(y) + ".0",
    lambda y: f"'{str(y)[2:]}",
    lambda y: ["nineteen ninety five", "two thousand", "two thousand and five",
               "twenty ten", "twenty twenty"][random.randint(0, 4)],
    lambda y: str(y) + " model",
    lambda y: None,
]

PRICE_FORMATS = [
    lambda p: str(p),
    lambda p: f"${p}",
    lambda p: f"${p:,}",
    lambda p: f"{p}.00",
    lambda p: str(p // 1000) + "k",
    lambda p: "N/A",
    lambda p: None,
    lambda p: "negotiable",
    lambda p: str(float(p)),
    lambda p: str(p) + " USD",
]

MILEAGE_FORMATS = [
    lambda m: str(m),
    lambda m: f"{m} km",
    lambda m: f"{m:,}",
    lambda m: str(round(m / 1.609, 1)) + " miles",
    lambda m: str(m) + "KM",
    lambda m: None,
    lambda m: "unknown",
    lambda m: str(m) + ".0",
]

OWNER_FORMATS = [
    lambda o: str(o),
    lambda o: ["one", "two", "three", "four", "five"][min(o - 1, 4)],
    lambda o: f"{o} owner" + ("s" if o > 1 else ""),
    lambda o: str(float(o)),
    lambda o: None,
    lambda o: "N/A",
    lambda o: str(o) + "st" if o == 1 else str(o) + "nd" if o == 2 else str(o) + "rd" if o == 3 else str(o) + "th",
]

def maybe_none(value, null_chance=0.08):
    """Randomly replace a value with a missing value indicator."""
    if random.random() < null_chance:
        return random.choice([None, "", "N/A", "NA", "n/a", "#N/A", "null", "NULL", "-"])
    return value

def generate_row():
    make = random.choice(MAKES)
    model = random.choice(MODELS)
    year_val = random.randint(1990, 2024)
    price_val = random.randint(2000, 95000)
    mileage_val = random.randint(0, 350000)
    color = random.choice(COLORS)
    fuel = random.choice(FUEL_TYPES)
    owners = random.randint(1, 5)

    # Apply messy formatting
    year = random.choice(YEAR_FORMATS)(year_val)
    price = random.choice(PRICE_FORMATS)(price_val)
    mileage = random.choice(MILEAGE_FORMATS)(mileage_val)
    owner_str = random.choice(OWNER_FORMATS)(owners)

    # Inject random missing values
    row = [
        maybe_none(make, 0.05),
        maybe_none(model, 0.06),
        maybe_none(year, 0.07),
        maybe_none(price, 0.09),
        maybe_none(mileage, 0.08),
        maybe_none(color, 0.10),
        maybe_none(fuel, 0.07),
        maybe_none(owner_str, 0.08),
    ]

    # Occasionally swap make and model columns
    if random.random() < 0.03:
        row[0], row[1] = row[1], row[0]

    # Occasionally put price in mileage column
    if random.random() < 0.02:
        row[4] = f"${random.randint(5000, 80000)}"

    # Occasionally add junk/whitespace
    for i in range(len(row)):
        if row[i] and isinstance(row[i], str) and random.random() < 0.05:
            row[i] = "  " + row[i] + "  "

    return row

output_file = "messy_cars.csv"

with open(output_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(HEADERS)
    for _ in range(1000):
        writer.writerow(generate_row())

print(f"Generated {output_file} with 1000 rows.")
