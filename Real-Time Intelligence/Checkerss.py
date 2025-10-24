"""
CHECKERS LIVE SALES DATA GENERATOR WITH MSSQL
==============================================
Generates continuous live Checkers grocery sales and saves to MS SQL Server

Installation:
pip install faker pyodbc

Setup:
1. Create database: CREATE DATABASE CheckersDB;
2. Update connection settings below
3. Run: python checkers_live_sales.py

Press Ctrl+C to stop
"""
import uuid
import json
import random
import time
from datetime import datetime
from faker import Faker
import pyodbc

fake = Faker()

# ==================== DATABASE CONFIGURATION ====================
USE_MSSQL = True

# Option 1: Windows Authentication (Recommended for local development)
USE_WINDOWS_AUTH = True

# Option 2: SQL Server Authentication
MSSQL_CONFIG = {
    'server': 'localhost',              # or 'localhost\\SQLEXPRESS' or your server IP
    'database': 'CheckersDB',
    'username': 'sa',                   # Only used if USE_WINDOWS_AUTH = False
    'password': 'YourPassword123',      # Only used if USE_WINDOWS_AUTH = False
    'driver': 'ODBC Driver 17 for SQL Server'  # or 'ODBC Driver 17 for SQL Server'
}

# ==================== PRODUCTS DATA ====================
PRODUCTS = [
    # Fresh Produce
    {"id": "G001", "name": "Bananas (1kg)", "category": "Fresh Produce", "price": 19.00, "sku": "BAN-1KG"},
    {"id": "G002", "name": "Tomatoes (1kg)", "category": "Fresh Produce", "price": 24.99, "sku": "TOM-1KG"},
    {"id": "G003", "name": "Potatoes (2kg)", "category": "Fresh Produce", "price": 29.99, "sku": "POT-2KG"},
    {"id": "G004", "name": "Onions (1kg)", "category": "Fresh Produce", "price": 22.99, "sku": "ONI-1KG"},
    {"id": "G005", "name": "Apples (1kg)", "category": "Fresh Produce", "price": 32.99, "sku": "APP-1KG"},
    {"id": "G006", "name": "Carrots (1kg)", "category": "Fresh Produce", "price": 16.99, "sku": "CAR-1KG"},
    {"id": "G007", "name": "Lettuce (each)", "category": "Fresh Produce", "price": 19.99, "sku": "LET-EACH"},
    {"id": "G008", "name": "Peppers (500g)", "category": "Fresh Produce", "price": 28.99, "sku": "PEP-500G"},
    {"id": "G051", "name": "Bananas Bag (1.2kg)", "category": "Fresh Produce", "price": 39.99, "sku": "BAN-1.2KG"},
    {"id": "G052", "name": "Brown Onions Bag (1kg)", "category": "Fresh Produce", "price": 24.99, "sku": "ONI-BAG-1KG"},
    {"id": "G053", "name": "Blueberries Tub (125g)", "category": "Fresh Produce", "price": 34.99, "sku": "BLB-125G"},
    {"id": "G054", "name": "Red Seedless Grapes (500g)", "category": "Fresh Produce", "price": 39.99, "sku": "GRP-500G"},
    {"id": "G059", "name": "Crisp Lettuce Head", "category": "Fresh Produce", "price": 19.99, "sku": "LET-HEAD"},
    {"id": "G060", "name": "Baby Potatoes Bag (1kg)", "category": "Fresh Produce", "price": 19.99, "sku": "POT-BABY-1KG"},
    
    # Dairy & Eggs
    {"id": "G009", "name": "Full Cream Milk (2L)", "category": "Dairy & Eggs", "price": 34.99, "sku": "MLK-2L"},
    {"id": "G010", "name": "Cheese Slices (200g)", "category": "Dairy & Eggs", "price": 45.99, "sku": "CHS-200G"},
    {"id": "G011", "name": "Eggs (18 pack)", "category": "Dairy & Eggs", "price": 54.99, "sku": "EGG-18PK"},
    {"id": "G012", "name": "Yoghurt (500g)", "category": "Dairy & Eggs", "price": 28.99, "sku": "YOG-500G"},
    {"id": "G013", "name": "Butter (500g)", "category": "Dairy & Eggs", "price": 52.99, "sku": "BUT-500G"},
    {"id": "G014", "name": "Margarine (500g)", "category": "Dairy & Eggs", "price": 38.99, "sku": "MAR-500G"},
    {"id": "G061", "name": "Clover UHT Full Cream Milk (1L)", "category": "Dairy & Eggs", "price": 16.99, "sku": "MLK-UHT-1L"},
    {"id": "G062", "name": "Crystal Valley Cheddar Cheese (750g)", "category": "Dairy & Eggs", "price": 124.99, "sku": "CHS-CV-750G"},
    
    # Meat & Poultry
    {"id": "G015", "name": "Chicken Breasts (1kg)", "category": "Meat & Poultry", "price": 94.99, "sku": "CHK-BRS-1KG"},
    {"id": "G016", "name": "Beef Mince (500g)", "category": "Meat & Poultry", "price": 74.99, "sku": "BEF-MIN-500G"},
    {"id": "G017", "name": "Pork Chops (1kg)", "category": "Meat & Poultry", "price": 95.99, "sku": "PRK-CHP-1KG"},
    {"id": "G018", "name": "Lamb Chops (500g)", "category": "Meat & Poultry", "price": 119.99, "sku": "LMB-CHP-500G"},
    {"id": "G019", "name": "Chicken Pieces (2kg)", "category": "Meat & Poultry", "price": 79.99, "sku": "CHK-PCS-2KG"},
    {"id": "G020", "name": "Boerewors (1kg)", "category": "Meat & Poultry", "price": 84.99, "sku": "BWS-1KG"},
    
    # Bakery
    {"id": "G021", "name": "White Bread (700g)", "category": "Bakery", "price": 16.99, "sku": "BRD-WHT-700G"},
    {"id": "G022", "name": "Brown Bread (700g)", "category": "Bakery", "price": 18.99, "sku": "BRD-BRN-700G"},
    {"id": "G023", "name": "Rolls (6 pack)", "category": "Bakery", "price": 14.99, "sku": "ROL-6PK"},
    {"id": "G024", "name": "Croissants (4 pack)", "category": "Bakery", "price": 32.99, "sku": "CRS-4PK"},
    
    # Pantry Staples
    {"id": "G025", "name": "Rice (2kg)", "category": "Pantry Staples", "price": 29.99, "sku": "RIC-2KG"},
    {"id": "G026", "name": "Pasta (500g)", "category": "Pantry Staples", "price": 22.99, "sku": "PST-500G"},
    {"id": "G027", "name": "Sugar (2.5kg)", "category": "Pantry Staples", "price": 44.99, "sku": "SUG-2.5KG"},
    {"id": "G028", "name": "Flour (2.5kg)", "category": "Pantry Staples", "price": 39.99, "sku": "FLR-2.5KG"},
    {"id": "G029", "name": "Cooking Oil (2L)", "category": "Pantry Staples", "price": 68.99, "sku": "OIL-2L"},
    {"id": "G030", "name": "Maize Meal (2.5kg)", "category": "Pantry Staples", "price": 38.99, "sku": "MAZ-2.5KG"},
    {"id": "G031", "name": "Canned Beans (410g)", "category": "Pantry Staples", "price": 18.99, "sku": "BNS-410G"},
    {"id": "G032", "name": "Tomato Sauce (700ml)", "category": "Pantry Staples", "price": 24.99, "sku": "TMS-700ML"},
    {"id": "G055", "name": "Tastic Long Grain Rice (2kg)", "category": "Pantry Staples", "price": 47.99, "sku": "RIC-TAS-2KG"},
    {"id": "G056", "name": "Lucky Star Pilchards (400g)", "category": "Pantry Staples", "price": 26.99, "sku": "PIL-400G"},
    {"id": "G057", "name": "Sunfoil Sunflower Oil (2L)", "category": "Pantry Staples", "price": 87.99, "sku": "OIL-SUN-2L"},
    {"id": "G058", "name": "Maggi 2 Minute Noodles (5×68g)", "category": "Pantry Staples", "price": 38.99, "sku": "NOD-MAG-5PK"},
    {"id": "G063", "name": "Spekko Long Grain Rice (2kg)", "category": "Pantry Staples", "price": 47.99, "sku": "RIC-SPK-2KG"},
    
    # Beverages
    {"id": "G033", "name": "Coca Cola (2L)", "category": "Beverages", "price": 19.99, "sku": "COK-2L"},
    {"id": "G034", "name": "Orange Juice (1L)", "category": "Beverages", "price": 32.99, "sku": "ORJ-1L"},
    {"id": "G035", "name": "Bottled Water (6x500ml)", "category": "Beverages", "price": 29.99, "sku": "WTR-6PK"},
    {"id": "G036", "name": "Tea Bags (100 pack)", "category": "Beverages", "price": 45.99, "sku": "TEA-100PK"},
    {"id": "G037", "name": "Coffee (250g)", "category": "Beverages", "price": 69.99, "sku": "COF-250G"},
    
    # Snacks & Treats
    {"id": "G038", "name": "Potato Chips (125g)", "category": "Snacks & Treats", "price": 19.99, "sku": "CHP-125G"},
    {"id": "G039", "name": "Chocolate Bar (80g)", "category": "Snacks & Treats", "price": 16.99, "sku": "CHC-80G"},
    {"id": "G040", "name": "Biscuits (200g)", "category": "Snacks & Treats", "price": 24.99, "sku": "BSC-200G"},
    {"id": "G041", "name": "Sweets (150g)", "category": "Snacks & Treats", "price": 22.99, "sku": "SWT-150G"},
    {"id": "G064", "name": "Simba Salt & Vinegar Chips (120g)", "category": "Snacks & Treats", "price": 19.99, "sku": "CHP-SIM-120G"},
    {"id": "G065", "name": "OREO Original Biscuits (128g)", "category": "Snacks & Treats", "price": 18.99, "sku": "BSC-ORE-128G"},
    
    # Frozen Foods
    {"id": "G042", "name": "Frozen Vegetables (1kg)", "category": "Frozen Foods", "price": 42.99, "sku": "FVG-1KG"},
    {"id": "G043", "name": "Ice Cream (2L)", "category": "Frozen Foods", "price": 64.99, "sku": "ICE-2L"},
    {"id": "G044", "name": "Frozen Pizza (400g)", "category": "Frozen Foods", "price": 54.99, "sku": "PIZ-400G"},
    {"id": "G045", "name": "Fish Fingers (400g)", "category": "Frozen Foods", "price": 59.99, "sku": "FSH-400G"},
    
    # Household
    {"id": "G046", "name": "Washing Powder (2kg)", "category": "Household", "price": 89.99, "sku": "WSH-2KG"},
    {"id": "G047", "name": "Dishwashing Liquid (750ml)", "category": "Household", "price": 34.99, "sku": "DSH-750ML"},
    {"id": "G048", "name": "Toilet Paper (9 pack)", "category": "Household", "price": 54.99, "sku": "TLT-9PK"},
    {"id": "G049", "name": "Soap (4 pack)", "category": "Household", "price": 29.99, "sku": "SOP-4PK"},
    {"id": "G050", "name": "Bleach (750ml)", "category": "Household", "price": 26.99, "sku": "BLC-750ML"},
]

STORES = [
    {"name": "Checkers", "location": "Cresta Shopping Centre", "province": "Gauteng"},
    {"name": "Checkers", "location": "Gateway Theatre of Shopping", "province": "KwaZulu-Natal"},
    {"name": "Checkers", "location": "Baywest Mall", "province": "Eastern Cape"},
    {"name": "Checkers", "location": "Canal Walk", "province": "Western Cape"},
    {"name": "Checkers", "location": "Waterfall Mall", "province": "North West"},
    {"name": "Checkers", "location": "Highveld Mall", "province": "Mpumalanga"},
    {"name": "Checkers", "location": "Tyger Valley", "province": "Western Cape"},
    {"name": "Checkers", "location": "Fleurdal Mall", "province": "Free State"},
    {"name": "Checkers", "location": "Polokwane Place", "province": "Limpopo"},
    {"name": "Checkers", "location": "Eastgate Shopping Centre", "province": "Gauteng"},
    {"name": "Checkers", "location": "Diamond Pavilion Mall", "province": "Northern Cape"},
    {"name": "Checkers", "location": "Blue Route Mall", "province": "Western Cape"},
    {"name": "Checkers", "location": "Walmer Park", "province": "Eastern Cape"},
    {"name": "Checkers", "location": "Rosebank Mall", "province": "Gauteng"},
    {"name": "Checkers", "location": "Riverside Mall", "province": "Mpumalanga"},
    {"name": "Checkers", "location": "Mimosa Mall", "province": "Free State"},
    {"name": "Checkers", "location": "Somerset Mall", "province": "Western Cape"},
    {"name": "Checkers", "location": "Pavilion Shopping Centre", "province": "KwaZulu-Natal"},
    {"name": "Checkers", "location": "Sandton City", "province": "Gauteng"},
    {"name": "Checkers", "location": "Loch Logan Waterfront", "province": "Free State"},
    {"name": "Checkers", "location": "Ballito Junction", "province": "KwaZulu-Natal"},
    {"name": "Checkers", "location": "V&A Waterfront", "province": "Western Cape"},
    {"name": "Checkers", "location": "Menlyn Park", "province": "Gauteng"},
    {"name": "Checkers", "location": "Mall of the North", "province": "Limpopo"},
    {"name": "Checkers", "location": "Hyde Park Corner", "province": "Gauteng"},
    {"name": "Checkers", "location": "La Lucia Mall", "province": "KwaZulu-Natal"},
    {"name": "Checkers", "location": "Galleria Mall Amanzimtoti", "province": "KwaZulu-Natal"},
    {"name": "Checkers", "location": "Woodlands Boulevard", "province": "Eastern Cape"},
    {"name": "Checkers", "location": "Cavendish Square", "province": "Western Cape"},
    {"name": "Checkers", "location": "Hemingways Mall", "province": "Eastern Cape"},
    {"name": "Checkers", "location": "Fourways Mall", "province": "Gauteng"},
    {"name": "Checkers", "location": "Mall of Africa", "province": "Gauteng"},
]

PAYMENT_METHODS = ["Cash", "Credit Card", "Debit Card", "Checkers Xtra Savings Card", "SnapScan", "Zapper", "EFT"]

transaction_count = 0
conn = None
cursor = None

# ==================== DATABASE CONNECTION ====================
def connect_to_database():
    """Connect to MS SQL Server"""
    global conn, cursor
    
    try:
        if USE_WINDOWS_AUTH:
            # Windows Authentication
            conn_str = (
                f"DRIVER={{{MSSQL_CONFIG['driver']}}};"
                f"SERVER={MSSQL_CONFIG['server']};"
                f"DATABASE={MSSQL_CONFIG['database']};"
                f"Trusted_Connection=yes;"
                f"TrustServerCertificate=yes;"
            )
        else:
            # SQL Server Authentication
            conn_str = (
                f"DRIVER={{{MSSQL_CONFIG['driver']}}};"
                f"SERVER={MSSQL_CONFIG['server']};"
                f"DATABASE={MSSQL_CONFIG['database']};"
                f"UID={MSSQL_CONFIG['username']};"
                f"PWD={MSSQL_CONFIG['password']};"
                f"TrustServerCertificate=yes;"
            )
        
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        print("✅ Connected to MS SQL Server successfully")
        
        # Create table if not exists
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='checkers_sales' AND xtype='U')
        CREATE TABLE checkers_sales (
            id INT IDENTITY(1,1) PRIMARY KEY,
            transaction_id VARCHAR(20) UNIQUE NOT NULL,
            receipt_number VARCHAR(15),
            timestamp DATETIME NOT NULL,
            date DATE,
            time TIME,
            day_of_week VARCHAR(15),
            store_name VARCHAR(50),
            store_location VARCHAR(100),
            province VARCHAR(30),
            till_number VARCHAR(15),
            cashier_id VARCHAR(15),
            customer_id VARCHAR(15),
            has_xtra_savings_card BIT,
            xtra_savings_number VARCHAR(20),
            num_items INT,
            basket NVARCHAR(MAX),
            subtotal FLOAT,
            discount FLOAT,
            discount_percent INT,
            vat FLOAT,
            vat_rate FLOAT,
            total_amount FLOAT,
            currency VARCHAR(5),
            payment_method VARCHAR(50),
            status VARCHAR(20),
            failure_reason VARCHAR(100),
            shopping_duration_minutes INT,
            queue_time_minutes INT,
            created_at DATETIME DEFAULT GETDATE()
        )
        """)
        conn.commit()
        print("✅ Table 'checkers_sales' ready\n")
        return True
        
    except Exception as e:
        print(f"❌lone Database connection failed: {e}")
        print("\nTroubleshooting:")
        print("1. Check if SQL Server is running")
        print("2. Verify server name (use 'localhost' or 'localhost\\SQLEXPRESS')")
        print("3. Ensure database 'CheckersDB' exists")
        print("4. For Windows Auth: Run as administrator or check permissions")
        print("5. For SQL Auth: Verify username and password\n")
        return False


# ==================== DATA GENERATION ====================
def generate_checkers_sale():
    """Generate a single Checkers grocery sale"""
    global transaction_count
    
    current_hour = datetime.now().hour
    day_of_week = datetime.now().weekday()
    
    # More items during peak times
    if day_of_week >= 4 or (17 <= current_hour <= 19):
        num_items = random.randint(5, 15)
    else:
        num_items = random.randint(2, 8)
    
    store = random.choice(STORES)
    basket = []
    basket_subtotal = 0
    
    # Build shopping basket
    for _ in range(num_items):
        product = random.choice(PRODUCTS)
        quantity = random.randint(1, 3)
        price_variation = random.uniform(0.90, 1.05)
        unit_price = round(product["price"] * price_variation, 2)
        item_total = round(unit_price * quantity, 2)
        
        basket.append({
            "product_id": product["id"],
            "product_name": product["name"],
            "product_sku": product["sku"],
            "category": product["category"],
            "quantity": quantity,
            "unit_price": unit_price,
            "item_total": item_total
        })
        basket_subtotal += item_total
    
    # Apply Xtra Savings discount
    discount = 0
    discount_percent = 0
    has_xtra_savings = random.random() < 0.70
    
    if has_xtra_savings and basket_subtotal > 100:
        discount_percent = random.choice([5, 10, 15, 20])
        discount = round(basket_subtotal * (discount_percent / 100), 2)
    
    # Calculate VAT (15%)
    vat_rate = 0.15
    vat = round((basket_subtotal - discount) * vat_rate, 2)
    total = round(basket_subtotal - discount + vat, 2)
    
    # Transaction status (1% failure rate)
    status = "failed" if random.random() < 0.01 else "completed"
    failure_reason = "Card declined" if status == "failed" else None
    
    transaction_count += 1
    
    return {
        "transaction_id": f"GRO-{uuid.uuid4().hex[:8].upper()}",
        "receipt_number": f"RCP-{random.randint(100000, 999999)}",
        "timestamp": datetime.now(),
        "date": datetime.now().date(),
        "time": datetime.now().time(),
        "day_of_week": datetime.now().strftime("%A"),
        "store_name": store["name"],
        "store_location": store["location"],
        "province": store["province"],
        "till_number": f"TILL-{random.randint(1, 25):02d}",
        "cashier_id": f"CHK-{random.randint(1000, 9999)}",
        "customer_id": f"CUST-{random.randint(100000, 999999)}",
        "has_xtra_savings_card": has_xtra_savings,
        "xtra_savings_number": f"XS-{random.randint(10000000, 99999999)}" if has_xtra_savings else None,
        "num_items": num_items,
        "basket": basket,
        "subtotal": round(basket_subtotal, 2),
        "discount": discount,
        "discount_percent": discount_percent,
        "vat": vat,
        "vat_rate": round(vat_rate * 100, 2),
        "total_amount": total,
        "currency": "ZAR",
        "payment_method": random.choice(PAYMENT_METHODS),
        "status": status,
        "failure_reason": failure_reason,
        "shopping_duration_minutes": random.randint(10, 45),
        "queue_time_minutes": random.randint(2, 15)
    }


def save_to_database(txn):
    """Save transaction to database"""
    try:
        insert_query = """
        INSERT INTO checkers_sales (
            transaction_id, receipt_number, timestamp, date, time, day_of_week,
            store_name, store_location, province, till_number, cashier_id, customer_id,
            has_xtra_savings_card, xtra_savings_number, num_items, basket,
            subtotal, discount, discount_percent, vat, vat_rate, total_amount,
            currency, payment_method, status, failure_reason,
            shopping_duration_minutes, queue_time_minutes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        cursor.execute(insert_query, (
            txn["transaction_id"],
            txn["receipt_number"],
            txn["timestamp"],
            txn["date"],
            txn["time"],
            txn["day_of_week"],
            txn["store_name"],
            txn["store_location"],
            txn["province"],
            txn["till_number"],
            txn["cashier_id"],
            txn["customer_id"],
            txn["has_xtra_savings_card"],
            txn["xtra_savings_number"],
            txn["num_items"],
            json.dumps(txn["basket"]),
            txn["subtotal"],
            txn["discount"],
            txn["discount_percent"],
            txn["vat"],
            txn["vat_rate"],
            txn["total_amount"],
            txn["currency"],
            txn["payment_method"],
            txn["status"],
            txn["failure_reason"],
            txn["shopping_duration_minutes"],
            txn["queue_time_minutes"]
        ))
        conn.commit()
        return True
        
    except Exception as e:
        print(f"\n❌ Error saving to database: {e}")
        return False


def display_sale(txn, saved):
    """Display transaction in console"""
    status_icon = "✅" if txn["status"] == "completed" else "❌"
    save_icon = "💾" if saved else "⚠️"
    
    top_items = sorted(txn["basket"], key=lambda x: x["item_total"], reverse=True)[:2]
    items_str = ", ".join([f"{item['product_name']}" for item in top_items])
    
    print(f"{status_icon}{save_icon} [{txn['time'].strftime('%H:%M:%S')}] "
          f"{txn['transaction_id']} | "
          f"R{txn['total_amount']:>8.2f} | "
          f"{txn['store_location']:<30} | "
          f"{txn['province']:<20} | "
          f"{txn['num_items']} items | "
          f"{items_str[:45]}")


# ==================== MAIN STREAM ====================
def start_live_stream(transactions_per_second=2):
    """Start generating and saving live Checkers sales data"""
    
    print("\n" + "="*100)
    print("🛒 CHECKERS LIVE SALES DATA GENERATOR")
    print("="*100)
    
    if not USE_MSSQL:
        print("❌ Database is disabled. Set USE_MSSQL = True in configuration.")
        return
    
    if not connect_to_database():
        return
    
    print(f"⚡ Generating {transactions_per_second} transactions per second")
    print("💾 Saving to table: checkers_sales")
    print("⏹️  Press Ctrl+C to stop")
    print("="*100 + "\n")
    
    interval = 1.0 / transactions_per_second
    
    try:
        while True:
            txn = generate_checkers_sale()
            saved = save_to_database(txn)
            display_sale(txn, saved)
            time.sleep(interval)
            
    except KeyboardInterrupt:
        print("\n\n" + "="*100)
        print("⏹️  STREAM STOPPED")
        
        # Get final count
        cursor.execute("SELECT COUNT(*) FROM checkers_sales")
        total_records = cursor.fetchone()[0]
        print(f"📊 Total records in database: {total_records}")
        print("="*100 + "\n")
        
    finally:
        if conn:
            conn.close()
            print("✅ Database connection closed\n")



if __name__ == "__main__":
    start_live_stream(transactions_per_second=2)