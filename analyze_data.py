import pandas as pd
import xmltodict
import mysql.connector
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# 1. Load and process data
def load_customer_data(file_path):
    """Load customer data from CSV"""
    return pd.read_csv(file_path)

def load_orders_data(file_path):
    """Load orders data from XML"""
    with open(file_path, 'r') as file:
        xml_data = file.read()
    data_dict = xmltodict.parse(xml_data)
    return pd.DataFrame(data_dict['orders']['order'])

# 2. Database connection
def get_connection():
    """Get MySQL connection using environment variables"""
    try:
        # Connect to the existing database with essential settings
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            user=os.getenv('DB_USER', 'root'),
            password=os.getenv('DB_PASSWORD', ''),
            database=os.getenv('DB_NAME', 'AkasaAir'),
            port=int(os.getenv('DB_PORT', '3306')),
            use_pure=True,            # Use pure Python implementation
            autocommit=False,         # Explicit transaction control
            buffered=True,            # Use buffered cursors
            connection_timeout=60      # Longer timeout
        )
        logger.info("Successfully connected to database")
        return connection
    except mysql.connector.Error as e:
        logger.error(f"Error connecting to database: {e}")
        raise

# 3. Create tables
def create_tables():
    """Create necessary database tables"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Create customers table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id VARCHAR(10) PRIMARY KEY,
            customer_name VARCHAR(100) NOT NULL,
            mobile_number VARCHAR(15) UNIQUE NOT NULL,
            region VARCHAR(50) NOT NULL
        )
    """)
    
    # Create orders table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id VARCHAR(15),
            mobile_number VARCHAR(15),
            order_date_time DATETIME,
            sku_id VARCHAR(10),
            sku_count INT,
            total_amount DECIMAL(10,2),
            PRIMARY KEY (order_id, sku_id)
        )
    """)
    
    conn.commit()
    cursor.close()
    conn.close()

# 4. Load data into database
def load_data_to_db(customers_df, orders_df):
    """Load data into MySQL tables"""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # Insert customers with update on duplicate
        for _, row in customers_df.iterrows():
            cursor.execute("""
                INSERT INTO customers (customer_id, customer_name, mobile_number, region)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                customer_name = VALUES(customer_name),
                mobile_number = VALUES(mobile_number),
                region = VALUES(region)
            """, (row['customer_id'], row['customer_name'], row['mobile_number'], row['region']))
        
        # Insert orders with update on duplicate
        orders_df['order_date_time'] = pd.to_datetime(orders_df['order_date_time'])
        for _, row in orders_df.iterrows():
            # Convert timestamp to string in MySQL format
            order_datetime = row['order_date_time'].strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute("""
                INSERT INTO orders (order_id, mobile_number, order_date_time, sku_id, sku_count, total_amount)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                mobile_number = VALUES(mobile_number),
                order_date_time = VALUES(order_date_time),
                sku_count = VALUES(sku_count),
                total_amount = VALUES(total_amount)
            """, (row['order_id'], row['mobile_number'], order_datetime,
                  row['sku_id'], int(row['sku_count']), float(row['total_amount'])))
        
        # Commit the changes
        conn.commit()
        logging.getLogger(__name__).info("Successfully loaded data into database")
    except mysql.connector.Error as err:
        conn.rollback()
        logging.getLogger(__name__).error(f"Error loading data into database: {err}")
        raise
    finally:
        cursor.close()
        conn.close()

# 5. KPI Calculations
def calculate_kpis():
    """Calculate all KPIs using SQL queries"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # 1. Repeat Customers
    print("\n=== Repeat Customers ===")
    cursor.execute("""
        SELECT c.customer_name, COUNT(DISTINCT o.order_id) as order_count
        FROM customers c
        JOIN orders o ON c.mobile_number = o.mobile_number
        GROUP BY c.customer_id, c.customer_name
        HAVING order_count > 1
        ORDER BY order_count DESC
    """)
    for row in cursor.fetchall():
        print(f"Customer: {row[0]}, Orders: {row[1]}")
    
    # 2. Monthly Order Trends
    print("\n=== Monthly Order Trends ===")
    cursor.execute("""
        SELECT 
            DATE_FORMAT(order_date_time, '%Y-%m') as month,
            COUNT(DISTINCT order_id) as total_orders,
            SUM(total_amount) as total_revenue
        FROM orders
        GROUP BY DATE_FORMAT(order_date_time, '%Y-%m')
        ORDER BY month
    """)
    for row in cursor.fetchall():
        print(f"Month: {row[0]}, Orders: {row[1]}, Revenue: {row[2]}")
    
    # 3. Regional Revenue
    print("\n=== Regional Revenue ===")
    cursor.execute("""
        SELECT 
            c.region,
            SUM(o.total_amount) as total_revenue
        FROM customers c
        JOIN orders o ON c.mobile_number = o.mobile_number
        GROUP BY c.region
        ORDER BY total_revenue DESC
    """)
    for row in cursor.fetchall():
        print(f"Region: {row[0]}, Revenue: {row[1]}")
    
    # 4. Top Customers (Last 30 Days)
    print("\n=== Top Customers (Last 30 Days) ===")
    cursor.execute("""
        SELECT 
            c.customer_name,
            SUM(o.total_amount) as total_spend
        FROM customers c
        JOIN orders o ON c.mobile_number = o.mobile_number
        WHERE o.order_date_time >= DATE_SUB(NOW(), INTERVAL 30 DAY)
        GROUP BY c.customer_id, c.customer_name
        ORDER BY total_spend DESC
        LIMIT 5
    """)
    for row in cursor.fetchall():
        print(f"Customer: {row[0]}, Spend: {row[1]}")
    
    cursor.close()
    conn.close()

def main():
    try:
        logger.info("Starting data analysis process")
        
        # Create database tables
        logger.info("Creating database tables")
        create_tables()
        
        # Load data from files
        logger.info("Loading data from files")
        customers_df = load_customer_data('task_DE_new_customers.csv')
        orders_df = load_orders_data('task_DE_new_orders.xml')
        
        # Load data into database
        logger.info("Loading data into database")
        load_data_to_db(customers_df, orders_df)
        
        # Calculate and display KPIs
        logger.info("Calculating KPIs")
        calculate_kpis()
        
        logger.info("Analysis completed successfully")
        
    except Exception as e:
        logger.error(f"Error in main process: {e}")
        raise

if __name__ == "__main__":
    main()