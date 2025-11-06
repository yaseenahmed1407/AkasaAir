import pandas as pd
import xmltodict
from datetime import datetime, timedelta
import pytz
import logging
from pathlib import Path
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Set timezone
TIMEZONE = 'Asia/Kolkata'
tz = pytz.timezone(TIMEZONE)

class DataProcessor:
    def __init__(self):
        self.customers_df = None
        self.orders_df = None
        self.current_date = datetime.now(tz)

    def load_customer_data(self, file_path: str) -> None:
        """Load and clean customer data from CSV."""
        try:
            logger.info(f"Loading customer data from {file_path}")
            df = pd.read_csv(file_path)
            
            # Data validation and cleaning
            required_columns = {'customer_id', 'customer_name', 'mobile_number', 'region'}
            if not all(col in df.columns for col in required_columns):
                raise ValueError("Missing required columns in customer data")
            
            # Clean and standardize data
            df['mobile_number'] = df['mobile_number'].astype(str)
            df['customer_id'] = df['customer_id'].astype(str)
            df['region'] = df['region'].str.strip()
            
            self.customers_df = df
            logger.info("Customer data loaded and cleaned successfully")
            
        except Exception as e:
            logger.error(f"Error loading customer data: {e}")
            raise

    def load_orders_data(self, file_path: str) -> None:
        """Load and clean orders data from XML."""
        try:
            logger.info(f"Loading orders data from {file_path}")
            with open(file_path, 'r') as file:
                xml_data = file.read()
            
            # Parse XML to dict
            data_dict = xmltodict.parse(xml_data)
            orders = data_dict['orders']['order']
            
            # Convert to DataFrame
            df = pd.DataFrame(orders)
            
            # Data cleaning and type conversion
            df['order_date_time'] = pd.to_datetime(df['order_date_time'])
            df['total_amount'] = pd.to_numeric(df['total_amount'])
            df['sku_count'] = pd.to_numeric(df['sku_count'])
            df['mobile_number'] = df['mobile_number'].astype(str)
            
            # Localize timestamps
            df['order_date_time'] = df['order_date_time'].dt.tz_localize(TIMEZONE)
            
            self.orders_df = df
            logger.info("Orders data loaded and cleaned successfully")
            
        except Exception as e:
            logger.error(f"Error loading orders data: {e}")
            raise

    def get_repeat_customers(self) -> pd.DataFrame:
        """Identify customers with more than one order."""
        try:
            # Merge orders with customers
            merged_df = pd.merge(
                self.orders_df, 
                self.customers_df, 
                on='mobile_number'
            )
            
            # Group by customer and count unique orders
            repeat_customers = (
                merged_df.groupby(['customer_id', 'customer_name'])
                ['order_id'].nunique()
                .reset_index()
                .rename(columns={'order_id': 'order_count'})
            )
            
            # Filter customers with more than one order
            repeat_customers = repeat_customers[repeat_customers['order_count'] > 1]
            return repeat_customers.sort_values('order_count', ascending=False)
            
        except Exception as e:
            logger.error(f"Error calculating repeat customers: {e}")
            raise

    def get_monthly_order_trends(self) -> pd.DataFrame:
        """Aggregate orders by month to observe trends."""
        try:
            monthly_trends = (
                self.orders_df.groupby(
                    self.orders_df['order_date_time'].dt.strftime('%Y-%m')
                ).agg({
                    'order_id': 'nunique',
                    'total_amount': 'sum',
                    'mobile_number': 'nunique'
                }).reset_index()
                .rename(columns={
                    'order_date_time': 'month',
                    'order_id': 'total_orders',
                    'total_amount': 'total_revenue',
                    'mobile_number': 'unique_customers'
                })
            )
            return monthly_trends.sort_values('month')
            
        except Exception as e:
            logger.error(f"Error calculating monthly trends: {e}")
            raise

    def get_regional_revenue(self) -> pd.DataFrame:
        """Calculate revenue by region."""
        try:
            merged_df = pd.merge(
                self.orders_df, 
                self.customers_df, 
                on='mobile_number'
            )
            
            regional_revenue = (
                merged_df.groupby('region').agg({
                    'order_id': 'nunique',
                    'total_amount': 'sum',
                    'customer_id': 'nunique'
                }).reset_index()
                .rename(columns={
                    'order_id': 'total_orders',
                    'total_amount': 'total_revenue',
                    'customer_id': 'unique_customers'
                })
            )
            
            # Calculate average order value
            regional_revenue['avg_order_value'] = (
                regional_revenue['total_revenue'] / regional_revenue['total_orders']
            )
            
            return regional_revenue.sort_values('total_revenue', ascending=False)
            
        except Exception as e:
            logger.error(f"Error calculating regional revenue: {e}")
            raise

    def get_top_customers_last_30_days(self) -> pd.DataFrame:
        """Rank customers by total spend in the last 30 days."""
        try:
            cutoff_date = self.current_date - timedelta(days=30)
            
            # Filter orders for last 30 days
            recent_orders = self.orders_df[
                self.orders_df['order_date_time'] >= cutoff_date
            ]
            
            # Merge with customer data and calculate total spend
            merged_df = pd.merge(
                recent_orders, 
                self.customers_df, 
                on='mobile_number'
            )
            
            top_customers = (
                merged_df.groupby(['customer_id', 'customer_name', 'region'])
                .agg({
                    'order_id': 'nunique',
                    'total_amount': 'sum'
                })
                .reset_index()
                .rename(columns={
                    'order_id': 'order_count',
                    'total_amount': 'total_spend'
                })
                .sort_values('total_spend', ascending=False)
                .head(10)
            )
            
            return top_customers
            
        except Exception as e:
            logger.error(f"Error calculating top customers: {e}")
            raise

def main():
    """Main function to run the analysis using DataFrame approach."""
    try:
        # Initialize data processor
        processor = DataProcessor()
        
        # Load data
        processor.load_customer_data('task_DE_new_customers.csv')
        processor.load_orders_data('task_DE_new_orders.xml')
        
        # Calculate and display KPIs
        print("\n=== Analysis Results (DataFrame Approach) ===\n")
        
        # 1. Repeat Customers
        print("1. Repeat Customers:")
        repeat_customers = processor.get_repeat_customers()
        print(repeat_customers)
        
        # 2. Monthly Order Trends
        print("\n2. Monthly Order Trends:")
        monthly_trends = processor.get_monthly_order_trends()
        print(monthly_trends)
        
        # 3. Regional Revenue
        print("\n3. Regional Revenue:")
        regional_revenue = processor.get_regional_revenue()
        print(regional_revenue)
        
        # 4. Top Customers (Last 30 Days)
        print("\n4. Top Customers (Last 30 Days):")
        top_customers = processor.get_top_customers_last_30_days()
        print(top_customers)
        
        logger.info("Analysis completed successfully")
        
    except Exception as e:
        logger.error(f"Error in analysis: {e}")
        raise

if __name__ == "__main__":
    main()