import time
import random
import datetime
from core.agent_base import BaseAgent

class DataCollectionAgent(BaseAgent):
    def __init__(self, agent_id=None):
        super().__init__(agent_id, "data_collection")
        self.collection_frequency = 86400  # Daily collection
    
    def run(self, db_connector):
        self.update_status(db_connector, "active")
        
        while True:
            try:
                # Get current date
                current_date = datetime.datetime.now().strftime("%Y-%m-%d")
                self.logger.info("Collecting sales data for %s", current_date)
                
                # Collect sales data for current date
                records = self.collect_sales_data(db_connector, current_date)
                self.logger.info("Collected and stored sales data for %s (%d records)", current_date, records)
                
                # Sleep until next collection time
                time.sleep(self.collection_frequency)
                
            except Exception as e:
                self.logger.error("Error in data collection: %s", str(e))
                self.update_status(db_connector, "error")
                time.sleep(60)  # Wait before retrying
    
    def collect_sales_data(self, db_connector, date):
        query = """
        SELECT 
            ? as date,
            SUM(amount_total) as total_sales,
            COUNT(*) as total_orders,
            AVG(amount_total) as average_order_value,
            source,
            COUNT(DISTINCT client_id) as unique_customers
        FROM orders
        WHERE date = ?
        GROUP BY source
        """
        
        # This would normally query a real orders table
        # For demo purposes, we'll simulate data
        
        # Define sources
        sources = ["web", "mobile", "store", "partner"]
        
        # Get day of week (0 = Monday, 6 = Sunday)
        day_of_week = datetime.datetime.strptime(date, "%Y-%m-%d").weekday()
        
        # Higher sales on weekends
        is_weekend = day_of_week >= 5
        weekend_multiplier = 3.0 if is_weekend else 1.0
        
        # Higher sales on Friday evening
        is_friday = day_of_week == 4
        friday_multiplier = 2.0 if is_friday else 1.0
        
        # Apply the appropriate multiplier
        multiplier = max(weekend_multiplier, friday_multiplier)
        
        # Store metrics for each source
        record_count = 0
        
        for source in sources:
            # Generate random variations for each source
            source_variation = random.uniform(0.8, 1.2)
            
            # Calculate metrics with appropriate multipliers
            total_sales = 5000 * multiplier * source_variation
            total_orders = int(120 * multiplier * source_variation)
            average_order_value = total_sales / total_orders if total_orders > 0 else 0
            unique_customers = int(100 * multiplier * source_variation)
            
            # Store each metric separately
            metrics = {
                "total_sales": round(total_sales, 2),
                "total_orders": total_orders,
                "average_order_value": round(average_order_value, 2),
                "unique_customers": unique_customers
            }
            
            for metric_type, value in metrics.items():
                insert_query = """
                INSERT INTO sales_metrics (date, source, metric_type, value)
                VALUES (?, ?, ?, ?)
                """
                
                db_connector.execute(insert_query, (date, source, metric_type, value))
                record_count += 1
        
        return record_count