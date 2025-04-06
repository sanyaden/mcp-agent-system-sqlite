import os
import json
import time
import random
import datetime
from core.agent_base import BaseAgent

class ReportingAgent(BaseAgent):
    def __init__(self, agent_id=None):
        super().__init__(agent_id, "reporting")
        self.daily_report_time = "08:00"  # Generate daily reports at 8 AM
        self.weekly_report_day = 1  # Monday (0=Monday, 6=Sunday)
        self.monthly_report_day = 1  # 1st day of the month
        
        # Create reports directory if it doesn't exist
        self.report_directory = "reports"
        os.makedirs(self.report_directory, exist_ok=True)
    
    def run(self, db_connector):
        self.update_status(db_connector, "active")
        
        # Generate yesterday's report on startup
        yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        self.logger.info("Generating daily report for %s", yesterday)
        self.generate_daily_report(db_connector, yesterday)
        
        # Check if we need to generate weekly or monthly reports
        today = datetime.datetime.now().date()
        
        # If today is the weekly report day, generate last week's report
        if today.weekday() == self.weekly_report_day:
            end_date = (today - datetime.timedelta(days=1)).strftime("%Y-%m-%d")  # Yesterday
            start_date = (today - datetime.timedelta(days=7)).strftime("%Y-%m-%d")  # 7 days ago
            self.generate_weekly_report(db_connector, start_date, end_date)
        
        # If today is the monthly report day, generate last month's report
        if today.day == self.monthly_report_day:
            last_month = today.month - 1 if today.month > 1 else 12
            last_month_year = today.year if today.month > 1 else today.year - 1
            
            # Calculate start and end dates for last month
            start_date = datetime.date(last_month_year, last_month, 1)
            if last_month == 12:
                end_date = datetime.date(last_month_year, 12, 31)
            else:
                end_date = datetime.date(last_month_year, last_month + 1, 1) - datetime.timedelta(days=1)
                
            self.generate_monthly_report(db_connector, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
        
        # Sleep until tomorrow
        self.logger.info("Reports generated. Sleeping until next reporting cycle.")
        
        # In a real system, we would calculate the time until the next report
        # and sleep until then. For simplicity, we'll just exit.
        return
    
    def generate_daily_report(self, db_connector, date):
        """Generate a daily sales report"""
        try:
            # Get sales metrics for the day
            query = """
            SELECT 
                source,
                metric_type,
                value
            FROM sales_metrics
            WHERE date = ?
            """
            
            sales_data = db_connector.query(query, (date,))
            
            # Get insights for the day
            insights_query = """
            SELECT insight_type, description, severity
            FROM sales_insights
            WHERE date = ?
            """
            
            insights = db_connector.query(insights_query, (date,))
            
            # Process sales metrics
            metrics = self._process_sales_metrics(sales_data)
            
            # Generate report content
            report_data = {
                "date": date,
                "sales_data": sales_data,
                "insights": insights,
                "metrics": metrics
            }
            
            # Save report to file
            report_file_path = os.path.join(self.report_directory, f"daily_report_{date}.json")
            with open(report_file_path, 'w') as f:
                json.dump(report_data, f, indent=2)
            
            # Store report reference in database
            store_query = """
            INSERT INTO report_archive (
                report_type, generated_at, file_path, parameters
            ) VALUES (?, CURRENT_TIMESTAMP, ?, ?)
            """
            
            parameters = json.dumps({
                "date": date,
                "report_id": f"daily_{date}"
            })
            
            archive_id = db_connector.execute(store_query, (
                "daily",
                report_file_path,
                parameters
            ))
            
            self.logger.info("Daily report archived with ID: %s", archive_id)
            return {
                "status": "success",
                "report_id": f"daily_{date}",
                "archive_id": archive_id,
                "file_path": report_file_path
            }
            
        except Exception as e:
            self.logger.error("Error generating daily report: %s", str(e))
            return {
                "status": "error",
                "date": date,
                "error": str(e)
            }
    
    def generate_weekly_report(self, db_connector, start_date, end_date):
        """Generate a weekly sales report"""
        self.logger.info("Generating weekly report for %s to %s", start_date, end_date)
        
        try:
            # Get sales data for the week
            query = """
            SELECT 
                source,
                metric_type,
                value,
                date
            FROM sales_metrics
            WHERE date >= ? AND date <= ?
            ORDER BY date ASC
            """
            
            sales_data = db_connector.query(query, (start_date, end_date))
            
            # Get top insights for the week
            insights_query = """
            SELECT insight_type, description, severity, date
            FROM sales_insights
            WHERE date >= ? AND date <= ?
            ORDER BY severity DESC, date DESC
            LIMIT 10
            """
            
            insights = db_connector.query(insights_query, (start_date, end_date))
            
            # Process sales data to get weekly metrics
            weekly_metrics = self._process_weekly_metrics(sales_data)
            
            # Generate report content
            report_data = {
                "period": {
                    "start": start_date,
                    "end": end_date
                },
                "sales_data": weekly_metrics,
                "insights": insights
            }
            
            # Generate report artifact
            artifact_id = f"weekly_report_{start_date}_to_{end_date}_{random.randint(1000, 9999)}"
            
            # Store report reference
            report_id = f"weekly_{start_date}_to_{end_date}"
            store_query = """
            INSERT INTO report_archive (
                report_type, generated_at, file_path, parameters
            ) VALUES (?, CURRENT_TIMESTAMP, ?, ?)
            """
            
            # Create report file path
            report_file_path = os.path.join(self.report_directory, f"weekly_report_{start_date}_to_{end_date}.json")
            
            # Save report data to file
            with open(report_file_path, 'w') as f:
                json.dump(report_data, f, indent=2)
            
            # Store report reference in database
            parameters = json.dumps({
                "start_date": start_date,
                "end_date": end_date,
                "report_id": report_id
            })
            
            archive_id = db_connector.execute(store_query, (
                "weekly",
                report_file_path,
                parameters
            ))
            
            self.logger.info("Weekly report archived with ID: %s", archive_id)
            return {
                "status": "success",
                "report_id": report_id,
                "archive_id": archive_id,
                "artifact_id": artifact_id
            }
            
        except Exception as e:
            self.logger.error("Error generating weekly report: %s", str(e))
            return {
                "status": "error",
                "period": {"start": start_date, "end": end_date},
                "error": str(e)
            }
    
    def generate_monthly_report(self, db_connector, start_date, end_date):
        """Generate a monthly sales report"""
        self.logger.info("Generating monthly report for %s to %s", start_date, end_date)
        
        try:
            # Get sales data for the month
            query = """
            SELECT 
                source,
                metric_type,
                value,
                date
            FROM sales_metrics
            WHERE date >= ? AND date <= ?
            ORDER BY date ASC
            """
            
            sales_data = db_connector.query(query, (start_date, end_date))
            
            # Get insights for the month
            insights_query = """
            SELECT insight_type, description, severity, date
            FROM sales_insights
            WHERE date >= ? AND date <= ?
            ORDER BY severity DESC, date DESC
            """
            
            insights = db_connector.query(insights_query, (start_date, end_date))
            
            # Process monthly data
            # Similar to weekly processing but with additional month-specific metrics
            monthly_metrics = self._process_weekly_metrics(sales_data)  # Reuse weekly processing
            
            # Generate report content
            report_data = {
                "period": {
                    "start": start_date,
                    "end": end_date
                },
                "sales_data": monthly_metrics,
                "insights": insights
            }
            
            # Generate report artifact
            artifact_id = f"monthly_report_{start_date}_to_{end_date}_{random.randint(1000, 9999)}"
            
            # Store report reference
            report_id = f"monthly_{start_date}_to_{end_date}"
            store_query = """
            INSERT INTO report_archive (
                report_type, generated_at, file_path, parameters
            ) VALUES (?, CURRENT_TIMESTAMP, ?, ?)
            """
            
            # Create report file path
            report_file_path = os.path.join(self.report_directory, f"monthly_report_{start_date}_to_{end_date}.json")
            
            # Save report data to file
            with open(report_file_path, 'w') as f:
                json.dump(report_data, f, indent=2)
            
            # Store report reference in database
            parameters = json.dumps({
                "start_date": start_date,
                "end_date": end_date,
                "report_id": report_id
            })
            
            archive_id = db_connector.execute(store_query, (
                "monthly",
                report_file_path,
                parameters
            ))
            
            self.logger.info("Monthly report archived with ID: %s", archive_id)
            return {
                "status": "success",
                "report_id": report_id,
                "archive_id": archive_id,
                "artifact_id": artifact_id
            }
            
        except Exception as e:
            self.logger.error("Error generating monthly report: %s", str(e))
            return {
                "status": "error",
                "period": {"start": start_date, "end": end_date},
                "error": str(e)
            }
    
    def _process_sales_metrics(self, sales_data):
        """
        Process sales metrics from the new database structure.
        
        Args:
            sales_data (list): Raw sales metrics data from the database
            
        Returns:
            dict: Processed metrics by source and type
        """
        metrics = {}
        
        # Group metrics by source and type
        for item in sales_data:
            source = item.get("source")
            metric_type = item.get("metric_type")
            value = item.get("value")
            
            if source not in metrics:
                metrics[source] = {}
                
            metrics[source][metric_type] = value
        
        # Calculate totals
        total_sales = 0
        total_orders = 0
        
        for source in metrics:
            if "total_sales" in metrics[source]:
                total_sales += metrics[source]["total_sales"]
            if "total_orders" in metrics[source]:
                total_orders += metrics[source]["total_orders"]
        
        # Add summary metrics
        summary = {
            "total_sales": total_sales,
            "total_orders": total_orders,
            "sources": list(metrics.keys())
        }
        
        return {
            "by_source": metrics,
            "summary": summary
        }
        
    def _process_weekly_metrics(self, sales_data):
        """
        Process weekly sales metrics from the new database structure.
        
        Args:
            sales_data (list): Raw sales metrics data from the database
            
        Returns:
            dict: Processed weekly metrics with daily breakdown
        """
        # Group by date and source
        daily_metrics = {}
        sources = set()
        metric_types = set()
        
        for item in sales_data:
            date = item.get("date")
            source = item.get("source")
            metric_type = item.get("metric_type")
            value = item.get("value")
            
            sources.add(source)
            metric_types.add(metric_type)
            
            if date not in daily_metrics:
                daily_metrics[date] = {}
                
            if source not in daily_metrics[date]:
                daily_metrics[date][source] = {}
                
            daily_metrics[date][source][metric_type] = value
        
        # Calculate weekly totals by source
        weekly_totals = {}
        for source in sources:
            weekly_totals[source] = {}
            for metric_type in metric_types:
                total = 0
                count = 0
                
                for date in daily_metrics:
                    if source in daily_metrics[date] and metric_type in daily_metrics[date][source]:
                        value = daily_metrics[date][source][metric_type]
                        total += value
                        count += 1
                
                if count > 0:
                    if metric_type in ["average_order_value"]:
                        # For averages, we take the average of daily averages
                        weekly_totals[source][metric_type] = total / count
                    else:
                        # For other metrics, we sum them up
                        weekly_totals[source][metric_type] = total
        
        # Calculate overall totals
        overall_totals = {}
        for metric_type in metric_types:
            total = 0
            for source in weekly_totals:
                if metric_type in weekly_totals[source]:
                    total += weekly_totals[source][metric_type]
            overall_totals[metric_type] = total
        
        return {
            "daily": daily_metrics,
            "weekly": weekly_totals,
            "total": overall_totals,
            "sources": list(sources),
            "metric_types": list(metric_types)
        }
    
    def list_reports(self, report_type=None):
        """
        List available reports, optionally filtered by type
        """
        pass