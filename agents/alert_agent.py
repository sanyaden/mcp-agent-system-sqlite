from core.agent_base import BaseAgent
import json
import time
import datetime

class AlertAgent(BaseAgent):
    def __init__(self, agent_id=None):
        super().__init__(agent_id, "alert")
        self.alert_check_frequency = 300  # Check every 5 minutes
        self.alert_channels = ["system"]  # Default channel
    
    def run(self, db_connector):
        self.update_status(db_connector, "active")
        
        while True:
            try:
                # Process configuration messages
                messages = self.get_messages(db_connector)
                for message in messages:
                    if message["message_type"] == "configuration":
                        config = json.loads(message["content"])
                        if "alert_channels" in config:
                            self.alert_channels = config["alert_channels"]
                    
                    # Process anomaly notifications
                    elif message["message_type"] == "anomalies_detected":
                        content = json.loads(message["content"])
                        anomalies = content.get("anomalies", [])
                        date = content.get("date")
                        
                        for anomaly in anomalies:
                            self.process_anomaly(db_connector, date, anomaly)
                
                # Check for unprocessed high-severity insights
                self.check_unprocessed_insights(db_connector)
                
                time.sleep(self.alert_check_frequency)
                
            except Exception as e:
                self.logger.error("Error in alert agent: %s", str(e))
                self.update_status(db_connector, "error")
                time.sleep(60)  # Wait before retrying
    
    def process_anomaly(self, db_connector, date, anomaly):
        """Process a single anomaly and generate appropriate alerts"""
        anomaly_type = anomaly.get("type")
        source = anomaly.get("source")
        z_score = anomaly.get("z_score", 0)
        
        if anomaly_type == "sales_anomaly":
            direction = "increase" if z_score > 0 else "decrease"
            severity = "critical" if abs(z_score) > 3 else "warning"
            
            # Create notification
            subject = f"{severity.upper()}: Unusual sales {direction} detected for {source}"
            content = (
                f"Date: {date}\n"
                f"Source: {source}\n"
                f"Actual: {anomaly.get('value'):.2f}\n"
                f"Expected: {anomaly.get('expected'):.2f}\n"
                f"Deviation: {abs(z_score):.2f} standard deviations\n"
                f"\nThis {direction} is unusual based on historical patterns."
            )
            
            self.create_notification(db_connector, "sales_anomaly", subject, content)
            
            # Send alert through configured channels
            for channel in self.alert_channels:
                if channel == "system":
                    self.logger.info("ALERT: %s", subject)
                # Additional channels (email, SMS, etc.) would be implemented here
    
    def check_unprocessed_insights(self, db_connector):
        """Check for high-severity insights that haven't been processed yet"""
        # Get current date and date 3 days ago
        import datetime
        current_date = datetime.datetime.now().strftime("%Y-%m-%d")
        three_days_ago = (datetime.datetime.now() - datetime.timedelta(days=3)).strftime("%Y-%m-%d")
        
        # First get all notifications from the last 3 days
        notifications_query = """
        SELECT message
        FROM system_notifications
        WHERE notification_type = 'insight_notification'
        AND timestamp >= ?
        """
        
        notifications = db_connector.query(notifications_query, (three_days_ago,))
        
        # Extract insight IDs from notifications
        processed_insight_ids = []
        for notification in notifications:
            try:
                message = json.loads(notification.get("message", "{}"))
                if "insight_id" in message:
                    processed_insight_ids.append(message["insight_id"])
            except Exception as e:
                self.logger.error("Error parsing notification message: %s", str(e))
        
        # Now get high severity insights from the last 3 days
        query = """
        SELECT id, date, insight_type, description, severity, metrics
        FROM sales_insights
        WHERE severity = 'high' AND date >= ?
        """
        
        insights = db_connector.query(query, (three_days_ago,))
        
        
        # Filter out already processed insights
        unprocessed_insights = [insight for insight in insights if insight["id"] not in processed_insight_ids]
        
        for insight in unprocessed_insights:
            subject = f"HIGH PRIORITY INSIGHT: {insight['insight_type']} on {insight['date']}"
            content = json.dumps({
                "insight_id": insight["id"],
                "date": insight["date"],
                "description": insight["description"],
                "metrics": insight["metrics"]
            })
            
            self.create_notification(db_connector, "insight_notification", subject, content)
    
    def create_notification(self, db_connector, notification_type, subject, content):
        """Create a system notification"""
        query = """
        INSERT INTO system_notifications (notification_type, message, severity, acknowledged)
        VALUES (?, ?, ?, 0)
        """
        
        # Use 'high' severity for all notifications for simplicity
        notification_id = db_connector.execute(query, (notification_type, content, "high"))
        self.logger.info("Created notification: %s (ID: %s)", subject, notification_id)
        
        return notification_id