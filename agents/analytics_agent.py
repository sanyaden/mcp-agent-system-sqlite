import time
import json
import datetime
import statistics
from core.agent_base import BaseAgent

class AnalyticsAgent(BaseAgent):
    def __init__(self, agent_id=None):
        super().__init__(agent_id, "analytics")
        self.analysis_frequency = 3600  # Hourly analysis
        self.anomaly_threshold = 2.0  # Z-score threshold for anomalies
    
    def run(self, db_connector):
        self.update_status(db_connector, "active")
        self.logger.info("Analytics agent started")
        
        try:
            # Analyze historical data
            self.analyze_historical_data(db_connector)
            
            # Update status to inactive since we're done
            self.update_status(db_connector, "inactive")
            
        except Exception as e:
            self.logger.error("Error in analytics agent: %s", str(e))
            self.update_status(db_connector, "error")
    
    def analyze_historical_data(self, db_connector):
        """Analyze historical sales data to detect patterns and anomalies"""
        # Get current date
        current_date = datetime.datetime.now().date()
        
        # Analyze last 30 days of data
        start_date = (current_date - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
        end_date = current_date.strftime("%Y-%m-%d")
        
        # Get sales data for analysis
        query = """
        SELECT date, source, metric_type, value
        FROM sales_metrics
        WHERE date >= ? AND date <= ?
        ORDER BY date ASC
        """
        
        sales_data = db_connector.query(query, (start_date, end_date))
        
        # Group data by source and metric type
        grouped_data = {}
        for record in sales_data:
            source = record["source"]
            metric_type = record["metric_type"]
            date = record["date"]
            value = record["value"]
            
            if source not in grouped_data:
                grouped_data[source] = {}
                
            if metric_type not in grouped_data[source]:
                grouped_data[source][metric_type] = {}
                
            grouped_data[source][metric_type][date] = value
        
        # Detect anomalies
        anomalies = []
        for source in grouped_data:
            for metric_type in grouped_data[source]:
                if metric_type == "total_sales":  # Focus on sales anomalies
                    time_series = []
                    dates = []
                    
                    # Convert to time series
                    for date in sorted(grouped_data[source][metric_type].keys()):
                        dates.append(date)
                        time_series.append(grouped_data[source][metric_type][date])
                    
                    # Need at least 7 data points for meaningful analysis
                    if len(time_series) >= 7:
                        # Detect anomalies using z-score
                        anomalies.extend(self._detect_anomalies(
                            source, metric_type, dates, time_series
                        ))
        
        # Send anomalies to alert agent if any found
        if anomalies:
            alert_agent_ids = self._find_alert_agents(db_connector)
            
            for alert_agent_id in alert_agent_ids:
                self.send_message(
                    db_connector,
                    alert_agent_id,
                    "anomalies_detected",
                    {
                        "anomalies": anomalies,
                        "date": current_date.strftime("%Y-%m-%d")
                    }
                )
                
            self.logger.info("Sent %d anomalies to alert agent", len(anomalies))
    
    def _detect_anomalies(self, source, metric_type, dates, values):
        """Detect anomalies in a time series using z-score"""
        anomalies = []
        
        # Calculate mean and standard deviation
        mean = statistics.mean(values)
        stdev = statistics.stdev(values) if len(values) > 1 else 0
        
        if stdev == 0:
            return anomalies  # Can't detect anomalies without variation
        
        # Check last 3 days for anomalies
        for i in range(max(0, len(values) - 3), len(values)):
            value = values[i]
            date = dates[i]
            
            # Calculate z-score
            z_score = (value - mean) / stdev
            
            # If absolute z-score exceeds threshold, it's an anomaly
            if abs(z_score) >= self.anomaly_threshold:
                anomalies.append({
                    "date": date,
                    "source": source,
                    "type": "sales_anomaly",
                    "metric_type": metric_type,
                    "value": value,
                    "expected": mean,
                    "z_score": z_score
                })
        
        return anomalies
    
    def _find_alert_agents(self, db_connector):
        """Find active alert agents"""
        query = """
        SELECT agent_id
        FROM agent_registry
        WHERE agent_type = 'alert' AND status = 'active'
        """
        
        agents = db_connector.query(query, ())
        return [agent["agent_id"] for agent in agents]