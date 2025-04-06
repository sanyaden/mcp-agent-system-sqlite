import threading
import logging
from agents.data_collection_agent import DataCollectionAgent
from agents.analytics_agent import AnalyticsAgent
from agents.alert_agent import AlertAgent
from agents.reporting_agent import ReportingAgent

class AgentScheduler:
    def __init__(self, db_connector):
        self.db_connector = db_connector
        self.logger = logging.getLogger("agent.scheduler")
        self.agents = {}
        self.agent_threads = {}
        
    def register_agent(self, agent):
        """Register an agent with the scheduler"""
        agent.register(self.db_connector)
        self.agents[agent.agent_id] = agent
        self.logger.info("Agent %s registered with scheduler", agent.agent_id)
        
    def start_agent(self, agent_id):
        """Start a specific agent in a separate thread"""
        if agent_id not in self.agents:
            self.logger.error("Agent %s not registered", agent_id)
            return False
            
        agent = self.agents[agent_id]
        agent_thread = threading.Thread(
            target=agent.run,
            args=(self.db_connector,),
            daemon=True
        )
        agent_thread.start()
        
        self.agent_threads[agent_id] = agent_thread
        self.logger.info("Agent %s started", agent_id)
        return True
        
    def start_agents(self):
        """Start all registered agents"""
        for agent_id in self.agents:
            self.start_agent(agent_id)
            
    def stop_agent(self, agent_id):
        """Stop a specific agent"""
        if agent_id not in self.agent_threads:
            self.logger.error("Agent %s not running", agent_id)
            return False
            
        # Note: This doesn't actually stop the agent thread
        # The agents would need to implement a stop mechanism
        agent = self.agents[agent_id]
        agent.update_status(self.db_connector, "inactive")
        self.logger.info("Agent %s stopped", agent_id)
        return True
        
    def initialize_default_agents(self):
        """Initialize and register default agent set"""
        data_agent = DataCollectionAgent()
        analytics_agent = AnalyticsAgent()
        alert_agent = AlertAgent()
        reporting_agent = ReportingAgent()
        
        self.register_agent(data_agent)
        self.register_agent(analytics_agent)
        self.register_agent(alert_agent)
        self.register_agent(reporting_agent)
        
        return {
            "data_collection": data_agent.agent_id,
            "analytics": analytics_agent.agent_id,
            "alert": alert_agent.agent_id,
            "reporting": reporting_agent.agent_id
        }