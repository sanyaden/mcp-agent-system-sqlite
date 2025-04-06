# main.py
import os
import logging
import logging.config
import time
from config.settings import LOGGING_CONFIG
from core.db_connector import DBConnector
from core.agent_scheduler import AgentScheduler

def main():
    # Ensure logs directory exists
    os.makedirs("logs", exist_ok=True)
    os.makedirs("reports", exist_ok=True)
    
    # Configure logging
    logging.config.dictConfig(LOGGING_CONFIG)
    logger = logging.getLogger("agent.main")
    
    # Initialize database connector
    db_connector = DBConnector()
    if not db_connector.connect():
        logger.error("Failed to connect to database. Exiting.")
        return
    
    logger.info("MCP Agent System starting...")
    
    # Initialize and start agent scheduler
    scheduler = AgentScheduler(db_connector)
    agent_ids = scheduler.initialize_default_agents()
    
    logger.info("Initialized agents: %s", agent_ids)
    scheduler.start_agents()
    
    logger.info("All agents started. System running...")
    
    try:
        # Keep main thread alive
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Shutdown requested. Stopping agents...")
        for agent_id in agent_ids.values():
            scheduler.stop_agent(agent_id)
    
    logger.info("MCP Agent System shutdown complete.")

if __name__ == "__main__":
    main()