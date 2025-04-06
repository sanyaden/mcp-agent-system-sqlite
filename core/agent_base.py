import json
import uuid
import datetime
import logging
from abc import ABC, abstractmethod

class BaseAgent(ABC):
    def __init__(self, agent_id=None, agent_type=None):
        self.agent_id = agent_id or f"{agent_type}_{uuid.uuid4()}"
        self.agent_type = agent_type
        self.status = "inactive"
        self.logger = logging.getLogger(f"agent.{self.agent_type}")
        
    def register(self, db_connector):
        """Register agent in the agent_registry table"""
        # First try to update existing record
        update_query = """
        UPDATE agent_registry 
        SET status = ?, last_heartbeat = CURRENT_TIMESTAMP
        WHERE agent_id = ?
        """
        result = db_connector.execute(update_query, (self.status, self.agent_id))
        
        # If no rows were affected, insert new record
        if not result or result == 0:
            insert_query = """
            INSERT INTO agent_registry (agent_id, agent_type, status)
            VALUES (?, ?, ?)
            """
            db_connector.execute(insert_query, (self.agent_id, self.agent_type, self.status))
        
        self.logger.info("Agent %s registered successfully", self.agent_id)
        
    def update_status(self, db_connector, status):
        """Update agent status in the registry"""
        self.status = status
        query = """
        UPDATE agent_registry 
        SET status = ?, last_heartbeat = CURRENT_TIMESTAMP
        WHERE agent_id = ?
        """
        db_connector.execute(query, (status, self.agent_id))
        self.logger.info("Agent %s status updated to %s", self.agent_id, status)
    
    def send_message(self, db_connector, recipient_id, message_type, content):
        """Send a message to another agent"""
        query = """
        INSERT INTO agent_messages (sender_id, recipient_id, message_type, content)
        VALUES (?, ?, ?, ?)
        """
        message_id = db_connector.execute(query, (
            self.agent_id, recipient_id, message_type, json.dumps(content)
        ))
        self.logger.info("Message sent to %s, type: %s, id: %s", recipient_id, message_type, message_id)
        return message_id
    
    def get_messages(self, db_connector, mark_as_read=True):
        """Get messages sent to this agent"""
        query = """
        SELECT id, sender_id, message_type, content, timestamp
        FROM agent_messages
        WHERE recipient_id = ? AND read = 0
        ORDER BY timestamp ASC
        """
        messages = db_connector.query(query, (self.agent_id,))
        
        if mark_as_read and messages:
            for message in messages:
                update_query = """
                UPDATE agent_messages
                SET read = 1
                WHERE id = ?
                """
                db_connector.execute(update_query, (message["id"],))
            
        return messages
    
    def create_task(self, db_connector, task_data, priority=5):
        """Create a new task for this agent"""
        task_id = f"task_{uuid.uuid4()}"
        query = """
        INSERT INTO agent_tasks (agent_id, task_type, parameters, status)
        VALUES (?, ?, ?, ?)
        """
        task_db_id = db_connector.execute(query, (
            self.agent_id, task_data.get("type", "default"), json.dumps(task_data), "pending"
        ))
        self.logger.info("Task created with id: %s", task_db_id)
        return task_db_id
    
    def get_pending_tasks(self, db_connector):
        """Get pending tasks for this agent"""
        query = """
        SELECT id, task_type, parameters, created_at
        FROM agent_tasks
        WHERE agent_id = ? AND status = 'pending'
        ORDER BY created_at ASC
        """
        return db_connector.query(query, (self.agent_id,))
    
    def update_task_status(self, db_connector, task_id, status, result=None):
        """Update task status and optionally add result"""
        if status in ["completed", "failed"]:
            query = """
            UPDATE agent_tasks
            SET status = ?, result = ?, completed_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """
        else:
            query = """
            UPDATE agent_tasks
            SET status = ?, result = ?
            WHERE id = ?
            """
        
        result_json = json.dumps(result) if result else None
        db_connector.execute(query, (status, result_json, task_id))
        self.logger.info("Task %s status updated to %s", task_id, status)
    
    @abstractmethod
    def run(self, db_connector):
        """Main agent execution method, must be implemented by subclasses"""
        pass