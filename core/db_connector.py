import sqlite3
import logging
import json
import threading
from config.settings import DATABASE_CONFIG

class DBConnector:
    """Database connector for SQLite with thread-local storage for connections"""
    
    _local = threading.local()
    
    def __init__(self):
        self.logger = logging.getLogger("agent.db_connector")
        self.db_config = DATABASE_CONFIG
        self.db_type = self.db_config.get("type", "sqlite")
        self.db_path = self.db_config.get("database", "mcp_agent_system.db")
        
    def connect(self):
        """Connect to the database and initialize tables if needed"""
        try:
            # Create connection for the main thread
            self._get_connection()
            
            # Initialize database schema
            self._initialize_schema()
            return True
        except Exception as e:
            self.logger.error("Database connection error: %s", str(e))
            return False
    
    def _get_connection(self):
        """Get or create a thread-local database connection"""
        thread_id = threading.current_thread().name
        
        if not hasattr(self._local, 'connection') or self._local.connection is None:
            self._local.connection = sqlite3.connect(self.db_path)
            self._local.connection.row_factory = self._dict_factory
            self.logger.info("Database connection established for thread %s", thread_id)
            
        return self._local.connection
    
    def _dict_factory(self, cursor, row):
        """Convert database row to dictionary"""
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d
    
    def _initialize_schema(self):
        """Initialize database schema if tables don't exist"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Agent registry table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS agent_registry (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            agent_id TEXT UNIQUE NOT NULL,
            agent_type TEXT NOT NULL,
            status TEXT NOT NULL,
            last_heartbeat TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        # Agent messages table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS agent_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender_id TEXT NOT NULL,
            recipient_id TEXT NOT NULL,
            message_type TEXT NOT NULL,
            content TEXT NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            read INTEGER DEFAULT 0
        )
        """)
        
        # Agent tasks table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS agent_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            agent_id TEXT NOT NULL,
            task_type TEXT NOT NULL,
            parameters TEXT NOT NULL,
            status TEXT NOT NULL,
            result TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP
        )
        """)
        
        # Sales metrics table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS sales_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            source TEXT NOT NULL,
            metric_type TEXT NOT NULL,
            value REAL NOT NULL
        )
        """)
        
        # Sales insights table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS sales_insights (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            insight_type TEXT NOT NULL,
            description TEXT NOT NULL,
            severity TEXT NOT NULL,
            metrics TEXT
        )
        """)
        
        # System notifications table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS system_notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            notification_type TEXT NOT NULL,
            message TEXT NOT NULL,
            severity TEXT NOT NULL,
            acknowledged INTEGER DEFAULT 0
        )
        """)
        
        # Report archive table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS report_archive (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_type TEXT NOT NULL,
            generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            file_path TEXT,
            parameters TEXT
        )
        """)
        
        conn.commit()
    
    def execute(self, query, params=()):
        """Execute a query and return the last row id"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute(query, params)
            conn.commit()
            return cursor.lastrowid
        except Exception as e:
            conn.rollback()
            self.logger.error("Query error: %s", str(e))
            raise
    
    def query(self, query, params=()):
        """Execute a query and return all results as a list of dictionaries"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute(query, params)
            return cursor.fetchall()
        except Exception as e:
            self.logger.error("Query error: %s", str(e))
            raise
    
    def close(self):
        """Close the database connection for the current thread"""
        if hasattr(self._local, 'connection') and self._local.connection:
            self._local.connection.close()
            self._local.connection = None