# config/settings.py
DATABASE_CONFIG = {
    "type": "sqlite",
    "database": "mcp_agent_system.db"
}

LOGGING_CONFIG = {
    "version": 1,
    "formatters": {
        "standard": {
            "format": "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "INFO",
            "formatter": "standard",
            "stream": "ext://sys.stdout"
        },
        "file": {
            "class": "logging.FileHandler",
            "level": "INFO",
            "formatter": "standard",
            "filename": "logs/mcp_agent_system.log",
            "mode": "a"
        }
    },
    "loggers": {
        "agent": {
            "handlers": ["console", "file"],
            "level": "INFO",
            "propagate": False
        }
    }
}