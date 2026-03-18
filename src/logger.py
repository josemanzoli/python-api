import logging
import sys
import logging_json

def setup_logger(name: str) -> logging.Logger:
    """Configura e retorna um logger formatado em JSON para a aplicação."""
    logger = logging.getLogger(name)
    
    # Evitar adicionar handlers duplicados se a função for chamada múltiplas vezes
    if not logger.handlers:
        logHandler = logging.StreamHandler(sys.stdout)
        formatter = logging_json.JSONFormatter(fields={
            "level": "levelname", 
            "loggerName": "name", 
            "processName": "processName",
            "processID": "process", 
            "threadName": "threadName", 
            "threadID": "thread",
            "lineNumber" : "lineno",
            "timestamp": "asctime"
        })
        logHandler.setFormatter(formatter)
        logger.addHandler(logHandler)
        logger.setLevel(logging.INFO)
        
    return logger
