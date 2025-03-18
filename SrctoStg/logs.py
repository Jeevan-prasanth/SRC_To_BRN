import os
import sys
import json
import logging
import traceback
import functools
import datetime as dt
from dateutil import tz
from collections import deque

class LoggerManager:
    def __init__(self, log_dir="Logs"):
        self._logs = deque()
        self.log_dir = os.path.join(os.getcwd(), log_dir)
        os.makedirs(self.log_dir, exist_ok=True)
        self.logger = self._configure_logger()
    
    def _configure_logger(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        
        # Prevent adding handlers multiple times
        if not logger.handlers:
            file_path = os.path.join(self.log_dir, f"{dt.datetime.now().strftime('%Y-%m-%d %H-%M-%S')}.log")
            file_handler = logging.FileHandler(file_path, encoding="UTF-8")
            stream_handler = logging.StreamHandler()
            logger.addHandler(file_handler)
            logger.addHandler(stream_handler)
        
        return logger

    
    def log_error(self, exc, **kwargs):
        event = {**self._error_info(exc), **kwargs}
        self._logs.append(event)
        self.logger.error(json.dumps(event, indent=4))
    
    def log_event(self, **kwargs):
        self._logs.append(kwargs)
        self.logger.info(json.dumps(kwargs, indent=4))
    
    def _error_info(self, exc):
        return {
            'type': type(exc).__name__,
            'args': [str(arg) for arg in exc.args],
            'traceback': traceback.format_exc(),
            'timestamp': dt.datetime.now(tz=tz.gettz()).astimezone(tz.tzutc()).isoformat()
        }
    
    def reset_logs(self):
        self._logs.clear()
    
    def save_logs(self):
        if self._logs:
            file_path = os.path.join(self.log_dir, f"{dt.datetime.now().strftime('%Y-%m-%d %H-%M-%S')}.json")
            with open(file_path, 'w') as file:
                json.dump(list(self._logs), file, indent=4)
    
    def handle_error(self, function):
        @functools.wraps(function)
        def wrapper(*args, **kwargs):
            try:
                return function(*args, **kwargs)
            except Exception as exc:
                self.log_error(
                    exc,
                    function=f'{function.__module__}.{function.__name__}',
                    function_args=args,
                    function_kwargs=kwargs,
                )
        return wrapper
