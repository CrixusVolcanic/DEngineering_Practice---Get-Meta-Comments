import logging
import unicodedata
import uuid
from globack_utils.globack.util.secret_manager import Secrets
import os
import requests
import time
from functools import wraps

__secrets_sellers = None

def setup_logger(name, log_file, level=logging.INFO):
    """Function to set up a logger with the given name, log file, and level."""
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # File handler
    file_handler = logging.FileHandler(log_file)        
    file_handler.setFormatter(formatter)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Add handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

# Set up default logger
default_logger = setup_logger('default_logger', 'default.log')

def normalize_text(text):
    if isinstance(text, str):
        # Normalize the text to NFC (Normalization Form C)
        text = unicodedata.normalize('NFC', text)
        return text
    return text

def clean_text(text):
    if isinstance(text, str):
        return text.encode('ascii', errors='ignore').decode('ascii')
    return text

def get_uuid():
    """generate and get uuid4 string"""

    return str(uuid.uuid4())

def get_secrets_sellers() -> dict:
    """Get secrets from aws service"""

    global __secrets_sellers
    if not __secrets_sellers:
        secrets = Secrets(os.getenv("ENV_SECRET_DB_PG"))
        try:
            __secrets_sellers = secrets.get_keys()
        except Exception as e:
            default_logger.error(f"Failed to retrieve secrets sellers: {e}")
            raise

    return __secrets_sellers

def retry_on_rate_limit(max_retries=5, initial_backoff=60):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retry_count = 0
            backoff_time = initial_backoff
            
            while retry_count < max_retries:
                try:
                    return func(*args, **kwargs)
                except requests.exceptions.HTTPError as http_err:
                    if http_err.response is not None:
                        error_info = http_err.response.json()
                        error_code = error_info.get('error', {}).get('code')
                        if error_code == 80004:  # Rate limit error code
                            default_logger.warning(f"Rate limit hit. Retrying in {backoff_time} seconds...")
                            time.sleep(backoff_time)
                            backoff_time *= 2  # Exponential backoff
                            retry_count += 1
                        else:
                            raise http_err
                    else:
                        raise http_err
            raise Exception(f"Max retries exceeded for {func.__name__}")
        return wrapper
    return decorator