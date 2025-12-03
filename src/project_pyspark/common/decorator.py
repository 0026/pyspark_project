import logging

def log_decorator(function_name:str):
    def decorator(func):
        def wrapper(*args, **kwargs):
            print(f"start: {function_name}")
            logging.info(f"start - {function_name}")
            reesult = func(*args, **kwargs)
            logging.info(f"finish - {function_name}")
            return reesult
        return wrapper
    return decorator