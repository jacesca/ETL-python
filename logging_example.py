import logging

# Configuration
# logging.basicConfig(filename=LOGS_PATH, level=logging.INFO)
logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)

# Create different types of logs
path = 'Test'
logging.debug(f"Variable has value {path}")
logging.info("Data has been transformed and will now be loaded.")
logging.warning("Unexpected number of rows detected.")
logging.error("The key arose in execution.")
