import logging
log_file = 'logs/service.log'

# create logger with "spam_application"
logger = logging.getLogger('fang_app_rec')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler(log_file)
fh.setLevel(logging.INFO)

# create formatter and add it to the handlers
formatter =  logging.Formatter("[%(levelname)s][%(asctime)s][%(name)s][%(message)s]")
fh.setFormatter(formatter)

# add the handlers to the logger
logger.addHandler(fh)

