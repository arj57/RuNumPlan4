import logging
import config

cfg = config.Config()

def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(cfg.get_param_val("./Logging/LogLevel"))
    if not cfg.get_param_val("./Logging/Silence"):
        ch = logging.StreamHandler()
        # ch.setLevel(log_level)
        formatter = logging.Formatter('%(asctime)s  %(levelname)-8s%(name)-8s  %(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    log_file = cfg.get_param_val("./Logging/LogFile")
    if log_file is not None and len(log_file) > 0:
        fh = logging.FileHandler(log_file)
        # fh.setLevel(log_level)
        formatter = logging.Formatter('%(asctime)s  %(levelname)-8s%(name)-8s  %(message)s')
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger
