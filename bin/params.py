import argparse
import logging

def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--topic', default = 'test_topic')
    parser.add_argument('--key')
    parser.add_argument('--stable_prefix', default = 'data')
    parser.add_argument('--table_prefix', default = 'tbl')
    parser.add_argument('--topic_prefix', default = 'topic')
    parser.add_argument('--hostname', default = 'localhost')
    parser.add_argument('--port', default = 6030)
    parser.add_argument('--log_level', default = 'info')
    parser.add_argument('--database', default = 'test')
    return parser

def get_logger(log_name, log_level):
    logger = logging.getLogger(log_name)
    if 'info' == log_level:
        logger.setLevel(logging.INFO)
    else:
        logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    console.setFormatter(formatter)
    logger.addHandler(console)
    return logger