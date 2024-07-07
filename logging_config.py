import logging

logging.basicConfig(
    format='%(name)s: %(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO
)

logger = logging.getLogger('final_pipeline.log')