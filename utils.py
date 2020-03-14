import logging
import re
from constants import REQUESTER_ANNOTATION


def setup_logger():
    """Sets up the logger to output entries to the console."""
    logger = logging.getLogger("monitor")
    logger.setLevel(logging.INFO)
    stream_handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s [%(levelname)s]: %(message)s")
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    return logger


def batch_id_match(hit, batch_id):
    """Checks if the 'hit' belongs to the batch based on the 'batch_id'."""
    req_ann = hit[REQUESTER_ANNOTATION]
    hit_batch_id = re.search(r'BatchId:\d+', req_ann).group(0)
    hit_batch_id = int(hit_batch_id.split(":")[-1])
    return hit_batch_id == batch_id
