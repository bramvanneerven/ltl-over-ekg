import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


def execute(action: str, f):
    logging.info(f"{action} started.")
    f()
    logging.info(f"{action} ended.")
