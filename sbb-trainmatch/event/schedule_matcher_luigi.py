import os

from vibepy.load_logger import load_logger
from vibepy.read_config import read_config
import vibepy.class_postgres as class_postgres
from run_single_batch import process_batch


def main(list_mot_id):
    load_logger()
    CONFIG = read_config(ini_filename='application.ini', ini_path=os.path.dirname(__file__))
    DB = class_postgres.PostgresManager(CONFIG, 'database')

    process_batch(list_mot_id, CONFIG, DB)

    return

if __name__ == "__main__":
    main() # Placeholder -- calling main() without arguments will crash it obviously



