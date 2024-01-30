import logging
from pathlib import Path

import polars as pl

import config as cfg
from graph_construct import GraphConstruct


class Saver(object):
    _instance = None
    _path = None

    def __new__(cls, nuke: bool = False):
        if cls._instance is None:
            cls._instance = super(Saver, cls).__new__(cls)
            cls._path = Path(cfg.transform_export_dir)
            cls._path.mkdir(parents=True, exist_ok=True)

            if nuke:
                logging.info("☢️ Nuking export directory...")
                for f in cls._path.glob("*.csv"):
                    f.unlink()

        return cls._instance

    def save_df(self, df: pl.DataFrame, construct: GraphConstruct):
        file = self._path.joinpath(f"{construct.file_name()}.csv")

        file.touch(exist_ok=True)

        df.write_csv(file=file, separator=";")

        logging.info(f"Wrote to file {file}")
