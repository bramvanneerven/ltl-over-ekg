import logging
import os
from pathlib import Path

import polars as pl

import config as cfg


class Loader(object):
    _instance = None
    _csvs = None
    _dfs = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Loader, cls).__new__(cls)

            import_path = Path(cfg.csv_import_dir)
            files = import_path.glob("*.csv")
            cls._csvs = {f.stem.lower(): f for f in files}

            logging.info(
                f"Found the following files to import:\n{os.linesep.join([f'- {f}' for f in cls._csvs])}"
            )

            cls._dfs = {}

        return cls._instance

    def get_df(
        self,
        name: str,
        schema: dict[str, pl.DataType | pl.PolarsDataType] = None,
        dtypes: dict[str, pl.DataType | pl.PolarsDataType] = None,
        columns: list[str] = None,
    ) -> pl.DataFrame:
        if name in self._csvs:
            if name not in self._dfs:
                if columns is not None:
                    logging.info(f"Loading {name} with columns {columns}...")
                else:
                    logging.info(f"Loading {name}...")

                df = pl.read_csv(
                    self._csvs[name],
                    separator=";",
                    schema=schema,
                    dtypes=dtypes,
                    columns=columns,
                    infer_schema_length=10_000,
                )

                if columns is None:
                    self._dfs[name] = df

                return df

            return self._dfs[name]

        raise Exception(f"Could not find file {name}.csv")
