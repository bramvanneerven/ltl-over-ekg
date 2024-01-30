import logging

from neo4j import GraphDatabase

import config as cfg


class SystemDriver(object):
    _instance = None
    _driver = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SystemDriver, cls).__new__(cls)
            cls._driver = GraphDatabase.driver(
                cfg.neo4j_uri,
                auth=cfg.neo4j_auth,
                database="system",
            )
            cls._driver.verify_connectivity()

        return cls._instance

    def __del__(self):
        self._driver.close()

    def drop_database(self):
        logging.info(f"🗑️ Dropping 'neo4j' database.")
        with self._driver.session() as session:
            session.run(f"DROP DATABASE neo4j IF EXISTS")

    def create_database(self):
        logging.info(f"🚧 Creating 'neo4j' database.")
        with self._driver.session() as session:
            session.run(f"CREATE OR REPLACE DATABASE neo4j")


class Driver(object):
    _instance = None
    _driver = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Driver, cls).__new__(cls)
            cls._driver = GraphDatabase.driver(
                cfg.neo4j_uri,
                auth=cfg.neo4j_auth,
            )
            cls._driver.verify_connectivity()

        return cls._instance

    def __del__(self):
        self._driver.close()

    def query(self, query: str):
        logging.info(f"Executing query: \n {query}")

        with self._driver.session() as session:
            res = session.run(query)
            if res is not None:
                return res.data()
            return res
