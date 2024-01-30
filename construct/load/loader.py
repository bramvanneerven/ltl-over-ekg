import csv
from pathlib import Path

import config as cfg
from graph_construct import GraphConstruct, Relation


class DataFile:
    _uri: str = None
    _headers: list[str] = []
    _construct: GraphConstruct = None

    def __init__(self, path: Path):
        self._headers = _csv_headers(path)
        self._construct = GraphConstruct.parse(path)
        self._uri = path.as_uri().replace(
            Path(cfg.neo4j_import_dir).as_uri(), "file://"
        )

    def query(self):
        assert not isinstance(self._construct, Relation)

        query = f"LOAD CSV WITH HEADERS FROM \"{self._uri}\" as line\n FIELDTERMINATOR ';'\n"
        query += "CALL {\n"
        query += " WITH line\n"
        for col in self._headers:
            if col in ["Timestamp", "start", "end"]:
                column = f"datetime(line.{col})"
            elif col == "Date":
                column = f'apoc.date.parse(line.{col}, "ms", "dd-MM-yyyy")'
            elif col == "Time":
                column = f"time(line.{col})"
            else:
                column = "line." + col

            new_line = ""
            if self._headers.index(col) == 0:
                new_line += f" CREATE (e:{self._construct.type()} {{ {self._construct.type()}Type: '{self._construct.name()}'"
                if len(self._headers) > 0:
                    new_line += ", "

            new_line += f"{col}: {column}"

            if (
                len(self._headers) > 1
                and not self._headers.index(col) == len(self._headers) - 1
            ):
                new_line += ", "

            if self._headers.index(col) == len(self._headers) - 1:
                new_line += f" }})"

            query += new_line

        query += "\n"
        query += "} IN TRANSACTIONS OF 1000 ROWS;"
        return query


class RelationDataFile(DataFile):
    def __init__(self, path: Path):
        super().__init__(path)

    def query(self):
        assert isinstance(self._construct, Relation)

        src_type = self._construct.source().type()
        tgt_type = self._construct.target().type()
        src_name = self._construct.source().name()
        tgt_name = self._construct.target().name()
        rel_name = self._construct.name()

        src_col = list(filter(lambda x: x.startswith("from_"), self._headers))[0]
        tgt_col = list(filter(lambda x: x.startswith("to_"), self._headers))[0]
        rest_col = [col for col in self._headers if col not in [src_col, tgt_col]]

        src_col = src_col.replace("from_", "")
        tgt_col = tgt_col.replace("to_", "")

        query = f"LOAD CSV WITH HEADERS FROM \"{self._uri}\" as line\n FIELDTERMINATOR ';'\n"
        query += "CALL {\n"
        query += " WITH line\n"

        query += f"  MATCH (s:{src_type} {{ {src_type}Type: '{src_name}', {src_col}: line.from_{src_col} }}),\n"
        query += f"        (t:{tgt_type} {{ {tgt_type}Type: '{tgt_name}', {tgt_col}: line.to_{tgt_col} }}) \n"

        query += f"  CREATE (s)"

        if src_type != tgt_type:
            rel_block = f"[:CORR"
            ltr = src_type == "Event"
        else:
            rel_block = f"[:REL"
            ltr = True

        rel_block += f" {{ RelationType: '{rel_name}'"

        if len(rest_col) > 0:
            rel_block += ", "

        for col in rest_col:
            rel_block += f"{col}: line.{col}"
            if len(rest_col) > 1 and not rest_col.index(col) == len(rest_col) - 1:
                rel_block += ", "

        rel_block += f" }}]"

        query += f"-{rel_block}->" if ltr else f"<-{rel_block}-"
        query += f"(t) \n"

        query += "} IN TRANSACTIONS OF 1000 ROWS;"

        return query


class RelationIndexDataFile(DataFile):
    def __init__(self, path: Path):
        super().__init__(path)

    def query(self):
        assert isinstance(self._construct, Relation)

        src_type = self._construct.source().type()
        tgt_type = self._construct.target().type()
        src_name = self._construct.source().name()
        tgt_name = self._construct.target().name()

        src_col = list(filter(lambda x: x.startswith("from_"), self._headers))[
            0
        ].replace("from_", "")
        tgt_col = list(filter(lambda x: x.startswith("to_"), self._headers))[0].replace(
            "to_", ""
        )

        return [
            f"CREATE INDEX IF NOT EXISTS FOR (s:{src_type}) ON (s.{src_col})",
            f"CREATE INDEX IF NOT EXISTS FOR (t:{tgt_type}) ON (t.{tgt_col})",
        ]


class Loader:
    _import_path: Path
    _data: list[DataFile]

    def __init__(self):
        self._import_path = Path(cfg.neo4j_import_dir)
        self._data = []

    def __iter__(self):
        return iter(self._data)


class EventLoader(Loader):
    def __init__(self):
        super().__init__()

        for file in self._import_path.glob("EVENT*.csv"):
            self._data.append(DataFile(file))


class EntityLoader(Loader):
    def __init__(self):
        super().__init__()

        for file in self._import_path.glob("ENTITY*.csv"):
            self._data.append(DataFile(file))


class RelationLoader(Loader):
    def __init__(self):
        super().__init__()

        for file in self._import_path.glob("RELATION*.csv"):
            self._data.append(RelationDataFile(file))


class RelationIndexLoader(Loader):
    def __init__(self):
        super().__init__()

        for file in self._import_path.glob("RELATION*.csv"):
            self._data.append(RelationIndexDataFile(file))


def _csv_headers(file: Path) -> list[str]:
    f = file.open()
    reader = csv.reader(f, delimiter=";")
    return next(reader, None)
