import re
from pathlib import Path


class GraphConstruct:
    def __init__(self, name: str):
        self._name = name

    def name(self) -> str:
        return self._name.upper()

    def file_name(self) -> str:
        pass

    def type(self) -> str:
        return self.__class__.__name__

    @staticmethod
    def parse(file: Path) -> "Event | Entity | Relation":
        if file.name.startswith("EVENT"):
            result = re.search(r"EVENT\((\w+)\)", file.name)
            return Event(result.group(1))
        elif file.name.startswith("ENTITY"):
            result = re.search(r"ENTITY\((\w+)\)", file.name)
            return Entity(result.group(1))
        elif file.name.startswith("RELATION"):
            result = re.search(
                r"RELATION\((\w+)\)_(\w+)\((\w+)\)_(\w+)\((\w+)\)",
                file.name,
            )
            relation_name = result.group(1)
            source_type = result.group(2)
            source_name = result.group(3)
            target_type = result.group(4)
            target_name = result.group(5)

            source = (
                Event(source_name) if source_type == "EVENT" else Entity(source_name)
            )
            target = (
                Event(target_name) if target_type == "EVENT" else Entity(target_name)
            )

            return Relation(relation_name, source, target)


class Event(GraphConstruct):
    def __init__(self, name: str):
        super().__init__(name)

    def file_name(self) -> str:
        return f"EVENT({self.name()})"


class Entity(GraphConstruct):
    def __init__(self, name: str):
        super().__init__(name)

    def file_name(self) -> str:
        return f"ENTITY({self.name()})"


class Relation(GraphConstruct):
    def __init__(self, name: str, source: GraphConstruct, target: GraphConstruct):
        super().__init__(name)
        self._source = source
        self._target = target

    def file_name(self) -> str:
        source = self.source().file_name()
        target = self.target().file_name()
        return f"RELATION({self.name()})_{source}_{target}"

    def source(self) -> GraphConstruct:
        return self._source

    def target(self) -> GraphConstruct:
        return self._target

    def set_source(self, source: GraphConstruct) -> "Relation":
        self._source = source
        return self
