import itertools
from abc import ABC, abstractmethod
from enum import Enum

i = itertools.count()


class ConstraintClause(ABC):
    def __init__(self, _type: str):
        self.id = f"{_type}{next(i)}"

    @abstractmethod
    def __str__(self):
        pass


class ConstraintAtom(ABC):
    def __init__(self, _type: str):
        self.id = f"{_type}{next(i)}"

    @abstractmethod
    def __str__(self):
        pass


class Attribute(ConstraintAtom):
    def __init__(self, binding: str, name: str):
        super().__init__("attr")
        self.binding = binding
        self.name = name

    def __str__(self):
        return f"{self.binding}.{self.name}"


class StrLiteral(ConstraintAtom):
    def __init__(self, value: str):
        super().__init__("lit")
        self.value = value

    def __str__(self):
        return f"'{self.value}'"


class ComparisonOperator(Enum):
    EQUALS = 1
    NOT_EQUALS = 2
    LESS_THAN = 3
    LESS_THAN_OR_EQUALS = 4
    GREATER_THAN = 5
    GREATER_THAN_OR_EQUALS = 6

    def __str__(self):
        match self:
            case ComparisonOperator.EQUALS:
                return "="
            case ComparisonOperator.NOT_EQUALS:
                return "<>"
            case ComparisonOperator.LESS_THAN:
                return "<"
            case ComparisonOperator.LESS_THAN_OR_EQUALS:
                return "<="
            case ComparisonOperator.GREATER_THAN:
                return ">"
            case ComparisonOperator.GREATER_THAN_OR_EQUALS:
                return ">="


class Comparison(ConstraintClause):
    def __init__(
        self,
        left: ConstraintClause | ConstraintAtom,
        right: ConstraintClause | ConstraintAtom,
        operator: ComparisonOperator,
    ):
        super().__init__("comp")
        self.left = left
        self.right = right
        self.operator = operator

    def __str__(self):
        return f"{self.left} {self.operator} {self.right}"


class And(ConstraintClause):
    def __init__(self, *clauses: ConstraintClause):
        super().__init__("andc")
        self.clauses = clauses

    def __str__(self):
        return f"{' AND '.join([str(clause) for clause in self.clauses])}"
