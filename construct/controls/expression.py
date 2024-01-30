import itertools
from abc import ABC, abstractmethod

from construct.controls.constraint import ConstraintClause

i = itertools.count()


class LTLExpression(ABC):
    def __init__(self, _type: str):
        self.id = f"{_type}{next(i)}"

    @abstractmethod
    def __str__(self):
        pass

    @abstractmethod
    def find_events(self, events: set):
        pass


class LTLAtom(ABC):
    def __init__(self, _type: str):
        self.id = f"{_type}{next(i)}"

    @abstractmethod
    def __str__(self):
        pass

    @abstractmethod
    def find_events(self, events: set[str]):
        pass


class Implies(LTLExpression):
    def __init__(
        self,
        left: LTLExpression | LTLAtom,
        right: LTLExpression | LTLAtom,
        subset: ConstraintClause | None = None,
    ):
        super().__init__("impl")
        self.left = left
        self.right = right
        self.subset = subset

    def __str__(self):
        return f"({self.left} => {self.right})"

    def find_events(self, events: set[str]):
        self.left.find_events(events)
        self.right.find_events(events)

    def resolve(self, control):
        if isinstance(self.left, Implies) and isinstance(self.right, Constraint):
            return ImpliedPathConstraintStrategy(self).resolve(control)


class Eventually(LTLExpression):
    def __init__(self, event: LTLAtom):
        super().__init__("evty")
        self.event = event

    def __str__(self):
        return f"E {self.event}"

    def find_events(self, events: set[str]):
        self.event.find_events(events)


class And(LTLExpression):
    def __init__(self, *expressions: LTLExpression):
        super().__init__("and")
        self._expressions = expressions

    def __str__(self):
        return f"({' ∧ '.join(map(str, self._expressions))})"

    def find_events(self, events: set[str]):
        for expr in self._expressions:
            expr.find_events(events)

    def resolve(self, control):
        return f"""
            UNION
        """.join(
            expr.resolve(control) for expr in self._expressions
        )


class Not(LTLExpression):
    def __init__(self, expression: LTLExpression):
        super().__init__("not")
        self._expression = expression

    def __str__(self):
        return f"¬({self._expression})"

    def find_events(self, events: set[str]):
        self._expression.find_events(events)


class Constraint(LTLExpression):
    def __init__(self, clause: ConstraintClause):
        super().__init__("cst")
        self._clause = clause

    def __str__(self):
        return str(self._clause)

    def find_events(self, events: set[str]):
        pass


class Event(LTLAtom):
    def __init__(self, binding: str, event_type: str):
        super().__init__("e")
        self.binding = binding
        self.event_type = event_type

    def __str__(self):
        return f"{self.binding}({self.event_type})"

    def find_events(self, events: set[str]):
        events.add(self.event_type)


class QueryStrategy(ABC):
    @abstractmethod
    def resolve(self, control):
        pass


class ImpliedPathConstraintStrategy(QueryStrategy):
    def __init__(self, implies: Implies):
        super().__init__()
        self._implies = implies

    def resolve(self, control):
        left: Implies = self._implies.left
        right: Constraint = self._implies.right

        source: Event = left.left
        eventually: Eventually = left.right
        target: Event = eventually.event

        h = hash(control.find_events())

        return f"""
            MATCH ({source.binding}:Event {{ EventType: '{source.event_type}' }}),
                  ({target.binding}:Event {{ EventType: '{target.event_type}' }}),
                  p=({source.binding})-[:DF_PROJECTION {{ ID: '{h}' }}]->({target.binding})
            {f'WHERE {self._implies.subset}' if self._implies.subset else ''}
            {'   AND' if self._implies.subset else 'WHERE'} NOT ({str(right)})
            WITH {source.binding}, {target.binding}, p
            MATCH ({control.id}:Control {{ID: '{control.id}'}})
            MERGE ({source.binding})-[:VIOLATES]->({control.id})
            MERGE ({target.binding})-[:VIOLATES]->({control.id})
        """
