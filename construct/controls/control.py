import itertools

from construct.controls.expression import LTLExpression

i = itertools.count()


class Control:
    def __init__(self, description: str, expression: LTLExpression):
        self.id = f"c{next(i)}"
        self.description = description
        self.expression = expression

    def check_query(self):
        query = f"""
            CREATE ({self.id}:Control {{ ID: '{self.id}', Description: '{self.description}' }})
            WITH {self.id}
        """

        query += self.expression.resolve(self)

        return query

    def find_events(self) -> tuple[str, ...]:
        events = set()
        self.expression.find_events(events)
        return tuple(sorted(events))
