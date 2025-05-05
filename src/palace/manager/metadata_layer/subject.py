from __future__ import annotations


class SubjectData:
    def __init__(
        self,
        type: str,
        identifier: str | None,
        name: str | None = None,
        weight: int = 1,
    ) -> None:
        self.type = type

        # Because subjects are sometimes evaluated according to keyword
        # matching, it's important that any leading or trailing white
        # space is removed during import.
        self.identifier = identifier
        if identifier:
            self.identifier = identifier.strip()

        self.name = name
        if name:
            self.name = name.strip()

        self.weight = weight

    @property
    def key(self) -> tuple[str, str | None, str | None, int]:
        return self.type, self.identifier, self.name, self.weight

    def __repr__(self) -> str:
        return '<SubjectData type="%s" identifier="%s" name="%s" weight=%d>' % (
            self.type,
            self.identifier,
            self.name,
            self.weight,
        )
