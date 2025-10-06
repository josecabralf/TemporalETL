from dataclasses import dataclass


@dataclass
class QueuerInput:
    """Input parameters for the Queuer workflow."""

    source_kind_id: str

    # date initialization should be handled in a params.py
    date_start: str | None = None
    date_end: str | None = None

    def __post_init__(self):
        if not self.source_kind_id:
            raise ValueError("source_kind_id must be provided")
