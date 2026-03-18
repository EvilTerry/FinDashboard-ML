import pandas as pd
from abc import ABC, abstractmethod


class BankAdapter(ABC):
    """
    Base class for bank adapters.
    Each bank adapter reads a raw export and returns a normalized DataFrame
    with columns: date, amount, merchant, description, currency, source_account
    """

    @abstractmethod
    def parse(self, filepath: str) -> pd.DataFrame:
        """Parse a raw bank export CSV into the standard transaction shape."""
        pass
