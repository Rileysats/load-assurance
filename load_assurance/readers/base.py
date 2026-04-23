import pyarrow as pa

from abc import ABC, abstractmethod
from typing import BinaryIO

class AbstractReader(ABC):
    @abstractmethod
    def get_row_count(self, file: BinaryIO) -> int: ...

    @abstractmethod
    def get_schema(self, file: BinaryIO) -> pa.Schema: ...

    @abstractmethod
    def sample_nulls(self, file: BinaryIO, columns: list[str]) -> dict[str, int]: ...