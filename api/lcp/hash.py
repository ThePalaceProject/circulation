import hashlib
from abc import ABC, abstractmethod
from enum import Enum

from core.exceptions import BaseError


class HashingAlgorithm(Enum):
    SHA256 = "http://www.w3.org/2001/04/xmlenc#sha256"
    SHA512 = "http://www.w3.org/2001/04/xmlenc#sha512"


class HashingError(BaseError):
    """Raised in the case of errors occurred during hashing"""


class Hasher(ABC):
    """Base class for all implementations of different hashing algorithms"""

    def __init__(self, hashing_algorithm: HashingAlgorithm) -> None:
        """Initializes a new instance of Hasher class

        :param hashing_algorithm: Hashing algorithm
        """
        self._hashing_algorithm = hashing_algorithm

    @abstractmethod
    def hash(self, value: str) -> str:
        ...


class UniversalHasher(Hasher):
    def hash(self, value: str) -> str:
        assert type(value) == str

        if self._hashing_algorithm in [
            HashingAlgorithm.SHA256,
            HashingAlgorithm.SHA256.value,
        ]:
            return hashlib.sha256(value.encode("utf-8")).hexdigest()
        elif self._hashing_algorithm in [
            HashingAlgorithm.SHA512,
            HashingAlgorithm.SHA512.value,
        ]:
            return hashlib.sha512(value.encode("utf-8")).hexdigest()
        else:
            raise HashingError(f"Unknown hashing algorithm {self._hashing_algorithm}")


class HasherFactory:
    def create(self, hashing_algorithm: HashingAlgorithm) -> Hasher:
        return UniversalHasher(hashing_algorithm)
