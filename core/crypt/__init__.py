from abc import abstractmethod


class CryptBase:
    @abstractmethod
    def encrypt(self, content: bytes) -> bytes:
        ...

    @abstractmethod
    def decrypt(self, content: bytes) -> bytes:
        ...
