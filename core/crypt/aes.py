from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes

from core.crypt import CryptBase


class CryptAESCBC(CryptBase):
    IV_LENGTH = 16
    KEY_LENGTH = 32
    PADDING = 16

    def __init__(self, key: bytes, iv: bytes = None) -> None:
        self._iv = iv
        self.key = key

    @classmethod
    def generate_key(cls):
        return get_random_bytes(cls.KEY_LENGTH)

    @property
    def iv(self):
        if self._iv is None:
            self._iv = get_random_bytes(self.IV_LENGTH)
        return self._iv

    def _pad_content(self, content: bytes) -> bytes:
        pad_length = self.PADDING - len(content) % self.PADDING
        if pad_length == 0:
            pad_length = self.PADDING
        return content + pad_length.to_bytes(1, "big") * pad_length

    def _unpad_content(self, content: bytes) -> bytes:
        pad_byte = content[-1]
        return content[:-pad_byte]

    @iv.setter
    def iv(self, value):
        if len(value) != self.IV_LENGTH:
            raise ValueError(f"IV length must be {self.IV_LENGTH} bytes.")
        self._iv = value

    def encrypt(self, content: bytes) -> bytes:
        cipher = AES.new(self.key, AES.MODE_CBC, iv=self.iv)
        padded = self._pad_content(content)
        encrypted = cipher.encrypt(padded)
        return self.iv + encrypted

    def decrypt(self, content: bytes) -> bytes:
        self.iv, encrypted = content[: self.IV_LENGTH], content[self.IV_LENGTH :]
        cipher = AES.new(self.key, AES.MODE_CBC, iv=self.iv)
        decrypted = cipher.decrypt(encrypted)
        return self._unpad_content(decrypted)
