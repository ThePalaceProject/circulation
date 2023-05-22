import pytest

from core.crypt.aes import CryptAESCBC


class TestCryptAESCBC:
    def test_padding(self):
        cipher = CryptAESCBC(b"key")
        padded = cipher._pad_content(b"7 bytes")
        expected = 9
        assert len(padded) == cipher.PADDING
        assert padded == b"7 bytes" + expected.to_bytes(1, "big") * expected

        unpadded = cipher._unpad_content(padded)
        assert unpadded == b"7 bytes"

        # Test the max block size
        limit_pad = cipher._pad_content(b"A" * cipher.PADDING)
        assert len(limit_pad) == cipher.PADDING * 2
        assert cipher._unpad_content(limit_pad) == b"A" * cipher.PADDING

        # 0 block size
        pad0 = cipher._pad_content(b"")
        assert len(pad0) == cipher.PADDING
        assert cipher._unpad_content(pad0) == b""

    def test_iv_getter_setter(self):
        iv = CryptAESCBC(b"key").iv
        assert len(iv) == CryptAESCBC.IV_LENGTH

        aes = CryptAESCBC(b"key")
        with pytest.raises(ValueError):
            aes.iv = "not12bytes"

        aes.iv = "iv" * 8
        assert aes.iv == "iv" * 8

    def test_encrypt_decrypt(self):
        cipher = CryptAESCBC(b"keys" * 8)
        encrypted = cipher.encrypt(b"somecontent")

        assert encrypted[: cipher.IV_LENGTH] == cipher.iv

        decryptor = CryptAESCBC(b"keys" * 8)
        content = decryptor.decrypt(encrypted)

        assert content == b"somecontent"
        assert decryptor.iv == cipher.iv
