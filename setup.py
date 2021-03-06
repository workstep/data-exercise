# AES 256 encryption/decryption using pycrypto library

import base64
import hashlib
from Crypto.Cipher import AES
from Crypto import Random

BLOCK_SIZE = 16

password = input("Enter setup password: ")


def pad(s):
    return s + (BLOCK_SIZE - len(s) % BLOCK_SIZE) * chr(BLOCK_SIZE - len(s) % BLOCK_SIZE)


def unpad(s):
    return s[:-ord(s[len(s) - 1:])]


def encrypt(raw, password):
    private_key = hashlib.sha256(password.encode("utf-8")).digest()
    raw = pad(raw).encode()
    iv = Random.new().read(AES.block_size)
    cipher = AES.new(private_key, AES.MODE_CBC, iv)
    return base64.b64encode(iv + cipher.encrypt(raw))


def decrypt(enc, password):
    private_key = hashlib.sha256(password.encode("utf-8")).digest()
    enc = base64.b64decode(enc)
    iv = enc[:16]
    cipher = AES.new(private_key, AES.MODE_CBC, iv)
    return unpad(cipher.decrypt(enc[16:]))


encrypted = (
    "6lTgpOZQglF+i/JpheV9wnfocD8Cj+K4CjWAc5kIJ4aXfrGQrGnLe9ZuDl9ENZEcWxiADUf"
    "r5Ru0RBS4zAgM5fKxhFYnOL9kfYJcOuBvEQo="
)

# Let us decrypt using our original password
decrypted = decrypt(encrypted, password)
msg = bytes.decode(decrypted)

if not len(msg):
    print("Invalid password.")

with open(".env", "w") as f:
    # Writing data to a file
    f.write(msg)
