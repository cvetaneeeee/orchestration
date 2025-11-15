# import base64
# from cryptography.hazmat.primitives import hashes
# from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
# from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
# from cryptography.hazmat.backends import default_backend
# from cryptography.hazmat.primitives import padding

# # Qwt = '5b9a8f2c3e6d1a4b7c8e9d0f1a2b3c4d'
# # Uwt = 'BDOU99-h67HcA6JeFXHbSNMu7e2yNNu3RzoMj8TM4W88jITfq7ZmPvIM1Iv-4_l2LxQcYwhqby2xGpWwzjfAnG4'

# def decrypt_oddsportal(encrypted_input: str, password: str, salt: str) -> str:
#     """
#     Decrypt OddsPortal encrypted API response using PBKDF2 (SHA-256) + AES-CBC.
#     Mirrors the JS function `xNt`.
#     """

#     # 1. Base64 decode the input string
#     decoded_input = base64.b64decode(encrypted_input)
#     decoded_str = decoded_input.decode("utf-8")
#     print("Decoded string (should contain ':'):", decoded_str[:120])

#     # 2. Split into l and u
#     l_str, u_hex = decoded_str.split(":")

#     # 3. IV from hex
#     iv = bytes.fromhex(u_hex)

#     # 4. Derive key with PBKDF2 (SHA-256, 1000 iterations, 32 bytes)
#     print("Salt used for KDF:", salt)
#     kdf = PBKDF2HMAC(
#         algorithm=hashes.SHA256(),
#         length=32,
#         salt=salt.encode("utf-8"),
#         iterations=1000,
#         backend=default_backend(),
#     )
#     print("Password used for KDF:", password)
#     key = kdf.derive(password.encode("utf-8"))

#     # 5. Decode encrypted data (base64 inside l_str)
#     encrypted_data = base64.b64decode(l_str)

#     # 6. AES-CBC decrypt
#     cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
#     decryptor = cipher.decryptor()
#     padded_plaintext = decryptor.update(encrypted_data) + decryptor.finalize()

#     # 7. Unpad (PKCS7, block size = 128 bits)
#     unpadder = padding.PKCS7(128).unpadder()
#     plaintext_bytes = unpadder.update(padded_plaintext) + unpadder.finalize()

#     return plaintext_bytes.decode("utf-8")


import base64
from cryptography.hazmat.primitives import hashes, padding
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend

def decrypt_oddsportal(encrypted_input: str, password: str, salt: str) -> str:
    """
    Decrypt OddsPortal encrypted API response using PBKDF2 (SHA-256) + AES-CBC.
    Mirrors the JS function `xNt`.
    """
    try:
        # 1. Base64 decode
        decoded_input = base64.b64decode(encrypted_input)
        decoded_str = decoded_input.decode("utf-8")
        print("Decoded string (should contain ':'):", decoded_str[:120])

        # 2. Split into l_str and IV
        l_str, u_hex = decoded_str.split(":", 1)
        iv = bytes.fromhex(u_hex)

        # 3. Key derivation (PBKDF2 + SHA-256, 1000 iterations)
        salt_bytes = salt.encode("utf-8")  # <-- treat literally, like JS TextEncoder
        print("Salt used for KDF:", salt_bytes)
        print("Password used for KDF:", password.encode("utf-8"))

        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt_bytes,
            iterations=1000,
            backend=default_backend(),
        )
        key = kdf.derive(password.encode("utf-8"))

        # 4. Decode encrypted payload
        encrypted_data = base64.b64decode(l_str)

        # 5. AES-CBC decrypt
        cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
        decryptor = cipher.decryptor()
        padded_plaintext = decryptor.update(encrypted_data) + decryptor.finalize()

        # 6. Remove PKCS7 padding
        unpadder = padding.PKCS7(128).unpadder()
        plaintext_bytes = unpadder.update(padded_plaintext) + unpadder.finalize()

        return plaintext_bytes.decode("utf-8", errors="ignore")

    except Exception as e:
        print(f"Error decrypting OddsPortal data: {type(e).__name__} - {e}")
        return ""
    

if __name__ == "__main__":
    sample_enc = "tWUknSroboMnU+a8uGaNh/IiZeICa4TURHrk3XL3s8RZHEMI8x2z/EuP76mJKBATBx4YN76swqLjAfH6BkbjnL5jl7HIa77Pf6GTzcMUnqzYW2WGkKUO4xIp"
    password = "J*8sQ!p$7aD_fR2yW@gHn*3bVp#sAdLd_k"
    salt = "3562396138663263336536643161346237633865396430663161326233633464"
    salt = "5b9a8f2c3e6d1a4b7c8e9d0f1a2b3c4d"

    print(decrypt_oddsportal(sample_enc, password, salt))

