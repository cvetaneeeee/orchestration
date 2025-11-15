import json
import base64
import binascii
from cryptography.hazmat.primitives import hashes, padding
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend

# in m(r.data, "%RtR8AB&nWsh=AQC+v!=pgAe@dSQG3kQ", "orieC_jQQWRmhkPvR6u2kzXeTube6aYupiOddsPortal") in app.js?v=version;
Qwt = '5b9a8f2c3e6d1a4b7c8e9d0f1a2b3c4d'
# Uwt = 'BDOU99-h67HcA6JeFXHbSNMu7e2yNNu3RzoMj8TM4W88jITfq7ZmPvIM1Iv-4_l2LxQcYwhqby2xGpWwzjfAnG4'
Uwt = "J*8sQ!p$7aD_fR2yW@gHn*3bVp#sAdLd_k"
# ODDSPORTAL_PASSWORD = '%RtR8AB&nWsh=AQC+v!=pgAe@dSQG3kQ'
# ODDSPORTAL_SALT = 'orieC_jQQWRmhkPvR6u2kzXeTube6aYupiOddsPortal'
ODDSPORTAL_PASSWORD = Uwt
ODDSPORTAL_SALT = Qwt

def decrypt_data(encrypted_input, oddsportal_password=ODDSPORTAL_PASSWORD, oddsportal_salt=ODDSPORTAL_SALT):
    try:
        # Step 1: Base64 decode the input string
        def fix_base64_padding(b64_string):
            while len(b64_string) % 4 != 0:
                b64_string += '='
            return b64_string
        
        def base64_decode_with_padding(data: str) -> bytes:
            """
            Decodes a Base64 string, adding padding if necessary.
            """
            # Add padding to make the length a multiple of 4
            missing_padding = len(data) % 4
            if missing_padding:
                data += "=" * (4 - missing_padding)
            try:
                return base64.b64decode(data)
            except Exception as e:
                raise ValueError(f"Base64 decoding failed: {e}")

        # Fix padding and decode
        decoded_input = base64.b64decode(fix_base64_padding(encrypted_input))
        # decoded_input = base64_decode_with_padding(encrypted_input)

        # Convert bytes to string for splitting
        decoded_str = decoded_input.decode('utf-8')

        # Step 2: Split the decoded string into 'l' and 'u'
        l_str, u_hex = decoded_str.split(':')

        # Step 3: Process the IV from hex to bytes
        iv = bytes.fromhex(u_hex)

        # Step 4: Prepare the oddsportal_password and oddsportal_salt
        password_bytes = oddsportal_password.encode('utf-8')
        salt_bytes = oddsportal_salt.encode('utf-8')

        # Step 5: Key derivation using PBKDF2 with SHA-256
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,  # 256 bits
            salt=salt_bytes,
            iterations=1000,
            backend=default_backend()
        )
        key = kdf.derive(password_bytes)

        # Step 6: Decrypt the data
        # Base64 decode 'l' to get the encrypted data
        encrypted_data = base64.b64decode(fix_base64_padding(l_str))

        # Initialize cipher
        cipher = Cipher(
            algorithms.AES(key),
            modes.CBC(iv),
            backend=default_backend()
        )
        decryptor = cipher.decryptor()

        # Decrypt and unpad the data
        padded_plaintext = decryptor.update(encrypted_data) + decryptor.finalize()

        # AES-CBC requires PKCS7 padding
        # unpadder = padding.PKCS7(algorithms.AES.block_size).unpadder()
        unpadder = padding.PKCS7(128).unpadder()
        plaintext_bytes = unpadder.update(padded_plaintext) + unpadder.finalize()

        # Step 7: Decode the decrypted data to a string
        plaintext = plaintext_bytes.decode('utf-8')
        return plaintext

    except (ValueError, binascii.Error) as e:
        raise ValueError(f"Decryption failed: {e}")


if __name__ == '__main__':
    with open('test.csv', 'r') as f:
        file_content = f.read()

    data = decrypt_data(encrypted_input=file_content)
    print(json.dumps(data, indent=4))