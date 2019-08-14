"""Provides data security functions
"""

import os
import logging
import math
import base64
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend

# The names of the environment variables associated with encryption
PIPELINE_KEY_NAME = "PIPELINE_KEY"

# Attempt to get the encryption envrionment variables
CRYPT_KEY = os.getenv(PIPELINE_KEY_NAME)

# Lengths of encryption key, iv (initialization vector), and other required sizes
CRYPT_KEY_BYTE_LEN = (256 / 8)      # 256 bits length
CRYPT_IV_BYTE_LEN = (16)            # We are using AES -> block size is 16
CRYPT_SOURCE_BLOCK_SIZE = (16)      # Reesize the source block to a multiple of this value; zero for no change

def _perform_encrypt(plain_text):
    """Encrypts the plain text using environmental variables
    Args:
        plain_text(str): the string to encrypt
    Returns:
        Returns a tuple of the encrypted string, the length of the plain text string, and the IV. If
        a problem occurs then a tuple of (None, 0, None) is returned
    Notes:
        Will log warnings and return None if the PIPELINE_KEY or PIPELINE_IV environment
        variables haven't been set
    """
    global CRYPT_KEY   # pylint: disable=global-statement

    if not CRYPT_KEY:
        logging.warning("Missing %s environment variable needed for securing data", PIPELINE_KEY_NAME)
        return (None, 0, None)

    # Setup the encryption variables
    crypt_iv = os.urandom(CRYPT_IV_BYTE_LEN)
    size_the_str = lambda source, length: source if len(source) == length else source[:length] if len(source) > length \
                            else '{str:{fill}{align}{width}}'.format(str=source, fill=' ', align='<', width=length)
    key = size_the_str(CRYPT_KEY, CRYPT_KEY_BYTE_LEN)

    # See if we need to resize the source string
    encrypt_text = plain_text
    original_plain_len = len(plain_text)
    if CRYPT_SOURCE_BLOCK_SIZE:
        plain_block_count = math.trunc(float(original_plain_len + CRYPT_SOURCE_BLOCK_SIZE - 1.0) / \
                                                                                        float(CRYPT_SOURCE_BLOCK_SIZE))
        if plain_block_count * CRYPT_SOURCE_BLOCK_SIZE > original_plain_len:
            encrypt_text = size_the_str(encrypt_text, plain_block_count * CRYPT_SOURCE_BLOCK_SIZE)

    # Perform the encryption
    backend = default_backend()
    cipher = Cipher(algorithms.AES(key), modes.CBC(crypt_iv), backend=backend)
    encryptor = cipher.encryptor()
    result = encryptor.update(encrypt_text.encode()) + encryptor.finalize()

    return (result, original_plain_len, crypt_iv)

def _perform_decrypt(encrypted, plain_len, crypt_iv):
    """Decrypts an encrypted string.
    Args:
        encrypted(str): the encrypted string
        plain_len(int): the length of the recovered string (in cases where padding ocurred)
        crypt_iv(str): the iv (initialization vector) of the encryption
    Return:
        The un-encrypted string or None if a problem is found
    Note:
        No checks are made to the suitability of the parameters for decryption. For example,
        the iv parameter is not checked to see if it's a valid size
    """
    global CRYPT_KEY   # pylint: disable=global-statement

    if not CRYPT_KEY:
        logging.warning("Missing %s environment variable needed for securing data", PIPELINE_KEY_NAME)
        return (None, 0, None)

    # Make sure the key is the right size
    size_the_str = lambda source, length: source if len(source) == length else source[:length] if len(source) > length \
                                else '{str:{fill}{align}{width}}'.format(str=source, fill=' ', align='<', width=length)
    key = size_the_str(CRYPT_KEY, CRYPT_KEY_BYTE_LEN)

    # Perform the decryption
    backend = default_backend()
    cipher = Cipher(algorithms.AES(key), modes.CBC(crypt_iv), backend=backend)
    decryptor = cipher.decryptor()
    plain_text = decryptor.update(encrypted) + decryptor.finalize()

    recovered_len = len(plain_text)
    return plain_text if recovered_len <= plain_len else plain_text[:plain_len]

def _package_encrypted(encrypted, plain_len, crypt_iv):
    """Packages up the parameters into a base64 encoded string
    Args:
        encrypted(str): an string representing encrypted data
        plain_len(int): the length of the plain text data
        crypt_iv(str): the iv (initialization vector) encryption value
    Return:
        A base64 string representing the data.
    Note:
        The strings are concatenated as 6 characters each for the length
        of the encrypted parameter and plain_len, followed by the encrypted
        parameter contents, and finally the iv value. The concatenated string
        is then base64 encoded and returned
    """
    len_str = '{0:06d}'.format(plain_len)
    encrypt_len = '{0:06d}'.format(len(encrypted))

    return base64.b64encode(encrypt_len + len_str + encrypted + crypt_iv)

def _unpackage_encoded(encoded):
    """The encoded string fron _package_encrypted()
    Args:
        encoded(str): the base64 encoded string to take apart
    Return:
        Returns the tuple of encoded string, plain text length, and the iv
    """
    base_str = base64.b64decode(encoded)

    encrypt_len = int(base_str[0:6])
    plain_len = int(base_str[6:12])
    encrypted = base_str[12:encrypt_len+12]
    crypt_iv = base_str[encrypt_len+12:]

    return (encrypted, plain_len, crypt_iv)

def encrypt_pipeline_string(plain_text):
    """Fixes the metadata so that the drone pipeline easily reference it
    Args:
        plain_text(str): the string to secure
    Returns:
        The secured string or None if the string can't be secured
    """
    try:
        encrypted, orig_len, crypt_iv = _perform_encrypt(plain_text)
        if not encrypted is None:
            return _package_encrypted(encrypted, orig_len, crypt_iv)
    except Exception as ex:
        logging.warning("Exception caught while encrypting string: %s", str(ex))

    return None

def decrypt_pipeline_string(crypt_str):
    """Recovers a string that has been secured.
    Args:
        crypt_str(str): the string containing information to recover
    Return:
        The decrypted string when successful and None when not
    """
    return_str = None

    try:
        # Decode the base64 string and get the parts
        encrypted, plain_len, crypt_iv = _unpackage_encoded(crypt_str)

        # Decrypt the string
        plain_str = _perform_decrypt(encrypted, plain_len, crypt_iv)
        plain_str_len = len(plain_str)

        # Make sure we have what is expected
        if plain_str_len == plain_len:
            return_str = plain_str
        else:
            logging.warning("Decrypting string failed - recovered plain text has the wrong size: >> %s <<",
                            str(crypt_str))
    except Exception as ex:
        logging.warning("Exception caught decrypting string: %s", str(ex))

    return return_str
