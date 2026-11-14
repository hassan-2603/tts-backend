"""Security, password hashing, and token encryption utilities."""
import base64
import hashlib
import secrets
from typing import Optional
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from app.config import CFG


def _get_fernet() -> Fernet:
    salt = b"tts_token_salt_constant_v1"
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=100000,
    )
    key = base64.urlsafe_b64encode(kdf.derive(CFG.TOKEN_ENCRYPTION_KEY.encode()))
    return Fernet(key)


FERNET = _get_fernet()


def encrypt_token(plain_token: Optional[str]) -> Optional[str]:
    """Encrypt sensitive broker access/refresh token for storage at rest."""
    if not plain_token:
        return None
    return FERNET.encrypt(plain_token.encode("utf-8")).decode("utf-8")


def decrypt_token(encrypted_token: Optional[str]) -> Optional[str]:
    """Decrypt sensitive broker token for in-memory use."""
    if not encrypted_token:
        return None
    try:
        return FERNET.decrypt(encrypted_token.encode("utf-8")).decode("utf-8")
    except Exception:
        # Backward compatibility if an unencrypted token was stored
        return encrypted_token


def hash_pw(pw: str, salt: Optional[str] = None) -> str:
    """PBKDF2-HMAC-SHA256 password hash matching existing TTS format."""
    salt = salt or secrets.token_hex(8)
    h = hashlib.pbkdf2_hmac("sha256", pw.encode(), salt.encode(), 120000).hex()
    return f"{salt}${h}"


def check_pw(pw: str, stored: str) -> bool:
    """Verifies a password against the stored salt$hash string."""
    if not stored or "$" not in stored:
        return False
    salt = stored.split("$", 1)[0]
    return secrets.compare_digest(hash_pw(pw, salt), stored)


def generate_session_token() -> str:
    return secrets.token_urlsafe(32)


def generate_link_nonce() -> str:
    return secrets.token_urlsafe(16)
