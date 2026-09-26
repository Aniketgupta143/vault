"""
API Key authentication middleware and token verification.
"""
import hashlib
import hmac
from typing import Optional
from fastapi import Request, HTTPException, Security, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

import config

security = HTTPBearer(auto_error=False)

def hash_token(token: str) -> str:
    """Return SHA-256 hash of API token."""
    return hashlib.sha256(token.encode()).hexdigest()

def is_valid_token(provided_token: Optional[str]) -> bool:
    """Check if provided token matches default API key."""
    if not config.AUTH_ENABLED:
        return True
    if not provided_token:
        return False
    expected_hash = hash_token(config.DEFAULT_API_KEY)
    provided_hash = hash_token(provided_token)
    return hmac.compare_digest(expected_hash, provided_hash)

async def require_auth(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Security(security)
) -> str:
    """
    FastAPI dependency enforcing API Key Bearer authentication.
    Bypasses when config.AUTH_ENABLED is False or for static assets/dashboard.
    """
    if not config.AUTH_ENABLED:
        return "anonymous"

    # Allow query parameter token for WebSocket or direct browser links
    token = None
    if credentials:
        token = credentials.credentials
    elif request and "api_key" in request.query_params:
        token = request.query_params["api_key"]

    if not token or not is_valid_token(token):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API Key. Provide 'Authorization: Bearer <key>'.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return token
