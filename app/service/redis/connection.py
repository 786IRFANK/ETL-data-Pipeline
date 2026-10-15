import ssl as ssl_module
import redis
from app.config.settings import settings

_redis_client = None


def get_redis_client():
    """
    Get Redis client with SSL support for GCP Memorystore.

    SSL is enabled when port 6378 is detected (Memorystore with TLS).
    This matches the backend's redis_cache_service.py approach.
    """
    global _redis_client

    if _redis_client is None:
        redis_host = settings.REDIS_HOST or "localhost"

        # Ensure port is an integer for SSL detection logic
        try:
            redis_port = int(settings.REDIS_PORT) if settings.REDIS_PORT else 6379
        except (ValueError, TypeError):
            redis_port = 6379

        redis_password = getattr(settings, 'REDIS_PASSWORD', None)

        try:
            redis_db = int(getattr(settings, 'REDIS_DB', 0))
        except (ValueError, TypeError):
            redis_db = 0

        # Detect if using Memorystore with in-transit encryption (port 6378)
        use_ssl = redis_port == 6378

        connection_params = {
            'host': redis_host,
            'port': redis_port,
            'db': redis_db,
            'password': redis_password if redis_password else None,
            'decode_responses': True,
        }

        if use_ssl:
            # GCP Memorystore with in-transit encryption requires SSL
            # Using ssl_cert_reqs=CERT_NONE to skip certificate verification
            # since GCP Memorystore uses self-signed certificates
            connection_params['ssl'] = True
            connection_params['ssl_cert_reqs'] = ssl_module.CERT_NONE
            connection_params['ssl_check_hostname'] = False

        _redis_client = redis.Redis(**connection_params)

    return _redis_client