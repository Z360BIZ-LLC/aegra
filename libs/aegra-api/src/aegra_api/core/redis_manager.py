"""Redis connection manager for the event broker."""

from urllib.parse import urlparse

import redis.asyncio as aioredis
import structlog
from helpers.redis_url import redis_credentials_configured, redis_url_with_credentials

from aegra_api.settings import settings

logger = structlog.get_logger(__name__)


class RedisManager:
    """Manages Redis connection pool lifecycle.

    Follows the same pattern as DatabaseManager: a global singleton
    initialized during app lifespan and closed on shutdown.
    """

    def __init__(self) -> None:
        self._pool: aioredis.ConnectionPool | None = None
        self._client: aioredis.Redis | None = None

    async def initialize(self) -> None:
        """Create connection pool and verify connectivity."""
        if self._client is not None:
            return

        # HIPAA-024: the beta cache is moving to an ElastiCache RBAC user group.
        # REDIS_URL is the credential-free CloudFormation export, so the RBAC
        # username and password arrive separately as REDIS_SECRET and are
        # injected here. A no-op wherever REDIS_SECRET is unset -- local dev,
        # docker-compose, and beta between the credential being created and
        # the user group being attached -- which is exactly what lets this
        # deploy before the cache starts demanding it.
        authenticated_url = redis_url_with_credentials(settings.redis.REDIS_URL)

        # health_check_interval keeps pooled connections alive across long BLPOP
        # idles so the next blocking call doesn't raise on a half-closed socket.
        self._pool = aioredis.ConnectionPool.from_url(
            authenticated_url,
            max_connections=settings.redis.REDIS_MAX_CONNECTIONS,
            decode_responses=True,
            socket_keepalive=True,
            health_check_interval=30,
        )
        self._client = aioredis.Redis(connection_pool=self._pool)

        await self._client.ping()  # type: ignore[invalid-await]  # redis.asyncio stubs
        # Log only host info, not full URL which may contain credentials.
        # Parse the ORIGINAL setting, not authenticated_url -- that one now
        # carries a password. `authenticated` records which side of the
        # HIPAA-024 rollout this process is on without printing anything.
        parsed = urlparse(settings.redis.REDIS_URL)
        logger.info(
            "Redis broker initialized",
            host=parsed.hostname,
            port=parsed.port,
            authenticated=redis_credentials_configured(),
        )

    async def close(self) -> None:
        """Close Redis connection pool."""
        if self._client:
            await self._client.aclose()
            self._client = None
        if self._pool:
            await self._pool.disconnect()
            self._pool = None
        logger.info("Redis broker connections closed")

    def get_client(self) -> aioredis.Redis:
        """Return the shared async Redis client."""
        if self._client is None:
            raise RuntimeError("Redis not initialized. Set REDIS_BROKER_ENABLED=true and ensure Redis is running.")
        return self._client


# Global Redis manager instance
redis_manager = RedisManager()
