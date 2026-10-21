import asyncio
import ipaddress
import socket

import httpx

from app.domain.exceptions import WebhookDeliveryError
from app.schemas.events import WebhookNotification

RETRYABLE_STATUSES = frozenset({408, 429})


class WebhookClient:
    def __init__(
        self, client: httpx.AsyncClient, timeout_seconds: float, *, allow_private_networks: bool
    ) -> None:
        self._client = client
        self._timeout = timeout_seconds
        self._allow_private_networks = allow_private_networks

    async def send(self, url: str, notification: WebhookNotification) -> None:
        body = notification.model_dump(mode="json")
        try:
            async with asyncio.timeout(self._timeout):
                if not self._allow_private_networks:
                    # raw_host is the punycode name httpx connects to; .host is decoded and may
                    # resolve elsewhere (IDNA 2003 maps "ß" to "ss")
                    await ensure_public_host(httpx.URL(url).raw_host.decode("ascii"))
                async with self._client.stream("POST", url, json=body) as response:
                    # only the status matters, so the receiver's body is never downloaded
                    status_code = response.status_code
        except (httpx.HTTPError, TimeoutError) as exc:
            raise WebhookDeliveryError(f"{type(exc).__name__}: {exc}") from exc
        if httpx.codes.is_success(status_code):
            return
        retryable = status_code >= 500 or status_code in RETRYABLE_STATUSES
        raise WebhookDeliveryError(f"receiver answered {status_code}", permanent=not retryable)


def is_private_literal(host: str) -> bool:
    """Cheap check before accepting a URL; DNS names are verified after resolution on delivery."""
    if host == "localhost" or host.endswith(".localhost"):
        return True
    try:
        return not ipaddress.ip_address(host.strip("[]")).is_global
    except ValueError:
        return False


async def ensure_public_host(host: str) -> None:
    """Refuse hosts that resolve into private networks (SSRF into the service's own network)."""
    try:
        infos = await asyncio.get_running_loop().getaddrinfo(host, None, type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        raise WebhookDeliveryError(f"cannot resolve {host}: {exc}") from exc
    for *_, sockaddr in infos:
        address = ipaddress.ip_address(str(sockaddr[0]).split("%", 1)[0])
        if not address.is_global:
            raise WebhookDeliveryError(f"{host} resolves to non-public {address}", permanent=True)
