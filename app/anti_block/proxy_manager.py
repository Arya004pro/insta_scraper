from __future__ import annotations

from dataclasses import dataclass
from itertools import cycle

from app.core.config import ProxyConfig, Settings


@dataclass
class ActiveProxy:
    proxy_id: str
    server: str
    username: str | None = None
    password: str | None = None

    def as_playwright_proxy(self) -> dict[str, str]:
        payload = {"server": self.server}
        if self.username:
            payload["username"] = self.username
        if self.password:
            payload["password"] = self.password
        return payload


class ProxyManager:
    def __init__(self, settings: Settings):
        self._rotation_every_n = max(1, settings.proxy_rotation_every_n_requests)
        self._requests = 0
        self._proxies = [ActiveProxy(p.proxy_id, p.server, p.username, p.password) for p in settings.proxies]
        self._iter = cycle(self._proxies) if self._proxies else None
        self._active = next(self._iter) if self._iter else None

    @property
    def active(self) -> ActiveProxy | None:
        return self._active

    def current_proxy_id(self) -> str | None:
        return self._active.proxy_id if self._active else None

    def mark_request(self) -> ActiveProxy | None:
        self._requests += 1
        if self._iter and self._requests % self._rotation_every_n == 0:
            self._active = next(self._iter)
        return self._active

    def rotate_now(self) -> ActiveProxy | None:
        if self._iter:
            self._active = next(self._iter)
        return self._active

