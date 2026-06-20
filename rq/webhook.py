from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any, Literal
from urllib.parse import urlparse
from urllib.request import Request, urlopen

if TYPE_CHECKING:
    from .job import Job

logger = logging.getLogger('rq.webhook')


@dataclass
class Webhook:
    """A pure-data description of an HTTP request to perform when a job
    reaches a terminal state (``finished`` or ``failed``).

    Unlike callbacks, webhooks are JSON-serializable and don't require an
    importable function. Send failures are logged and never raised, so an
    unreachable endpoint cannot fail the job.
    """

    url: str
    job_status: Literal['finished', 'failed']
    method: Literal['GET', 'POST']
    headers: dict[str, str] | None
    timeout: int

    def __init__(
        self,
        url: str,
        job_status: Literal['finished', 'failed'],
        *,
        method: Literal['GET', 'POST'] = 'GET',
        headers: dict[str, str] | None = None,
        timeout: int = 10,
    ) -> None:
        parsed = urlparse(url) if isinstance(url, str) else None
        if parsed is None or parsed.scheme not in ('http', 'https') or not parsed.netloc:
            raise ValueError(f'url must be an http:// or https:// URL, got {url!r}')

        if job_status not in ('finished', 'failed'):
            raise ValueError(f"job_status must be 'finished' or 'failed', got {job_status!r}")

        if method not in ('GET', 'POST'):
            raise ValueError(f"method must be 'GET' or 'POST', got {method!r}")

        if headers and not isinstance(headers, dict):
            raise TypeError(f'headers must be a dict or None, got {type(headers).__name__}')

        if not isinstance(timeout, int) or timeout <= 0:
            raise ValueError(f'timeout must be a positive integer, got {timeout!r}')

        self.url = url
        self.job_status = job_status
        self.method = method
        self.headers = headers
        self.timeout = timeout

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Webhook:
        return cls(
            data['url'],
            data['job_status'],
            method=data.get('method', 'GET'),
            headers=data.get('headers'),
            timeout=data.get('timeout', 10),
        )

    def get_payload(self, job: Job, *, exc_string: str | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {
            'job_id': job.id,
            'func_name': job.func_name,
            'status': self.job_status,
            'enqueued_at': job.enqueued_at.isoformat() if job.enqueued_at else None,
            'ended_at': job.ended_at.isoformat() if job.ended_at else None,
        }
        if self.job_status == 'failed':
            payload['exc_info'] = exc_string
        return payload

    def send(self, job: Job, *, exc_string: str | None = None) -> None:
        """Performs the HTTP request. Errors are logged, never raised."""
        try:
            if self.method == 'GET':
                request = Request(self.url, headers=self.headers or {}, method='GET')
            else:
                body = json.dumps(self.get_payload(job, exc_string=exc_string)).encode('utf-8')
                headers = {'Content-Type': 'application/json', **(self.headers or {})}
                request = Request(self.url, data=body, headers=headers, method='POST')
            with urlopen(request, timeout=self.timeout):
                pass
        except Exception:
            logger.warning('Failed to send webhook to %s for job %s', self.url, job.id, exc_info=True)
