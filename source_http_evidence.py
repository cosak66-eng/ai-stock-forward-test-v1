"""Preserve public-source failures for reviewed repair; never choose a new URL."""
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import urllib.error
import uuid
from zoneinfo import ZoneInfo

MAX_ERROR_BYTES = 1024 * 1024


def preserve_failure(url, error, attempt):
    directory = os.environ.get('SOURCE_HTTP_EVIDENCE_DIR')
    if not directory:
        return None
    root = Path(directory)
    root.mkdir(parents=True, exist_ok=True)
    identity = uuid.uuid4().hex
    record = {
        'request_url': url, 'observed_at_kst': datetime.now(ZoneInfo('Asia/Seoul')).isoformat(),
        'request_attempt': attempt, 'run_id': os.environ.get('GITHUB_RUN_ID'),
        'run_attempt': os.environ.get('GITHUB_RUN_ATTEMPT'),
        'code_sha': os.environ.get('SOURCE_CODE_SHA') or os.environ.get('GITHUB_SHA'),
        'error_type': type(error).__name__, 'http_status': getattr(error, 'code', None),
        'response_available': False, 'automatic_url_substitution': False,
        'recovery_action': 'PRESERVE_AND_REVIEW_KRX_EQUIVALENT_ROUTE',
    }
    if isinstance(error, urllib.error.HTTPError):
        # Do not persist request credentials, cookies, or authorization headers.
        record['content_type'] = error.headers.get('Content-Type') if error.headers else None
        try:
            body = error.read(MAX_ERROR_BYTES + 1)
            truncated = len(body) > MAX_ERROR_BYTES
            body = body[:MAX_ERROR_BYTES]
            path = root / (identity + '.response.bin')
            with path.open('xb') as stream:
                stream.write(body)
            record.update(response_available=True, response_file=path.name,
                          size_bytes=len(body), sha256=hashlib.sha256(body).hexdigest(),
                          truncated=truncated, complete_response_preserved=not truncated)
        except (OSError, ValueError) as exc:
            record['response_read_error'] = type(exc).__name__
    with (root / (identity + '.json')).open('x', encoding='utf-8') as stream:
        json.dump(record, stream, ensure_ascii=False, indent=2)
        stream.write('\n')
    return record
