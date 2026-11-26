"""Failure provenance, bounded retries and immutable response bytes."""
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
import urllib.error
from unittest.mock import patch

import shared_market_data as source
import source_http_evidence as evidence


class HttpEvidenceTests(unittest.TestCase):
    def test_404_preserves_raw_body_and_identity_without_repeating_failed_path(self):
        raw = b'<html>404: removed endpoint</html>'
        url = 'https://stock.naver.com/fixture-only'
        error = urllib.error.HTTPError(url, 404, 'missing', {'Content-Type': 'text/html'}, io.BytesIO(raw))
        with tempfile.TemporaryDirectory() as tmp, patch.dict(os.environ, {
            'SOURCE_HTTP_EVIDENCE_DIR': tmp, 'GITHUB_RUN_ID': '123',
            'GITHUB_RUN_ATTEMPT': '2', 'SOURCE_CODE_SHA': 'actual-checkout-fixture',
        }), patch.object(source.urllib.request, 'urlopen', side_effect=error) as fetch, patch.object(source.time, 'sleep') as wait:
            with self.assertRaisesRegex(RuntimeError, '404'):
                source.request_bytes(url)
            record = json.loads(next(Path(tmp).glob('*.json')).read_text())
            self.assertEqual((Path(tmp) / record['response_file']).read_bytes(), raw)
            self.assertEqual(record['sha256'], hashlib.sha256(raw).hexdigest())
            self.assertEqual(record['http_status'], 404)
            self.assertEqual(record['request_url'], url)
            self.assertEqual(record['code_sha'], 'actual-checkout-fixture')
            self.assertEqual(record['run_attempt'], '2')
            self.assertFalse(record['automatic_url_substitution'])
            self.assertTrue(record['complete_response_preserved'])
            fetch.assert_called_once()
            wait.assert_not_called()

    def test_truncated_body_is_not_claimed_as_complete_and_attempts_do_not_overwrite(self):
        with tempfile.TemporaryDirectory() as tmp, patch.dict(os.environ, {'SOURCE_HTTP_EVIDENCE_DIR': tmp}), patch.object(evidence, 'MAX_ERROR_BYTES', 4):
            for attempt in (1, 2):
                error = urllib.error.HTTPError('https://example.com', 503, 'busy', {}, io.BytesIO(b'abcdef'))
                record = evidence.preserve_failure('https://example.com', error, attempt)
                self.assertTrue(record['truncated'])
                self.assertFalse(record['complete_response_preserved'])
                self.assertEqual(record['size_bytes'], 4)
            self.assertEqual(len(list(Path(tmp).glob('*.json'))), 2)

    def test_timeout_is_missing_response_not_a_fabricated_404(self):
        with tempfile.TemporaryDirectory() as tmp, patch.dict(os.environ, {'SOURCE_HTTP_EVIDENCE_DIR': tmp}):
            record = evidence.preserve_failure('https://example.com', TimeoutError(), 1)
            self.assertFalse(record['response_available'])
            self.assertIsNone(record['http_status'])


if __name__ == '__main__':
    unittest.main()
