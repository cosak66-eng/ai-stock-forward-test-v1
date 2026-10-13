import socket
import ssl
import unittest
import urllib.error
from unittest.mock import patch
import environment_probe as m

class ProbeTests(unittest.TestCase):
 def test_dns_error(self):
  with patch.object(m.socket,'getaddrinfo',side_effect=socket.gaierror(-3,'DNS failed')):
   x=m.probe('naver_market','https://example.com/api')
  self.assertEqual(x['status'],'DNS_ERROR')
 def test_http_error(self):
  with patch.object(m.socket,'getaddrinfo',return_value=[('x',)]),patch.object(m.urllib.request,'urlopen',side_effect=urllib.error.HTTPError('x',403,'Forbidden',{},None)):
   x=m.probe('daum_daily','https://example.com/api')
  self.assertEqual(x['status'],'HTTP_ERROR');self.assertEqual(x['http_status'],403)
 def test_tls_error(self):
  self.assertEqual(m.classify_error(urllib.error.URLError(ssl.SSLError('cert'))),'TLS_ERROR')
 def test_timeout(self):
  self.assertEqual(m.classify_error(urllib.error.URLError(socket.timeout('timed out'))),'TIMEOUT')
 def test_no_false_success(self):
  def fake(name,url):return {'status':'JSON_OK','regular_session_proven':False}
  with patch.object(m,'package_status',return_value={x:{'status':'INSTALLED'} for x in m.PACKAGES}):r=m.build_report(fake)
  self.assertFalse(r['data_verified']);self.assertFalse(r['prediction_eligible']);self.assertEqual(r['http_reachable'],4)
 def test_correct_fdr_distribution(self):
  self.assertEqual(m.PACKAGES['FinanceDataReader'],'finance-datareader')

if __name__=='__main__':unittest.main()
