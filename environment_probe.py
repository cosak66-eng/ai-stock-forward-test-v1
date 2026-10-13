#!/usr/bin/env python3
"""Read-only V1.0 connectivity diagnostic. Never generates predictions."""
import argparse
import importlib.metadata
import json
import os
import socket
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

PACKAGES = {'requests':'requests','pandas':'pandas','pykrx':'pykrx','FinanceDataReader':'finance-datareader','yfinance':'yfinance'}
TARGETS = {
 'naver_market': 'https://stock.naver.com/api/domestic/market/stock/default?tradeType=KRX&marketType=ALL&orderType=marketSum&startIdx=0&pageSize=1',
 'daum_daily': 'https://finance.daum.net/api/quote/A005930/days?symbolCode=A005930&page=1&perPage=2',
 'yahoo_daily': 'https://query1.finance.yahoo.com/v8/finance/chart/005930.KS?range=5d&interval=1d',
 'krx_portal': 'https://data.krx.co.kr/'
}

def package_status():
 out={}
 for module,dist in PACKAGES.items():
  try: out[module]={'status':'INSTALLED','version':importlib.metadata.version(dist)}
  except importlib.metadata.PackageNotFoundError: out[module]={'status':'MISSING','distribution':dist}
 return out

def classify_error(err):
 if isinstance(err, urllib.error.HTTPError):return 'HTTP_ERROR'
 if isinstance(err, ssl.SSLError):return 'TLS_ERROR'
 if isinstance(err, socket.gaierror):return 'DNS_ERROR'
 if isinstance(err, (TimeoutError,socket.timeout)):return 'TIMEOUT'
 if isinstance(err, urllib.error.URLError):
  r=err.reason
  if isinstance(r,socket.gaierror):return 'DNS_ERROR'
  if isinstance(r,ssl.SSLError):return 'TLS_ERROR'
  if isinstance(r,(TimeoutError,socket.timeout)):return 'TIMEOUT'
 return 'NETWORK_ERROR'

def probe(label,url,timeout=10):
 host=urllib.parse.urlparse(url).hostname
 r={'host':host,'url':url,'dns':'NOT_CHECKED','http_status':None,'status':'UNKNOWN','records_found':0,'regular_session_proven':False}
 try:
  socket.getaddrinfo(host,443,type=socket.SOCK_STREAM)
  r['dns']='OK'
 except OSError as e:
  r.update(dns='FAILED',status='DNS_ERROR',error=str(e)[:220]);return r
 headers={'User-Agent':'Mozilla/5.0','Accept':'application/json,text/html;q=0.8,*/*;q=0.5'}
 if label=='daum_daily':headers['Referer']='https://finance.daum.net/'
 if label=='naver_market':headers['Referer']='https://stock.naver.com/'
 try:
  with urllib.request.urlopen(urllib.request.Request(url,headers=headers),timeout=timeout) as resp:
   r['http_status']=resp.status
   ctype=resp.headers.get('Content-Type','')
   body=resp.read(262144)
   r['content_type']=ctype
   r['bytes_received']=len(body)
   if 'json' in ctype.lower() or body.lstrip().startswith((b'{',b'[')):
    try:
     obj=json.loads(body)
     r['json_type']=type(obj).__name__
     r['root_keys']=list(obj.keys())[:20] if isinstance(obj,dict) else []
     if isinstance(obj,dict):
      for k in ('data','items','stocks','result','output'):
       v=obj.get(k)
       if isinstance(v,list):r['records_found']=len(v);break
       if isinstance(v,dict):
        for inner in ('items','data','list','stocks'):
         if isinstance(v.get(inner),list):r['records_found']=len(v[inner]);break
     elif isinstance(obj,list):r['records_found']=len(obj)
     r['status']='JSON_OK' if r['records_found'] else 'JSON_NO_LIST_ROWS'
    except (ValueError,UnicodeDecodeError) as e:r.update(status='JSON_INVALID',error=str(e)[:220])
   else:r['status']='HTML_OK' if body else 'EMPTY_RESPONSE'
 except Exception as e:
  r['status']=classify_error(e);r['error']=str(e)[:220]
  if isinstance(e,urllib.error.HTTPError):r['http_status']=e.code
 return r

def build_report(prober=probe):
 packages=package_status()
 sites={k:prober(k,v) for k,v in TARGETS.items()}
 return {'schema':'V1_ENV_PROBE_2','timestamp_utc':datetime.now(timezone.utc).isoformat(),
  'python':sys.version.split()[0], 'packages':packages,'sites':sites,
  'dependency_pass':all(x['status']=='INSTALLED' for x in packages.values()),
  'http_reachable':sum(x['status'] in ('JSON_OK','JSON_NO_LIST_ROWS','HTML_OK') for x in sites.values()),
  'data_verified':False,'prediction_eligible':False,
  'warning':'Connectivity alone does not establish KRX regular-session data or cross-source consistency.'}

def main():
 p=argparse.ArgumentParser();p.add_argument('--output',default='output/environment');p.add_argument('--strict',action='store_true');a=p.parse_args()
 report=build_report();out=Path(a.output);out.mkdir(parents=True,exist_ok=True)
 path=out/'environment_probe.json';path.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
 print('ENV_PROBE',json.dumps({'dependency_pass':report['dependency_pass'],'http_reachable':report['http_reachable'],'sites':{k:v['status'] for k,v in report['sites'].items()}},ensure_ascii=False))
 print('REPORT_PATH',path)
 if a.strict and (not report['dependency_pass'] or report['http_reachable']<2):return 2
 return 0

if __name__=='__main__':raise SystemExit(main())
