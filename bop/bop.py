import asyncio, aiohttp
from aiohttp import web
from sys import argv, stderr

subscription_key = 'f7cc29509a8443c5b3a5e56b0e38b5a6'
bop_url = 'https://oxfordhk.azure-api.net/academic/v1.0/evaluate'

# TYPE_UNKNOWN = 0
# TYPE_PAPER = 1
# TYPE_AUTHOR = 2

# # TODO: exception handle
# async def get_id_type(id):
  # with aiohttp.ClientSession() as session:
    # params = add_key({'count': 10, 
      # 'attributes': ','.join(['Id', 'AA.AuId']),
      # 'expr': 'OR(Id=%d,Composite(AA.AuId=%d))' % (id, id)})
    # async with session.get(bop_url, params=params) as resp:
      # entities = await resp.json()['entities']
      # if entities:
        # return TYPE_PAPER if entities[0]['Id'] == id else TYPE_AUTHOR
  # return TYPE_UNKNOWN

class Paper(object):
  def __init__(self, id, fid, cid, jid, auid, rid):
    self.id = id
    self.fid = list(fid) if fid else []
    self.cid = list(cid) if cid else []
    self.jid = list(jid) if jid else []
    self.auid = list(auid) if auid else []
    self.rid = list(rid) if rid else []

def has_intersection(b1, b2):
  return bool(set(b1).intersection(set(b2)))

async def http_request(expr, count=None, attributes=None):
  params = {'expr': expr, 'subscription-key': subscription_key}
  if count:
    params['count'] = count
  if attributes:
    params['attributes'] = ','.join(attributes)
  with aiohttp.ClientSession() as session:
    async with session.get(bop_url, params=params) as resp:
      return await resp.json()

# fetch a detailed information of a paper
async def fetch_paper(paid):
  paids = [paid] if isinstance(paid, int) else paid
  expr = ''
  for paid in paids:
    tmp = 'Id=%d' % (paid)
    expr = 'OR(%s,%s)' % (expr,tmp) if expr else tmp
  resp = await http_request(expr, count=len(paids), attributes=['Id', 'F.FId', 'C.CId', 'J.JId', 'AA.AuId', 'RId'])
  entities = resp['entities']
  if len(entities) != len(paids):
    return None
  indices = {}
  for i in range(len(paids)):
    indices[paids[i]] = i
  papers = [None] * len(paids)
  for entity in entities:
    id, fid, cid, jid, auid, rid = 0, None, None, None, None, None
    id = entity['Id']
    if 'F' in entity:
      fid = map(lambda d: d['FId'], entity['F'])
    if 'C' in entity:
      cid = map(lambda d: d['CId'], entity['C'])
    if 'J' in entity:
      jid = map(lambda d: d['JId'], entity['J'])
    if 'AA' in entity:
      auid = map(lambda d: d['AuId'], entity['AA'])
    if 'RId' in entity:
      rid = entity['RId']
    papers[indices[id]] = Paper(id, fid, cid, jid, auid, rid)
  return papers

# TODO: fetch papers of one author
async def fetch_author(auid, count=1000):
  print(await http_request('Composite(AA.AuId=%d)' % (auid), count=count, attributes=['Id', 'AA.AuId', 'AA.AfId']))

async def solve_1hop(id1, id2):
  # TODO: assert both ids are Id, not AA.AuId
  papers = await fetch_paper([id1, id2])
  if not papers or len(papers) != 2:
    return []
  paper1, paper2 = papers
  if has_intersection(paper1.rid, paper2.rid):
    return [[id1, id2]]
  return []

async def worker(request):
  d = request.GET
  id1, id2 = int(d['id1']), int(d['id2'])
  print(await solve_1hop(id1, id2))
  return web.Response(body=b"It works!")

if __name__ == '__main__':
  if len(argv) == 1:
    port = 8080
  elif len(argv) == 2:
    port = int(argv[1])
  else:
    stderr.write('usage: %s [port]\n' % (argv[0]))
    exit(0)
  app = web.Application()
  app.router.add_route('GET', '/bop', worker)
  web.run_app(app, port=port)

# http://127.0.0.1:8080/bop?id1=2145115012&id2=1821048088
