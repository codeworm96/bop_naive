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
    self.fid = fid if fid else []
    self.cid = cid if cid else []
    self.jid = jid if jid else []
    self.auid = auid if auid else []
    self.rid = rid if rid else []

def get_intersection(b1, b2):
  return list(set(b1).intersection(set(b2)))

def has_intersection(b1, b2):
  return bool(set(b1).intersection(set(b2)))

async def send_http_request(expr, count=None, attributes=None):
  params = {'expr': expr, 'subscription-key': subscription_key}
  if count:
    params['count'] = count
  if attributes:
    params['attributes'] = ','.join(attributes)
  with aiohttp.ClientSession() as session:
    async with session.get(bop_url, params=params) as resp:
      return await resp.json()

# fetch detailed information of papers
async def fetch_papers(paids):
  expr = ''
  for paid in paids:
    tmp = 'Id=%d' % (paid)
    expr = 'OR(%s,%s)' % (expr, tmp) if expr else tmp
  resp = await send_http_request(expr, count=len(paids), attributes=['Id', 'F.FId', 'C.CId', 'J.JId', 'AA.AuId', 'RId'])
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
      fid = list(map(lambda d: d['FId'], entity['F']))
    if 'C' in entity:
      cid = list(map(lambda d: d['CId'], entity['C']))
    if 'J' in entity:
      jid = list(map(lambda d: d['JId'], entity['J']))
    if 'AA' in entity:
      auid = list(map(lambda d: d['AuId'], entity['AA']))
    if 'RId' in entity:
      rid = entity['RId']
    papers[indices[id]] = Paper(id, fid, cid, jid, auid, rid)
  return papers

# TODO: fetch papers of one author
async def fetch_author(auid, count=1000):
  print(await send_http_request('Composite(AA.AuId=%d)' % (auid), count=count, attributes=['Id', 'AA.AuId', 'AA.AfId']))

def solve_1hop(paper1, paper2):
  if paper2.id in paper1.rid:
    return [[paper1.id, paper2.id]]
  return []

def solve_2hop(paper1, paper2):
  def find(list1, list2):
    intersection = get_intersection(list1, list2)
    return list(map(lambda x: [paper1.id, x, paper2.id], intersection))
  return find(paper1.fid, paper2.fid) + find(paper1.cid, paper2.cid) + find(paper1.jid, paper2.jid) + find(paper1.auid, paper2.auid)

# TODO: assert both ids are Id, not AA.AuId
async def solve(id1, id2):
  papers = await fetch_papers([id1, id2])
  if not papers or len(papers) != 2:
    return []
  paper1, paper2 = papers
  assert paper1.id == id1 and paper2.id == id2
  return solve_1hop(paper1, paper2) + solve_2hop(paper1, paper2)

async def worker(request):
  d = request.GET
  id1, id2 = int(d['id1']), int(d['id2'])
  result = await solve(id1, id2)
  return web.json_response(result)

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
