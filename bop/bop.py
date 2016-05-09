import asyncio, aiohttp
from aiohttp import web
from sys import argv, stderr
from functools import reduce

# TODO list:
# 1. refactor solve_pp, solve_aa, solve_pa, solve_ap into different modules;
# 2. split asynchronous IO operations into a suitable granularity, to get partial search results before time expired;
# 3. comprehensive logger.

subscription_key = 'f7cc29509a8443c5b3a5e56b0e38b5a6'
bop_url = 'https://oxfordhk.azure-api.net/academic/v1.0/evaluate'

def get_intersection(b1, b2):
  return list(set(b1).intersection(set(b2)))

async def send_http_request(expr, count=None, attributes=None):
  params = {'expr': expr, 'subscription-key': subscription_key}
  if count:
    params['count'] = count
  if attributes:
    params['attributes'] = ','.join(attributes)
  with aiohttp.ClientSession() as session:
    async with session.get(bop_url, params=params) as resp:
      print(resp.url)
      return await resp.json()

class Paper(object):
  def __init__(self, id, fid, cid, jid, auid, rid):
    self.id = id
    self.fid = set(fid if fid else [])
    self.cid = set(cid if cid else [])
    self.jid = set(jid if jid else [])
    self.auid = set(auid if auid else [])
    self.rid = set(rid if rid else [])

def parse_paper_json(entity):
  id, fid, cid, jid, auid, rid = 0, None, None, None, None, None
  id = entity['Id']
  if 'F' in entity:
    fid = list(map(lambda d: d['FId'], entity['F']))
  if 'C' in entity:
    cid = [entity['C']['CId']]
  if 'J' in entity:
    jid = [entity['J']['JId']]
  if 'AA' in entity:
    auid = list(map(lambda d: d['AuId'], entity['AA']))
  if 'RId' in entity:
    rid = entity['RId']
  return Paper(id, fid, cid, jid, auid, rid)

paper_attributes = ['Id', 'F.FId', 'C.CId', 'J.JId', 'AA.AuId', 'RId']

TYPE_UNKNOWN = 0
TYPE_PAPER = 1
TYPE_AUTHOR = 2

# get the type of one id, return a pair (TYPE_XXX, Paper object if TYPE_PAPER / AA.AuId if TYPE_AUTHOR)
async def get_id_type(id):
  resp = await send_http_request('OR(Id=%d,Composite(AA.AuId=%d))' % (id, id), count=1, attributes=paper_attributes+['Ti'])
  entities = resp['entities']
  if entities:
    entity = entities[0]
    return (TYPE_PAPER, parse_paper_json(entity)) if 'Ti' in entity else (TYPE_AUTHOR, id)
  return (TYPE_UNKNOWN, None)

# fetch detailed information of papers, paids shall have a reasonable length to avoid 'Request URL Too Long'
async def fetch_papers(paids):
  expr = ''
  for paid in paids:
    tmp = 'Id=%d' % (paid)
    expr = 'OR(%s,%s)' % (expr, tmp) if expr else tmp
  resp = await send_http_request(expr, count=len(paids), attributes=paper_attributes)
  entities = resp['entities']
  if len(entities) != len(paids):
    return None
  indices = {}
  for i in range(len(paids)):
    indices[paids[i]] = i
  papers = [None] * len(paids)
  for entity in entities:
    paper = parse_paper_json(entity)
    papers[indices[paper.id]] = paper
  return papers

async def search_papers_by_rid(rid, count=10000):
  resp = await send_http_request('RId=%d' % (rid), count=count, attributes=paper_attributes)
  return list(map(parse_paper_json, resp['entities']))

# fetch papers of one author
async def search_papers_by_author(auid, count=10000):
  resp = await send_http_request('Composite(AA.AuId=%d)' % (auid), count=count, attributes=paper_attributes)
  return list(map(parse_paper_json, resp['entities']))

# TODO: this function seems not to be correct
async def search_authors_by_affiliation(afid, count=10000):
  resp = await send_http_request('Composite(AA.AfId=%d)' % (afid), count=count, attributes=paper_attributes)
  papers = list(map(lambda e: set(parse_paper_json(e).auid), resp['entities']))
  return list(reduce(lambda s1, s2: s1 | s2, papers))

async def solve_pp(paper1: Paper, paper2: Paper):
  async def solve_1hop(paper1, paper2):
    if paper2.id in paper1.rid:
      return [[paper1.id, paper2.id]]
    return []

  async def solve_2hop(paper1, paper2):
    def find(list1, list2):
      intersection = get_intersection(list1, list2)
      return list(map(lambda x: [paper1.id, x, paper2.id], intersection))

    paper2_ref = await search_papers_by_rid(paper2.id)
    paper2_refids = map(lambda paper: paper.id, paper2_ref)

    fjoint = find(paper1.fid, paper2.fid)
    cjoint = find(paper1.cid, paper2.cid)
    jjoint = find(paper1.jid, paper2.jid)
    aujoint = find(paper1.auid, paper2.auid)
    rjoint = list(map(lambda x: [paper1.id, x, paper2.id], get_intersection(paper1.rid, paper2_refids)))
    return fjoint + cjoint + jjoint + aujoint + rjoint

  # TODO: lower granularity
  return await solve_1hop(paper1, paper2) + await solve_2hop(paper1, paper2)

async def solve_aa(auid1: int, auid2: int):
  async def solve_1hop(auid1, auid2):
    return [] # don't be confused, indeed there is no possible path lol

  async def solve_2hop(auid1, auid2):
    async def search_by_paper(count=10000):
      resp = await send_http_request('AND(Composite(AA.AuId=%d),Composite(AA.AuId=%d))' % (auid1, auid2), count=count, attributes=paper_attributes)
      papers = list(map(parse_paper_json, resp['entities']))
      return list(map(lambda paper: [auid1, paper.id, auid2]), papers)

    async def search_by_affiliation(count=10000):
      return []

    path1, path2 = await asyncio.gather(search_by_paper(), search_by_affiliation())
    return path1 + path2

  # TODO: lower granularity
  return await solve_1hop(auid1, auid2) + await solve_2hop(auid1, auid2)

async def solve_ap(auid: int, paper: Paper):
  async def solve_1hop(auid, paper):
    if auid in paper.auid:
      return [[auid, paper.id]]
    return []

  async def solve_2hop(auid, paper, count=10000):
    # author -> ? (paper) -> paper
    resp = await send_http_request('AND(Composite(AA.AuId=%d),RId=%d)' % (auid, paper.id), count=count, attributes=paper_attributes)
    papers = list(map(parse_paper_json, resp['entities']))
    return list(map(lambda middle_paper: [auid, middle_paper.id, paper.id]), papers)

  # TODO: lower granularity
  return await solve_1hop(auid, paper) + await solve_2hop(auid, paper)

async def solve_pa(paper: Paper, auid: int):
  async def solve_1hop(paper, auid):
    if auid in paper.auid:
      return [[paper.id, auid]]
    return []

  async def solve_2hop(paper, auid):
    # paper -> ? (paper) -> author
    papers = await search_papers_by_author(auid)
    return [[paper.id, middle_paper.id, auid] for middle_paper in papers if middle_paper.id in paper.rid]

  # TODO: lower granularity
  return await solve_1hop(paper, auid) + await solve_2hop(paper, auid)

async def solve(id1, id2):
  (type1, obj1), (type2, obj2) = await asyncio.gather(get_id_type(id1), get_id_type(id2))
  if type1 == TYPE_PAPER and type2 == TYPE_PAPER:
    assert obj1.id == id1 and obj2.id == id2
    return await solve_pp(obj1, obj2)
  elif type1 == TYPE_AUTHOR and type2 == TYPE_AUTHOR:
    return await solve_aa(obj1, obj2)
  elif type1 == TYPE_AUTHOR and type2 == TYPE_PAPER:
    return await solve_ap(obj1, obj2)
  elif type1 == TYPE_PAPER and type2 == TYPE_AUTHOR:
    return await solve_pa(obj1, obj2)
  else:
    # TODO: TYPE_UNKNOWN encountered, log some warnings
    return []

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

# http://127.0.0.1:8080/bop?id1=2187851011&id2=1520890006
