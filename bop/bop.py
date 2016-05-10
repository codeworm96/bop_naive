import asyncio, aiohttp
import logging, time
from aiohttp import web
from sys import argv, stderr
from functools import reduce

# TODO list:
# 1. refactor solve_pp, solve_aa, solve_pa, solve_ap into different modules;
# 2. split asynchronous IO operations into a suitable granularity, to get partial search results before time expired;

start_time = time.time() 
logger = logging.getLogger(__name__)
subscription_key = 'f7cc29509a8443c5b3a5e56b0e38b5a6'
bop_url = 'https://oxfordhk.azure-api.net/academic/v1.0/evaluate'
default_count = 1000
time_limit = 300

def get_elapsed_time():
  return time.time() - start_time

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
      logger.info('sending HTTP request: %s' % resp.url)
      json = await resp.json()
      return json['entities']

class Paper(object):
  def __init__(self, id, fid, cid, jid, auid, afid, rid):
    # warning: NEVER tries to make id list a set, we require order on auid and afid
    self.id = id
    self.fid = fid if fid else []
    self.cid = cid if cid else []
    self.jid = jid if jid else []
    self.auid = auid if auid else []
    self.afid = afid if afid else [] # afid can be None if not available
    self.rid = rid if rid else []

def parse_paper_json(entity):
  id, fid, cid, jid, auid, rid = 0, None, None, None, None, None
  id = entity['Id']
  if 'F' in entity:
    fid = [d['FId'] for d in entity['F']]
  if 'C' in entity:
    cid = [entity['C']['CId']]
  if 'J' in entity:
    jid = [entity['J']['JId']]
  if 'AA' in entity:
    auid = [d['AuId'] for d in entity['AA']]
    afid = [d['AfId'] if 'AfId' in d else None for d in entity['AA']]
  if 'RId' in entity:
    rid = entity['RId']
  assert len(auid) == len(afid)
  return Paper(id, fid, cid, jid, auid, afid, rid)

paper_attributes = ['Id', 'F.FId', 'C.CId', 'J.JId', 'AA.AuId', 'AA.AfId', 'RId']

TYPE_UNKNOWN = 0
TYPE_PAPER = 1
TYPE_AUTHOR = 2

def show_type(ty):
  if ty == TYPE_PAPER:
    return 'type-paper'
  if ty == TYPE_AUTHOR:
    return 'type-author'
  return 'type-unknown'

# get the type of one id, return a pair (TYPE_XXX, Paper object if TYPE_PAPER / AA.AuId if TYPE_AUTHOR)
async def get_id_type(id):
  resp = await send_http_request('OR(Id=%d,Composite(AA.AuId=%d))' % (id, id), count=1, attributes=paper_attributes+['Ti'])
  if resp:
    entity = resp[0]
    return (TYPE_PAPER, parse_paper_json(entity)) if 'Ti' in entity else (TYPE_AUTHOR, id)
  return (TYPE_UNKNOWN, None)

# fetch detailed information of papers, paids shall have a reasonable length to avoid 'Request URL Too Long'
async def fetch_papers(paids):
  expr = ''
  for paid in paids:
    tmp = 'Id=%d' % (paid)
    expr = 'OR(%s,%s)' % (expr, tmp) if expr else tmp
  resp = await send_http_request(expr, count=len(paids), attributes=paper_attributes)
  if len(resp) != len(paids):
    return None
  indices = {}
  for i in range(len(paids)):
    indices[paids[i]] = i
  papers = [None] * len(paids)
  for entity in resp:
    paper = parse_paper_json(entity)
    papers[indices[paper.id]] = paper
  return papers

async def search_papers_by_rid(rid, count=default_count):
  resp = await send_http_request('RId=%d' % (rid), count=count, attributes=paper_attributes)
  return list(map(parse_paper_json, resp))

# fetch papermport times of one author
async def search_papers_by_author(auid, count=default_count):
  resp = await send_http_request('Composite(AA.AuId=%d)' % (auid), count=count, attributes=paper_attributes)
  return list(map(parse_paper_json, resp))

async def search_authors_by_affiliation(afid, count=default_count):
  def filter_afid(auid_list, afid_list):
    def check(x):
      au, af = x
      return af == afid
    auf_zip = list(filter(check, list(zip(auid_list, afid_list))))
    return [a for (a, b) in auf_zip]

  resp = await send_http_request('Composite(AA.AfId=%d)' % (afid), count=count, attributes=paper_attributes)
  papers = list(map(parse_paper_json, resp))
  authors = map(lambda p: filter_afid(p.auid, p.afid), papers)
  return list(reduce(lambda s1, s2: set(s1) | set(s2), authors))

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
  hop12 = await solve_1hop(paper1, paper2) + await solve_2hop(paper1, paper2)

  # TODO: the following codes are just wild codes to improve benchmarks, need refactoring
  async def search_forward_reference(rid):
    papers = await fetch_papers([rid])
    if not papers:
      return []
    result = await solve_2hop(papers[0], paper2)
    return list(map(lambda l: [paper1.id] + l, result))

  async def search_backward_reference(rid):
    papers = await fetch_papers([rid])
    if not papers:
      return []
    result = await solve_2hop(paper1, papers[0])
    return list(map(lambda l: l + [paper2.id], result))

  paper2_ref = await search_papers_by_rid(paper2.id)
  paper2_refids = map(lambda paper: paper.id, paper2_ref) # TODO: duplicate query
  tasks = list(map(search_forward_reference, paper1.rid))
  tasks += list(map(search_backward_reference, paper2_refids))
  if tasks:
    hop3_done, _ = await asyncio.wait(tasks, timeout=time_limit-get_elapsed_time())
    hop3 = list(reduce(lambda l1, l2: l1+l2, map(lambda future: future.result(), hop3_done)))
  else:
    hop3 = []
  return hop12 + hop3

async def solve_aa(auid1: int, auid2: int):
  async def solve_1hop(auid1, auid2):
    return [] # don't be confused, indeed there is no possible path lol

  async def solve_2hop(auid1, auid2):
    async def search_by_paper(count=default_count):
      resp = await send_http_request('AND(Composite(AA.AuId=%d),Composite(AA.AuId=%d))' % (auid1, auid2), count=count, attributes=paper_attributes)
      papers = list(map(parse_paper_json, resp))
      return list(map(lambda paper: [auid1, paper.id, auid2]), papers)

    async def search_by_affiliation(count=default_count):
      # TODO: really we can make it?
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

  async def solve_2hop(auid, paper, count=default_count):
    # author -> ? (paper) -> paper
    resp = await send_http_request('AND(Composite(AA.AuId=%d),RId=%d)' % (auid, paper.id), count=count, attributes=paper_attributes)
    papers = list(map(parse_paper_json, resp))
    return list(map(lambda middle_paper: [auid, middle_paper.id, paper.id], papers))

  # TODO: lower granularity
  return await solve_1hop(auid, paper) + await solve_2hop(auid, paper)

async def solve_pa(paper: Paper, auid: int):
  async def solve_1hop(paper, auid):
    if auid in paper.auid:
      return [[paper.id, auid]]
    return []

  async def solve_2hop(paper, auid):
    # paper -> ? (paper) -> author
    rid_set = set(paper.rid)
    papers = filter(lambda p: p.id in rid_set, await search_papers_by_author(auid))
    return list(map(lambda middle_paper: [paper.id, middle_paper.id, auid], papers))

  # TODO: lower granularity
  return await solve_1hop(paper, auid) + await solve_2hop(paper, auid)

async def solve(id1, id2):
  (type1, obj1), (type2, obj2) = await asyncio.gather(get_id_type(id1), get_id_type(id2))
  logger.info('solving test (%d,%s),(%d,%s)' % (id1, show_type(type1), id2, show_type(type2)))
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
    if type1 == TYPE_UNKNOWN:
      logger.warn('type-unknown found id=%d' % id1)
    if type2 == TYPE_UNKNOWN:
      logger.warn('type-unknown found id=%d' % id2)
    return []

async def worker(request):
  d = request.GET
  try:
    id1, id2 = int(d['id1']), int(d['id2'])
  except (ValueError, KeyError):
    logger.warn('invalid request \'%s\'' % request.query_string)
    return web.json_response([])
  logger.info('accepting request with id1=%d id2=%d' % (id1, id2))
  result = await solve(id1, id2)
  logger.info('%d->%d: %d path(s) found, %s' % (len(result), id1, id2, str(result)))
  return web.json_response(result)

if __name__ == '__main__':
  if len(argv) == 1:
    port = 8080
  elif len(argv) == 2:
    port = int(argv[1])
  else:
    stderr.write('usage: %s [port]\n' % (argv[0]))
    exit(0)

  logging.basicConfig(filename='bop.log',
      filemode='a',
      format='[%(asctime)s,%(msecs)d] [%(name)s] [%(levelname)s] %(message)s',
      datefmt='%H:%M:%S',
      level=logging.DEBUG)

  ### begin DEBUG section ###
  async def debug_f():
    # await fetch_papers([2166559705, 2002089154, 1679644680, 2243171526, 1632114991, 2158864412, 1597161471, 1515932031, 1558832481, 2138709157, 2100406636, 1833785989, 1520890006, 1545155892, 1578959085, 1597561788, 2160293203])
    await search_authors_by_affiliation(79576946)
  debug = False
  if debug:
    loop = asyncio.get_event_loop()
    loop.run_until_complete(debug_f())
    loop.close()
    exit(0)
  ### end DEBUG section ###

  app = web.Application()
  app.router.add_route('GET', '/bop', worker)

  logger.info('bop server has started, listening on port %d' % (port))

  web.run_app(app, port=port)
