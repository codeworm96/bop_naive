import asyncio, aiohttp
import logging, time
from aiohttp import web
from sys import argv, stderr
from functools import reduce

start_time = 0
logger = logging.getLogger(__name__)
default_count = 1000
time_limit = 300 # TODO: 300 is not a suitable value, see how score is evaluated

def set_start_time():
  global start_time
  start_time = time.time()

def get_elapsed_time():
  return time.time() - start_time

def get_intersection(b1, b2):
  return list(set(b1).intersection(set(b2)))

def get_union(b1, b2):
  return list(set(b1) | set(b2))

def make_unique(l):
  return list(set(l))

async def send_http_request(expr, count=None, attributes=None):
  subscription_key = 'f7cc29509a8443c5b3a5e56b0e38b5a6'
  bop_url = 'https://oxfordhk.azure-api.net/academic/v1.0/evaluate'
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
    self.afid = afid if afid else [] # element of afid can be None if not available
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

PAPER_ATTR = ('Id', 'F.FId', 'C.CId', 'J.JId', 'AA.AuId', 'AA.AfId', 'RId')
TYPE_UNKNOWN, TYPE_PAPER, TYPE_AUTHOR = 0, 1, 2

def show_type(ty):
  if ty == TYPE_PAPER:
    return 'type-paper'
  if ty == TYPE_AUTHOR:
    return 'type-author'
  return 'type-unknown'

# get the type of one id, return a pair (TYPE_XXX, Paper object if TYPE_PAPER / AA.AuId if TYPE_AUTHOR)
async def get_id_type(id):
  resp = await send_http_request('OR(Id=%d,Composite(AA.AuId=%d))' % (id, id), count=1, attributes=PAPER_ATTR+('Ti',))
  if resp:
    entity = resp[0]
    return (TYPE_PAPER, parse_paper_json(entity)) if 'Ti' in entity else (TYPE_AUTHOR, id)
  return (TYPE_UNKNOWN, None)

# fetch information of papers, paids shall have a reasonable length to avoid HTTP error 'Request URL Too Long'
async def fetch_papers(paids):
  expr = ''
  for paid in paids:
    tmp = 'Id=%d' % (paid)
    expr = 'OR(%s,%s)' % (expr, tmp) if expr else tmp
  resp = await send_http_request(expr, count=len(paids), attributes=PAPER_ATTR)
  if len(resp) != len(paids):
    logger.error('fetched incomplete paper list of %s' % (str(paids)))
    return None
  indices = {}
  for i in range(len(paids)):
    indices[paids[i]] = i
  papers = [None] * len(paids)
  for entity in resp:
    paper = parse_paper_json(entity)
    papers[indices[paper.id]] = paper
  return papers

# search papers which references this paper
async def search_papers_by_ref(id, count=default_count):
  resp = await send_http_request('RId=%d' % (id), count=count, attributes=PAPER_ATTR)
  return list(map(parse_paper_json, resp))

# search papers of this author
async def search_papers_by_author(auid, count=default_count):
  resp = await send_http_request('Composite(AA.AuId=%d)' % (auid), count=count, attributes=PAPER_ATTR)
  return list(map(parse_paper_json, resp))

# search authors which is attached to this affiliation
async def search_authors_by_affiliation(afid, count=default_count):
  def filter_by_afid(auid_list, afid_list):
    def check(x):
      return x[1] == afid
    auf_zip = list(filter(check, list(zip(auid_list, afid_list))))
    return [a for (a, b) in auf_zip]

  resp = await send_http_request('Composite(AA.AfId=%d)' % (afid), count=count, attributes=PAPER_ATTR)
  papers = list(map(parse_paper_json, resp))
  authors = map(lambda p: filter_by_afid(p.auid, p.afid), papers)
  return list(reduce(get_union, authors, []))

# search affiliations which the author attaches to
async def search_affiliations_by_author(auid, count=default_count):
  def filter_by_auid(auid_list, afid_list):
    def check(x):
      return x[0] == auid and x[1]
    auf_zip = list(filter(check, list(zip(auid_list, afid_list))))
    return [b for (a, b) in auf_zip]

  papers = await search_papers_by_author(auid, count=count)
  affiliations = list(map(lambda p: filter_by_auid(p.auid, p.afid), papers))
  return list(reduce(get_union, affiliations, []))

class pp_solver(object):
  @staticmethod
  async def solve_1hop(paper1: Paper, paper2: Paper):
    if paper2.id in paper1.rid:
      return [[paper1.id, paper2.id]]
    return []

  @staticmethod
  async def solve_2hop(paper1: Paper, paper2: Paper, paper2_refids=None):
    def find_joint(list1, list2):
      intersection = get_intersection(list1, list2)
      return list(map(lambda x: [paper1.id, x, paper2.id], intersection))

    if not paper2_refids:
      paper2_refids = map(lambda p: p.id, await search_papers_by_ref(paper2.id))
    return reduce(lambda a, b: a + b, [find_joint(paper1.fid, paper2.fid),
      find_joint(paper1.cid, paper2.cid),
      find_joint(paper1.jid, paper2.jid),
      find_joint(paper1.auid, paper2.auid),
      find_joint(paper1.rid, paper2_refids)])

  @staticmethod
  async def solve(paper1: Paper, paper2: Paper):
    async def search_forward_reference(rid):
      papers = await fetch_papers([rid])
      if not papers:
        return []
      result = await pp_solver.solve_2hop(papers[0], paper2)
      return list(map(lambda l: [paper1.id] + l, result))

    async def search_backward_reference(rid):
      papers = await fetch_papers([rid])
      if not papers:
        return []
      result = await pp_solver.solve_2hop(paper1, papers[0])
      return list(map(lambda l: l + [paper2.id], result))

    paper2_refids = map(lambda p: p.id, await search_papers_by_ref(paper2.id))

    tasks  = [pp_solver.solve_1hop(paper1, paper2), pp_solver.solve_2hop(paper1, paper2, paper2_refids)]
    tasks += list(map(search_forward_reference, paper1.rid))
    tasks += list(map(search_backward_reference, paper2_refids))
    return tasks

class aa_solver(object):
  @staticmethod
  async def solve_1hop(auid1: int, auid2: int):
    return [] # don't be confused, indeed there is no possible path lol

  @staticmethod
  async def solve_2hop(auid1: int, auid2: int):
    async def search_by_paper(count=default_count):
      resp = await send_http_request('AND(Composite(AA.AuId=%d),Composite(AA.AuId=%d))' % (auid1, auid2), count=count, attributes=PAPER_ATTR)
      papers = list(map(parse_paper_json, resp))
      return list(map(lambda p: [auid1, p.id, auid2]), papers)

    async def search_by_affiliation(count=default_count):
      aff1 = await search_affiliations_by_author(auid1)
      aff2 = await search_affiliations_by_author(auid2)
      intersection = get_intersection(aff1, aff2)
      return list(map(lambda x: [auid1, x, auid2], intersection))

    way1, way2 = await asyncio.gather(search_by_paper(), search_by_affiliation())
    return way1 + way2

  @staticmethod
  async def solve(auid1: int, auid2: int):
    return [aa_solver.solve_1hop(auid1, auid2), aa_solver.solve_2hop(auid1, auid2)]

class ap_solver(object):
  @staticmethod
  async def solve_1hop(auid: int, paper: Paper):
    if auid in paper.auid:
      return [[auid, paper.id]]
    return []

  @staticmethod
  async def solve_2hop(auid: int, paper: Paper, count=default_count):
    resp = await send_http_request('AND(Composite(AA.AuId=%d),RId=%d)' % (auid, paper.id), count=count, attributes=PAPER_ATTR)
    papers = list(map(parse_paper_json, resp))
    return list(map(lambda mp: [auid, mp.id, paper.id], papers))

  @staticmethod
  async def solve(auid: int, paper: Paper):
    async def search_forward_affiliation(afid):
      authors = get_intersection(paper.auid, await search_authors_by_affiliation(afid))
      return list(map(lambda a: [auid, afid, a, paper.id]), authors)

    # async def search_backward_reference(rid):
      # papers = await fetch_papers([rid])
      # if not papers:
        # return []
      # result = await ap_solver.solve_2hop(auid, papers[0])
      # return list(map(lambda l: l + [paper.id], result))

    async def search_forward_paper(paper1, paper2):
      ways = await pp_solver.solve_2hop(paper1, paper2)
      return list(map(lambda l: [auid]+l, ways))

    tasks = [ap_solver.solve_1hop(auid, paper), ap_solver.solve_2hop(auid, paper)]

    # TODO: gather those three asynchronous IO actions
    # TODO: search_papers_by_author/search_affiliations_by_author do the same HTTP query, merged?
    author_papers = await search_papers_by_author(auid)
    affiliations = await search_affiliations_by_author(auid)
    # paper_refids = map(lambda p: p.id, await search_papers_by_ref(paper.id))

    tasks += list(map(search_forward_affiliation, affiliations)) # author->affiliation->author->paper
    # tasks += list(map(search_backward_reference, paper_refids)) # author->?->paper->paper
    tasks += list(map(lambda p: search_forward_paper(p, paper), author_papers)) # author->paper->?->paper
    return tasks

class pa_solver(object):
  @staticmethod
  async def solve_1hop(paper: Paper, auid: int):
    return [[paper.id, auid]] if auid in paper.auid else []

  @staticmethod
  async def solve_2hop(paper: Paper, auid: int):
    rid_set = set(paper.rid)
    papers = filter(lambda p: p.id in rid_set, await search_papers_by_author(auid))
    return list(map(lambda mp: [paper.id, mp.id, auid], papers))

  @staticmethod
  async def solve(paper: Paper, auid: int):
    return [pa_solver.solve_1hop(paper, auid), pa_solver.solve_2hop(paper, auid)]

async def solve(id1, id2):
  (type1, obj1), (type2, obj2) = await asyncio.gather(get_id_type(id1), get_id_type(id2))
  logger.info('solving test (%d,%s),(%d,%s)' % (id1, show_type(type1), id2, show_type(type2)))
  fs = None
  if type1 == TYPE_PAPER and type2 == TYPE_PAPER:
    assert obj1.id == id1 and obj2.id == id2
    fs = await pp_solver.solve(obj1, obj2)
  elif type1 == TYPE_AUTHOR and type2 == TYPE_AUTHOR:
    fs = await aa_solver.solve(obj1, obj2)
  elif type1 == TYPE_AUTHOR and type2 == TYPE_PAPER:
    fs = await ap_solver.solve(obj1, obj2)
  elif type1 == TYPE_PAPER and type2 == TYPE_AUTHOR:
    fs = await pa_solver.solve(obj1, obj2)
  else:
    if type1 == TYPE_UNKNOWN:
      logger.warn('type-unknown found id=%d' % id1)
    if type2 == TYPE_UNKNOWN:
      logger.warn('type-unknown found id=%d' % id2)
    return []

  done, _ = await asyncio.wait(fs, timeout=time_limit-get_elapsed_time())
  return make_unique(list(reduce(lambda l1, l2: l1+l2, map(lambda f: f.result(), done), [])))

async def worker(request):
  d = request.GET
  try:
    id1, id2 = int(d['id1']), int(d['id2'])
  except (ValueError, KeyError):
    logger.warn('invalid request \'%s\'' % request.query_string)
    return web.json_response([])
  logger.info('accepting request with id1=%d id2=%d' % (id1, id2))
  set_start_time()
  result = await solve(id1, id2)
  logger.info('%d->%d: elapsed_time=%f' % (id1, id2, get_elapsed_time()))
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
