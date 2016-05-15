import asyncio, aiohttp
import logging, time
import urllib
import random
from aiohttp import web
from sys import argv, stderr
from functools import reduce

start_time = 0
client_session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(keepalive_timeout=58))
logger = logging.getLogger(__name__)

subscription_key = 'f7cc29509a8443c5b3a5e56b0e38b5a6'
bop_url = 'http://oxfordhk.azure-api.net/academic/v1.0/evaluate'
default_attrs = ('Id','F.FId','C.CId','J.JId','AA.AuId','AA.AfId','RId')

# parameters (need adjusting)
default_count = 50000
time_limit = 8
io_time_limit = 2

def set_start_time():
  global start_time
  start_time = time.time()

def get_elapsed_time():
  return time.time() - start_time

def get_intersection(b1, b2):
  return set(b1).intersection(set(b2))

def get_union(b1, b2):
  return set(b1).union(set(b2))

def make_unique(l):
  return list(set(l))

def split_list(l, k):
  return [l[x:x+k] for x in range(0, len(l), k)]

async def send_http_request(expr, count=None, attributes=None, critical=True):
  params = {'expr': expr, 'subscription-key': subscription_key}
  if count:
    params['count'] = count
  if attributes:
    params['attributes'] = ','.join(attributes)

  async def shoot():
    async with client_session.get(bop_url, params=params) as resp:
      # logger.info('sending HTTP request: %s' % urllib.parse.unquote(resp.url))
      json = await resp.json()
      if 'entities' in json:
        return json['entities']
      else:
        logger.error('invalid response from server')
        return []

  if critical:
    done, pending = await asyncio.wait([shoot()]*3, return_when=asyncio.FIRST_COMPLETED)
  else:
    done, pending = await asyncio.wait([shoot()], timeout=io_time_limit)
  for future in pending:
    future.cancel()
  done = list(done)
  if done:
    return done[0].result()
  return []

class Paper(object):
  def __init__(self, id, fjcid, auid, afid, rid):
    # WARNING: NEVER tries to make id list a set, we require order on auid and afid
    # TIPS: some fields may be missing because we don't need it
    self.id = id
    self.fjcid = fjcid if fjcid else []
    self.auid = auid if auid else []
    self.afid = afid if afid else [] # element of afid can be None if not available
    self.rid = rid if rid else []

def parse_paper_json(entity):
  id, fid, cid, jid, auid, afid, rid = 0, [], [], [], [], [], []
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
  return Paper(id, fid+cid+jid, auid, afid, rid)

TYPE_PAPER, TYPE_AUTHOR = 1, 2

def show_type(ty):
  if ty == TYPE_PAPER:
    return 'type-paper'
  if ty == TYPE_AUTHOR:
    return 'type-author'
  return 'type-unknown'

# get the type of one id, return a pair (TYPE_XXX, Paper object if TYPE_PAPER / AA.AuId if TYPE_AUTHOR)
async def get_id_type(id1, id2):
  resp = await send_http_request('Or(Id=%d,Id=%d)' % (id1, id2), count=2, attributes=default_attrs+('Ti',))
  if id1 == id2:
    if len(resp) == 1:
      entity = resp[0]
      if 'Ti' in entity:
        paper1 = parse_paper_json(entity)
        paper2 = parse_paper_json(entity)
        return (TYPE_PAPER, paper1), (TYPE_PAPER, paper2)
    return (TYPE_AUTHOR, id1), (TYPE_AUTHOR, id2)
  else:
    if len(resp) == 2:
      entity1, entity2 = resp
      if entity1['Id'] != id1:
        entity1, entity2 = entity2, entity1
      ty1 = (TYPE_PAPER, parse_paper_json(entity1)) if 'Ti' in entity1 else (TYPE_AUTHOR, id1)
      ty2 = (TYPE_PAPER, parse_paper_json(entity2)) if 'Ti' in entity2 else (TYPE_AUTHOR, id2)
      return ty1, ty2
    if len(resp) == 1:
      entity = resp[0]
      ty1 = (TYPE_AUTHOR, id1)
      ty2 = (TYPE_AUTHOR, id2)
      if entity['Id'] == id1:
        ty1 = (TYPE_PAPER, parse_paper_json(entity))
      else:
        ty2 = (TYPE_PAPER, parse_paper_json(entity))
      return ty1, ty2
    return (TYPE_AUTHOR, id1), (TYPE_AUTHOR, id2)

# fetch information of papers
async def fetch_papers(paper_ids, attrs=default_attrs):
  if paper_ids == []:
    return []
  paper_ids = make_unique(paper_ids)
  paper_ids_group = split_list(paper_ids, 23) # split list to avoid HTTP error 'Request URL Too Long'

  async def fetch_papers_safe(paper_ids):
    expr = ''
    for paper_id in paper_ids:
      tmp = 'Id=%d' % (paper_id)
      expr = 'Or(%s,%s)' % (expr, tmp) if expr else tmp
    resp = await send_http_request(expr, count=len(paper_ids), attributes=attrs)
    if len(resp) != len(paper_ids):
      logger.error('fetched incomplete paper list of %s' % (str(paper_ids)))
      return None
    indices = {}
    for i in range(len(paper_ids)):
      indices[paper_ids[i]] = i
    papers = [None] * len(paper_ids)
    for entity in resp:
      paper = parse_paper_json(entity)
      papers[indices[paper.id]] = paper
    return papers

  tasks = list(map(asyncio.ensure_future, map(fetch_papers_safe, paper_ids_group)))
  await asyncio.wait(tasks)
  result = []
  for task in tasks:
    if task.result() == None:
      return None
    result += task.result()
  return result 

# search papers which references this paper
async def search_papers_by_ref(rid, count=default_count, attrs=default_attrs):
  resp = await send_http_request('RId=%d' % (rid), count=count, attributes=attrs)
  return list(map(parse_paper_json, resp))

# search papers of this author
async def search_papers_by_author(auid, count=default_count, attrs=default_attrs):
  resp = await send_http_request('Composite(AA.AuId=%d)' % (auid), count=count, attributes=attrs)
  return list(map(parse_paper_json, resp))

# search papers and affiliations which the author attaches to
async def search_papers_and_affiliations_by_author(auid, count=default_count, attrs=default_attrs):
  def filter_by_auid(auid_list, afid_list):
    auf_zip = list(filter(lambda x: x[0] == auid and x[1], list(zip(auid_list, afid_list))))
    return [b for (a, b) in auf_zip]

  papers = await search_papers_by_author(auid, count=count, attrs=attrs)
  affiliations = list(map(lambda p: filter_by_auid(p.auid, p.afid), papers))
  return papers, reduce(get_union, affiliations, set())

# search affiliations which the author attaches to
# FIXME: contains duplicate code from search_papers_and_affiliations_by_author, be careful with attributes!
def search_affiliations_by_author(auid, au_papers, count=default_count):
  def filter_by_auid(auid_list, afid_list):
    auf_zip = list(filter(lambda x: x[0] == auid and x[1], list(zip(auid_list, afid_list))))
    return [b for (a, b) in auf_zip]

  affiliations = list(map(lambda p: filter_by_auid(p.auid, p.afid), au_papers))
  return reduce(get_union, affiliations, set())

# search only paper id which is referenced by one papar and written by one author
async def search_paper_ids_by_author_and_ref(auid, paper_id, count=default_count):
  resp = await send_http_request('And(Composite(AA.AuId=%d),RId=%d)' % (auid, paper_id), count=count, attributes=('Id',))
  return list(map(lambda p: p['Id'], resp))

# test if author in one affiliation
async def test_author_in_affiliation(auid, afid):
  resp = await send_http_request('Composite(And(AA.AuId=%d,AA.AfId=%d))' % (auid, afid), attributes=('Id',), count=1, critical=False)
  return len(resp) == 1

# notes on pp_solver/pa_solver/ap_solver/aa_solver:
# all solvers provide three static methods `solve_1hop`, `solve_2hop`, `prefetch` and `solve`,
# the first two do the same thing as its name stated, note that they are primitive up to asynchronous.
# `prefetch` fetches ALL asynchronous stuffs that `solve_2hop` needs during computation, so it can run
# concurrently with `get_id_type`.
# `solve` returns two list of futures (tasks) to be executed, one is pure computation, another one requires some 
# network IO. Pure computation is run in another thread to avoid blocking main thread.
# See pp_solver.solve for an example.
class pp_solver(object):
  @staticmethod
  def solve_1hop(paper1, paper2):
    return [(paper1.id, paper2.id)] if paper2.id in paper1.rid else []

  @staticmethod
  async def prefetch(paper2_id):
    paper2_refs = await search_papers_by_ref(paper2_id)
    return paper2_refs

  @staticmethod
  def solve_2hop(paper1, paper2, paper2_refids):
    def find_way(list1, list2):
      intersection = get_intersection(list1, list2)
      return list(map(lambda x: (paper1.id, x, paper2.id), intersection))

    return find_way(paper1.fjcid, paper2.fjcid) + find_way(paper1.auid, paper2.auid) + find_way(paper1.rid, paper2_refids)

  @staticmethod
  async def solve(paper1, paper2, prefetched=None):
    if prefetched != None:
      paper2_refs = prefetched
    else:
      paper2_refs = await pp_solver.prefetch(paper2.id)
    paper2_refids = list(map(lambda p: p.id, paper2_refs))

    async def search_forward_reference(paper1):
      paper1_refs = await fetch_papers(paper1.rid)
      result = []
      if paper1_refs:
        for ref_paper in paper1_refs:
          result += pp_solver.solve_2hop(ref_paper, paper2, paper2_refids)
      return list(map(lambda l: (paper1.id,) + l, result))

    def search_backward_reference(ref_paper):
      # search_forward_reference and search_backward_reference both search path Paper->Paper->Paper->Paper,
      # setting one paper2_refids to empty list avoids duplicate search records.
      result = pp_solver.solve_2hop(paper1, ref_paper, [])
      return list(map(lambda l: l + (paper2.id,), result))

    tasks_pure = [lambda: pp_solver.solve_1hop(paper1, paper2), 
        lambda: pp_solver.solve_2hop(paper1, paper2, paper2_refids)]
    tasks_pure += list(map(lambda p: (lambda: search_backward_reference(p)), paper2_refs))
    tasks_io = [search_forward_reference(paper1)]
    return tasks_pure, tasks_io

class aa_solver(object):
  @staticmethod
  def solve_1hop(auid1, auid2):
    return [] # don't be confused, indeed there is no possible path lol

  @staticmethod
  async def prefetch(auid1, auid2):
    au1_papers, au2_papers = await asyncio.gather(
        search_papers_by_author(auid1, attrs=('Id','RId','AA.AuId','AA.AfId')),
        search_papers_by_author(auid2, attrs=('Id','AA.AuId','AA.AfId')))
    return au1_papers, au2_papers

  @staticmethod
  def solve_2hop(auid1, auid2, au1_papers, au2_papers):
    def search_by_paper(au1_papers, au2_papers):
      intersection = get_intersection(map(lambda p: p.id, au1_papers), map(lambda p: p.id, au2_papers))
      return list(map(lambda x: (auid1, x, auid2), intersection))

    def search_by_affiliation(au1_papers, au2_papers):
      aff1, aff2 = search_affiliations_by_author(auid1, au1_papers), search_affiliations_by_author(auid2, au2_papers)
      intersection = get_intersection(aff1, aff2)
      return list(map(lambda x: (auid1, x, auid2), intersection))

    return search_by_paper(au1_papers, au2_papers) + search_by_affiliation(au1_papers, au2_papers)

  @staticmethod
  async def solve(auid1, auid2, prefetched=None):
    if prefetched != None:
      au1_papers, au2_papers = prefetched
    else:
      au1_papers, au2_papers = await aa_solver.prefetch(auid1, auid2)

    def search_bidirection_papers():
      paper_id2 = set(list(map(lambda p: p.id, au2_papers)))

      def find(paper):
        intersection = get_intersection(paper.rid, paper_id2)
        return list(map(lambda x: (auid1, paper.id, x, auid2), intersection))

      return reduce(lambda a, b: a + b, list(map(find, au1_papers)), [])

    tasks_pure = [lambda: aa_solver.solve_1hop(auid1, auid2),
        lambda: aa_solver.solve_2hop(auid1, auid2, au1_papers, au2_papers),
        lambda: search_bidirection_papers()]
    tasks_io = []
    return tasks_pure, tasks_io

class ap_solver(object):
  @staticmethod
  def solve_1hop(auid, paper):
    return [(auid, paper.id)] if auid in paper.auid else []

  @staticmethod
  async def prefetch(auid, paper_id):
    (au_papers, affiliations), paper_refs, au_ref_paper_ids = await asyncio.gather(
        search_papers_and_affiliations_by_author(auid),
        search_papers_by_ref(paper_id, attrs=('Id',)),
        search_paper_ids_by_author_and_ref(auid, paper_id)) 
    # Note: seems the last operation can be computed from the first two, but don't do it because 'count' may not suffice.
    return (au_papers, affiliations), paper_refs, au_ref_paper_ids

  @staticmethod
  def solve_2hop(auid, paper, au_ref_paper_ids):
    return list(map(lambda x: (auid, x, paper.id), au_ref_paper_ids))

  @staticmethod
  async def solve(auid, paper, prefetched=None):
    if prefetched != None:
      (au_papers, affiliations), paper_refs, au_ref_paper_ids = prefetched
    else:
      (au_papers, affiliations), paper_refs, au_ref_paper_ids = await ap_solver.prefetch(auid, paper.id)
    paper_refids = list(map(lambda p: p.id, paper_refs))

    def search_forward_paper(paper1):
      ways = pp_solver.solve_2hop(paper1, paper, paper_refids)
      return list(map(lambda l: (auid,) + l, ways))

    async def search_both_affiliation_and_author(afid, auid2):
      if await test_author_in_affiliation(auid2, afid):
        return [(auid, afid, auid2, paper.id)]
      return []

    tasks_pure = [lambda: ap_solver.solve_1hop(auid, paper), 
        lambda: ap_solver.solve_2hop(auid, paper, au_ref_paper_ids)]
    tasks_pure += list(map(lambda p: (lambda: search_forward_paper(p)), au_papers))
    auid_unique = make_unique(paper.auid)
    tasks_io = [search_both_affiliation_and_author(afid, auid2) for afid in affiliations for auid2 in auid_unique]
    return tasks_pure, tasks_io

class pa_solver(object):
  @staticmethod
  def solve_1hop(paper, auid):
    return [(paper.id, auid)] if auid in paper.auid else []

  @staticmethod
  async def prefetch(auid):
    au_papers, affiliations = await search_papers_and_affiliations_by_author(auid)
    return au_papers, affiliations

  @staticmethod
  def solve_2hop(paper, auid, au_papers):
    rid_set = set(paper.rid)
    ok_papers = list(filter(lambda p: p.id in rid_set, au_papers))
    return list(map(lambda mp: (paper.id, mp.id, auid), ok_papers))

  @staticmethod
  async def solve(paper, auid, prefetched=None):
    if prefetched != None:
      au_papers, affiliations = prefetched
    else:
      au_papers, affiliations = await pa_solver.prefetch(auid)

    async def search_backward_paper(paper2_list, paper):
      # ALRAM: paper1_refs.rid is a set!!!
      paper1_refs = await fetch_papers(paper.rid, attrs=('Id','RId'))
      if paper1_refs:
        for p in paper1_refs:
          p.rid = set(p.rid)
      ways = []
      for paper2 in paper2_list:
        ways += pp_solver.solve_2hop(paper, paper2, [])
        if paper1_refs:
          ways += [(paper.id, paper1.id, paper2.id) for paper1 in paper1_refs if paper2.id in paper1.rid]
      return list(map(lambda l: l + (auid,), ways))

    async def search_both_author_and_affiliation(auid2, afid):
      if await test_author_in_affiliation(auid2, afid):
        return [(paper.id, auid2, afid, auid)]
      return []

    tasks_pure = [lambda: pa_solver.solve_1hop(paper, auid), 
        lambda: pa_solver.solve_2hop(paper, auid, au_papers)]
    tasks_io  = [search_backward_paper(au_papers, paper)]
    auid_unique = make_unique(paper.auid)
    tasks_io += [search_both_author_and_affiliation(auid2, afid) for afid in affiliations for auid2 in auid_unique]
    return tasks_pure, tasks_io

async def solve(id1, id2):
  set_start_time()

  tasks = list(map(asyncio.ensure_future, [get_id_type(id1, id2),
    pp_solver.prefetch(id2),
    aa_solver.prefetch(id1, id2),
    ap_solver.prefetch(id1, id2),
    pa_solver.prefetch(id2)]))
  fs, task_index = None, 0

  def classify(types, prefetched, wt):
    (type1,obj1), (type2,obj2) = types
    if type1 == TYPE_PAPER and type2 == TYPE_PAPER:
      assert obj1.id == id1 and obj2.id == id2
      return pp_solver.solve(obj1, obj2, prefetched=prefetched) if wt else 1
    elif type1 == TYPE_AUTHOR and type2 == TYPE_AUTHOR:
      assert obj1 == id1 and obj2 == id2
      return aa_solver.solve(obj1, obj2, prefetched=prefetched) if wt else 2
    elif type1 == TYPE_AUTHOR and type2 == TYPE_PAPER:
      assert obj1 == id1 and obj2.id == id2
      return ap_solver.solve(obj1, obj2, prefetched=prefetched) if wt else 3
    elif type1 == TYPE_PAPER and type2 == TYPE_AUTHOR:
      assert obj1.id == id1 and obj2 == id2
      return pa_solver.solve(obj1, obj2, prefetched=prefetched) if wt else 4
    else:
      return None if wt else 0

  # logger.info('PREFETCH SECTION') 

  pendings = tasks
  while True:
    _, pendings = await asyncio.wait(pendings, return_when=asyncio.FIRST_COMPLETED)
    if tasks[0].done():
      types = tasks[0].result()
      task_index = classify(types, None, False)
      if task_index == 0:
        break
      elif tasks[task_index].done():
        prefetched = tasks[task_index].result()
        fs = classify(types, prefetched, True)
        break
  if task_index == 0:
    logger.error('type-unknown found id=%d,%d' % (id1, id2))
    return []
  for task in tasks:
    task.cancel()
  logger.info('solving test (%d,%s),(%d,%s)' % (id1, show_type(types[0][0]), id2, show_type(types[1][0])))

  # logger.info('INIT SOLVER SECTION')
  tasks_pure, tasks_io = await fs
  # logger.info('SOLVE ALL TASKS SECTION')

  def run_tasks_pure(tasks):
    result = []
    for task in tasks:
      result += task()
    return result

  computation = asyncio.get_event_loop().run_in_executor(None, run_tasks_pure, tasks_pure)
  if tasks_io:
    io_done, _ = await asyncio.wait(tasks_io, timeout=time_limit-get_elapsed_time())
  else:
    io_done = []
  pure_result = await computation
  # TODO: make_unique
  return pure_result + reduce(lambda l1, l2: l1 + l2, map(lambda f: f.result(), io_done), [])

async def bop_handler(request):
  logger.info(' ')
  d = request.GET
  try:
    id1, id2 = int(d['id1']), int(d['id2'])
  except (ValueError, KeyError):
    logger.warn('invalid request \'%s\'' % request.query_string)
    return web.json_response([])
  logger.info('accepting request with id1=%d id2=%d' % (id1, id2))
  result = await solve(id1, id2)
  logger.info('%d->%d: elapsed_time=%f' % (id1, id2, get_elapsed_time()))
  logger.info('%d->%d: %d path(s) found' % (id1, id2, len(result)))
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
      format='[%(asctime)s] [%(name)-12s] [%(levelname)-8s] %(message)s',
      datefmt='%m-%d %H:%M:%S',
      level=logging.DEBUG)

  loop = asyncio.get_event_loop()

  async def tcp_shake():
    async def tcp_shake_once():
      x = random.randint(100000000, 200000000)
      params = {'expr': 'Id=%d' % (x), 'subscription-key': subscription_key, 'count': 1, 'attributes': 'Id'}
      async with client_session.head(bop_url, params=params) as resp:
        asyncio.sleep(0.03) # yield
    while True:
      await tcp_shake_once()
      await asyncio.sleep(52)

  for _ in range(28): # about 24 connections are needed in first round query
    asyncio.ensure_future(tcp_shake())

  app = web.Application(loop=loop)
  app.router.add_route('GET', '/bop', bop_handler)

  logger.info('bop server has started, listening on port %d' % (port))

  web.run_app(app, port=port)
