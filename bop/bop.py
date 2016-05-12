import asyncio, aiohttp
import logging, time
from aiohttp import web
from sys import argv, stderr
from functools import reduce

start_time = 0
aggressive = False
client_session = aiohttp.ClientSession() # TODO: keep-alive timeout
logger = logging.getLogger(__name__)

default_attrs = ('Id','F.FId','C.CId','J.JId','AA.AuId','AA.AfId','RId')

# parameters (need adjusting)
default_count = 50     # TODO: maybe to small
time_limit = 300       # TODO: 300 is not a suitable value, see how score is evaluated
single_time_limit = 1  # time limit on single HTTP request

def enter_aggressive():
  global aggressive
  aggressive = True

def leave_aggressive():
  global aggressive
  aggressive = False

def set_start_time():
  global start_time
  start_time = time.time()

def get_elapsed_time():
  return time.time() - start_time

def get_intersection(b1, b2):
  return set(b1).intersection(set(b2))

def get_union(b1, b2):
  return set(b1).union(set(b2))

# TODO: searching strategy shall make sure there is no duplicate element
def make_unique(l):
  return l # list(set(l))

async def send_http_request(expr, count=None, attributes=None):
  subscription_key = 'f7cc29509a8443c5b3a5e56b0e38b5a6'
  bop_url = 'https://oxfordhk.azure-api.net/academic/v1.0/evaluate'
  params = {'expr': expr, 'subscription-key': subscription_key}
  if count:
    params['count'] = count
  if attributes:
    params['attributes'] = ','.join(attributes)

  async def shoot():
    async with client_session.get(bop_url, params=params) as resp:
      # logger.info('sending HTTP request: %s' % resp.url)
      json = await resp.json()
      if 'entities' in json:
        return json['entities']
      else:
        logger.error('invalid response from server')
        return []

  if aggressive:
    done, pending = await asyncio.wait([shoot()]*3, return_when=asyncio.FIRST_COMPLETED)
  else:
    done, pending = await asyncio.wait([shoot()], timeout=single_time_limit)
  for future in pending:
    future.cancel()
  done = list(done)
  if done:
    return done[0].result()
  return []

class Paper(object):
  def __init__(self, id, fid, cid, jid, auid, afid, rid):
    # WARNING: NEVER tries to make id list a set, we require order on auid and afid
    # TIPS: some fields may be missing because we don't need it
    self.id = id
    self.fid = fid if fid else []
    self.cid = cid if cid else []
    self.jid = jid if jid else []
    self.auid = auid if auid else []
    self.afid = afid if afid else [] # element of afid can be None if not available
    self.rid = rid if rid else []

def parse_paper_json(entity):
  id, fid, cid, jid, auid, afid, rid = 0, None, None, None, None, None, None
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
  return Paper(id, fid, cid, jid, auid, afid, rid)

TYPE_PAPER, TYPE_AUTHOR = 1, 2

def show_type(ty):
  if ty == TYPE_PAPER:
    return 'type-paper'
  if ty == TYPE_AUTHOR:
    return 'type-author'
  return 'type-unknown'

# get the type of one id, return a pair (TYPE_XXX, Paper object if TYPE_PAPER / AA.AuId if TYPE_AUTHOR)
async def get_id_type(id1, id2):
  resp = await send_http_request('OR(Id=%d,Id=%d)' % (id1, id2), count=2, attributes=default_attrs+('Ti',))
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

# fetch information of papers, paids shall have a reasonable length to avoid HTTP error 'Request URL Too Long'
async def fetch_papers(paids):
  expr = ''
  for paid in paids:
    tmp = 'Id=%d' % (paid)
    expr = 'OR(%s,%s)' % (expr, tmp) if expr else tmp
  resp = await send_http_request(expr, count=len(paids), attributes=default_attrs)
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
async def search_papers_by_ref(rid, count=default_count, attrs=default_attrs):
  resp = await send_http_request('RId=%d' % (rid), count=count, attributes=attrs)
  return list(map(parse_paper_json, resp))

# search papers of this author
async def search_papers_by_author(auid, count=default_count, attrs=default_attrs):
  resp = await send_http_request('Composite(AA.AuId=%d)' % (auid), count=count, attributes=default_attrs)
  return list(map(parse_paper_json, resp))

# search authors which is attached to this affiliation
async def search_authors_by_affiliation(afid, count=default_count):
  def filter_by_afid(auid_list, afid_list):
    def check(x):
      return x[1] == afid
    auf_zip = list(filter(check, zip(auid_list, afid_list)))
    return [a for (a, b) in auf_zip]

  resp = await send_http_request('Composite(AA.AfId=%d)' % (afid), count=count, attributes=('Id','AA.AuId','AA.AfId'))
  papers = list(map(parse_paper_json, resp))
  authors = list(map(lambda p: filter_by_afid(p.auid, p.afid), papers))
  return reduce(get_union, authors, set())

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
async def search_affiliations_by_author(auid, count=default_count, au_papers=None):
  def filter_by_auid(auid_list, afid_list):
    auf_zip = list(filter(lambda x: x[0] == auid and x[1], list(zip(auid_list, afid_list))))
    return [b for (a, b) in auf_zip]

  if not au_papers:
    au_papers = await search_papers_by_author(auid, count=count, attrs=('Id', 'AA.AuId', 'AA.AfId'))
  affiliations = list(map(lambda p: filter_by_auid(p.auid, p.afid), au_papers))
  return reduce(get_union, affiliations, set())

# notes on pp_solver/pa_solver/ap_solver/aa_solver:
# all solvers provide three static methods `solve_1hop`, `solve_2hop`, `prefetch` and `solve`,
# the first two do the same thing as its name stated, note that they are primitive up to asynchronous.
# `prefetch` fetches ALL asynchronous stuffs that `solve_2hop` needs during computation, so it can run
# concurrently with `get_id_type`.
# `solve` returns a list of futures (tasks) to be executed (asyncio.wait with timeout) by caller.
# See pp_solver.solve for an example.
class pp_solver(object):
  @staticmethod
  async def solve_1hop(paper1, paper2):
    return [(paper1.id, paper2.id)] if paper2.id in paper1.rid else []

  @staticmethod
  async def solve_2hop(paper1, paper2, paper2_refids=None):
    def find_way(list1, list2):
      intersection = get_intersection(list1, list2)
      return list(map(lambda x: (paper1.id, x, paper2.id), intersection))

    if paper2_refids == None and isinstance(paper1, int):
      paper2_refs, paper1 = await asyncio.gather(search_papers_by_ref(paper2.id, attrs=('Id',)), fetch_papers([paper1]))
      paper2_refids = list(map(lambda p: p.id, paper2_refs))
      if not paper1:
        return []
      else:
        paper1 = paper1[0]
    elif paper2_refids == None:
      paper2_refids = list(map(lambda p: p.id, await search_papers_by_ref(paper2.id, attrs=('Id',))))
    elif isinstance(paper1, int):
      paper1 = await fetch_papers([paper1])
      if not paper1:
        return []
      else:
        paper1 = paper1[0]
    return reduce(lambda a, b: a + b, [find_way(paper1.fid, paper2.fid),
      find_way(paper1.cid, paper2.cid),
      find_way(paper1.jid, paper2.jid),
      find_way(paper1.auid, paper2.auid),
      find_way(paper1.rid, paper2_refids)])

  @staticmethod
  async def prefetch(paper2_id):
    paper2_refs = await search_papers_by_ref(paper2_id)
    return paper2_refs

  @staticmethod
  async def solve(paper1, paper2, prefetched=None): # TODO: optimize, batch rids.
    if prefetched != None:
      paper2_refs = prefetched
    else:
      paper2_refs = await pp_solver.prefetch(paper2.id)
    paper2_refids = list(map(lambda p: p.id, paper2_refs))

    async def search_forward_reference(rid):
      result = await pp_solver.solve_2hop(rid, paper2, paper2_refids=paper2_refids)
      return list(map(lambda l: (paper1.id,) + l, result))

    async def search_backward_reference(ref_paper):
      # search_forward_reference and search_backward_reference both search path Paper->Paper->Paper->Paper,
      # setting one paper2_refids to empty list avoids duplicate search records.
      result = await pp_solver.solve_2hop(paper1, ref_paper, paper2_refids=[])
      return list(map(lambda l: l + (paper2.id,), result))

    tasks  = [pp_solver.solve_1hop(paper1, paper2), pp_solver.solve_2hop(paper1, paper2, paper2_refids)]
    tasks += list(map(search_forward_reference, paper1.rid))
    tasks += list(map(search_backward_reference, paper2_refs))
    return tasks

class aa_solver(object):
  @staticmethod
  async def solve_1hop(auid1, auid2):
    return [] # don't be confused, indeed there is no possible path lol

  @staticmethod
  async def solve_2hop(auid1, auid2, au1_papers=None, au2_papers=None): # TODO: this can prefetch
    async def search_by_paper(count=default_count):
      resp = await send_http_request('AND(Composite(AA.AuId=%d),Composite(AA.AuId=%d))' % (auid1, auid2), count=count, attributes=('Id',))
      return list(map(lambda p: (auid1, p['Id'], auid2), resp))

    async def search_by_affiliation(count=default_count):
      # no await actually!
      aff1, aff2 = await asyncio.gather(search_affiliations_by_author(auid1, au_papers=au1_papers), search_affiliations_by_author(auid2, au_papers=au2_papers))
      intersection = get_intersection(aff1, aff2)
      return list(map(lambda x: (auid1, x, auid2), intersection))

    way1, way2 = await asyncio.gather(search_by_paper(), search_by_affiliation())
    return way1 + way2

  @staticmethod
  async def prefetch(auid1, auid2):
    au1_papers, au2_papers = await asyncio.gather(search_papers_by_author(auid1, attrs=('Id','RId','AA.AuId','AA.AfId')),
        search_papers_by_author(auid2, attrs=('Id','AA.AuId','AA.AfId')))
    return au1_papers, au2_papers

  @staticmethod
  async def solve(auid1, auid2, prefetched=None):
    if prefetched != None:
      au1_papers, au2_papers = prefetched
    else:
      au1_papers, au2_papers = await aa_solver.prefetch(auid1, auid2)

    async def search_bidirection_papers():
      paper_id2 = set(list(map(lambda p: p.id, au2_papers)))

      def find(paper):
        intersection = get_intersection(paper.rid, paper_id2)
        return list(map(lambda id: (auid1, paper.id, id, auid2), intersection))

      return reduce(lambda a, b: a + b, list(map(find, au1_papers)), [])

    return [aa_solver.solve_1hop(auid1, auid2),
        aa_solver.solve_2hop(auid1, auid2, au1_papers=au1_papers, au2_papers=au2_papers),
        search_bidirection_papers()]

class ap_solver(object):
  @staticmethod
  async def solve_1hop(auid, paper):
    return [(auid, paper.id)] if auid in paper.auid else []

  @staticmethod
  async def solve_2hop(auid, paper, count=default_count): # TODO: this can prefetch
    resp = await send_http_request('AND(Composite(AA.AuId=%d),RId=%d)' % (auid, paper.id), count=count, attributes=('Id',))
    return list(map(lambda mp: (auid, mp['Id'], paper.id), resp))

  @staticmethod
  async def prefetch(auid, paper_id):
    (au_papers, affiliations), paper_refs = await asyncio.gather(search_papers_and_affiliations_by_author(auid),
        search_papers_by_ref(paper_id, attrs=('Id',)))
    return (au_papers, affiliations), paper_refs

  @staticmethod
  async def solve(auid, paper, prefetched=None):
    if prefetched != None:
      (au_papers, affiliations), paper_refs = prefetched
    else:
      (au_papers, affiliations), paper_refs = await ap_solver.prefetch(auid, paper.id)
    paper_refids = list(map(lambda p: p.id, paper_refs))

    async def search_forward_affiliation(afid):
      authors = get_intersection(paper.auid, await search_authors_by_affiliation(afid))
      return list(map(lambda a: (auid, afid, a, paper.id), authors))

    async def search_forward_paper(paper1):
      ways = await pp_solver.solve_2hop(paper1, paper, paper2_refids=paper_refids)
      return list(map(lambda l: (auid,) + l, ways))

    tasks = [ap_solver.solve_1hop(auid, paper), ap_solver.solve_2hop(auid, paper)]
    tasks += list(map(search_forward_affiliation, affiliations)) # author->affiliation->author->paper
    tasks += list(map(search_forward_paper, au_papers)) # author->paper->?->paper
    return tasks

# TODO: a bit time-consuming, consider a better strategy
class pa_solver(object):
  @staticmethod
  async def solve_1hop(paper, auid):
    return [(paper.id, auid)] if auid in paper.auid else []

  @staticmethod
  async def solve_2hop(paper, auid, au_papers=None):
    rid_set = set(paper.rid)
    if au_papers == None:
      au_papers = await search_papers_by_author(auid, attrs=('Id',))
    ok_papers = list(filter(lambda p: p.id in rid_set, au_papers))
    return list(map(lambda mp: (paper.id, mp.id, auid), ok_papers))

  @staticmethod
  async def prefetch(auid):
    au_papers, affiliations = await search_papers_and_affiliations_by_author(auid)
    return au_papers, affiliations

  @staticmethod
  async def solve(paper, auid, prefetched=None):
    if prefetched != None:
      au_papers, affiliations = prefetched
    else:
      au_papers, affiliations = await pa_solver.prefetch(auid)

    async def search_backward_paper(paper2):
      ways = await pp_solver.solve_2hop(paper, paper2)
      return list(map(lambda l: l + (auid,), ways))

    async def search_backward_affiliation(afid):
      authors = await search_authors_by_affiliation(afid)
      ok_authors = get_intersection(paper.auid, authors)
      return list(map(lambda a: (paper.id, a, afid, auid), ok_authors))

    tasks  = [pa_solver.solve_1hop(paper, auid), pa_solver.solve_2hop(paper, auid, au_papers)]
    tasks += list(map(search_backward_paper, au_papers))
    tasks += list(map(search_backward_affiliation, affiliations))
    return tasks

async def solve(id1, id2):
  enter_aggressive() # TODO: with syntax

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
    leave_aggressive()
    return []
  for task in tasks:
    task.cancel()
  logger.info('solving test (%d,%s),(%d,%s)' % (id1, show_type(types[0][0]), id2, show_type(types[1][0])))
  fs = await fs
  leave_aggressive()
  done, _ = await asyncio.wait(fs, timeout=time_limit-get_elapsed_time())
  return make_unique(reduce(lambda l1, l2: l1 + l2, map(lambda f: f.result(), done), []))

async def worker(request):
  logger.info(' ')
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
  logger.info('%d->%d: %d path(s) found, %s' % (id1, id2, len(result), str(result)))
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

  app = web.Application()
  app.router.add_route('GET', '/bop', worker)

  logger.info('bop server has started, listening on port %d' % (port))

  web.run_app(app, port=port)
