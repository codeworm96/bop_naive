import asyncio, aiohttp
import logging, time
from aiohttp import web
from sys import argv, stderr
from functools import reduce

start_time = 0
logger = logging.getLogger(__name__)
default_count = 50 # TODO: maybe to small
default_attrs = ('Id','F.FId','C.CId','J.JId','AA.AuId','AA.AfId','RId')
client_session = None
time_limit = 300 # TODO: 300 is not a suitable value, see how score is evaluated

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
  async with client_session.get(bop_url, params=params) as resp:
    # logger.info('sending HTTP request: %s' % resp.url)
    json = await resp.json()
    if 'entities' in json:
      return json['entities']
    else:
      logger.error('invalid response from server')
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
  resp = await send_http_request('OR(Id=%d,Id=%d)' % (id1, id2), count=2, attributes=('Id','F.FId','C.CId','J.JId','AA.AuId','AA.AfId','RId','Ti'))
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
  resp = await send_http_request(expr, count=len(paids), attributes=('Id','F.FId','C.CId','J.JId','AA.AuId','AA.AfId','RId'))
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
async def search_papers_by_ref(rid, count=default_count):
  resp = await send_http_request('RId=%d' % (rid), count=count, attributes=('Id','F.FId','C.CId','J.JId','AA.AuId','AA.AfId','RId'))
  return list(map(parse_paper_json, resp))

# search papers of this author
async def search_papers_by_author(auid, count=default_count):
  resp = await send_http_request('Composite(AA.AuId=%d)' % (auid), count=count, attributes=('Id','F.FId','C.CId','J.JId','AA.AuId','AA.AfId','RId'))
  return list(map(parse_paper_json, resp))

# search authors which is attached to this affiliation
async def search_authors_by_affiliation(afid, count=default_count):
  def filter_by_afid(auid_list, afid_list):
    def check(x):
      return x[1] == afid
    auf_zip = list(filter(check, list(zip(auid_list, afid_list))))
    return [a for (a, b) in auf_zip]

  resp = await send_http_request('Composite(AA.AfId=%d)' % (afid), count=count, attributes=('Id','AA.AuId','AA.AfId'))
  papers = list(map(parse_paper_json, resp))
  authors = list(map(lambda p: filter_by_afid(p.auid, p.afid), papers))
  return list(reduce(get_union, authors, set()))

# search papers and affiliations which the author attaches to
async def search_papers_and_affiliations_by_author(auid, count=default_count):
  def filter_by_auid(auid_list, afid_list):
    def check(x):
      return x[0] == auid and x[1] # also drop authors with AfId=None
    auf_zip = list(filter(check, list(zip(auid_list, afid_list))))
    return [b for (a, b) in auf_zip]

  papers = await search_papers_by_author(auid, count=count)
  affiliations = list(map(lambda p: filter_by_auid(p.auid, p.afid), papers))
  return papers, list(reduce(get_union, affiliations, set()))

# search affiliations which the author attaches to
async def search_affiliations_by_author(auid, count=default_count):
  _, affiliations = await search_papers_and_affiliations_by_author(auid, count)
  return affiliations

# notes on pp_solver/pa_solver/ap_solver/aa_solver:
# all solvers provide three static methods `solve_1hop`, `solve_2hop` and `solve`,
# the first two do the same thing as its name stated, note that they are primitive up to asynchronous.
# `solve` returns a list of futures (tasks) to be executed (asyncio.wait with timeout) by caller.
# See pp_solver.solve for an example.
class pp_solver(object):
  @staticmethod
  async def solve_1hop(paper1, paper2):
    if paper2.id in paper1.rid:
      return [(paper1.id, paper2.id)]
    return []

  @staticmethod
  async def solve_2hop(paper1, paper2, paper2_refids=None):
    def find_joint(list1, list2):
      intersection = get_intersection(list1, list2)
      return list(map(lambda x: (paper1.id, x, paper2.id), intersection))

    if paper2_refids == None and isinstance(paper1, int):
      paper2_refs, paper1 = await asyncio.gather(search_papers_by_ref(paper2.id), fetch_papers([paper1]))
      paper2_refids = list(map(lambda p: p.id, paper2_refs))
      if not paper1:
        return []
      else:
        paper1 = paper1[0]
    elif paper2_refids == None:
      paper2_refids = list(map(lambda p: p.id, await search_papers_by_ref(paper2.id)))
    elif isinstance(paper1, int):
      paper1 = await fetch_papers([paper1])
      if not paper1:
        return []
      else:
        paper1 = paper1[0]
    return list(reduce(lambda a, b: a + b, [find_joint(paper1.fid, paper2.fid),
      find_joint(paper1.cid, paper2.cid),
      find_joint(paper1.jid, paper2.jid),
      find_joint(paper1.auid, paper2.auid),
      find_joint(paper1.rid, paper2_refids)]))

  @staticmethod
  async def solve(paper1, paper2):
    async def search_forward_reference(rid):
      result = await pp_solver.solve_2hop(rid, paper2)
      return list(map(lambda l: (paper1.id,) + l, result))

    async def search_backward_reference(ref_paper):
      # search_forward_reference and search_backward_reference both search path Paper->Paper->Paper->Paper,
      # setting one paper2_refids to empty list avoids duplicate search records.
      result = await pp_solver.solve_2hop(paper1, ref_paper, paper2_refids=[])
      return list(map(lambda l: l + (paper2.id,), result))

    paper2_refs = await search_papers_by_ref(paper2.id)
    paper2_refids = list(map(lambda p: p.id, paper2_refs))

    tasks  = [pp_solver.solve_1hop(paper1, paper2), pp_solver.solve_2hop(paper1, paper2, paper2_refids)]
    tasks += list(map(search_forward_reference, paper1.rid))
    tasks += list(map(search_backward_reference, paper2_refs))
    return tasks

class aa_solver(object):
  @staticmethod
  async def solve_1hop(auid1, auid2):
    return [] # don't be confused, indeed there is no possible path lol

  @staticmethod
  async def solve_2hop(auid1, auid2):
    async def search_by_paper(count=default_count):
      resp = await send_http_request('AND(Composite(AA.AuId=%d),Composite(AA.AuId=%d))' % (auid1, auid2), count=count, attributes=('Id',))
      papers = list(map(parse_paper_json, resp))
      return list(map(lambda p: (auid1, p.id, auid2), papers))

    async def search_by_affiliation(count=default_count):
      # TODO: merge these two queries using primitive OR?
      aff1, aff2 = await asyncio.gather(search_affiliations_by_author(auid1), search_affiliations_by_author(auid2))
      intersection = get_intersection(aff1, aff2)
      return list(map(lambda x: (auid1, x, auid2), intersection))

    way1, way2 = await asyncio.gather(search_by_paper(), search_by_affiliation())
    return way1 + way2

  @staticmethod
  async def solve(auid1, auid2):
    # async def search_backward_paper(paper):
      # ways = await ap_solver.solve_2hop(auid1, paper)
      # return list(map(lambda l: l+(auid2,), ways))

    # papers2 = await search_papers_by_author(auid2)
    # tasks  = [aa_solver.solve_1hop(auid1, auid2), aa_solver.solve_2hop(auid1, auid2)]
    # tasks += list(map(search_backward_paper, papers2))
    # return tasks

    async def search_bidirection_papers(auid1, auid2):
      paper_list1, paper_list2 = await asyncio.gather(search_papers_by_author(auid1), search_papers_by_author(auid2))
      paper_id2 = set(list(map(lambda p: p.id, paper_list2)))

      def find(paper):
        intersection = get_intersection(paper.rid, paper_id2)
        return list(map(lambda id: (auid1, paper.id, id, auid2), intersection))

      return list(reduce(lambda a, b: a+b, list(map(find, paper_list1))))

    return [aa_solver.solve_1hop(auid1, auid2), aa_solver.solve_2hop(auid1, auid2), search_bidirection_papers(auid1, auid2)]

class ap_solver(object):
  @staticmethod
  async def solve_1hop(auid, paper):
    if auid in paper.auid:
      return [(auid, paper.id)]
    return []

  @staticmethod
  async def solve_2hop(auid, paper, count=default_count):
    resp = await send_http_request('AND(Composite(AA.AuId=%d),RId=%d)' % (auid, paper.id), count=count, attributes=('Id',))
    papers = list(map(parse_paper_json, resp))
    return list(map(lambda mp: (auid, mp.id, paper.id), papers))

  @staticmethod
  async def solve(auid, paper):
    async def search_forward_affiliation(afid):
      authors = get_intersection(paper.auid, await search_authors_by_affiliation(afid))
      return list(map(lambda a: (auid, afid, a, paper.id), authors))

    async def search_forward_paper(paper1, paper2):
      ways = await pp_solver.solve_2hop(paper1, paper2)
      return list(map(lambda l: (auid,) + l, ways))

    tasks = [ap_solver.solve_1hop(auid, paper), ap_solver.solve_2hop(auid, paper)]

    apapers, affiliations = await search_papers_and_affiliations_by_author(auid)

    tasks += list(map(search_forward_affiliation, affiliations)) # author->affiliation->author->paper
    tasks += list(map(lambda p: search_forward_paper(p, paper), apapers)) # author->paper->?->paper
    return tasks

class pa_solver(object):
  @staticmethod
  async def solve_1hop(paper, auid):
    return [(paper.id, auid)] if auid in paper.auid else []

  @staticmethod
  async def solve_2hop(paper, auid, apapers=None):
    rid_set = set(paper.rid)
    if apapers == None:
      apapers = await search_papers_by_author(auid)
    ok_papers = list(filter(lambda p: p.id in rid_set, apapers))
    return list(map(lambda mp: (paper.id, mp.id, auid), ok_papers))

  @staticmethod
  async def solve(paper, auid):
    async def search_backward_paper(paper2):
      ways = await pp_solver.solve_2hop(paper, paper2)
      return list(map(lambda l: l + (auid,), ways))

    async def search_backward_affiliation(afid):
      authors = await search_authors_by_affiliation(afid)
      ok_authors = get_intersection(paper.auid, authors)
      return list(map(lambda a: (paper.id, a, afid, auid), ok_authors))

    apapers, affiliations = await search_papers_and_affiliations_by_author(auid)
    tasks  = [pa_solver.solve_1hop(paper, auid), pa_solver.solve_2hop(paper, auid, apapers)]
    tasks += list(map(search_backward_paper, apapers))
    tasks += list(map(search_backward_affiliation, affiliations))
    return tasks

async def solve(id1, id2):
  (type1, obj1), (type2, obj2) = await get_id_type(id1, id2)
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
    logger.error('type-unknown found id=%d,%d' % (id1, id2))
    return []
  done, _ = await asyncio.wait(fs, timeout=time_limit-get_elapsed_time())
  return make_unique(list(reduce(lambda l1, l2: l1+l2, map(lambda f: f.result(), done), [])))

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
  global client_session
  with aiohttp.ClientSession() as client_session:
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
