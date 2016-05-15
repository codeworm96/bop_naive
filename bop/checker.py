import asyncio, aiohttp
from sys import argv

client_session = aiohttp.ClientSession()

async def send_http_request(expr, count=None, attributes=None):
  subscription_key = 'f7cc29509a8443c5b3a5e56b0e38b5a6'
  bop_url = 'https://oxfordhk.azure-api.net/academic/v1.0/evaluate'
  params = {'expr': expr, 'subscription-key': subscription_key}
  if count:
    params['count'] = count
  if attributes:
    params['attributes'] = ','.join(attributes)
  async with client_session.get(bop_url, params=params) as resp:
    json = await resp.json()
    if 'entities' in json:
      return json['entities']
    else:
      print('ERROR!! Invalid response from server.')
      return []

TYPE_PAPER = 1
TYPE_AUTHOR = 2
TYPE_AFFILIATION = 3
TYPE_FIELD = 4
TYPE_JOURNAL = 5
TYPE_CONFERENCE = 6

type_cache = { }

async def get_type(id):
  async def solve():
    r1, r2, r3, r4, r5, r6 = await asyncio.gather(
        send_http_request('Id=%d' % (id), count=1, attributes=('Id','Ti')),
        send_http_request('Composite(AA.AuId=%d)' % (id), count=1, attributes=('Id',)),
        send_http_request('Composite(AA.AfId=%d)' % (id), count=1, attributes=('Id',)),
        send_http_request('Composite(F.FId=%d)' % (id), count=1, attributes=('Id',)),
        send_http_request('Composite(J.JId=%d)' % (id), count=1, attributes=('Id',)),
        send_http_request('Composite(C.CId=%d)' % (id), count=1, attributes=('Id',)))
    assert len(r1) == 1
    entity = r1[0]
    if 'Ti' in entity:
      return TYPE_PAPER
    if r2:
      return TYPE_AUTHOR
    if r3:
      return TYPE_AFFILIATION
    if r4:
      return TYPE_FIELD
    if r5:
      return TYPE_JOURNAL
    if r6:
      return TYPE_CONFERENCE
    assert False

  if id in type_cache:
    return type_cache[id]
  type = await solve()
  type_cache[id] = type
  return type

mask = 0

async def test_connected(a, b):
  global mask
  id1, type1 = a
  id2, type2 = b
  resp = None
  if type1 == TYPE_PAPER and type2 == TYPE_PAPER:
    mask |= 1
    resp = await send_http_request('And(Id=%d,RId=%d)' % (id1, id2), count=1)
  elif type1 == TYPE_PAPER and type2 == TYPE_FIELD:
    mask |= 2
    resp = await send_http_request('And(Id=%d,Composite(F.FId=%d))' % (id1, id2), count=1)
  elif type1 == TYPE_FIELD and type2 == TYPE_PAPER:
    mask |= 4
    resp = await send_http_request('And(Id=%d,Composite(F.FId=%d))' % (id2, id1), count=1)
  elif type1 == TYPE_PAPER and type2 == TYPE_CONFERENCE:
    mask |= 8
    resp = await send_http_request('And(Id=%d,Composite(C.CId=%d))' % (id1, id2), count=1)
  elif type1 == TYPE_CONFERENCE and type2 == TYPE_PAPER:
    mask |= 16
    resp = await send_http_request('And(Id=%d,Composite(C.CId=%d))' % (id2, id1), count=1)
  elif type1 == TYPE_PAPER and type2 == TYPE_JOURNAL:
    mask |= 32
    resp = await send_http_request('And(Id=%d,Composite(J.JId=%d))' % (id1, id2), count=1)
  elif type1 == TYPE_JOURNAL and type2 == TYPE_PAPER:
    mask |= 64
    resp = await send_http_request('And(Id=%d,Composite(J.JId=%d))' % (id2, id1), count=1)
  elif type1 == TYPE_AUTHOR and type2 == TYPE_AFFILIATION:
    mask |= 128
    resp = await send_http_request('Composite(And(AA.AuId=%d,AA.AfId=%d))' % (id1, id2), count=1)
  elif type1 == TYPE_AFFILIATION and type2 == TYPE_AUTHOR:
    mask |= 256
    resp = await send_http_request('Composite(And(AA.AuId=%d,AA.AfId=%d))' % (id2, id1), count=1)
  elif type1 == TYPE_AUTHOR and type2 == TYPE_PAPER:
    mask |= 512
    resp = await send_http_request('And(Id=%d,Composite(AA.AuId=%d))' % (id2, id1), count=1)
  elif type1 == TYPE_PAPER and type2 == TYPE_AUTHOR:
    mask |= 1024
    resp = await send_http_request('And(Id=%d,Composite(AA.AuId=%d))' % (id1, id2), count=1)
  else:
    print('NO RULE MATCHES!')
    return False
  return len(resp) == 1

async def check(id1, id2, paths):
  async def check1(id1, id2, path):
    if len(path) == 1 or len(path) > 4:
      return False
    if id1 != path[0] or id2 != path[-1]:
      return False
    tasks = list(map(asyncio.ensure_future, map(get_type, path)))
    await asyncio.wait(tasks)
    id_types = list(zip(path, list(map(lambda f: f.result(), tasks))))
    print(mask, id_types)
    for i in range(0, len(id_types)-1):
      a, b = id_types[i], id_types[i+1]
      if not await test_connected(a, b):
        return False
    return True

  for path in paths:
    ok = await check1(id1, id2, path)
    if not ok:
      print('%s FAIL!!!' % (str(path)))
    
async def test(test_case):
  with open('tests.txt', 'r') as f:
    n = int(f.readline())
    if test_case == None:
      test_case = range(0, n)
    for i in range(n):
      id1, id2 = map(int, f.readline().split())
      d = f.readline()
      if i in test_case:
        paths = eval(d)
        await check(id1, id2, paths)
  print(mask)

test_case = None
if len(argv) == 2:
  test_case = [int(argv[1]) - 1]

loop = asyncio.get_event_loop()
loop.run_until_complete(test(test_case))
