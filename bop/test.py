import asyncio, logging
import bop.bop

bop.bop.single_time_limit = 60
bop.bop.logging.basicConfig(filename='debug.log',
    filemode='a',
    format='[%(asctime)s] [%(name)-12s] [%(levelname)-8s] %(message)s',
    datefmt='%m-%d %H:%M:%S',
    level=logging.DEBUG)

async def test():
  with open('tests.txt', 'r') as f:
    n = int(f.readline())
    for i in range(n):
      id1, id2 = map(int, f.readline().split())
      expect = eval(f.readline())
      actual = await bop.bop.solve(id1, id2)
      expect.sort()
      actual.sort()
      if actual == expect:
        print('%d OK' % i)
      else:
        print('%d FAIL' % i)
        print(expect, actual)

loop = asyncio.get_event_loop()
loop.run_until_complete(test())


