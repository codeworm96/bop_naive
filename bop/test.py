import asyncio, logging
import bop.bop
from sys import argv

bop.bop.single_time_limit = 60
bop.bop.logging.basicConfig(filename='debug.log',
    filemode='a',
    format='[%(asctime)s] [%(name)-12s] [%(levelname)-8s] %(message)s',
    datefmt='%m-%d %H:%M:%S',
    level=logging.DEBUG)

test_case = None
if len(argv) == 2:
  test_case = [int(argv[1]) - 1]

async def test(test_case):
  with open('tests.txt', 'r') as f:
    n = int(f.readline())
    if not test_case:
      test_case = range(0, n)
    for i in range(n):
      id1, id2 = map(int, f.readline().split())
      expect = list(map(tuple, eval(f.readline())))
      if i in test_case:
        actual = await bop.bop.solve(id1, id2)
        expect.sort()
        actual.sort()
        if actual == expect:
          print('%d OK' % (i+1))
        else:
          print('%d FAIL' % (i+1))
          print(expect, actual)

loop = asyncio.get_event_loop()
loop.run_until_complete(test(test_case))

