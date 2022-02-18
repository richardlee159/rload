from numpy import random
import sys

# unit of time: ms
distribution = sys.argv[1]
duration = 5 * 1000
rate = 100
interval = 1000 / rate


def gen_next():
    if distribution == 'const':
        return interval
    elif distribution == 'uniform':
        return random.uniform(0, 2 * interval)
    elif distribution == 'exp':
        return random.exponential(interval)


start = 0
with open('trace.txt', 'w') as f:
    while start < duration:
        f.write(f'{start:.0f}\n')
        start += gen_next()
