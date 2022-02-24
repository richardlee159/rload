from numpy import random
import argparse

dict_gen_next = {
    'const': lambda x: x,
    'uniform': lambda x: random.uniform(0, 2 * x),
    'exp': lambda x: random.exponential(x),
}

parser = argparse.ArgumentParser(description='Generate tracefile for rload.')
parser.add_argument('-D', '--dist', choices=dict_gen_next.keys(), required=True,
                    help='distribution of the inter-arrival time of requests')
parser.add_argument('-d', '--duration', type=int, required=True,
                    help='duration of test in seconds')
parser.add_argument('-R', '--rate', type=int, required=True,
                    help='work rate in requests/second')
args = parser.parse_args()

# unit of time: us
distribution = args.dist
duration = args.duration * 1e6
rate = args.rate
interval = 1e6 / rate
gen_next = dict_gen_next[distribution]

start = 0
with open('trace.txt', 'w') as f:
    while start < duration:
        f.write(f'{start:.0f}\n')
        start += gen_next(interval)
