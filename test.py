import cassandra.cluster
import subprocess
import time
import math

cluster = cassandra.cluster.Cluster(port=30003)
session = cluster.connect('jaeger_v1_dc1')

st = session.prepare('''
    select *
    from service_operation_index
    where service_name = 'nginx-web-server'
      and operation_name = 'CalledComposePost'
      and start_time > ?
''')


def rload(tracefile='trace.txt'):
    return subprocess.run([
        'target/release/rload',
        '-t', tracefile,
    ], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL)


def bench():
    st_arg = math.ceil(time.time() * 1e6)
    p = rload()
    assert p.returncode == 0, f'returncode: {p.returncode}'

    print('waiting for traces')
    time.sleep(15)
    starts = [i.start_time for i in session.execute(st, [st_arg])]
    starts.sort()
    base = starts[0]

    with open('trace_jaeger.txt', 'w') as f:
        for start in starts:
            start = (start - base) / 1e3
            f.write(f'{start:.1f}\n')


if __name__ == '__main__':
    bench()
