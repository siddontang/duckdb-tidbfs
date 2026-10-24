#!/usr/bin/env python3
"""Compare Quack on local files, tidbfs, and an optional baseline plugin.

Requires psutil, pymysql, boto3 and matching Python DuckDB 2.0. All filesystem
and object services, databases and credentials are disposable. A --tidb-server
wrapper may select an isolated real TiKV/PD cluster. See docs/read-performance.md.
"""
import argparse
import concurrent.futures
import csv
import datetime
import hashlib
import json
import os
from pathlib import Path
import platform
import queue
import random
import re
import secrets
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
import uuid

import boto3
from botocore.config import Config
import psutil
import pymysql

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
from test_native_live import port, wait

DUCKDB = ROOT / 'build/duckdb-2.0.0/bin/duckdb'
EXTENSION = ROOT / 'build/tidbfs.duckdb_extension'
QUACK = None
SCAN = 'SELECT sum(a), sum(b), bit_xor(c) FROM samples'


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def quantile(values, p):
    if not values:
        return None
    ordered = sorted(values)
    where = (len(ordered) - 1) * p
    lower = int(where)
    return ordered[lower] + (ordered[min(lower + 1, len(ordered)-1)] - ordered[lower]) * (where-lower)


def digest(path):
    h = hashlib.sha256()
    with open(path, 'rb') as source:
        for block in iter(lambda: source.read(1 << 20), b''):
            h.update(block)
    return h.hexdigest()


def vm_snapshot():
    if platform.system() == 'Darwin':
        text = subprocess.check_output(['vm_stat'], text=True)
        page_size = int(re.search(r'page size of (\d+) bytes', text).group(1))
        fields = {key.strip('"'): int(value) for key, value in re.findall(r'^([^:\n]+):\s+(\d+)\.', text, re.M)}
        sin, sout = fields.get('Swapins', 0)*page_size, fields.get('Swapouts', 0)*page_size
    else:
        swap = psutil.swap_memory()
        sin, sout = swap.sin, swap.sout
    return {'swapin_bytes': sin, 'swapout_bytes': sout,
            'available_bytes': psutil.virtual_memory().available}


class CLI:
    def __init__(self, logs, name):
        self.log_path = logs / (name + '.stderr.log')
        self.log = self.log_path.open('w')
        self.lines = queue.Queue()
        self.seq = 0
        env = {k:v for k,v in os.environ.items() if not k.startswith(('DRIVE9_', 'AWS_', 'RUSTFS_'))}
        env.update(TZ='UTC', DUCKDB_AGENT_MODE='0')
        self.process = subprocess.Popen([str(DUCKDB), '-unsigned', '-bail', '-csv', '-noheader'],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.log,
            text=True, bufsize=1, env=env)
        def receive():
            for line in self.process.stdout:
                self.lines.put(line.rstrip('\n'))
            self.lines.put(None)
        self.reader = threading.Thread(target=receive, daemon=True)
        self.reader.start()

    def execute(self, statement, timeout=180):
        self.seq += 1
        marker = '__bench_done_' + str(self.seq)
        started = time.perf_counter()
        self.process.stdin.write(statement.rstrip('; \n') + ';\n.print ' + marker + '\n')
        self.process.stdin.flush()
        rows = []
        while True:
            try:
                line = self.lines.get(timeout=max(0.001, started+timeout-time.perf_counter()))
            except queue.Empty:
                raise TimeoutError('SQL deadline exceeded; private stderr: '+str(self.log_path)) from None
            if line is None:
                raise RuntimeError('CLI exited; inspect '+str(self.log_path))
            if line == marker:
                return time.perf_counter()-started, rows
            if line:
                rows.append(line)

    def close(self):
        if self.process.poll() is None:
            self.process.stdin.close()
            try:
                self.process.wait(timeout=120)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=15)
        self.reader.join(timeout=5)
        for stream in (self.process.stdin, self.process.stdout, self.log):
            if stream and not stream.closed:
                stream.close()
        if self.process.returncode != 0:
            raise RuntimeError('CLI shutdown failed; inspect '+str(self.log_path))


class Monitor:
    def __init__(self, groups):
        self.groups = {name:[psutil.Process(pid) for pid in pids] for name,pids in groups.items()}
        self.vm_before=vm_snapshot()
        self.net_before=psutil.net_io_counters(pernic=True).get('lo0' if platform.system() == 'Darwin' else 'lo')
        self.before = self.cpu()
        self.rss = {name:0 for name in groups}
        self.host = []
        self.stop = threading.Event()
        def collect():
            while not self.stop.is_set():
                for name, processes in self.groups.items():
                    self.rss[name] = max(self.rss[name], sum(p.memory_info().rss for p in processes if p.is_running()))
                self.host.append(psutil.cpu_percent(interval=None))
                self.stop.wait(0.2)
        self.thread = threading.Thread(target=collect,daemon=True)
        self.thread.start()

    def cpu(self):
        return {name:sum(p.cpu_times().user+p.cpu_times().system for p in processes if p.is_running())
                for name,processes in self.groups.items()}

    def finish(self, elapsed):
        after = self.cpu()
        vm_after=vm_snapshot()
        net_after=psutil.net_io_counters(pernic=True).get('lo0' if platform.system() == 'Darwin' else 'lo')
        self.stop.set()
        self.thread.join(timeout=3)
        return {'cpu_cores':{k:(after[k]-self.before[k])/elapsed for k in after},
                'peak_rss_bytes':self.rss, 'host_cpu_percent_median':statistics.median(self.host) if self.host else 0,
                'swap_bytes':psutil.swap_memory().used,'vm_before':self.vm_before,'vm_after':vm_after,
                'swapin_delta_bytes':vm_after['swapin_bytes']-self.vm_before['swapin_bytes'],
                'swapout_delta_bytes':vm_after['swapout_bytes']-self.vm_before['swapout_bytes'],
                'loopback_bytes_sent':net_after.bytes_sent-self.net_before.bytes_sent if net_after and self.net_before else None}


class PythonClient:
    """Independent in-memory DuckDB client instance and Quack SQL session."""
    def __init__(self,logs,name):
        import duckdb
        from types import SimpleNamespace
        self.connection=duckdb.connect(config={'threads':1,'memory_limit':'32MB'})
        self.process=SimpleNamespace(pid=os.getpid())
    def execute(self,statement,timeout=180):
        started=time.perf_counter()
        result=self.connection.execute(statement)
        rows=result.fetchall() if result.description else []
        return time.perf_counter()-started,[','.join('' if value is None else str(value) for value in row) for row in rows]
    def close(self):
        self.connection.close()


def metrics(endpoint):
    try:
        with urllib.request.urlopen(endpoint+'/metrics',timeout=10) as response:
            text=response.read().decode()
        return {line.rsplit(' ',1)[0]:float(line.rsplit(' ',1)[1]) for line in text.splitlines()
                if line and not line.startswith('#') and ('_total' in line or '_sum' in line or '_count' in line)
                and re.search(r' (?:[\d.eE+-]+)$',line)}
    except Exception:
        return {}


def main():
    global DUCKDB, QUACK
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', required=True)
    parser.add_argument('--duckdb', required=True)
    parser.add_argument('--quack-extension', required=True, help='Matching installed official Quack file')
    parser.add_argument('--tidb-server', required=True)
    parser.add_argument('--fs-server', required=True)
    parser.add_argument('--rustfs-server', required=True)
    parser.add_argument('--baseline-extension', help='Optional old plugin, in a separate worker')
    parser.add_argument('--duration',type=float,default=8)
    parser.add_argument('--trials',type=int,default=3)
    parser.add_argument('--clients',default='1,2,4,8,16')
    parser.add_argument('--workloads',default='read_hot,write1,write128,mixed90')
    parser.add_argument('--rows',type=int,default=4194304)
    parser.add_argument('--mode',choices=['matrix','stream','pilot'],default='matrix')
    parser.add_argument('--extension',default=str(EXTENSION))
    parser.add_argument('--client-driver',choices=['python','cli'],default='python')
    args=parser.parse_args()
    DUCKDB = Path(args.duckdb).resolve()
    QUACK = Path(args.quack_extension).resolve()
    extension=Path(args.extension).resolve()
    backends = ['local', 'tidbfs']
    extensions = {'tidbfs': extension}
    if args.baseline_extension:
        backends.append('baseline')
        extensions['baseline'] = Path(args.baseline_extension).resolve()
    binaries = {'duckdb': DUCKDB, 'quack': QUACK, 'fs': Path(args.fs_server),
                'rustfs': Path(args.rustfs_server), 'tidb_entrypoint': Path(args.tidb_server), **extensions}
    logs=Path(args.out).resolve();logs.mkdir(parents=True,exist_ok=True)
    (logs/'bench.py').write_text(Path(__file__).read_text())
    clients_list=[int(v) for v in args.clients.split(',')]
    workloads=args.workloads.split(',')
    children=[]; handles=[]; active=[]
    report={'started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'platform':platform.platform(),'cpu':subprocess.check_output(['sysctl','-n','machdep.cpu.brand_string'],text=True).strip() if platform.system() == 'Darwin' else platform.processor(),
        'logical_cpus':psutil.cpu_count(),'memory_bytes':psutil.virtual_memory().total,
        'duckdb':subprocess.check_output([str(DUCKDB),'-version'],text=True).strip(),
        'feature_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
        'working_tree_diff_sha256':hashlib.sha256(subprocess.check_output(['git','diff','HEAD'],cwd=ROOT)).hexdigest(),
        'benchmark_sha256':digest(Path(__file__)),
        'main_commit':subprocess.check_output(['git','rev-parse','origin/main'],cwd=ROOT,text=True).strip(),
        'binaries':{k:digest(v) for k,v in binaries.items()},
        'method':{'duration_seconds':args.duration,'trials':args.trials,'clients':clients_list,'workloads':workloads,
                  'rows':args.rows,'row_bytes':32,'threads':4,'memory_limit':'1GB' if args.mode!='stream' else '64MB',
                  'checkpoint_threshold':'1TB','fs_maintenance_interval':'default 5m','request_model':'closed loop, one in-flight SQL per independent Quack connection',
                  'client_driver':args.client_driver,'python_client_limits':'threads=1, memory_limit=32MB per independent client instance; client and driver CPU sampled together',
                  'transport':'Quack loopback; direct fs HTTP and RustFS S3 without Python proxies',
                  'storage':'Caller-supplied TiDB and RustFS binaries, all services on this host; record backing TiKV topology separately',
                  'cache':'warm DuckDB buffer pool for matrix; fresh workers with 64MB buffer pool for stream; OS/object caches not purged',
                  'timing':'client end-to-end SQL through durable COMMIT acknowledgement; process creation/attach/checkpoint excluded and checkpoint measured separately',
                  'order':'same-configuration trials adjacent; rotate first backend each trial/configuration',
                  'latency_quantiles':'linear interpolation of all successful request latencies per run; raw samples saved; low N cannot establish stable p99'},
        'preparation':[],'runs':[]}
    if args.client_driver=='python':
        import duckdb,_duckdb
        with duckdb.connect(config={'threads':1}) as check:
            version=check.execute('PRAGMA version').fetchone()
        assert version[0]=='v2.0.0' and version[1]=='d54cbbe3df',version
        report['python_client_engine']=list(version)
        report['python_client_binary_sha256']=digest(_duckdb.__file__)

    def save():
        (logs/'results.json').write_text(json.dumps(report,indent=2)+'\n')

    def launch(command,name,env):
        handle=(logs/(name+'.log')).open('w');handles.append(handle)
        child=subprocess.Popen(command,env=env,stdout=handle,stderr=handle)
        children.append(child);return child

    def cli(name):
        instance=CLI(logs,name);active.append(instance);return instance

    def new_client(name):
        instance=(PythonClient if args.client_driver=='python' else CLI)(logs,name)
        active.append(instance);return instance

    clean={k:v for k,v in os.environ.items() if not k.startswith(('DRIVE9_','RUSTFS_','AWS_','MINIO_','OTEL_'))}
    clean.update(TZ='UTC',DUCKDB_AGENT_MODE='0')
    with tempfile.TemporaryDirectory(prefix='runtime-',dir=logs) as tmp:
        try:
            sql_port,status_port,fs_port,s3_port=port(),port(),port(),port()
            tidb=launch([str(Path(args.tidb_server).resolve()),'--host=127.0.0.1','-P='+str(sql_port),
                '--status='+str(status_port),'--status-host=127.0.0.1','--log-slow-query='+str(logs/'tidb-slow.log'),'-L=error'], 'tidb',clean)
            db=wait(lambda:pymysql.connect(host='127.0.0.1',port=sql_port,user='root'))
            database='quack_perf_'+uuid.uuid4().hex[:12]
            with db.cursor() as cursor:cursor.execute('CREATE DATABASE '+database)
            db.close()
            access,secret=secrets.token_hex(12).upper(),secrets.token_hex(24)
            objects=Path(tmp)/'objects';objects.mkdir()
            rustfs=launch([str(Path(args.rustfs_server).resolve()),'server',str(objects),'--address',f'127.0.0.1:{s3_port}'],'rustfs',
                dict(clean,RUSTFS_ACCESS_KEY=access,RUSTFS_SECRET_KEY=secret,RUSTFS_CONSOLE_ENABLE='false',RUSTFS_REGION='us-east-1'))
            s3url=f'http://127.0.0.1:{s3_port}'
            s3=boto3.client('s3',endpoint_url=s3url,region_name='us-east-1',aws_access_key_id=access,aws_secret_access_key=secret,
                config=Config(connect_timeout=2,read_timeout=5,retries={'max_attempts':0},s3={'addressing_style':'path'}))
            wait(s3.list_buckets);s3.create_bucket(Bucket='quack-perf')
            fs=launch([str(Path(args.fs_server).resolve())],'fs',dict(clean,DRIVE9_TENANT_PROVIDER='local',
                DRIVE9_LISTEN_ADDR=f'127.0.0.1:{fs_port}',
                DRIVE9_LOCAL_DSN=f'root@tcp(127.0.0.1:{sql_port})/{database}?parseTime=true',
                DRIVE9_TOKEN_SIGNING_KEY=secrets.token_hex(32),DRIVE9_MASTER_KEY=secrets.token_hex(32),
                DRIVE9_S3_BUCKET='quack-perf',DRIVE9_S3_ENDPOINT=s3url,DRIVE9_S3_REGION='us-east-1',
                DRIVE9_S3_FORCE_PATH_STYLE='true',DRIVE9_S3_ACCESS_KEY_ID=access,DRIVE9_S3_SECRET_ACCESS_KEY=secret,
                DRIVE9_EXTENT_ALLOW_STATIC_CREDENTIALS='1'))
            endpoint=f'http://127.0.0.1:{fs_port}'
            wait(lambda:urllib.request.urlopen(endpoint+'/healthz',timeout=3).read())
            with urllib.request.urlopen(urllib.request.Request(endpoint+'/v1/provision',method='POST'),timeout=90) as response:
                token=json.load(response)['api_key']
            request=urllib.request.Request(endpoint+'/v1/fs/',method='HEAD',headers={'Authorization':'Bearer '+token})
            wait(lambda:urllib.request.urlopen(request,timeout=5).read())
            service_pids={'tidb':[tidb.pid],'fs':[fs.pid],'rustfs':[rustfs.pid]}
            for process in psutil.process_iter(['name','cmdline']):
                command=' '.join(process.info['cmdline'] or [])
                if os.environ.get('TIDBFS_BENCH_CLUSTER_TAG') and os.environ['TIDBFS_BENCH_CLUSTER_TAG'] in command and process.info['name'] in ('pd-server','tikv-server'):
                    service_pids[process.info['name']]=[process.pid]
            report['service_pids']=service_pids
            (logs/'fs-metrics-ready.json').write_text(json.dumps(metrics(endpoint),indent=2))
            print('Disposable TiDB and RustFS services ready',flush=True)

            paths = {backend: str(Path(tmp)/'local.duckdb') if backend == 'local' else
                     'tidbfs+db:///perf-'+uuid.uuid4().hex+'.duckdb' for backend in backends}
            def worker(backend,name,memory='1GB'):
                worker=cli(name)
                init=f'LOAD {quote(QUACK)}; SET threads=4; SET memory_limit={quote(memory)}; SET checkpoint_threshold=\'1TB\'; SET force_compression=\'uncompressed\'; '
                if backend != 'local':
                    init+=f'LOAD {quote(extensions[backend])}; CREATE SECRET fs (TYPE tidbfs, ENDPOINT {quote(endpoint)}, TOKEN {quote(token)}); '
                worker.execute(init+f'ATTACH {quote(paths[backend])} AS shared; USE shared',timeout=180)
                return worker

            expected=None
            for backend in backends:
                prepare=worker(backend,'prepare-'+backend)
                seconds,_=prepare.execute('CREATE TABLE samples AS SELECT i::BIGINT id, hash(i)::UBIGINT a, hash(i+100000003)::UBIGINT b, hash(i+200000006)::UBIGINT c FROM range('+str(args.rows)+') t(i)',timeout=1800)
                checkpoint,_=prepare.execute('CHECKPOINT shared',timeout=1800)
                _,checksum=prepare.execute(SCAN,timeout=1800)
                if expected is None:expected=checksum
                assert checksum==expected,(backend,checksum,expected)
                report['preparation'].append({'backend':backend,'create_seconds':seconds,'checkpoint_seconds':checkpoint,'checksum':checksum})
                prepare.close();active.remove(prepare)
                print('Prepared '+backend+': '+str(args.rows)+' rows',flush=True);save()

            sizes=[]
            for page in s3.get_paginator('list_objects_v2').paginate(Bucket='quack-perf'):
                sizes.extend(obj['Size'] for obj in page.get('Contents',[]))
            report['prepared_object_layout']={'objects':len(sizes),'bytes':sum(sizes),
                'median_object_bytes':statistics.median(sizes) if sizes else 0,
                'size_bins':{label:sum(low<size<=high for size in sizes) for label,low,high in
                    (('up_to_4KiB',-1,4096),('4_to_64KiB',4096,65536),('64_to_256KiB',65536,262144),
                     ('256KiB_to_1MiB',262144,1048576),('over_1MiB',1048576,2**63))}}
            save()

            cases=[(trial,workload,n) for workload in workloads for n in clients_list for trial in range(args.trials)]
            for case_index,(trial,workload,n) in enumerate(cases):
                shift = case_index % len(backends)
                order = backends[shift:] + backends[:shift]
                for backend in order:
                    name=f'{args.mode}-{trial}-{workload}-{n}-{backend}'
                    memory='64MB' if args.mode=='stream' else '1GB'
                    owner=worker(backend,name+'-worker',memory)
                    owner.execute('CREATE TABLE IF NOT EXISTS events(client INTEGER, seq BIGINT, value BIGINT); DELETE FROM events; CHECKPOINT shared',timeout=1800)
                    if workload!='read_stream':owner.execute(SCAN,timeout=1800)
                    address='quack:127.0.0.1:'+str(port());qtoken=secrets.token_hex(24)
                    owner.execute(f'CALL quack_serve({quote(address)}, token={quote(qtoken)})')
                    clients=[new_client(name+'-client-'+str(i)) for i in range(n)]
                    for client in clients:
                        client.execute(f'LOAD {quote(QUACK)}; SET http_timeout=120; ATTACH {quote(address)} AS q (TOKEN {quote(qtoken)}); CONNECT q; USE shared;')
                    if n>1:
                        clients[0].execute('BEGIN; INSERT INTO events VALUES (-1,-1,-1)')
                        _,uncommitted=clients[1].execute('SELECT count(*) FROM events')
                        assert uncommitted==['0'],'Quack clients must have isolated transaction sessions'
                        clients[0].execute('ROLLBACK')
                    gate=threading.Event();ready=threading.Barrier(n+1,timeout=180)
                    state={};raw=[];errors=[]
                    def run_client(index):
                        client=clients[index];seq=0;written=0;local=[]
                        rng=random.Random(1009+index)
                        def request(measured):
                            nonlocal seq,written
                            seq+=1
                            writing=workload in ('write1','write128') or (workload=='mixed90' and seq%10==0)
                            batch=128 if workload=='write128' else 1
                            if writing:
                                sql=f'INSERT INTO events SELECT {index}, {seq*128}+i, i FROM range({batch}) t(i)'
                            elif workload=='read_stream':sql=SCAN
                            elif workload=='rpc_ping':sql='SELECT 42'
                            else:
                                low=rng.randrange(0,args.rows-4096)
                                sql=f'SELECT count(*),sum(a),sum(b),bit_xor(c) FROM samples WHERE id>={low} AND id<{low+4096}'
                            elapsed,rows=client.execute(sql,timeout=180)
                            if writing:written+=batch
                            elif workload == 'read_stream' and rows != expected:
                                raise AssertionError('full scan checksum mismatch')
                            elif len(rows)!=1:raise AssertionError('read returned unexpected rows')
                            elif workload not in ('read_stream','rpc_ping') and not rows[0].startswith('4096,'):
                                raise AssertionError('range read returned incomplete rows')
                            if measured:local.append({'client':index,'kind':'write' if writing else 'read','latency_ms':elapsed*1000})
                        if workload!='read_stream':
                            for _ in range(3):request(False)
                        ready.wait();gate.wait(timeout=180)
                        while time.perf_counter()<state['deadline']:
                            request(True)
                        return local,written
                    pool=concurrent.futures.ThreadPoolExecutor(max_workers=n)
                    futures=[pool.submit(run_client,i) for i in range(n)]
                    ready.wait()
                    before_metrics=metrics(endpoint)
                    groups=dict(service_pids,worker=[owner.process.pid],clients=[os.getpid()] if args.client_driver=='python' else [c.process.pid for c in clients],driver=[] if args.client_driver=='python' else [os.getpid()])
                    (logs/'active-run.json').write_text(json.dumps({'name':name,'groups':groups}))
                    monitor=Monitor(groups)
                    started=time.perf_counter();state['deadline']=started+args.duration;gate.set()
                    written=0
                    for future in futures:
                        try:
                            samples,count=future.result(timeout=args.duration+240);raw.extend(samples);written+=count
                        except Exception as error:
                            errors.append(type(error).__name__+': '+str(error))
                    elapsed=time.perf_counter()-started
                    resources=monitor.finish(elapsed)
                    pool.shutdown(wait=True)
                    after_metrics=metrics(endpoint)
                    delta={key:value-before_metrics.get(key,0) for key,value in after_metrics.items()
                        if value-before_metrics.get(key,0)!=0}
                    (logs/(name+'-metrics.json')).write_text(json.dumps(delta,indent=2))
                    if errors:raise RuntimeError(str(errors))
                    _,actual=owner.execute('SELECT count(*),count(DISTINCT (client,seq)),count(*) FILTER (WHERE client IS NULL OR seq IS NULL OR value IS NULL) FROM events')
                    assert actual==[f'{written},{written},0'],(name,actual,written)
                    _,checksum=owner.execute(SCAN,timeout=1800);assert checksum==expected
                    result={'name':name,'backend':backend,'trial':trial,'workload':workload,'clients':n,
                        'elapsed_seconds':elapsed,'completed':len(raw),'throughput_qps':len(raw)/elapsed,'errors':errors,
                        'latency_ms':{str(p):quantile([s['latency_ms'] for s in raw],p/100) for p in (50,95,99)},
                        'resources':resources,'resource_eligible':resources['swapout_delta_bytes'] <= 64*2**20 and resources['swapin_delta_bytes'] <= 128*2**20,'verified_rows':written,'metrics_delta':name+'-metrics.json',
                        'per_kind':{kind:{'completed':sum(s['kind']==kind for s in raw),
                            'qps':sum(s['kind']==kind for s in raw)/elapsed,
                            'p50_ms':quantile([s['latency_ms'] for s in raw if s['kind']==kind],.5),
                            'p95_ms':quantile([s['latency_ms'] for s in raw if s['kind']==kind],.95),
                            'p99_ms':quantile([s['latency_ms'] for s in raw if s['kind']==kind],.99)} for kind in ('read','write')}}
                    with (logs/(name+'-latencies.csv')).open('w') as output:
                        writer=csv.DictWriter(output,fieldnames=('client','kind','latency_ms'));writer.writeheader();writer.writerows(raw)
                    for client in clients:client.close();active.remove(client)
                    result['checkpoint_seconds'],_=owner.execute('CHECKPOINT shared',timeout=1800)
                    owner.close();active.remove(owner)
                    report['runs'].append(result);save()
                    print(f'{name}: {len(raw)/elapsed:.1f} qps, p50 {result["latency_ms"]["50"]:.2f} ms, p95 {result["latency_ms"]["95"]:.2f} ms, n={len(raw)}',flush=True)
            report['finished_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();save()
        finally:
            for instance in reversed(active):
                try:instance.close()
                except Exception:pass
            for child in reversed(children):
                if child.poll() is None:
                    child.terminate()
                    try:child.wait(timeout=15)
                    except subprocess.TimeoutExpired:child.kill();child.wait(timeout=10)
            for handle in handles:handle.close()


if __name__=='__main__':
    main()
