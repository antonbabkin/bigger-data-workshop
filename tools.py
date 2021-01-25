import sys
import os
import time
import json
import subprocess
import inspect
import warnings
import shutil
from zipfile import ZipFile
from pathlib import Path
from urllib.parse import urlparse, unquote

import requests
import psutil
from psutil._common import bytes2human



def download_file(url, dir=None, fname=None, overwrite=False):
    """Download file from given `url` and put it into `dir`.
    Current working directory is used as default. Missing directories are created.
    File name from `url` is used as default.
    Return absolute pathlib.Path of the downloaded file."""
    
    if dir is None:
        dir = '.'
    dpath = Path(dir).resolve()
    dpath.mkdir(parents=True, exist_ok=True)

    if fname is None:
        fname = Path(urlparse(url).path).name
    fpath = dpath / fname
    
    if not overwrite and fpath.exists():
        print(f'File {fname} already exists.')
        return fpath

    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(fpath, 'wb') as f:
            shutil.copyfileobj(r.raw, f)
    
    print(f'Download complete: {fname}.')
    return fpath

def unzip(fpath, dst=None, overwrite=False):
    """Extract all memberfs of Zip archive `fpath` into `dst` directory (current working directory by default)."""
    dst = Path('.' if dst is None else dst)
    with ZipFile(fpath) as zf:
        count = 0
        for member in zf.namelist():
            member_path = dst / member
            if overwrite or not member_path.exists():
                zf.extract(member, dst)
                count += 1
        print(f'Extracted {count} files from {fpath.name}.')


def usage_log(pid, interval=1):
    """Regularly write resource usage to stdout."""
    # local imports make function self-sufficient
    import time, psutil

    if psutil.MACOS:
        warnings.warn('Disk I/O stats are not available on MacOS.')

    p = psutil.Process(pid)

    def get_io():
        if psutil.MACOS:
            # io_counters() not available on MacOS
            return (0, 0, 0, 0)
        else:
            x = p.io_counters()
            return (x.read_bytes, x.read_chars, x.write_bytes, x.write_chars)

    print('time,cpu,memory,read_bytes,read_chars,write_bytes,write_chars')
    p.cpu_percent()
    io_before = get_io()
    while True:
        time.sleep(interval)
        io_after = get_io()
        io_rate = tuple((x1 - x0) / interval for x0, x1 in zip(io_before, io_after))
        io_before = io_after
        line = (time.time(), p.cpu_percent(), p.memory_info().rss) + io_rate
        print(','.join(str(x) for x in line))


class ResourceMonitor:
    def __init__(self, pid=None, interval=1):
        self.pid = os.getpid() if pid is None else pid
        self.interval = interval
        self.tags = []

    def start(self):
        code = inspect.getsource(usage_log) + f'\nusage_log({self.pid}, {self.interval})'
        self.process = subprocess.Popen([sys.executable, '-c', code], text=True,
                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def stop(self):
        self.process.send_signal(subprocess.signal.SIGINT)
        import pandas as pd
        self.process.wait(3)
        df = pd.read_csv(self.process.stdout)
        df['elapsed'] = df['time'] - df.loc[0, 'time']
        self.df = df.set_index('elapsed')

    def tag(self, label):
        self.tags.append((time.time(), label))

    def plot(self):
        import matplotlib.pyplot as plt
        fig, axes = plt.subplots(2, 2, figsize=(12, 8))

        ax = axes[0][0]
        ax.plot(self.df['cpu'])
        ax.set_title('cpu')

        ax = axes[1][0]
        ax.plot(self.df['memory'])
        ax.set_title('memory')
        ax.set_yticklabels([bytes2human(x) for x in ax.get_yticks()])

        ax = axes[0][1]
        ax.plot(self.df['read_bytes'], label='bytes')
        ax.plot(self.df['read_chars'], label='chars')
        ax.set_title('read')
        ax.legend()
        ax.set_yticklabels([bytes2human(x) for x in ax.get_yticks()])

        ax = axes[1][1]
        ax.plot(self.df['write_bytes'], label='bytes')
        ax.plot(self.df['write_chars'], label='chars')
        ax.set_title('write')
        ax.legend()
        ax.set_yticklabels([bytes2human(x) for x in ax.get_yticks()])

        t0 = self.df.loc[0, 'time']
        for ax in axes.flatten():
            y = min(l.get_data()[1].min() for l in ax.lines)
            for tag in self.tags:
                ax.text(tag[0] - t0, y, tag[1], rotation='vertical')

    def dump(self, filepath):
        d = {'tags': self.tags,
             'data': self.df.to_csv()}
        json.dump(d, open(filepath, 'w'))

    @classmethod
    def load(cls, filepath):
        import io
        d = json.load(open(filepath))
        m = cls()
        m.tags = d['tags']
        m.df = pd.read_csv(io.StringIO(d['data'])).set_index('elapsed')
        return m