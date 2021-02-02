import sys
import os
import time
import json
import subprocess
import inspect
import warnings
import shutil
import io
from zipfile import ZipFile
from pathlib import Path
from urllib.parse import urlparse, unquote

import requests
import psutil
from psutil._common import bytes2human



def download_file(url, dir=None, fname=None, overwrite=False, verbose=True):
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
            for chunk in r.iter_content(chunk_size=2**20):
                f.write(chunk)
    
    if verbose: print(f'Download complete: {fname}.')
    return fpath

def unzip(fpath, dst=None, overwrite=False, verbose=True):
    """Extract all memberfs of Zip archive `fpath` into `dst` directory (current working directory by default)."""
    dst = Path('.' if dst is None else dst)
    with ZipFile(fpath) as zf:
        count = 0
        for member in zf.namelist():
            member_path = dst / member
            if overwrite or not member_path.exists():
                zf.extract(member, dst)
                count += 1
        if verbose: print(f'Extracted {count} files from {fpath.name}.')


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
        elif psutil.WINDOWS:
            x = p.io_counters()
            return (x.read_bytes, 0, x.write_bytes, 0)
        else:
            x = p.io_counters()
            return (x.read_bytes, x.read_chars, x.write_bytes, x.write_chars)

    print('time,cpu,memory,read_bytes,read_chars,write_bytes,write_chars')
    p.cpu_percent()
    io_before = get_io()
    while True:
        io_after = get_io()
        io_rate = tuple((x1 - x0) / interval for x0, x1 in zip(io_before, io_after))
        io_before = io_after
        line = (time.time(), p.cpu_percent(), p.memory_info().rss) + io_rate
        print(','.join(str(x) for x in line), flush=True)
        time.sleep(interval)


class ResourceMonitor:
    def __init__(self, pid=None, interval=1):
        self.pid = os.getpid() if pid is None else pid
        self.interval = interval
        self.tags = []
        self.df = None

    def start(self):
        code = inspect.getsource(usage_log) + f'\nusage_log({self.pid}, {self.interval})'
        self.process = subprocess.Popen([sys.executable, '-c', code], text=True,
                                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    def stop(self):
        self.process.terminate()
        import pandas as pd
        log_data = self.process.stdout.read()
        if log_data.count('\n') < 2:
            warnings.warn('ResourceMonitor: no entries in monitor log, execution time may be too short.')
            return            
        df = pd.read_csv(io.StringIO(log_data))
        df['elapsed'] = df['time'] - df.loc[0, 'time']
        self.df = df.set_index('elapsed')

    def tag(self, label):
        self.tags.append((time.time(), label))

    def plot(self):
        if self.df is None:
            print('ResourceMonitor: no entries in monitor log, execution time may be too short.')
            return
        
        import matplotlib.pyplot as plt
        
        # newer versions of mpl show a warning on ax.set_yticklabels()
        # other ways to fix the problem:
        # https://stackoverflow.com/questions/63723514/userwarning-fixedformatter-should-only-be-used-together-with-fixedlocator
        warnings.filterwarnings('ignore', message='FixedFormatter should only be used together with FixedLocator')
        
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
        d = json.load(open(filepath))
        m = cls()
        m.tags = d['tags']
        m.df = pd.read_csv(io.StringIO(d['data'])).set_index('elapsed')
        return m


state_00_aa = {'01': 'AL',
               '02': 'AK',
               '04': 'AZ',
               '05': 'AR',
               '06': 'CA',
               '08': 'CO',
               '09': 'CT',
               '10': 'DE',
               '11': 'DC',
               '12': 'FL',
               '13': 'GA',
               '15': 'HI',
               '16': 'ID',
               '17': 'IL',
               '18': 'IN',
               '19': 'IA',
               '20': 'KS',
               '21': 'KY',
               '22': 'LA',
               '23': 'ME',
               '24': 'MD',
               '25': 'MA',
               '26': 'MI',
               '27': 'MN',
               '28': 'MS',
               '29': 'MO',
               '30': 'MT',
               '31': 'NE',
               '32': 'NV',
               '33': 'NH',
               '34': 'NJ',
               '35': 'NM',
               '36': 'NY',
               '37': 'NC',
               '38': 'ND',
               '39': 'OH',
               '40': 'OK',
               '41': 'OR',
               '42': 'PA',
               '44': 'RI',
               '45': 'SC',
               '46': 'SD',
               '47': 'TN',
               '48': 'TX',
               '49': 'UT',
               '50': 'VT',
               '51': 'VA',
               '53': 'WA',
               '54': 'WV',
               '55': 'WI',
               '56': 'WY',
               '60': 'AS',
               '64': 'FM',
               '66': 'GU',
               '68': 'MH',
               '69': 'MP',
               '70': 'PW',
               '72': 'PR',
               '74': 'UM',
               '78': 'VI'}

state_aa_00 = {l:c for c,l in state_00_aa.items()}
