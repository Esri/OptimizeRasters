# ------------------------------------------------------------------------------
# Copyright 2024 Esri
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ------------------------------------------------------------------------------
# Name: CleanMRFCache.py
# Description: Cleans MRF Cache files by oldest access-time until free space
# requested has been achieved.
# Version: 20240507
# Requirements: Python
# Required Arguments: -input
# Optional Arguments: -mode -ext -size
# e.g.: -mode = [del,scan], -ext=txt,mrfcache -input=d:/mrfcache_folder
# Usage: python.exe CleanMRFCache.py <arguments>
# Author: Esri Imagery Workflows team
# ------------------------------------------------------------------------------
#!/usr/bin/env python

import sys
import operator
import argparse
import os
import ctypes
import platform


def Message(msg, status=0):
    try:
        if (log is not None):
            log.Message(msg, status)
            return
    except:
        pass
    print(msg)
   # for any paprent processes to receive the stdout realtime.
    sys.stdout.flush()


class Cleaner:
    def __init__(self):
        pass

    def init(self, input_path, extensions=()):
        self.m_extensions = extensions
        self.m_input_path = input_path.replace('\\', '/')
        if (self.m_input_path.endswith('/') is False):
            self.m_input_path += '/'
        self.m_info = []
        return True

    def getFreeDiskSpace(self, input_path):      # static
        try:
            fbytes = ctypes.c_ulonglong(0)
            ctypes.windll.kernel32.GetDiskFreeSpaceExW(
                ctypes.c_wchar_p(input_path),
                None,
                None,
                ctypes.pointer(fbytes))
        except:
            return -1
        return fbytes

    def getFileInfo(self, root_only=False):
        Message('[Generate file list]..')
        for r, d, f in os.walk(self.m_input_path):
            if (root_only):
                if (r != self.m_input_path):
                    continue
            for file in f:
                (f_, e_) = os.path.splitext(file)
                if ((e_[1:].lower() in self.m_extensions)):
                    mk_path = os.path.join(r, file).replace('\\', '/')

                    self.m_info.append({
                        'f': mk_path,
                        's': os.path.getsize(mk_path),
                        'at': os.path.getatime(mk_path)
                    })
                    try:
                        pass
                    except Exception as exp:
                        Message('Err: (%s)' % (str(inf)))
        return True


def main():
    pass


if __name__ == '__main__':
    main()


if __name__ == '__main__':
    main()

__program_ver__ = 'v1.0'
__program_name__ = 'CleanMRFCache.py %s' % __program_ver__

parser = argparse.ArgumentParser(description='Cleans MRF Cache files by '
                                 'oldest access-time until free space '
                                 'requested has been achieved.\n')

parser.add_argument('-input', help='Input directory', dest='input_path')
parser.add_argument('-mode', help='Processing mode. Valid modes [del]',
                    dest='mode', default='scan')
parser.add_argument('-ext',
                    help='Extensions to filter-in. e.g. -ext=mrfcache,txt',
                    dest='ext')
parser.add_argument('-size', type=int,
                    help='Free size requested in Gigabytes. e.g. -size=1',
                    dest='size', default=2000000000)

log = None

Message(__program_name__)
Message(parser.description)

args = parser.parse_args()

extensions = ['mrfcache']

# check for extensions
if (args.ext is not None):
    ext_ = args.ext.split(',')
    for e in ext_:
        e = e.strip().lower()
        if ((e in extensions) is False):
            extensions.append(e)
# ends

# check input path
if (args.input_path is None):
    Message('Err: -input is required.')
    exit(0)
# ends

# clean-up instance
cln = Cleaner()
cln.init(args.input_path, extensions)
# ends

# let's get the free space
space_available = cln.getFreeDiskSpace(os.path.dirname(args.input_path))
if (space_available == -1):  # an error has occured
    Message('Err: Unable to get the free-disk-space for the path (%s)' %
            (args.input_path))
    exit(1)
# ends

space_to_free = args.size * 1000000000
space_available = space_available.value

if (space_available >= space_to_free):
    Message('The disk already has the requested free space')
    exit(0)

# setup -mode
is_mode = not args.mode is None
arg_mode = args.mode.lower()

Message('Mode (%s)' % arg_mode)  # display the user/default selected (-mode)
# ends

ret = cln.getFileInfo()
if (ret is False):
    Message('Err: Unable to scan for files. Quitting..')
    exit(1)

process = sorted(cln.m_info, key=operator.itemgetter('at'), reverse=False)

print('\nResults:')
tot_savings = 0
for f in process:
    print('%s [%s] [%s]' % (f['f'], f['s'], f['at']))
    tot_savings += f['s']
    if (is_mode):
        if (arg_mode == 'del'):
            Message('[Del] %s' % (f['f']))
            # let's delete here.
            try:
                pass
                os.remove(f['f'])
            except Exception as exp:
                Message('Err: Unable to remove (%s). Skipping..' % (f['f']))
                continue
            space_available += f['s']
            if (space_available >= space_to_free):
                pass
                Message('\nRequired disk space has been freed.')
                break
            # ends

msg = '\nTotal savings if files get deleted: [%d] bytes.' % (tot_savings)
if (arg_mode == 'del'):
    msg = '\nTotal space freed [%d] bytes' % (space_available)
    if (space_available < space_to_free):
        Message('\nUnable to free space requested.')

Message(msg)

Message('\nDone..')
