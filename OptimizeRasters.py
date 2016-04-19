#------------------------------------------------------------------------------
# Copyright 2016 Esri
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
#------------------------------------------------------------------------------
# Name: OptimizeRasters.py
# Description: Optimizes rasters via gdal_translate/gdaladdo
# Version: 20160419
# Requirements: Python
# Required Arguments: -input -output
# Optional Arguments: -mode -cache -config -quality -prec -pyramids
# -tempinput -tempoutput -subs -clouddownload -cloudupload
# -inputprofile -outputprofile -op -job -inputprofile -outputprofile
# -inputbucket -outputbucket -clonepath -clouddownloadtype -clouduploadtype
# Usage: python.exe OptimizeRasters.py <arguments>
# Note: OptimizeRasters.xml (config) file is placed alongside OptimizeRasters.py
# OptimizeRasters.py is entirely case-sensitive, extensions/paths in the config
# file are case-sensitive and the program will fail if the correct paths are not
# entered at the cmd-line/UI or in the config file.
# Author: Esri Imagery Workflows team
#------------------------------------------------------------------------------
#!/usr/bin/env python

import sys
import os
import base64

import mmap
import threading
import time

from xml.dom import minidom
import subprocess
import shutil
import datetime

import argparse
import math
import ctypes

# enum error codes
eOK = 0
eFAIL = 1
# ends

# const related to (Reporter) class
CRPT_SOURCE = 'SOURCE'
CRPT_COPIED = 'COPIED'
CRPT_PROCESSED = 'PROCESSED'
CRPT_UPLOADED = 'UPLOADED'
CRPT_HEADER_KEY = 'config'

CRPT_YES = 'yes'
CRPT_NO = 'no'
CRPT_UNDEFINED = ''
# ends

# user hsh const
USR_ARG_UPLOAD = 'upload'
USR_ARG_DEL = 'del'
# ends

# Del delay
CDEL_DELAY_SECS = 20
# ends

CPRJ_NAME = 'ProjectName'
CLOAD_RESTORE_POINT = '__LOAD_RESTORE_POINT__'
CCMD_ARG_INPUT = '__CMD_ARG_INPUT__'

CUSR_TEMPINPUT = 'tempinput'

# utility const
CSIN_UPL = 'SIN_UPL'
CINC_SUB = 'INC_SUB'

COP_UPL = 'upload'
COP_DNL = 'download'
COP_RPT = 'report'
COP_NOCONVERT  = 'noconvert'
# ends

# clone specific
CCLONE_PATH = 'clonepath'
# ends

# -cache path
CCACHE_PATH = 'cache'
# ends

# resume constants
CRESUME = '_RESUME_'
CRESUME_MSG_PREFIX = '[Resume]'
CRESUME_ARG = 'resume'
CRESUME_HDR_INPUT = 'input'
# ends

CINPUT_PARENT_FOLDER = 'Input_ParentFolder'

# const node-names in the config file

CCLOUD_AMAZON = 'amazon'
CCLOUD_AZURE = 'azure'

# Azure constants
COUT_AZURE_PARENTFOLDER = 'Out_Azure_ParentFolder'
COUT_AZURE_ACCOUNTNAME = 'Out_Azure_AccountName'
COUT_AZURE_ACCOUNTKEY = 'Out_Azure_AccountKey'
COUT_AZURE_CONTAINER = 'Out_Azure_Container'
COUT_AZURE_ACCESS = 'Out_Azure_Access'
COUT_AZURE_PROFILENAME = 'Out_Azure_ProfileName'
CIN_AZURE_PARENTFOLDER = 'In_Azure_ParentFolder'
# ends

CCLOUD_UPLOAD_THREADS = 20          # applies to both (azure and amazon/s3)
CCLOUD_UPLOAD = 'CloudUpload'
CCLOUD_UPLOAD_OLD_KEY =  'Out_S3_Upload'
COUT_CLOUD_TYPE = 'Out_Cloud_Type'
COUT_S3_PARENTFOLDER = 'Out_S3_ParentFolder'
COUT_S3_ACL = 'Out_S3_ACL'
CIN_S3_PARENTFOLDER = 'In_S3_ParentFolder'
CIN_S3_PREFIX = 'In_S3_Prefix'
CIN_CLOUD_TYPE = 'In_Cloud_Type'
COUT_VSICURL_PREFIX = 'Out_VSICURL_Prefix'
CINOUT_S3_DEFAULT_DOMAIN = 's3.amazonaws.com'
# ends

# const
CCFG_FILE = 'OptimizeRasters.xml'
CCFG_GDAL_PATH = 'GDALPATH'
# ends

# til related
CTIL_EXTENSION_ = '.til'
CCACHE_EXT =  '.mrf_cache'
CMRF_DOC_ROOT = 'MRF_META'
# ends

# global dbg flags
CS3_MSG_DETAIL = False
CS3_UPLOAD_RETRIES = 3
# ends

# S3Storage direction
CS3STORAGE_IN = 0
CS3STORAGE_OUT = 1
# ends

class MEMORYSTATUSEX(ctypes.Structure):
    _fields_ = [
        ("dwLength", ctypes.c_ulong),
        ("dwMemoryLoad", ctypes.c_ulong),
        ("ullTotalPhys", ctypes.c_ulonglong),
        ("ullAvailPhys", ctypes.c_ulonglong),
        ("ullTotalPageFile", ctypes.c_ulonglong),
        ("ullAvailPageFile", ctypes.c_ulonglong),
        ("ullTotalVirtual", ctypes.c_ulonglong),
        ("ullAvailVirtual", ctypes.c_ulonglong),
        ("sullAvailExtendedVirtual", ctypes.c_ulonglong),
    ]
    def __init__(self):
        self.dwLength = ctypes.sizeof(self)
        super(MEMORYSTATUSEX, self).__init__()
    def memoryStatus(self):
        ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(self))
        return self
    def memoryPerDownloadChunk(self):
        return int(self.memoryStatus().ullAvailPhys * .01)  # download file isn't split in chunks, for now usage is set to 0.01
    def memoryPerUploadChunk(self, totalThreads):    # get upload payload size per thread for the total cloud upload threads required.
        return int(self.memoryStatus().ullAvailPhys * .10 / totalThreads)

class Base(object):
    # log status types enums
    const_general_text = 0
    const_warning_text = 1
    const_critical_text = 2
    const_status_text = 3
    # ends
    def __init__(self, msgHandler = None, msgCallback = None, userConfig = None):
        self._m_log  = msgHandler
        self._m_msg_callback = msgCallback
        self._m_user_config = userConfig
        if (self._m_msg_callback):
            if (self._m_log):
                self._m_log.isPrint = False
    def init(self):
        return True
    def message(self, msg, status = const_general_text):
        if (self._m_log):
            self._m_log.Message(msg, status)
        if (self._m_msg_callback):
            self._m_msg_callback(msg, status)
    def convertToForwardSlash(self, input, endSlash = True):
        if (not input):
            return None
        _input = input.replace('\\', '/').strip()
        if (endSlash and
             not _input.endswith('/') and
             not _input.lower().startswith('http')):
            _input += '/'
        return _input
    def getBooleanValue(self, value):        # helper function
        if (value is None):
            return False
        if (isinstance(value, bool) == True):
            return value
        val = value.lower()
        if (val == 'true' or
            val == 'yes' or
            val == 't' or
            val == '1' or
            val == 'y'):
            return True
        return False
    @property
    def getUserConfiguration(self):
        return self._m_user_config
    @property
    def getMessageHandler(self):
        return self._m_log
    @property
    def getMessageCallback(self):
        return self._m_msg_callback
    def close(self):
        if (self._m_log):
            self._m_log.WriteLog('#all')   #persist information/errors collected.
    def S3Upl(self, input_file, user_args, *args):
        global _rpt
        if (_rpt):
            _source_path = getSourcePathUsingTempOutput(input_file)
            if (_source_path):
                _ret_val = _rpt.getRecordStatus(_source_path, CRPT_UPLOADED)
                if (_ret_val == CRPT_YES):
                    return True
        ret_buff = []
        internal_err_msg = 'Internal error at [S3Upl]'
        upload_cloud_type = cfg.getValue(COUT_CLOUD_TYPE, True)
        if (upload_cloud_type == CCLOUD_AMAZON):
            if (S3_storage is None):    # globally declared: S3_storage
                self.message (internal_err_msg, const_critical_text)
                return False
            _single_upload = _include_subs = False    # def
            if (user_args):
                if (CSIN_UPL in user_args):
                    _single_upload = getBooleanValue(user_args[CSIN_UPL])
                if (CINC_SUB in user_args):
                    _include_subs = getBooleanValue(user_args[CINC_SUB])
            ret_buff = S3_storage.upload_group(input_file, single_upload = _single_upload, include_subs = _include_subs)
            if (len(ret_buff) == 0):
                return False
        elif (upload_cloud_type == CCLOUD_AZURE):
            if(azure_storage is None):
                self.message (internal_err_msg, const_critical_text)
                return False
            properties = {
            'tempoutput' : cfg.getValue('tempoutput', False),
            'access' : cfg.getValue(COUT_AZURE_ACCESS, True)
            }
            if (True in [input_file.endswith(x) for x in cfg.getValue(CCFG_RASTERS_NODE)]):
                _input_file = input_file.replace('\\', '/')
                (p, n) = os.path.split(_input_file)
                indx = n.find('.')
                file_name_prefix = n
                if (indx >= 0): file_name_prefix = file_name_prefix[:indx]
                input_folder = os.path.dirname(_input_file)
                for r,d,f in os.walk(input_folder):
                    r = r.replace('\\', '/')
                    if (r == input_folder):
                        for _file in f:
    ##                        if (_file.endswith('.lrc')):
    ##                            continue
                            if (_file.startswith(file_name_prefix)):
                                file_to_upload = os.path.join(r, _file)
                                if (azure_storage.upload(
                                file_to_upload,
                                cfg.getValue (COUT_AZURE_CONTAINER, False),
                                cfg.getValue (CCFG_PRIVATE_OUTPUT, False),
                                properties
                                )):
                                    ret_buff.append(file_to_upload)
                        break
            else:
                if (azure_storage.upload(
                input_file,
                cfg.getValue (COUT_AZURE_CONTAINER, False),
                cfg.getValue (CCFG_PRIVATE_OUTPUT, False),
                properties
                )):
                    ret_buff.append(input_file)
        if (CS3_MSG_DETAIL == True):
            self.message ('Following file(s) uploaded to ({})'.format(CCLOUD_AMAZON if upload_cloud_type == CCLOUD_AMAZON else CCLOUD_AZURE))
            [self.message ('{}'.format(f)) for f in ret_buff]
        if (user_args != None):
            if (USR_ARG_DEL in user_args):
                if (user_args[USR_ARG_DEL] and
                    user_args[USR_ARG_DEL] == True):
                    for f in ret_buff:
                        try:
                            _is_remove = True
                            if (til):
                                if (til.fileTILRelated(f)):
                                    _is_remove = False
                            if (_is_remove):
                                try:
                                    os.remove(f)
                                except:
                                    time.sleep(CDEL_DELAY_SECS)
                                    os.remove(f)
                                self.message ('[Del] %s' % (f))
                        except Exception as exp:
                            self.message ('[Del] Err. (%s)' % (str(exp)), const_critical_text)
        if (ret_buff):
            setUploadRecordStatus (input_file, CRPT_YES)
        return (len(ret_buff) > 0)

class GDALInfo(object):
    CGDAL_ADDO_EXE = 'gdalinfo.exe'
    CW = 'width'
    CH = 'height'
    def __init__ (self, base, msgCallback = None):
        self._GDALPath =  None
        self._GDALInfo = []
        self._propertyNames = [self.CW, self.CH]
        self._base = base
        self._m_msg_callback = msgCallback
    def init(self, GDALPath):
        if (not GDALPath):
            return False
        self._GDALPath = GDALPath.replace('\\', '/')
        if (not self._GDALPath.endswith('/{}'.format(self.CGDAL_ADDO_EXE))):
            self._GDALPath = os.path.join(self._GDALPath, self.CGDAL_ADDO_EXE).replace('\\', '/')
        # check for path existence / e.t.c
        if (not os.path.exists(self._GDALPath)):
            self.message('Invalid GDALInfo/Path ({})'.format(self._GDALPath), self._base.const_critical_text)
            return False
        if (self._base and
            not isinstance(self._base, Base)):
            return False
        for p in self._propertyNames:       # init-property names
            setattr(self, p, None)
        return True
    def process(self, input):
        if (not self._GDALPath):
            self.message('Not initialized!', self._base.const_critical_text)
            return False
        if (not input):             # invalid input
            return False
        args = [self._GDALPath]
        args.append (input)
        self.message('Using GDALInfo..', self._base.const_general_text)
        return self.__call_external(args)
    def message(self, msg, status):
        self._m_msg_callback(msg, status) if self._m_msg_callback else self._base.message(msg, status)
    @property
    def pyramidLevels(self):
        if (not self.width or
            not self.height):
            return False        # fn/process not called.
        _max = max(self.width, self.height)
        _BS = CCFG_BLOCK_SIZE       # def (512)
        if (self._base.getUserConfiguration):
            __BS = self._base.getUserConfiguration.getValue('BlockSize')
            if (__BS):
                try:
                    _BS = int(__BS)     # catch invalid val types
                except:
                    pass
        _levels = int(2 ** math.ceil(math.log(_max / _BS, 2)))
        _steps = ''
        while (_levels >= 2):
            _steps = '{} {}'.format(_levels, _steps)
            _levels >>= 1
        _steps = _steps.strip()
        self.message ('<PyramidFactor> set to ({})'.format(_steps), self._base.const_general_text)
        return _steps
    def __call_external(self, args):
        p = subprocess.Popen(args, creationflags=subprocess.SW_HIDE, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        message = '/'
        first_pass_ = True
        CSIZE_PREFIX = 'Size is'
        while (message):
            message = p.stdout.readline()
            if (message):
                _strip = message.strip()
                if (_strip.find(CSIZE_PREFIX) != -1):
                    wh  = _strip.split(CSIZE_PREFIX)
                    if (len(wh) > 1):
                        wh  = wh[1].split(',')
                        if (self.CW in self._propertyNames):
                            self.width = int(wh[0].strip())
                        if (self.CH in self._propertyNames):
                            self.height = int(wh[1].strip())
                self._GDALInfo.append(_strip)
        return len(self._GDALInfo) > 0

class UpdateMRF:
    def __init__(self, base = None):
        self._mode = \
        self._cachePath = \
        self._input = \
        self._output = \
        self._homePath = \
        self._outputURLPrefix = None
        self._base = base
    def init(self, input, output, mode = None,
        cachePath = None, homePath = None, outputURLPrefix = None):
        if (not input or
            not output):
            return False
        if (not os.path.exists(output)):
            return False
        if (input.rfind('.') == -1):
            return False
        if (self._base and
            not isinstance(self._base, Base)):
                return False
        self._or_mode = self._base.getUserConfiguration.getValue('Mode')    # mode/output
        if (not self._or_mode):
            self._base.message('UpdateMRF> (Mode) not defined.', self._base.const_critical_text)
            return False
        _type = self._or_mode.split('_')
        if (len(_type) > 1):
            self._or_mode = _type[0]
        if (self._or_mode.endswith('mrf')):         # to trap modes (cachingmrf/clonemrf).
            self._or_mode = 'mrf'
        self._mode = mode
        self._input = self._convertToForwardSlash(input)
        self._output = self._convertToForwardSlash(output)
        self._cachePath = self._convertToForwardSlash(cachePath)
        self._homePath = self._convertToForwardSlash(homePath)
        self._outputURLPrefix = self._convertToForwardSlash(outputURLPrefix)
        return True
    def _convertToForwardSlash(self, input):
        if (not input):
            return None
        return input.replace('\\', '/')
    def copyInputMRFFilesToOutput(self, doUpdate = True):
        if (not self._input or
            not self._output):
            if (self._base):
                self._base.message('Not initialized!', self._base.const_critical_text)
            return False
        _prefix  = self._input[:self._input.rfind('.')]
        input_folder = os.path.dirname(self._input)
        for r,d,f in os.walk(input_folder):
            r = r.replace('\\', '/')
            if (r == input_folder):
                for _file in f:
                    if (True in [_file.lower().endswith(x) for x in ['.lrc', '.idx', '.pjg', '.ppng', '.pft', '.pjp', '.pzp']]):
                        continue
                    _mk_path = r + '/' + _file
                    if (_mk_path.startswith(_prefix)):
                        try:
                            _output_path = self._output
                            if (self._homePath):
                                _output_path = os.path.join(self._output, os.path.dirname(self._input.replace(self._homePath, '')))
                            if (not os.path.exists(_output_path)):
                                os.makedirs(_output_path)
                            _mk_copy_path = os.path.join(_output_path, _file).replace('\\', '/')
                            if (_file.lower() == os.path.basename(self._input).lower()):
                                if (doUpdate):
                                    if (not self.update(_mk_copy_path)):
                                        if (self._base):
                                            self._base.message('Updating ({}) failed!'.format(_mk_copy_path), self._base.const_critical_text)
                                continue
                            if (_mk_path.lower().endswith(self._or_mode) or
                                _mk_path.lower().endswith('.ovr')):
                                continue
                            if (not os.path.exists(_mk_copy_path)):
                                shutil.copy(_mk_path, _mk_copy_path)
                        except Exception as e:
                            if (self._base):
                                self._base.message('-clonepath/{}'.format(str(e)), self._base.const_critical_text)
                            continue
    def update(self, output):
        try:
            _CCACHE_EXT =  '.mrf_cache'
            _CDOC_ROOT = 'MRF_META'
            comp_val =  None         # for (splitmrf)
            doc = minidom.parse(self._input)
            _rasterSource = self._input
            if (self._outputURLPrefix and
                self._homePath):
                _rasterSource = '{}{}'.format(self._outputURLPrefix, _rasterSource.replace(self._homePath, ''))
            nodeMeta = doc.getElementsByTagName(_CDOC_ROOT)
            nodeRaster = doc.getElementsByTagName('Raster')
            if (not nodeMeta or
                not nodeRaster):
                raise Exception('Err. Invalid header')
            cachedNode = doc.getElementsByTagName('CachedSource')
            if (not cachedNode):
                cachedNode.append(doc.createElement('CachedSource'))
                nodeSource = doc.createElement('Source')
                nodeSource.appendChild(doc.createTextNode(_rasterSource))
                cachedNode[0].appendChild(nodeSource)
                nodeMeta[0].insertBefore(cachedNode[0], nodeRaster[0])
            if (self._mode):
                if (self._mode.startswith('mrf') or
                    self._mode == 'clonemrf'):
                    node = doc.getElementsByTagName('Source')
                    if (node):
                        node[0].setAttribute('clone', 'true')
                elif(self._mode == 'splitmrf'):
                    CONST_LBL_COMP = 'Compression'
                    node = doc.getElementsByTagName(CONST_LBL_COMP)
                    if (node):
                        if (node[0].hasChildNodes()):
                            comp_val = node[0].firstChild.nodeValue.lower()
            cache_output = self._base.convertToForwardSlash(os.path.dirname(output))
            if (self._cachePath):
                cache_output = self._cachePath
            if (not self._base.getUserConfiguration):
                raise Exception('Err/Internal. UpdateMRF/getUserConfiguration')
            cacheSubFolders = ''
            if (self._cachePath):
                cacheSubFolders = self._base.convertToForwardSlash(os.path.dirname(output)).replace(self._output if self._cachePath else self._homePath, '')
            (f, ext) = os.path.splitext(os.path.basename(self._input))
            rep_data_file = rep_indx_file = os.path.abspath('{}{}{}{}'.format(cache_output, cacheSubFolders, f, _CCACHE_EXT)).replace('\\', '/') # Get abs path in case the -output was relative for cache to function properly.
            nodeData = nodeIndex = None
            if (comp_val):
                extensions_lup = {
                'lerc' : {'data' : '.lrc', 'index' : '.idx' }
                }
            nodeData = nodeRaster[0].getElementsByTagName('DataFile')
            if (not nodeData):
                nodeData.append(doc.createElement('DataFile'))
                nodeData[0].appendChild(doc.createTextNode(''))
                nodeRaster[0].appendChild(nodeData[0])
            nodeIndex = nodeRaster[0].getElementsByTagName('IndexFile')
            if (not nodeIndex):
                nodeIndex.append(doc.createElement('IndexFile'))
                nodeIndex[0].appendChild(doc.createTextNode(''))
                nodeRaster[0].appendChild(nodeIndex[0])
            if (nodeData):
                if (comp_val and
                    comp_val in extensions_lup):
                    rep_data_file = rep_data_file.replace(_CCACHE_EXT, extensions_lup[comp_val]['data'])
                nodeData[0].firstChild.nodeValue = rep_data_file
            if (nodeIndex):
                if (comp_val and
                    comp_val in extensions_lup):
                    rep_indx_file = rep_indx_file.replace(_CCACHE_EXT, extensions_lup[comp_val]['index'])
                nodeIndex[0].firstChild.nodeValue = rep_indx_file
            with open (output, "w") as c:
                _mrfBody = doc.toxml().replace('&quot;', '"')       # GDAL mrf driver can't handle XML entity names.
                _indx = _mrfBody.find ('<{}>'.format(_CDOC_ROOT))
                if (_indx == -1):
                    raise Exception('Err. Invalid MRF/header')
                _mrfBody = _mrfBody[_indx:]
                c.write(_mrfBody)
        except Exception as e:
            if (self._base):
                self._base.message('Updating ({}) was not successful!\nPlease make sure the input is (MRF) format.\n{}'.format(output, str(e)), self._base.const_critical_text)
            return False
        return True

class Report:
    CHEADER_PREFIX = '#'
    CJOB_EXT = '.orjob'
    CVSCHAR  = '\t'
    CRPT_URL_TRUENAME = 'URL_NAME'
    def __init__(self, base):
        self._input_list = []
        self._input_list_info = {}
        self._header = {}
        self._base = base
        self._isInputHTTP = False
    def init(self, report_file, root = None):
        if (not self._base or
            not isinstance(self._base, Base)):
                return False
        if (not report_file):
            return False
        if (not report_file.lower().endswith(self.CJOB_EXT)):
            return False
        self._report_file = report_file
        if (root):
            _root = root.replace('\\', '/')
            if (root.lower().startswith('http://') or
                root.lower().startswith('https://')):
                    self._input_list.append(_root)
                    return True
            if (_root[-1:] != '/'):
                _root += '/'
            self._input_list.append(_root)          # first element in the report is the -input path to source
        return True
    def getRecordStatus(self, input, type):         # returns (true or false)
        if (input is None or
            type is None):
            return CRPT_UNDEFINED
        try:
            return (self._input_list_info[input][type.upper()])
        except:
            pass
        return CRPT_UNDEFINED
    @staticmethod
    def getUniqueFileName():
        from datetime import datetime
        _dt =  datetime.now()
        _prefix = 'OR'
        _jobName  = _prefix + "_%04d%02d%02dT%02d%02d%02d%06d" % (_dt.year, _dt.month, _dt.day, \
        _dt.hour, _dt.minute, _dt.second, _dt.microsecond)
        return _jobName
    def updateRecordStatus(self, input, type, value):  # input is the (src) path name which is case sensitive.
        if (input is None or
            type is None or
            value is None):
            return False
        _input = input.strip()
        if (CUSR_TEMPINPUT in self._header):
            if (_input.startswith(self._header[CUSR_TEMPINPUT])):
                _input = _input.replace(self._header[CUSR_TEMPINPUT], self.root)
                (p, e) = os.path.split(_input)
                for _k in self._input_list_info:
                    if (_k.startswith(p)):
                        if (self.CRPT_URL_TRUENAME in self._input_list_info[_k]):
                            if (self._input_list_info[_k][self.CRPT_URL_TRUENAME] == e):
                                _input = _k
                                break
        _path = os.path.dirname(_input.replace('\\', '/'))
        if (not _path.endswith('/')):
            _path += '/'
        if (_path == self._header['output']):
            _input  = _input.replace(_path, self._header[CRESUME_HDR_INPUT])
        (p, e) = os.path.splitext(_input)
        while(e):
            _input = '{}{}'.format(p, e)
            if (_input in self._input_list_info):
                break
            (p, e) = os.path.splitext(p)
        if (not e):
            if (self.CRPT_URL_TRUENAME in self._input_list_info[_input]):
                (p, e) = os.path.splitext(self._input_list_info[_input][self.CRPT_URL_TRUENAME])
            if (not e): # still no extension?
                self._base.message('Invalid input ({}) at (Reporter)'.format (_input), self._base.const_warning_text)
                return False
        _type = type.upper()
        if (not _type in [CRPT_COPIED, CRPT_PROCESSED, CRPT_UPLOADED]):
            self._base.message('Invalid type ({}) at (Reporter)'.format(type), self._base.const_critical_text)
            return False
        _value = value.lower()
        if (not _value in [CRPT_YES, CRPT_NO]):
            self._base.message('Invalid value ({}) at (Reporter)'.format(_value), self._base.const_critical_text)
            return False
        self._input_list_info[_input][_type] = _value
        return True
    def addHeader(self, key, value):
        if (not key or
            not value):
            return False
        self._header[key.lower()] = value
        return True
    def removeHeader(self, key):
        if (not key):
            return False
        if (not key.lower() in self._header):
            return False
        del self._header[key.lower()]
        return True
    def addFile(self, file):
        if (not file):
            return False
        _file = file.replace('\\', '/')
        _get_store =  self.findWith(_file)
        if (_get_store and
             _get_store == _file):
            return False        # no duplicate entires allowed.
        self._input_list.append(_file)
        return True
    @property
    def operation(self):
        if (not 'op' in self._header):
            return None
        return self._header['op'].lower() if (self._header['op']) else None
    @property
    def root(self):
        if (not self._input_list):
            return ''
        _root = self._input_list[0]
        if (CRESUME_HDR_INPUT in self._header):
            _root  = self._header[CRESUME_HDR_INPUT]
            if (_root.lower().startswith('http')):
                if (not _root.endswith('/')):
                    _root += '/'
        return _root
    def read(self, readCallback = None):
        try:
            with open(self._report_file , 'r') as _fptr:
                ln = _fptr.readline()
                hdr_skipped = False
                while(ln):
                    ln = ln.strip()
                    if (not ln or
                        ln.startswith('##')):        # ignore empty-lines and comment lines (beginning with '##')
                        ln = _fptr.readline()
                        continue
                    if (readCallback):      # client side callback support.
                        readCallback(ln)
                    lns = ln.split(self.CVSCHAR)
                    _fname = lns[0].strip().replace('\\', '/')
                    if (_fname.startswith(self.CHEADER_PREFIX)):
                        _hdr = _fname.replace(self.CHEADER_PREFIX, '').split('=')
                        if (len(_hdr) > 1):
                            self.addHeader(_hdr[0].strip(), _hdr[1].strip())
                            ln = _fptr.readline()
                            continue
                    if (not _fname or
                        not hdr_skipped):        # do not accept empty lines.
                        if (len(lns) == 4):      # skip line if it's the column header without the '#' prefix?
                            ln = _fptr.readline()
                        if (_fname):
                            hdr_skipped = True
                            if (CRESUME_HDR_INPUT in self._header):
                                _input = self._header[CRESUME_HDR_INPUT].lower()
                                if (_input.startswith('http://') or
                                    _input.startswith('https://')):
                                    self._isInputHTTP = True
                        continue
                    _copied = '' if len(lns) <= 1 else lns[1].strip()       # for now, previously stored status values aren't used.
                    _processed  = '' if len(lns) <= 2 else lns[2].strip()
                    _uploaded = '' if len(lns) <= 3 else lns[3].strip()
                    if (self.addFile(_fname)):
                        self._input_list_info[_fname] = {
                            CRPT_COPIED : _copied,
                            CRPT_PROCESSED : _processed,
                            CRPT_UPLOADED : _uploaded
                            }
                    ln = _fptr.readline()
        except Exception as exp:
            self._base.message('{}'.format(str(exp)), self._base.const_critical_text)
            return False
        return True
    def findExact(self, input):
        if (not self._input_list):
            return None
        for f in self._input_list:
            if (f == input):
                return f
        return None
    def findWith(self, input):
        if (not self._input_list):
            return None
        for f in self._input_list:
            if (f.find(input) != -1):
                return f
        return None
    def moveJobFileToPath(self, path):  # successful job files can be moved over to a given folder.
        if (not path):
            return False
        try:
            get_tile = os.path.basename(self._report_file)
            mk_path = os.path.join(path, get_tile)
            if (not os.path.exists(path)):
                os.makedirs(path)
            self._base.message('[MV] {}'.format(mk_path))
            shutil.move(self._report_file, mk_path)
        except Exception as e:
            self._base.message('({})'.format(str(e)), self._base.const_critical_text)
            return False
        return True
    def hasFailures(self):
        if (not self._input_list):
            return False
        for f in self:
            if (self._input_list_info[f][CRPT_COPIED] == CRPT_NO or
                self._input_list_info[f][CRPT_PROCESSED] == CRPT_NO or
                self._input_list_info[f][CRPT_UPLOADED] == CRPT_NO):
                    return True
        return False
    def write(self):
        try:
            CCSV_HEADER_ = 'csv_header'
            _frmt = '{}/{}/{}/{}\n'.replace('/', self.CVSCHAR)
            with open(self._report_file , 'w+') as _fptr:
                for key in self._header:
                    _fptr.write('{} {}={}\n'.format(self.CHEADER_PREFIX, key, self._header[key]))
                _fptr.write(_frmt.format(CRPT_SOURCE, CRPT_COPIED, CRPT_PROCESSED, CRPT_UPLOADED))
                for f in self._input_list:
                    _fptr.write(_frmt.format(f,
                    self._input_list_info[f][CRPT_COPIED] if f in self._input_list_info else '',
                    self._input_list_info[f][CRPT_PROCESSED] if f in self._input_list_info else '',
                    self._input_list_info[f][CRPT_UPLOADED] if f in self._input_list_info else ''
                    ));
        except Exception as exp:
            self._base.message('{}'.format(str(exp)), self._base.const_critical_text)
            return False
        return True
    def walk(self):
        walk_tree = []
        for f in self:
            (d, f) = os.path.split (f)
            walk_tree.append (('{}/'.format(d), (), (f.strip(),)))
        return walk_tree
    def __iter__(self):
        return iter(self._input_list)

# class to read/gather info on til files.
class TIL:
    CRELATED_FILE_COUNT = 'related_file_count'
    CPROCESSED_FILE_COUNT = 'processed_file_count'
    CKEY_FILES = 'files'
    def __init__(self):
        self._rasters = []
        self._tils = []
        self._tils_info = {}
        self._output_path = {}
    @property
    def TILCount(self):
        return len(self._tils)
    def addTIL(self, input):        # add (til) files to process later via (fnc: process).
                                    # This when the (til) files are found before the associated (files) could be not found at the (til) location because they may not have been downloaded yet.
        _input = input.replace('\\', '/')
        if (not _input in self._tils):
            self._tils.append(_input)
        if (not input.lower() in self._tils_info):
            self._tils_info[_input.lower()] = {
            self.CRELATED_FILE_COUNT : 0,
            self.CPROCESSED_FILE_COUNT : 0,
            self.CKEY_FILES : []
            }
        return True
    def fileTILRelated(self, input):
        idx = input.split('.')
        f = idx[0]
        f = f.replace('\\', '/').split('/')
        f = f[len(f) -1]
        for t in self._tils:
            if (t.find(f) >= 0):
                return True
        for t in self._rasters:
            if (t.startswith(f)):
                return True
        return False
    def addFileToProcessed(self, input):
        for t in self._tils:
            _key_til_info = t.lower()
            if (_key_til_info in self._tils_info):
                if (input in self._tils_info[_key_til_info][self.CKEY_FILES]):
                    self._tils_info[_key_til_info][self.CPROCESSED_FILE_COUNT] += 1
                    return True
        return False
    def isAllFilesProcessed(self, input):
        if (not input):
            return False
        if (not input.lower() in self._tils_info):
            return False
        _key_til_info = input.lower()
        if (self._tils_info[_key_til_info][self.CRELATED_FILE_COUNT] ==
            self._tils_info[_key_til_info][self.CPROCESSED_FILE_COUNT]):
            return True
        return False
    def process(self, input):
        if (not input or
            len(input) == 0):
            return False
        if (not os.path.exists(input)):
            return False
        with open(input, 'r') as _fp:
            _line = _fp.readline()
            while (_line):
                ln = _line.strip()
                CBREAK = 'filename ='
                if (ln.find(CBREAK) != -1):
                    splt = ln.replace('"', '').replace(';', '').split(CBREAK)
                    if (len(splt) == 2):
                        file_name = splt[1].strip()
                        if (not file_name in self._rasters):
                            self._rasters.append(file_name)
                            _key_til_info = input.lower()
                            if (_key_til_info in self._tils_info):
                                self._tils_info[_key_til_info][self.CRELATED_FILE_COUNT] += 1
                                self._tils_info[_key_til_info][self.CKEY_FILES].append(file_name)
                _line = _fp.readline()
##        self._tils.append (input)
        return True
    def setOutputPath(self, input, output):
        if (not input in self._output_path):
            self._output_path[input] = output
    def getOutputPath(self, input):
        if (not input in self._output_path):
            return None
        return self._output_path[input]
    def find(self, input):
        for _t in self._tils_info:
            if (input in self._tils_info[_t][self.CKEY_FILES]):
                if (self._tils_info[_t][self.CRELATED_FILE_COUNT] <= 1):
                    return False
                return True
        return False
    def __iter__(self):
        return iter(self._tils)

# classes of S3Upload module to merge as a single source.
class S3Upload:
    def __init__(self, base):
        self._base = base
    def run(self, bobj, fobj, id, tot_ids):
        fobj.seek(0)
        msg_frmt = '[Push] block'
        self._base.message ('{} ({}/{})'.format(msg_frmt, id, tot_ids));
        bobj.upload_part_from_file(fobj, id)
        self._base.message ('{} ({}/{}) - Done'.format(msg_frmt, id, tot_ids));
        fobj.close()
        del fobj

class S3Upload_:
    def __init__(self, base, s3_bucket, s3_path, local_file, acl_policy = 'private'):
        self._base = base       # base
        self.m_s3_path =  s3_path
        self.m_local_file = local_file
        self.m_s3_bucket = s3_bucket
        self.m_acl_policy = 'private' if acl_policy is None or acl_policy.strip() == '' else acl_policy
        self.mp = None
        pass;
    def init(self):
        # multip-upload test
        try:
            self.mp = self.m_s3_bucket.initiate_multipart_upload(self.m_s3_path, policy=self.m_acl_policy)
        except Exception as exp:
            self._base.message('({})'.format(str(exp)), self._base.const_critical_text)
            return False
        return True
        # ends
    def upload(self):
        # read in big-file in chunks
        CHUNK_MIN_SIZE = MEMORYSTATUSEX().memoryStatus().memoryPerUploadChunk(CCLOUD_UPLOAD_THREADS)
##        if (self.m_local_file.endswith('.lrc')):        # debug. Must be removed before release.
##            return True                                 # "
        self._base.message('[S3-Push] {}..'.format(self.m_local_file))
##        return True   # debug. Must be removed before release.
        self._base.message('Upload block-size is set to ({}) bytes.'.format(CHUNK_MIN_SIZE))
        s3upl = S3Upload(self._base);
        idx = 1
        f = None
        try:         # see if we can open it
            f = open (self.m_local_file, 'rb')
            f_size = os.path.getsize(self.m_local_file)
            if (f_size == 0):       # support uploading of (zero) byte files.
                s3upl.run (self.mp, SlnTMStringIO(1), idx, idx)
                try:
                    self.mp.complete_upload()
                    if (f):
                        f.close()
                    return True
                except:
                    self.mp.cancel_upload()
                    raise
        except Exception as e:
            self._base.message ('File open/Upload: ({})'.format(str(e)), self._base.const_critical_text)
            if (f):
                f.close()
            return False
        threads = []
        pos_buffer = upl_blocks = 0
        len_buffer = CCLOUD_UPLOAD_THREADS     # set this to no of parallel (chunk) uploads at once.
        tot_blocks = (f_size / CHUNK_MIN_SIZE) + 1
        self._base.message ('Total blocks to upload ({})'.format(tot_blocks))
        while(1):
            len_threads = len(threads)
            while(len_threads > 0):
                alive = [t.isAlive() for t in threads]
                cnt_dead = sum(not x for x in alive)
                if (cnt_dead):
                    upl_blocks += cnt_dead
                    len_buffer = cnt_dead
                    threads = [t for t in threads if t.isAlive()]
                    break
            buffer = []
            for i in range(0, len_buffer):
                chunk = f.read(CHUNK_MIN_SIZE)
                if not chunk:
                    break
                buffer.append(SlnTMStringIO(CHUNK_MIN_SIZE))
                buffer[len(buffer) - 1].write(chunk)
            if (len(buffer) == 0 and
                len(threads) == 0):
                break
            for e in buffer:
                try:
                    t = threading.Thread(target = s3upl.run,
                    args = (self.mp, e, idx, tot_blocks))
                    t.daemon = True
                    t.start()
                    threads.append(t)
                    idx += 1
                except Exception as e:
                    self._base.message ('{}'.format(str(e)), self._base.const_critical_text);
                    if (f):
                        f.close()
                    return False
        try:
            self.mp.complete_upload()
        except Exception as e:
            self._base.message ('{}'.format(str(e)), self._base.const_critical_text)
            self.mp.cancel_upload()
            return False
        finally:
            if (f):
                f.close()
        return True
    def __del__(self):
        if (self.mp):
            self.mp = None

class SlnTMStringIO:
    def __init__(self, size, buf = ''):
        self.m_size = size
        self.m_buff= mmap.mmap(-1, self.m_size)
        self.m_spos = self.m_fsize = 0
    def close(self):
        self.m_buff.close()
        del self.m_buff
        pass
    def next(self):
        pass
    def seek(self, pos, mode = 0):
        if mode == 1:
            pos += self.m_spos
        elif mode == 2:
            pos += len(self.m_buff)
        self.m_spos = max(0, pos)
    def tell(self):
        return self.m_spos
    def read(self, n = -1):
        buff_len = self.m_fsize
        nRead = (self.m_spos + n)
        if (nRead > buff_len):
            n = n - (nRead - buff_len)
        self.m_buff.seek(self.m_spos, 0)
        self.m_spos += n
        return str(self.m_buff.read(n))
    def readline(self, length=None):
        pass
    def readlines(self, sizehint = 0):
        pass
    def truncate(self, size=None):
        pass
    def write(self, s):
        self.m_buff.write(s)
        self.m_fsize += len(s)
        pass
    def writelines(self, iterable):
        pass
    def flush(self):
        pass
    def getvalue(self):
        pass

class Store(object):
    # log error types
    const_general_text = 0
    const_warning_text = 1
    const_critical_text = 2
    const_status_text = 3
    # ends
    # class usage (Operation) modes
    CMODE_SCAN_ONLY = 0
    CMODE_DO_OPERATION = 1
    # ends
    def __init__(self, account_name, account_key, profile_name, base):
        self._account_name = account_name
        self._account_key = account_key
        self._profile_name = profile_name
        self._base = base
        self._event_postCopyToLocal = None
        self._include_subFolders = False
        self._mode = self.CMODE_DO_OPERATION
    def init(self):
        return True
    def upload(self, file_path, container_name, parent_folder, properties = None):
        self._input_file_path = file_path
        self._upl_container_name = container_name
        self._upl_parent_folder = parent_folder
        self._upl_properties = properties
        return True
    def setSource(self, container_name, parent_folder, properties = None):
        self._dn_container_name = container_name
        self._dn_parent_folder = parent_folder
        self._dn_properties = properties
        return True
    def readProfile (self, account_name, account_key):
        import ConfigParser
        config = ConfigParser.RawConfigParser()
        userHome = '{}/{}/{}'.format(os.path.join(os.getenv('HOMEDRIVE'),os.getenv('HOMEPATH')).replace('\\', '/'), '.optimizerasters', 'azure_credentials')
        with open (userHome) as fptr:
            config.readfp(fptr)
        if (not config.has_section(self._profile_name)):
            return (None, None)
        azure_account_name = config.get (self._profile_name, account_name) if config.has_option (self._profile_name, account_name) else None
        azure_account_key = config.get (self._profile_name, account_key) if config.has_option (self._profile_name, account_key) else None
        return (azure_account_name, azure_account_key)
    def message (self, msg, status = 0):     # type (0: general, 1: warning, 2: critical, 3: statusText)
        if (self._base):
            self._base.message(msg, status)
            return
        status_text = ''
        if (status == 1):
            status_text = 'Warning'
        elif (status == 2):
            status_text = 'Err'
        print ('{}{}{}'.format (status_text, '. ' if status_text else '', msg))

class Azure(Store):
    CHUNK_MIN_SIZE = 4 * 1024 * 1024
    COUT_AZURE_ACCOUNTNAME_INFILE = 'azure_account_name'
    COUT_AZURE_ACCOUNTKEY_INFILE = 'azure_account_key'
    def __init__(self, account_name, account_key, profile_name = None, base = None):
        super(Azure, self).__init__(account_name, account_key, profile_name, base)
        self._browsecontent = []
    def init(self):
        try:
            if (self._profile_name):    # profile name if defined supersedes (account_name, account_key)
                (self._account_name, self._account_key) = self.readProfile(self.COUT_AZURE_ACCOUNTNAME_INFILE, self.COUT_AZURE_ACCOUNTKEY_INFILE)
                if (not self._account_name or
                    not self._account_key):
                    return False
            from azure.storage.blob import BlobService
            self._blob_service = BlobService(account_name=self._account_name, account_key=self._account_key)
        except Exception as e:
            return False
        return True
    @property
    def getAccountName(self):
        return self._account_name
    def _runBlock(self, bobj, fobj, container_name, blob_name, block_id):
        fobj.seek(0)
        bobj.put_block(container_name, blob_name, fobj.read(), block_id)
        fobj.close()
        del fobj
    def _addBrowseContent(self, blobName):
        if (not blobName):
            return False
        if (self._mode == self.CMODE_SCAN_ONLY):
            self._browsecontent.append (blobName)
            return True
        return False
    def getBrowseContent(self):
        return self._browsecontent
    def browseContent(self, container_name, parent_folder, cb = None):
        super(Azure, self).setSource(container_name, parent_folder)
        blobs = []
        marker = None
        while (True):
            try:
                batch = self._blob_service.list_blobs(self._dn_container_name, marker=marker)
                blobs.extend(batch)
                if not batch.next_marker:
                    break
                marker = batch.next_marker
            except:
                self._base.message('Unable to read from ({}). Check container name ({})/credentials.'.format(CCLOUD_AZURE.capitalize(),
                self._dn_container_name),
                self._base.const_critical_text)
                return False
        parent_folder_ = None
        parent_folder_indx = -1
        if (parent_folder and
            parent_folder != '/'):
            parent_folder_ = parent_folder.replace('\\', '/')
            if (not parent_folder_.endswith('/')):
                parent_folder_ += '/'
            parent_folder_indx = len(parent_folder_)
        for blob in blobs:
            blob_name = blob.name.replace('\\', '/')
            if (not self._include_subFolders):
                if (not parent_folder or
                    parent_folder == '/'):
                    if (blob_name.find('/') != -1):
                        continue
            passed = False
            if (parent_folder_):
                if (parent_folder_ and
                    blob_name.startswith(parent_folder_) or
                    parent_folder_ == '/'):
                    passed = True
                if (not self._include_subFolders):
                    passed = False
                    if (blob_name.find('/', parent_folder_indx) == -1 and
                        blob_name.find('/') != -1):
                        passed = True
            if (not parent_folder_ or
                passed):
                self._addBrowseContent(blob_name)
                if (cb and
                    self._mode != self.CMODE_SCAN_ONLY):
                    cb(blob_name)
        return True
    def copyToLocal(self, blob_source):
        try:
            if (not blob_source):
                return False
            _user_config = self._base.getUserConfiguration
            _resumeReporter = _user_config.getValue('handler_resume_reporter')
            # what does the restore point say about the (S3_key) status?
            if (_resumeReporter):
                _get_rstr_val = _resumeReporter.getRecordStatus(blob_source, CRPT_COPIED)
                if (_get_rstr_val == CRPT_YES):
                    self._base.message ('{} {}'.format(CRESUME_MSG_PREFIX, blob_source))
                    return True
            # ends
            _azureParentFolder = _user_config.getValue(CIN_AZURE_PARENTFOLDER, False)
            _azurePath = blob_source if _azureParentFolder == '/' else blob_source.replace(_azureParentFolder, '')
            output_path = _user_config.getValue(CCFG_PRIVATE_OUTPUT, False) + _azurePath
            if (_user_config.getValue('istempoutput') == True):
                if (not _user_config.getValue('Mode').endswith('mrf')):
                    output_path = _user_config.getValue('tempoutput', False) + _azurePath
            is_raster = False
            is_tmp_input = getBooleanValue(_user_config.getValue('istempinput'))
            if (True in [_azurePath.endswith(x) for x in _user_config.getValue('ExcludeFilter')]):
                return False
            elif (True in [_azurePath.endswith(x) for x in _user_config.getValue(CCFG_RASTERS_NODE)]):
                if (is_tmp_input):
                    output_path = _user_config.getValue(CUSR_TEMPINPUT, False) + _azurePath
                is_raster = True
            if (_user_config.getValue('Pyramids') == CCMD_PYRAMIDS_ONLY):
                return False
            if (not blob_source or
                not output_path or
                not self._dn_parent_folder):
                    self._base.message ('Azure> Not initialized', self._base.const_critical_text)
                    return False
            flr =  os.path.dirname(output_path)
            if (os.path.exists(flr) == False):
                try:
                    os.makedirs(flr)
                except Exception as e:
                    raise
            if (is_raster):
                exclude_callback(_azurePath, _azureParentFolder, _user_config.getValue(CCFG_PRIVATE_OUTPUT, False))
                if (not is_tmp_input):
                    return True
            writeTo = output_path
            self._base.message ('[S3-Pull] {}'.format(blob_source))
            self._blob_service.get_blob_to_path(self._dn_container_name, blob_source, writeTo)
            if (self._event_postCopyToLocal):
                self._event_postCopyToLocal(writeTo);
            # mark download/copy status
            if (_resumeReporter):
                _resumeReporter.updateRecordStatus (blob_source, CRPT_COPIED, CRPT_YES)
            # ends
            # Handle any post-processing, if the final destination is to S3, upload right away.
            if (getBooleanValue(_user_config.getValue(CCLOUD_UPLOAD))):
                if (getBooleanValue(_user_config.getValue('istempinput'))):
                    if (is_raster):
                        return True
                _is_success = self._base.S3Upl(writeTo, user_args_Callback)
                if (not _is_success):
                    return False
            # ends
        except Exception as e:
            self._base.message ('({})'.format(str(e)), self._base.const_critical_text)
            if (_resumeReporter):
                _resumeReporter.updateRecordStatus (blob_source, CRPT_COPIED, CRPT_NO)
            return False
        return True
    def upload(self, input_path, container_name, parent_folder, properties = None):
        if (not input_path or
            not container_name or
            parent_folder is None):
            return False
        _parent_folder = parent_folder
        if (not _parent_folder):
            if (self._base.getUserConfiguration):
                _parent_folder = self._base.getUserConfiguration.getValue(CIN_AZURE_PARENTFOLDER)
        elif (_parent_folder == '/'):
            _parent_folder = ''
        if (properties):
            if ('tempoutput' in properties):
                _tempoutput = properties['tempoutput']
                _parent_folder = os.path.dirname(input_path.replace('\\', '/').replace(_tempoutput, _parent_folder))
        super(Azure, self).upload(input_path, container_name, _parent_folder, properties)

        blob_path = self._input_file_path
        blob_name = os.path.join(self._upl_parent_folder, os.path.basename(blob_path))
##        if (blob_name.endswith('.lrc')):         # debug. Must be removed before release.
##            return True                          #  "
##        return True     # debug. Must be removed before release.
        isContainerCreated = False

        t0 = datetime.datetime.now()
        time_to_wait_before_retry = 3
        max_time_to_wait = 60

        self.message ('Accessing container ({})..'.format(self._upl_container_name))
        while(True):
            try:
                _access = properties['access'] if properties and 'access' in properties else None
                self._blob_service.create_container(self._upl_container_name, x_ms_blob_public_access=_access, fail_on_exist=True)
                isContainerCreated = True
                break
            except Exception as e:
                get_err_msg = str(e).lower()
                if (get_err_msg.find('the specified container is being deleted') == -1):
                    if (get_err_msg.find('already exists')):
                        isContainerCreated = True
                    break
                tm_pre = datetime.datetime.now()
                while(True):
                    time_delta = datetime.datetime.now() - tm_pre
                    if (time_delta.seconds > time_to_wait_before_retry):
                        break
                t1 = datetime.datetime.now() - t0
                if (t1.seconds > max_time_to_wait):
                    self.message ('Timed out to create container.', self.const_critical_text)
                    break
        if (not isContainerCreated):
            self.message ('Unable to create the container ({})'.format(self._upl_container_name), self.const_critical_text)
            exit(1)
        self.message ('Done.')

        f = None
        try:         # see if we can open it
            f = open (blob_path, 'rb')
            f_size = os.path.getsize(blob_path)
        except Exception as e:
            self.message ('File open/upload: ({})'.format(str(e)), self.const_critical_text)
            if (f):
                f.close()
            return False

        threads = []
        block_ids = []

        pos_buffer = 0
        len_buffer = CCLOUD_UPLOAD_THREADS     # set this to no of parallel (chunk) uploads at once.

        tot_blocks = (f_size / Azure.CHUNK_MIN_SIZE) + 1
        upl_blocks = 0
        idx = 1

        self.message ('Uploading ({})'.format(blob_path))
        self.message ('Total blocks to upload ({})'.format(tot_blocks))

        st = datetime.datetime.now()

        while(1):
            len_threads = len(threads)
            while(len_threads > 0):
                alive = [t.isAlive() for t in threads]
                cnt_dead = sum(not x for x in alive)
                if (cnt_dead):
                    upl_blocks += cnt_dead
                    len_buffer = cnt_dead
                    threads = [t for t in threads if t.isAlive()]
                    break
            buffer = []
            for i in range(0, len_buffer):
                chunk = f.read(Azure.CHUNK_MIN_SIZE)
                if not chunk:
                    break
                buffer.append(SlnTMStringIO(len(chunk)))
                buffer[len(buffer) - 1].write(chunk)

            if (len(buffer) == 0 and
                len(threads) == 0):
                break

            for e in buffer:
                try:
                    block_id = base64.b64encode(str(idx))
                    self.message ('Adding block-id ({})'.format(idx))
                    t = threading.Thread(target = self._runBlock,
                    args = (self._blob_service, e, self._upl_container_name, blob_name, block_id))
                    t.daemon = True
                    t.start()
                    threads.append(t)
                    block_ids.append(block_id)
                    idx += 1
                except Exception as e:
                    self.message (str(e), self.const_critical_text);
                    if (f):
                        f.close()
                    return False
        try:
            self.message ('Finalizing uploads..');
            ret = self._blob_service.put_block_list(self._upl_container_name, blob_name, block_ids)
        except Exception as e:
            Message (str(e), self.const_critical_text)
            return False
        finally:
            if (f):
                f.close()

        self.message ('Duration. ({} sec)'.format((datetime.datetime.now() - st).seconds))
        self.message ('Done.')

        return True

class S3Storage:
    def __init__(self, base):
        self._base = base
        self._isBucketPublic = False
    def init(self, remote_path, s3_key, s3_secret, direction):
        if (not isinstance(self._base, Base)):
            return False
        self._input_flist = None
        self.__m_failed_upl_lst = {}
        self.m_user_config = self._base.getUserConfiguration
        self.CAWS_ACCESS_KEY_ID = s3_key
        self.CAWS_ACCESS_KEY_SECRET = s3_secret
        self.m_bucketname = ''         # no default bucket-name
        if (self.m_user_config):
            s3_bucket = self.m_user_config.getValue('{}_S3_Bucket'.format('Out' if direction == CS3STORAGE_OUT else 'In'), False)
            if (s3_bucket):
                self.m_bucketname = s3_bucket
            _profile_name = self.m_user_config.getValue('{}_S3_AWS_ProfileName'.format('Out' if direction == CS3STORAGE_OUT else 'In'), False)
            # setup s3 connection
            if (self.m_user_config.getValue(CCFG_PRIVATE_INC_BOTO) == True):    # return type is a boolean hence no need to explicitly convert.
                _calling_format = boto.config.get('s3', 'calling_format', 'boto.s3.connection.SubdomainCallingFormat' if len([c for c in self.m_bucketname if c.isupper()]) == 0 else 'boto.s3.connection.OrdinaryCallingFormat')
                try:
                    _servicePoint = self.m_user_config.getValue('{}_S3_ServicePoint'.format('Out' if direction == CS3STORAGE_OUT else 'In'), False)
                    self._isBucketPublic = self.CAWS_ACCESS_KEY_ID is None and self.CAWS_ACCESS_KEY_SECRET is None and _profile_name is None
                    con = boto.connect_s3(self.CAWS_ACCESS_KEY_ID if not _profile_name else None, self.CAWS_ACCESS_KEY_SECRET if not _profile_name else None,
                    profile_name = _profile_name if _profile_name else None, calling_format = _calling_format,
                    anon = True if self._isBucketPublic else False, host = _servicePoint if _servicePoint else boto.s3.connection.NoHostProvided)
                except Exception as e:
                    self._base.message (str(e), self._base.const_critical_text)
                    return False
                self.bucketupload = con.lookup(self.m_bucketname, True, None)
                if (not self.bucketupload):
                    self._base.message ('Invalid {} S3 bucket ({})/credentials.'.format(
                    'output' if direction == CS3STORAGE_OUT else 'input',
                    self.m_bucketname),
                    self._base.const_critical_text)
                    return False
            # ends
        _remote_path = remote_path
        if (os.path.isfile(_remote_path)):      # are we reading a file list?
            self._input_flist = _remote_path
            try:
                global _rpt
                _remote_path = _rpt.root
            except Exception as e:
                self._base.message ('Report ({})'.format(str(e)), self._base.const_critical_text)
                return False
        self.remote_path = _remote_path.replace("\\","/")
        if (self.remote_path[-1:] != '/'):
            self.remote_path += '/'
        return True
    @property
    def inputPath(self):
        return self.__m_input_path
    @inputPath.setter
    def inputPath(self, value):
        self.__m_input_path = value
    def getFailedUploadList(self):
        return self.__m_failed_upl_lst;
    # code to iterate a S3 bucket/folder
    def getS3Content(self, prefix, cb = None, precb = None):
        is_link = not self._input_flist is None;
        keys = self.bucketupload.list(prefix) if not is_link else _rpt
        root_only = True
        if (self.m_user_config):
            root_only_ = self.m_user_config.getValue('IncludeSubdirectories')
            if (root_only_ is not None):    # if there's a value, take it else defaults to (True)
                root_only = getBooleanValue(root_only_)
        # get the til files first
        if (til):
            try:
                for key in keys:
                    key = self.bucketupload.get_key(key) if is_link else key
                    if (not key):
                        continue
                    if (key.name.endswith('/') == False):
                        if (not root_only == True):
                            if (os.path.dirname(key.name) != os.path.dirname(self.remote_path)):
                                continue
                        if (key.name.lower().endswith(CTIL_EXTENSION_)):
                            cb(key, key.name.replace(self.remote_path, ''))       # callback on the client-side
            except Exception as e:
                self._base.message (e.message, self._base.const_critical_text)
                return False
        # ends
        try:
            for key in keys:
                key = self.bucketupload.get_key(key) if is_link else key
                if (not key):
                    continue
                if (key.name.endswith('/') == False):
                    if (not root_only == True):
                        if (os.path.dirname(key.name) != os.path.dirname(self.remote_path)):
                            continue
                    if (cb):
                        if (precb):
                            if (precb(key.name.replace(self.remote_path, ''), self.remote_path, self.inputPath) == True):     # if raster/exclude list, do not proceed.
                                if (getBooleanValue(self.m_user_config.getValue('istempinput')) == False):
                                    continue
                        if (til):
                            if (key.name.lower().endswith(CTIL_EXTENSION_)):
                                continue
                        cb(key, key.name.replace(self.remote_path, ''))       # callback on the client-side. Note. return value not checked.
        except Exception as e:
            self._base.message (e.message, const_critical_text)
            return False
        # Process (til) files once all the associate files have been copied from (cloud) to (local)
        if (til):
            for _til in til:
                til.process(_til)
        # ends
        return True
    # ends
    # code to deal with s3-local-cpy
    def S3_copy_to_local(self, S3_key, S3_path):
        err_msg_0 = 'S3/Local path is invalid'
        if (S3_key is None):   # get rid of invalid args.
                self._base.message (err_msg_0)
                return False
        # what does the restore point say about the (S3_key) status?
        if (_rpt):
            _get_rstr_val = _rpt.getRecordStatus(S3_key.name, CRPT_COPIED)
            if (_get_rstr_val == CRPT_YES):
                self._base.message ('{} {}'.format(CRESUME_MSG_PREFIX, S3_key.name))
                return True
        # ends
        if (self.m_user_config is None):     # shouldn't happen
            self._base.message ('Intenal/User config not initialized.', const_critical_text)
            return False
        input_path = self.m_user_config.getValue(CCFG_PRIVATE_OUTPUT, False) + S3_path
        if ((self.m_user_config.getValue('istempoutput')) == True):
            if (not self.m_user_config.getValue('Mode').endswith('mrf')):
                input_path = self.m_user_config.getValue('tempoutput', False) + S3_path  # -tempoutput must be set with -cloudinput=true
        is_raster = False
        is_tmp_input = getBooleanValue(self.m_user_config.getValue('istempinput'))
        if (True in [S3_path.endswith(x) for x in self.m_user_config.getValue('ExcludeFilter')]):
            return False
        elif (True in [S3_path.endswith(x) for x in self.m_user_config.getValue(CCFG_RASTERS_NODE)]):
            if (is_tmp_input == True):
                input_path = self.m_user_config.getValue(CUSR_TEMPINPUT, False) + S3_path
                is_raster = True
        if (self.m_user_config.getValue('Pyramids') == CCMD_PYRAMIDS_ONLY):
            return False
        # collect input file names.
        if (fn_collect_input_files(S3_key)):
            return False
        # ends
        is_cpy_to_s3 = getBooleanValue(self.m_user_config.getValue(CCLOUD_UPLOAD))
        mk_path = input_path
        self._base.message ('[S3-Pull] %s' % (mk_path))
        flr = os.path.dirname(mk_path)
        if (os.path.exists(flr) == False):
            try:
                os.makedirs(flr)
            except Exception as e:
                self._base.message ('(%s)' % (str(e)), const_critical_text)
                if (_rpt):
                    _rpt.updateRecordStatus (S3_key.name, CRPT_COPIED, CRPT_NO)
                return False
        # let's write remote to local
        fout = None
        try:
            memPerChunk = MEMORYSTATUSEX().memoryStatus().memoryPerDownloadChunk()
            self._base.message('Download block-size is set to ({}) bytes.'.format(memPerChunk))
            fout = open(mk_path, 'wb')        # can we open for output?
            startbyte = 0
            while(startbyte < S3_key.size):
                endbyte = startbyte + (memPerChunk - 1)
                if (endbyte > S3_key.size):
                    endbyte = S3_key.size - 1
                print ('Seek> {}-{} [{}]'.format(startbyte, endbyte, S3_key.name))         # Note> not routed to the log file.
                S3_key.get_contents_to_file(fout, headers={'Range': 'bytes={}-{}'.format(startbyte, endbyte)})
                fout.flush()
                startbyte = endbyte + 1
        except Exception as e:
            self._base.message ('({})'.format(str(e)), const_critical_text);
            if (_rpt):
                _rpt.updateRecordStatus (S3_key.name, CRPT_COPIED, CRPT_NO)
            return False
        finally:
            if (fout):
                fout.close()
            if (S3_key):
                S3_key.close()
        # ends
        # take care of (til) inputs.
        if (til):
            if (mk_path.lower().endswith(CTIL_EXTENSION_)):
                if (til.addTIL(mk_path)):
                    til.setOutputPath(mk_path, mk_path)
        # ends
        # mark download/copy status
        if (_rpt):
            _rpt.updateRecordStatus (S3_key.name, CRPT_COPIED, CRPT_YES)
        # ends
        # Handle any post-processing, if the final destination is to S3, upload right away.
        if (is_cpy_to_s3 == True):
            if (getBooleanValue(self.m_user_config.getValue('istempinput')) == True):
                if (is_raster == True):
                    return True
            _is_success = self._base.S3Upl(mk_path, user_args_Callback)
            if (not _is_success):
                return False
        # ends
        return True
    # ends
    def upload(self):
        self._base.message ('[S3-Push]..');
        for r,d,f in os.walk(self.inputPath):
            for file in f:
                lcl_file = os.path.join(r, file).replace('\\', '/')
                upl_file = lcl_file.replace(self.inputPath, self.remote_path)
                self._base.message (upl_file)
                try:
                    S3 = S3Upload_(self.bucketupload, upl_file, lcl_file, self.m_user_config.getValue(COUT_S3_ACL) if self.m_user_config else None);
                    if (S3.init() == False):
                        self._base.message ('Unable to initialize [S3-Push] for (%s=%s)' % (lcl_file, upl_file), const_warning_text)
                        continue
                    ret = S3.upload()
                    if (ret == False):
                        self._base.message ('[S3-Push] (%s)' % (upl_file), const_warning_text)
                        continue
                except Exception as inf:
                    self._base.message ('(%s)' % (str(inf)), const_warning_text)
                finally:
                    if (S3 is not None):
                        del S3
        return True
    def _addToFailedList(self, localPath, remotePath):
        if (not 'upl' in self.getFailedUploadList()):
            self.__m_failed_upl_lst['upl'] = []
        _exists = False
        for v in self.__m_failed_upl_lst['upl']:
            if (v['local'] == localPath):
                _exists = True
                break
        if (not _exists):
            self.__m_failed_upl_lst['upl'].append({'local' : localPath, 'remote' : remotePath})
        return True
    def upload_group(self, input_source, single_upload = False, include_subs = False):
        m_input_source = input_source.replace('\\', '/')
        input_path = os.path.dirname(m_input_source)
        upload_buff = []
        (p, e) = os.path.splitext(m_input_source)
        for r,d,f in os.walk(input_path):
            for file in f:
                mk_path = os.path.join(r, file).replace('\\', '/')
                if (mk_path.startswith(p)):
                    if (single_upload == True):
                        if (mk_path != m_input_source):
                            continue
                    try:
                        S3 = None
                        upl_file = mk_path.replace(self.inputPath, self.remote_path)
                        if (getBooleanValue(self.m_user_config.getValue(CCLOUD_UPLOAD)) == True):
                            rep = self.inputPath
                            if (rep.endswith('/') == False):
                                rep += '/'
                            if (getBooleanValue(self.m_user_config.getValue('istempoutput')) == True):
                                rep = self.m_user_config.getValue('tempoutput', False)
                            upl_file = mk_path.replace(rep, self.remote_path if self.m_user_config.getValue('iss3') == True else self.m_user_config.getValue(CCFG_PRIVATE_OUTPUT, False))
                        S3 = S3Upload_(self._base, self.bucketupload, upl_file, mk_path, self.m_user_config.getValue(COUT_S3_ACL) if self.m_user_config else None);
                        if (S3.init() == False):
                            self._base.message ('Unable to initialize S3-Upload for (%s=>%s)' % (mk_path, upl_file), self._base.const_warning_text)
                            self._addToFailedList(mk_path, upl_file)
                            continue
                        upl_retries = CS3_UPLOAD_RETRIES
                        ret  = False
                        while(upl_retries and ret == False):
                            ret = S3.upload()
                            if (ret == False):
                                time.sleep(10)   # let's sleep for a while until s3 kick-starts
                                upl_retries -= 1
                                self._base.message ('[S3-Push] (%s), retries-left (%d)' % (upl_file, upl_retries), self._base.const_warning_text)
                        if (ret == False):
                            self._addToFailedList(mk_path, upl_file)
                            if (S3 is not None):
                                del S3
                                S3 = None
                            continue
                    except Exception as inf:
                        self._base.message ('(%s)' % (str(inf)), self._base.const_critical_text)
                    finally:
                        if (S3 is not None):
                            del S3
                            S3 = None
                    upload_buff.append(mk_path);    # successful entries to return.
                    if (single_upload == True):
                        return upload_buff
            if (not include_subs):
                return upload_buff
        return upload_buff       # this could be empty.
# ends

CIDX_USER_CONFIG  = 2
CCFG_BLOCK_SIZE = 512
CCMD_PYRAMIDS_ONLY = 'only'
CCMD_PYRAMIDS_EXTERNAL = 'external'
CCFG_THREADS = 10
CCFG_RASTERS_NODE = 'RasterFormatFilter'
CCFG_EXCLUDE_NODE = 'ExcludeFilter'
CCFG_PRIVATE_INC_BOTO = '__inc_boto__'
CCFG_PRIVATE_OUTPUT = '__output__'
CCFG_INTERLEAVE = 'Interleave'
CCFG_PREDICTOR = 'Predictor'

# log status
const_general_text = 0
const_warning_text = 1
const_critical_text = 2
const_status_text = 3
# ends

def messageDebug(msg, status):
    print ('*{}'.format(msg))

def Message(msg, status=0):
    print (msg)

def args_Callback(args, user_data = None):
    _LERC =  'lerc'
    _LERC2 = 'lerc2'
    _JPEG = 'jpeg'
    _JPEG12 = 'jpeg12'
    m_compression = _LERC    # default if external config is faulty
    m_lerc_prec = None
    m_compression_quality = 85
    m_bsize = CCFG_BLOCK_SIZE
    m_mode = 'chs'
    m_nodata_value = None
    m_predictor = 1
    m_interleave = 'PIXELS'
    if (user_data):
        try:
            compression_ = user_data[CIDX_USER_CONFIG].getValue('Compression')
            if (compression_):
                m_compression = compression_
            compression_quality_ = user_data[CIDX_USER_CONFIG].getValue('Quality')
            if (compression_quality_):
                m_compression_quality = compression_quality_
            bsize_ = user_data[CIDX_USER_CONFIG].getValue('BlockSize')
            if (bsize_):
                m_bsize = bsize_
            lerc_prec_ = user_data[CIDX_USER_CONFIG].getValue('LERCPrecision')
            if (lerc_prec_):
                m_lerc_prec = lerc_prec_
            m_nodata_value = user_data[CIDX_USER_CONFIG].getValue('NoDataValue')
            m_ignorealphaband = getBooleanValue(user_data[CIDX_USER_CONFIG].getValue('IgnoreAlphaBand'))
            m_mode = user_data[CIDX_USER_CONFIG].getValue('Mode')
            m_predictor_ = user_data[CIDX_USER_CONFIG].getValue(CCFG_PREDICTOR)
            if (m_predictor_):
                m_predictor = m_predictor_
            m_interleave_ = user_data[CIDX_USER_CONFIG].getValue(CCFG_INTERLEAVE)
            if (m_interleave_):
                m_interleave = m_interleave_.upper()
            mode_ = m_mode.split('_')
            if (len(mode_) > 1):
                m_mode = mode_[0]      # mode/output
                m_compression = mode_[1]     # compression
            if (m_mode.startswith('tif')):
                    args.append ('-co')
                    args.append ('BIGTIFF=IF_NEEDED')
                    args.append ('-co')
                    args.append ('TILED=YES')
                    m_mode = 'GTiff'   # so that gdal_translate can understand.
                    if (m_interleave == 'PIXEL' and
                        m_compression.startswith(_JPEG)):
                        args.append ('-co')
                        args.append ('PHOTOMETRIC=YCBCR')
                        if (m_compression == _JPEG12):
                            args.append ('-co')
                            args.append ('NBITS=12')
                        m_compression  = _JPEG
                    if (m_interleave == 'PIXEL' and
                        m_compression == 'deflate'):
                        args.append ('-co')
                        args.append (' predictor={}'.format(m_predictor))
        except: # could throw if index isn't found
            pass    # ingnore with defaults.
    args.append ('-of')
    args.append (m_mode)
    args.append ('-co')
    args.append ('COMPRESS=%s' % (_LERC if m_compression == _LERC2 else m_compression))
    if (m_nodata_value):
        args.append ('-a_nodata')
        args.append (str(m_nodata_value))
    if (m_compression == _JPEG):
        args.append ('-co')
        if (m_mode == 'mrf'):   # if the output is (mrf)
            args.append ('QUALITY=%s' % (m_compression_quality))
            if (m_ignorealphaband):
                args.append ('-co')
                args.append ('OPTIONS="MULTISPECTRAL:1"')
        else:
            args.append ('JPEG_QUALITY=%s' % (m_compression_quality))
        args.append ('-co')
        args.append ('INTERLEAVE=%s' % (m_interleave))
    if (m_compression.startswith(_LERC)):
        if (m_lerc_prec or
            m_compression == _LERC2 or
            m_compression == _LERC):
            args.append ('-co')
            args.append ('OPTIONS={}{}'.format('' if not m_lerc_prec else 'LERC_PREC={}'.format( m_lerc_prec), '{}V2=ON'.format(' ' if m_lerc_prec else '') if m_compression == _LERC2 or m_compression == _LERC else ''))
    args.append ('-co')
    args.append ('{}={}'.format('BLOCKXSIZE' if m_mode.lower() == 'gtiff' else 'BLOCKSIZE', m_bsize))
    return args

def args_Callback_for_meta(args, user_data = None):
    _LERC =  'lerc'
    _LERC2 = 'lerc2'
    m_scale = 2
    m_bsize = CCFG_BLOCK_SIZE
    m_pyramid = True
    m_comp = _LERC
    m_lerc_prec = None
    m_compression_quality = 85
    if (user_data):
        try:
            scale_ = user_data[CIDX_USER_CONFIG].getValue('Scale')
            if (scale_):
                m_scale = scale_
            bsize_ = user_data[CIDX_USER_CONFIG].getValue('BlockSize')
            if (bsize_):
                m_bsize = bsize_
            ovrpyramid = user_data[CIDX_USER_CONFIG].getValue('isuniformscale')
            if (ovrpyramid):
                m_pyramid = ovrpyramid
            py_comp = user_data[CIDX_USER_CONFIG].getValue('Compression')
            if (py_comp):
                m_comp = py_comp
            compression_quality_ = user_data[CIDX_USER_CONFIG].getValue('Quality')
            if (compression_quality_):
                m_compression_quality = compression_quality_
            m_interleave = user_data[CIDX_USER_CONFIG].getValue(CCFG_INTERLEAVE)
            if (m_interleave):
                m_interleave = m_interleave.upper()
            lerc_prec = user_data[CIDX_USER_CONFIG].getValue('LERCPrecision')
            if (lerc_prec):
                m_lerc_prec = lerc_prec
        except:     # could throw if index isn't found
            pass    # ingnore with defaults.
    args.append ('-of')
    args.append ('MRF')
    args.append ('-co')
    args.append ('COMPRESS=%s' % (_LERC if m_comp == _LERC2 else m_comp))
    if (m_comp.startswith(_LERC)):
        if (m_lerc_prec or
            m_comp == _LERC2 or
            m_comp == _LERC):
            args.append ('-co')
            args.append ('OPTIONS={}{}'.format('' if not m_lerc_prec else 'LERC_PREC={}'.format( m_lerc_prec), '{}V2=ON'.format(' ' if m_lerc_prec else '') if m_comp == _LERC2 or m_comp == _LERC else ''))
    elif(m_comp == 'jpeg'):
        args.append ('-co')
        args.append ('QUALITY=%s' % (m_compression_quality))
        args.append ('-co')
        args.append ('INTERLEAVE=%s' % (m_interleave))
    args.append ('-co')
    args.append ('NOCOPY=True')
    if (m_pyramid == True):
        args.append ('-co')
        args.append ('UNIFORM_SCALE=%s' % (m_scale))
    args.append ('-co')
    args.append ('BLOCKSIZE=%s' % (m_bsize))
    args.append ('-co')
    # let's fix the cache extension
    cache_source = user_data[0]
    args.append ('CACHEDSOURCE=%s' % (cache_source))
    # ends
    return args

def copy_callback(file, src, dst):
    Message(file)
    return True

def exclude_callback(file, src, dst):
    if (file is None):
        return False
    (f, e) = os.path.splitext(file)
    if (e[1:] in cfg.getValue(CCFG_RASTERS_NODE) or
        src.lower().startswith('http')):
        raster_buff.append({'f' : file, 'src' : '' if src == '/' else src, 'dst' : dst if dst else ''})
        return True
    return False

def exclude_callback_for_meta(file, src, dst):
    exclude_callback (file, src, dst)

def getSourcePathUsingTempOutput(input):
    # cfg, _rpt are global vars.
    if (not _rpt or
        not getBooleanValue(cfg.getValue('istempoutput'))):
        return None
    _mk_path = input.replace(cfg.getValue('tempoutput', False), '')
    _indx  = -1
    if (True in [_mk_path.lower().endswith(i) for i in ['.idx', '.lrc', '.pjg', '.pzp', '.pft', '.ppng', '.pjp', '.aux.xml']]):       # if any one of these extensions fails,
        _indx = _mk_path.rfind('.')                                                          # the main (raster) file upload entry in (Reporter) would be set to (no) denoting a failure in one of its associated files.
    if (_indx == -1):
        return (_rpt.findExact('{}{}'.format(_rpt.root, _mk_path)))
    for i in _rpt:
        if (i.find(_mk_path[:_indx + 1]) != -1):
            if (True in [i.endswith(x) for x in cfg.getValue(CCFG_RASTERS_NODE)]):
                return i
    return None

def setUploadRecordStatus (input, rpt_status):
    _rpt_src = getSourcePathUsingTempOutput(input)
    if (_rpt_src and
        _rpt.updateRecordStatus(_rpt_src, CRPT_UPLOADED, rpt_status)):
        return True
    return False

class Copy:
    def __init__(self, base = None):
        self._base = base

    def init(self, src, dst, copy_list, cb_list, user_config = None):
        if (not dst or
            not src):
            return False
        self.src= src.replace('\\', '/')
        self._input_flist = None
        if (not os.path.isdir(self.src)):
            if (not os.path.exists(self.src)):
                self.message ('Invalid -input report file ({})'.format(self.src), const_critical_text)
                return False
            self._input_flist = self.src
            try:
                global _rpt
                self.src = _rpt.root;
            except Exception as e:
                self.message ('Report ({})'.format(str(e)), const_critical_text)
                return False
        if (self.src[-1:] != '/'):
                self.src += '/'

        self.dst = dst.replace('\\', '/')
        if (self.dst[-1:] != '/'):
            self.dst += '/'
        self.format = copy_list
        self.cb_list = cb_list

        self.m_user_config = None
        self.__m_include_subs = True

        if (user_config != None):
            self.m_user_config = user_config
            include_subs = self.m_user_config.getValue('IncludeSubdirectories')
            if (include_subs is not None):    # if there's a value either (!None), take it else defaults to (True)
                self.__m_include_subs = getBooleanValue(include_subs)
        return True

    def message(self, msg, msgType = None):
        if (self._base):
            return (self._base.message(msg, msgType))
        print (msg)

    def processs(self, post_processing_callback = None, post_processing_callback_args = None, pre_processing_callback = None):
        log = None
        if (self._base):
           log = self._base.getMessageHandler
        if (log):
            log.CreateCategory('Copy')

        self.message ('Copying non rasters/aux files (%s=>%s)..' % (self.src, self.dst))
        # init - TIL files
        is_link = not self._input_flist is None;
        if (til):
            for r,d,f in _rpt.walk() if is_link else os.walk(self.src):
                for file in f:
                    if (not file):
                        continue
                    if (self.__m_include_subs == False):
                        if ((r[:-1] if r[-1:] == '/' else r) != os.path.dirname(self.src)):     # note: first arg to walk (self.src) has a trailing '/'
                            continue
                    if (file.lower().endswith(CTIL_EXTENSION_)):
                        _til_filename = os.path.join(r, file)
                        if (til):
                            til.addTIL(_til_filename)
            for _til in til:
                til.process(_til)
        # ends
        for r,d,f in _rpt.walk() if is_link else os.walk(self.src):
            for file in f:
                if (not file):
                    continue
                if (self.__m_include_subs == False):
                    if ((r[:-1] if r[-1:] == '/' else r) != os.path.dirname(self.src)):     # note: first arg to walk (self.src) has a trailing '/'
                        continue
                free_pass = False
                dst_path = r.replace(self.src, self.dst)
                if (('*' in self.format['copy']) == True):
                    free_pass = True
                if (free_pass == False):
                    _isCpy = False
                    for _p in self.format['copy']:
                        if (file.endswith(_p)):
                            _isCpy = True
                            break
                    if (not _isCpy):
                        continue
                isInputWebAPI = False
                if (_rpt and
                    _rpt._isInputHTTP):
                    (f, e) = os.path.splitext(file)
                    if (not e):     # if no file extension at the end of URL, it's assumed we're talking to a service which in turn returns a raster.
                        isInputWebAPI = True
                if (True in [file.endswith('.{}'.format(x)) for x in self.format['exclude']] and
                    not file.lower().endswith(CTIL_EXTENSION_) or       # skip 'exclude' list items and always copy (.til) files to destination.
                    isInputWebAPI):
                    if (('exclude' in self.cb_list) == True):
                        if (self.cb_list['exclude'] is not None):
                            if (self.m_user_config is not None):
                                if (getBooleanValue(self.m_user_config.getValue('istempoutput')) == True):
                                    dst_path = r.replace(self.src, self.m_user_config.getValue('tempoutput', False))    # no checks on temp-output validty done here. It's assumed it has been prechecked at the time of its assignment.
                            _r  = r
                            if (self.m_user_config):
                                if (self.m_user_config.getValue(CLOAD_RESTORE_POINT)):
                                    if (getBooleanValue(self.m_user_config.getValue('istempinput'))):
                                        r = r.replace(self.src, self.m_user_config.getValue(CUSR_TEMPINPUT))
                            if (_rpt and
                                _rpt._isInputHTTP):
                                _mkRemoteURL = os.path.join(_r, file)
                                try:
                                    import urllib2
                                    file_url = urllib2.urlopen(_mkRemoteURL if not isInputWebAPI else os.path.splitext(_mkRemoteURL)[0])
                                    isFileNameInHeader = False
                                    for v in file_url.headers.headers:
                                        if (v.startswith('Content-Disposition')):
                                            token = 'filename='
                                            f = v.find(token)
                                            if (f != -1):
                                                e = v.find('\r',  f + len(token))
                                                if (_mkRemoteURL in _rpt._input_list_info):
                                                    _rpt._input_list_info[_mkRemoteURL][Report.CRPT_URL_TRUENAME] = v[f + len(token) : e].strip().replace('"', '').replace('?', '_')
                                                isFileNameInHeader = True
                                            break
                                    if (self.m_user_config.getValue(CUSR_TEMPINPUT)):    # we've to dn the file first and save to the name requested.
                                        r = r.replace(self.src, self.m_user_config.getValue(CUSR_TEMPINPUT))
                                        if (not os.path.exists(r)):
                                            os.makedirs(r)
                                        file = _rpt._input_list_info[_mkRemoteURL][Report.CRPT_URL_TRUENAME] if isFileNameInHeader else file
                                        with open(os.path.join(r, file), 'wb') as fp:
                                            buff = 2024 * 1024
                                            while True:
                                                chunk = file_url.read(buff)
                                                if not chunk: break
                                                fp.write(chunk)
                                        # mark download/copy status
                                        if (_rpt):
                                            _rpt.updateRecordStatus (_mkRemoteURL, CRPT_COPIED, CRPT_YES)
                                        # ends
                                except Exception as e:
                                    self._base.message('{}'.format(str(e), self._base.const_critical_text))
                            if (self.cb_list['exclude'](file, r, dst_path) == False):       # skip fruther processing if 'false' returned from the callback fnc
                                continue
                    continue
                try:
                    if (('copy' in self.cb_list) == True):
                        if (self.cb_list['copy'] is not None):
                            if (self.cb_list['copy'](file, r, dst_path) == False):       # skip fruther processing if 'false' returned
                                continue

                    if (not g_is_generate_report):              # do not create folders for op==reporting only.
                        if (os.path.exists(dst_path) == False):
                            os.makedirs(dst_path)

                    dst_file = os.path.join(dst_path, file)
                    src_file = os.path.join(r, file)
                    do_post_processing_cb = do_copy = True
                    if (os.path.dirname(src_file.replace('\\','/')) != os.path.dirname(dst_path.replace('\\', '/')) or
                        g_is_generate_report):
                        if (pre_processing_callback):
                            do_post_processing_cb = do_copy = pre_processing_callback(src_file, dst_file, self.m_user_config)
                        if (do_copy):
                             if (self.m_user_config.getValue(CLOAD_RESTORE_POINT)):
                                if (_rpt.getRecordStatus(src_file, CRPT_COPIED) == CRPT_YES or
                                    (getBooleanValue(self.m_user_config.getValue(CCLOUD_UPLOAD)) and
                                     _rpt.getRecordStatus(src_file, CRPT_UPLOADED) == CRPT_YES) or
                                     _rpt.operation == COP_UPL):
                                     do_copy = False
                             if (do_copy):
                                shutil.copyfile(src_file, dst_file)
                                if (self.m_user_config):
                                    # Clone folder will get all the metadata files by default.
                                    _clonePath = self.m_user_config.getValue(CCLONE_PATH, False)
                                    if (_clonePath):
                                        _cloneDstFile = dst_file.replace(self.dst, _clonePath)
                                        _cloneDirs = os.path.dirname(_cloneDstFile)
                                        if (not os.path.exists(_cloneDirs)):
                                            os.makedirs(_cloneDirs)
                                        shutil.copyfile(src_file, _cloneDstFile)
                                    # ends
                             if (self._input_flist):
                                _rpt.updateRecordStatus (src_file, CRPT_COPIED, CRPT_YES)
                             self.message ('{} {}'.format(CRESUME_MSG_PREFIX if not do_copy else 'CPY', src_file.replace(self.src, '')))
                    # copy post-processing
                    if (do_post_processing_cb):
                        if (post_processing_callback):
                            ret = post_processing_callback(dst_file, post_processing_callback_args)    # ignore errors from the callback
                    # ends
                except Exception as e:
                    if (self._input_flist):
                        _rpt.updateRecordStatus (os.path.join(r, file), CRPT_COPIED, CRPT_NO)
                    self.message ('(%s)' % (str(e)), const_critical_text)
                    continue
        self.message ('Done.')
        if (log):
            log.CloseCategory()
        return True
    def get_group_filelist(self, input_source):          # static
        m_input_source = input_source.replace('\\', '/')
        input_path = os.path.dirname(m_input_source)
        file_buff = []
        (p, e) = os.path.splitext(m_input_source)
        for r,d,f in os.walk(input_path):
            for file in f:
                mk_path = os.path.join(r, file).replace('\\', '/')
                if (mk_path.startswith(p)):
                    file_buff.append(mk_path)
        return file_buff
    def batch(self, file_lst, args = None,  pre_copy_callback = None):
        threads = []
        files_len = len(file_lst)
        batch = 1
        s = 0
        while 1:
            m = s + batch
            if (m >= files_len):
                m =  files_len
            threads = []
            for i in range(s, m):
                req = file_lst[i]
                (input_file , output_file) = getInputOutput(req['src'], req['dst'], req['f'], False)
                dst_path = os.path.dirname(output_file)
                if (os.path.exists(dst_path) == False):
                    os.makedirs(dst_path)
                CCOPY = 0
                CMOVE = 1
                mode_ = CCOPY        # 0 = copy, 1 = move
                if (args is not None):
                    if (isinstance(args, dict) == True):
                        if (('mode' in args) == True):
                            if (args['mode'].lower() == 'move'):
                                mode_ = CMOVE
                if (mode_ == CCOPY):
                    self.message ('[CPY] %s' % (output_file))
                    shutil.copyfile(input_file, output_file)
                elif (mode_ == CMOVE):
                    self.message ('[MV] %s' % (output_file))
                    shutil.move(input_file, output_file)
            s = m
            if s == files_len or s == 0:
                break
                pass
                # ends
        return True

class compression:
    def __init__(self, gdal_path, base):
        self.m_gdal_path = gdal_path
        self.CGDAL_TRANSLATE_EXE = 'gdal_translate.exe'
        self.CGDAL_BUILDVRT_EXE = 'gdalbuildvrt.exe'
        self.CGDAL_ADDO_EXE = 'gdaladdo.exe'
        self.m_id = None
        self.m_user_config = None
        self._base = base
    def init(self, id = None):
        if (id != None):
            self.m_id = id
        if (not self._base or
            not isinstance(self._base, Base) or
            not isinstance(self._base.getUserConfiguration, Config)):
            Message ('Err/Internal. (Compression) instance is not initialized with a valid (Base) instance.', const_critical_text)
            return False
        self.m_user_config = self._base.getUserConfiguration

        # intenal gdal_pathc could get modified here.
        if (not self.m_gdal_path or
            os.path.isdir(self.m_gdal_path) == False):
            if (self.m_gdal_path):
                self.message('Invalid GDAL path ({}) in paramter file. Using default location.'.format(self.m_gdal_path), const_warning_text)
            self.m_gdal_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), r'gdal/bin')
            if (not os.path.isdir(self.m_gdal_path)):
                self.message('GDAL not found at ({}).'.format(self.m_gdal_path), self._base.const_critical_text)
                return False
        # ends
        # set gdal_data enviornment path
        os.environ['GDAL_DATA'] = os.path.join(os.path.dirname(self.m_gdal_path), 'data')
        # ends
        msg_text = '(%s) is not found at (%s)'
        if (os.path.isfile(os.path.join(self.m_gdal_path, self.CGDAL_TRANSLATE_EXE)) == False):
            self.message(msg_text % (self.CGDAL_TRANSLATE_EXE, self.m_gdal_path), self._base.const_critical_text)
            return False
        if (os.path.isfile(os.path.join(self.m_gdal_path, self.CGDAL_ADDO_EXE)) == False):
            self.message(msg_text % (self.CGDAL_ADDO_EXE, self.m_gdal_path), self._base.const_critical_text)
            return False
        return True
    def message(self, msg, status = const_general_text):
        write = msg
        if (self.m_id != None):
            write = '[{}] {}'.format(threading.current_thread().name, msg)
        self._base.message(write, status)
        return True
    def buildMultibandVRT(self, input_files, output_file):
        if (len(input_files) ==  0):
            return False
        args = [os.path.join(self.m_gdal_path, self.CGDAL_BUILDVRT_EXE)]
        args.append (output_file)
        for f in (input_files):
            args.append(f)
        self.message('Creating VRT output file (%s)' % (output_file))
        return self.__call_external(args)
    def compress(self, input_file, output_file, args_callback = None, build_pyramids = True, post_processing_callback = None, post_processing_callback_args = None):
        if (_rpt):
            if (input_file in _rpt._input_list_info and
                Report.CRPT_URL_TRUENAME in _rpt._input_list_info[input_file]):
                output_file = '{}/{}'.format(os.path.dirname(output_file), _rpt._input_list_info[input_file][Report.CRPT_URL_TRUENAME])
        _vsicurl_input = self.m_user_config.getValue(CIN_S3_PREFIX, False)
        _input_file = input_file.replace(_vsicurl_input, '') if _vsicurl_input else input_file
        if (getBooleanValue(self.m_user_config.getValue('istempinput'))):
            if (_rpt):
                _input_file = _input_file.replace(self.m_user_config.getValue(CUSR_TEMPINPUT, False), '' if  _rpt.root == '/' else _rpt.root)
        _do_process = ret = True
        # get restore point snapshot
        if (self.m_user_config.getValue(CLOAD_RESTORE_POINT)):
            _get_rstr_val = _rpt.getRecordStatus(_input_file, CRPT_PROCESSED)
            if (_get_rstr_val == CRPT_YES or
                _rpt.operation == COP_UPL):
                if (_rpt.operation != COP_UPL):
                    self.message ('{} {}'.format(CRESUME_MSG_PREFIX, _input_file))
                _do_process = False
        # ends
        post_process_output = output_file
        if (_do_process):
            out_dir_path = os.path.dirname(output_file)
            if (os.path.exists(out_dir_path) == False):
                try:
                    os.makedirs(os.path.dirname(output_file))   # let's try to make the output dir-tree else GDAL would fail
                except Exception as exp:
                    time.sleep(2)    # let's try to sleep for few seconds and see if any other thread has created it.
                    if (os.path.exists(out_dir_path) == False):
                        self.message ('(%s)' % str(exp), const_critical_text)
                        if (_rpt):
                            _rpt.updateRecordStatus (_input_file, CRPT_PROCESSED, CRPT_NO)
                        return False
            # ends
            isModeClone = self.m_user_config.getValue('Mode') == 'clonemrf'
            do_process = (_rpt and _rpt.operation != COP_NOCONVERT) and not isModeClone
            if (not do_process):
                self.message('CPY {}'.format(_input_file))
                if (input_file.startswith('/vsicurl/')):
                    try:
                        _dn_vsicurl_ = input_file.split('/vsicurl/')[1]
                        import urllib2
                        file_url = urllib2.urlopen(_dn_vsicurl_)
                        validateForClone = isModeClone
                        with open(output_file, 'wb') as fp:
                            buff = 2024 * 1024
                            while True:
                                chunk = file_url.read(buff)
                                if (validateForClone):
                                    validateForClone = False
                                    if (chunk[:10] != '<{}>'.format(CMRF_DOC_ROOT)):
                                        self.message ('Invalid MRF ({})'.format(_dn_vsicurl_), const_critical_text)
                                        raise Exception
                                if not chunk: break
                                fp.write(chunk)
                    except Exception as e:
                        if (_rpt):
                            _rpt.updateRecordStatus (_input_file, CRPT_PROCESSED, CRPT_NO)
                            return False
                else:
                    if (getBooleanValue(self.m_user_config.getValue('istempinput')) or
                        not getBooleanValue(cfg.getValue('iss3'))):
                        shutil.copyfile(input_file, output_file)
                if (isModeClone):
                    # Simulate the MRF file update (to include the CachedSource) which was earlier done via the GDAL_Translate->MRF driver.
                    try:
                        _CDOC_ROOT = 'MRF_META'
                        _CDOC_CACHED_SOURCE = 'CachedSource'
                        _CDOC_SOURCE = 'Source'
                        doc = minidom.parse(output_file)
                        nodeMeta = doc.getElementsByTagName(_CDOC_ROOT)
                        nodeRaster = doc.getElementsByTagName('Raster')
                        if (not nodeMeta or
                            not nodeRaster):
                            raise Exception()
                        cachedNode = doc.getElementsByTagName(_CDOC_CACHED_SOURCE)
                        if (not cachedNode):
                            cachedNode.append(doc.createElement(_CDOC_CACHED_SOURCE))
                        nodeSource = doc.getElementsByTagName(_CDOC_SOURCE)
                        if (not nodeSource):
                            nodeSource.append(doc.createElement(_CDOC_SOURCE))
                        if (nodeSource[0].hasChildNodes()):
                            nodeSource[0].removeChild(nodeSource[0].firstChild)
                        nodeSource[0].appendChild(doc.createTextNode(input_file))
                        cachedNode[0].appendChild(nodeSource[0])
                        nodeMeta[0].insertBefore(cachedNode[0], nodeRaster[0])
                        with open (output_file, "w") as c:
                            _mrfBody = doc.toxml().replace('&quot;', '"')       # GDAL mrf driver can't handle XML entity names.
                            _indx = _mrfBody.find ('<{}>'.format(_CDOC_ROOT))
                            if (_indx == -1):
                                raise Exception()
                            _mrfBody = _mrfBody[_indx:]
                            c.write(_mrfBody)
                    except:
                        self.message ('Invalid MRF ({})'.format(input_file), const_critical_text)
                        if (_rpt):
                            _rpt.updateRecordStatus (_input_file, CRPT_PROCESSED, CRPT_NO)
                        return False
                    # ends
            do_pyramids = self.m_user_config.getValue('Pyramids')
            if (do_pyramids != CCMD_PYRAMIDS_ONLY and
                do_process):
                args = [os.path.join(self.m_gdal_path, self.CGDAL_TRANSLATE_EXE)]
                if (args_callback is None):      # defaults
                    args.append ('-of')
                    args.append ('MRF')
                    args.append ('-co')
                    args.append ('COMPRESS=LERC')
                    args.append ('-co')
                    args.append ('BLOCKSIZE=512')
                else:
                    args = args_callback(args, [input_file, output_file, self.m_user_config])      # callback user function to get arguments.
                args.append (input_file)
                args.append (output_file)
                self.message('Applying compression (%s)' % (input_file))
                ret = self.__call_external(args)
                self.message('Status: (%s).' % ('OK' if ret == True else 'FAILED'))
                if (ret == False):
                    if (_rpt):
                        _rpt.updateRecordStatus (_input_file, CRPT_PROCESSED, CRPT_NO)
                    return ret
            if (build_pyramids):        # build pyramids is always turned off for rasters that belong to (.til) files.
                if (getBooleanValue(do_pyramids) or     # accept any valid boolean value.
                    do_pyramids == CCMD_PYRAMIDS_ONLY or
                    do_pyramids == CCMD_PYRAMIDS_EXTERNAL):
                    iss3 = self.m_user_config.getValue('iss3')
                    if (iss3 == True and do_pyramids == CCMD_PYRAMIDS_ONLY):
                        if (do_pyramids != CCMD_PYRAMIDS_ONLY):     # s3->(local)->.ovr
                            input_file = output_file
                        output_file = output_file + '.__vrt__'
                        self.message ('BuildVrt (%s=>%s)' % (input_file, output_file))
                        ret = self.buildMultibandVRT([input_file], output_file)
                        self.message('Status: (%s).' % ('OK' if ret == True else 'FAILED'))
                        if (ret == False):
                            if (_rpt):
                                _rpt.updateRecordStatus (_input_file, CRPT_PROCESSED, CRPT_NO)
                            return ret  # we can't proceed if vrt couldn't be built successfully.
                    ret = self.createaOverview(output_file)
                    self.message('Status: (%s).' % ('OK' if ret == True else 'FAILED'), const_general_text if ret == True else const_critical_text)
                    if (ret == False):
                        if (_rpt):
                            _rpt.updateRecordStatus (_input_file, CRPT_PROCESSED, CRPT_NO)
                        return False
                    if (iss3 == True and
                        do_pyramids == CCMD_PYRAMIDS_ONLY):
                        try:
                            os.remove(output_file)      # *.ext__or__ temp vrt file.
                            in_  = output_file + '.ovr'
                            out_ = in_.replace('.__vrt__' + '.ovr', '.ovr')
                            if (os.path.exists(out_) == True):
                                os.remove(out_)         # probably leftover from a previous instance.
                            self.message ('rename (%s=>%s)' % (in_, out_))
                            os.rename(in_, out_)
                        except:
                            self.message ('Unable to rename/remove (%s)' % (output_file), const_warning_text)
                            if (_rpt):
                                _rpt.updateRecordStatus (_input_file, CRPT_PROCESSED, CRPT_NO)
                            return False
        # Do auto generate cloneMRF?
        if (self.m_user_config.getValue(CCLONE_PATH)):
            updateMRF = UpdateMRF(self._base)
            _output_home_path = self.m_user_config.getValue(CCFG_PRIVATE_OUTPUT, False)
            if (getBooleanValue(self.m_user_config.getValue(CCLOUD_UPLOAD))):
                _output_home_path = self.m_user_config.getValue('tempoutput', False)
            if (updateMRF.init(output_file, self.m_user_config.getValue(CCLONE_PATH), self.m_user_config.getValue('Mode'),
                self.m_user_config.getValue(CCACHE_PATH), _output_home_path, self.m_user_config.getValue(COUT_VSICURL_PREFIX, False))):
                updateMRF.copyInputMRFFilesToOutput();
        # ends
        # call any user-defined fnc for any post-processings.
        if (post_processing_callback):
            if (getBooleanValue(self.m_user_config.getValue(CCLOUD_UPLOAD)) == True):
                self.message ('[S3-Push]..');
            ret = post_processing_callback(post_process_output, post_processing_callback_args, {'f' : post_process_output, 'cfg' : self.m_user_config})
            self.message('Status: (%s).' % ('OK' if ret == True else 'FAILED'))
        # ends
        if (_rpt and
            _rpt.operation != COP_UPL):
            _rpt.updateRecordStatus (_input_file, CRPT_PROCESSED, CRPT_YES)
        return ret
    def createaOverview(self, input_file, isBQA = False):
        m_py_factor = '2'
        m_py_sampling = 'average'
        get_mode = self.m_user_config.getValue('Mode')
        if (get_mode):
            if (get_mode == 'cachingmrf' or
                get_mode == 'clonemrf' or
                get_mode == 'splitmrf'):
                    return True
        # skip pyramid creation on (tiffs) related to (til) files.
        if (til):
            (p, n) = os.path.split(input_file)
            if (til.find(n)):
                return True
        # ends
        self.message('Creating pyramid ({})'.format(input_file))
        # let's input cfg values..
        py_factor_ = self.m_user_config.getValue('PyramidFactor')
        if (py_factor_ and
            py_factor_.strip()):
            m_py_factor = py_factor_.replace(',', ' ')  # can be commna sep vals in the cfg file.
        else:
            gdalInfo = GDALInfo(self._base, self.message)
            gdalInfo.init(self.m_gdal_path)
            if (gdalInfo.process(input_file)):
                m_py_factor = gdalInfo.pyramidLevels
        py_sampling_ = self.m_user_config.getValue('PyramidSampling')
        if (py_sampling_):
            m_py_sampling = py_sampling_
            if (m_py_sampling.lower() == 'avg' and
                input_file.lower().endswith(CTIL_EXTENSION_)):
                    m_py_sampling = 'average'
        m_py_compression = self.m_user_config.getValue('PyramidCompression')
        args = [os.path.join(self.m_gdal_path, self.CGDAL_ADDO_EXE)]
        args.append ('-r')
        args.append ('nearest' if isBQA else m_py_sampling)
        m_py_quality = self.m_user_config.getValue('Quality')
        m_py_interleave = self.m_user_config.getValue(CCFG_INTERLEAVE)
        if (m_py_compression == 'jpeg' or
            m_py_compression == 'png'):
            if (not get_mode.startswith('mrf')):
                m_py_external = False
                py_external_ = self.m_user_config.getValue('Pyramids')
                if (py_external_):
                    m_py_external = py_external_ == CCMD_PYRAMIDS_EXTERNAL
                if (m_py_external):
                    args.append ('-ro')
                if (get_mode.startswith('tif') and
                    m_py_compression == 'jpeg' and
                    m_py_interleave == 'pixel'):
                    args.append ('--config')
                    args.append ('PHOTOMETRIC_OVERVIEW')
                    args.append ('YCBCR')
            args.append ('--config')
            args.append ('COMPRESS_OVERVIEW')
            args.append (m_py_compression)
            args.append ('--config')
            args.append ('INTERLEAVE_OVERVIEW')
            args.append (m_py_interleave)
            args.append ('--config')
            args.append ('JPEG_QUALITY_OVERVIEW')
            args.append (m_py_quality)
        args.append (input_file)
        m_ary_factors = m_py_factor.split()
        for f in m_ary_factors:
            args.append (f)
        return self.__call_external(args)
    def __call_external(self, args):
        p = subprocess.Popen(args, creationflags=subprocess.SW_HIDE, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        message = ''
        first_pass_ = True
        messages = []
        val = p.poll()
        while (val is None):
            time.sleep (0.5)
            val = p.poll()
            message = p.stdout.readline()
            if (message):
                messages.append(message.strip())
        if (messages):
            self.message ('messages:')
            for m in messages:
                    self.message (m)
        warnings = p.stderr.readlines()
        if (warnings):
            self.message ('warnings/errors:')
            is_error = False
            for w in warnings:
                if (not is_error):
                    if (w.find('ERROR') >= 0):
                        is_error = True
                self.message(w.strip())
            if (is_error):
                return False
        return True

class Config:
    def __init__(self):
        pass

    def init(self, config, root):
        try:
            self.m_doc = minidom.parse(config)
        except:
            return False

        nodes = self.m_doc.getElementsByTagName(root)
        if (len(nodes) == 0):
            return False

        node = nodes[0].firstChild
        self.m_cfgs = {}

        while (node != None):
            if (node.hasChildNodes() == False):
                node = node.nextSibling
                continue

            if ((node.nodeName in self.m_cfgs) == False):
                self.m_cfgs[node.nodeName] = node.firstChild.nodeValue

            node = node.nextSibling
            pass
        return True

    def getValue(self, key, toLower = True):  # returns (value) or None
        if ((key in self.m_cfgs) == True):
            if (toLower == True):
                try:    # trap any non-strings
                    return self.m_cfgs[key].lower()
                except:
                    pass
            return self.m_cfgs[key]
        return None

    def setValue(self, key, value):
        if (key in self.m_cfgs):
            if (hasattr(self.m_cfgs[key], '__setitem__') == True):
                self.m_cfgs[key].append(value)
                return
        self.m_cfgs[key] = value

def getInputOutput(inputfldr, outputfldr, file, isinput_s3):
    input_file = os.path.join(inputfldr, file)
    output_file = os.path.join(outputfldr, file)
    if (isinput_s3):
        input_file = cfg.getValue(CIN_S3_PREFIX, False) + input_file
        output_file = outputfldr
        if (getBooleanValue(cfg.getValue('istempinput')) == True or
            getBooleanValue(cfg.getValue('istempoutput')) == True):
            output_file = os.path.join(output_file, file)
            if (getBooleanValue(cfg.getValue('istempinput')) == True):
                input_file = os.path.join(cfg.getValue(CUSR_TEMPINPUT, False), file)
            if (getBooleanValue(cfg.getValue('istempoutput')) == True):
                output_file = os.path.join(cfg.getValue('tempoutput', False), file)
            return (input_file, output_file)
        output_file = os.path.join(output_file, file)
    return (input_file, output_file)

def getBooleanValue(value):
    if (value is None):
        return False
    if (isinstance(value, bool) == True):
        return value
    val = value.lower()
    if (val == 'true' or
        val == 'yes' or
        val == 't' or
        val == '1' or
        val == 'y'):
            return True
    return False

def formatExtensions (value):
    if (value is None or
        len(value.strip()) == 0):
        return []
    frmts = value.split(',')
    for i in range(0, len(frmts)):
        frmts[i] = frmts[i].strip()
    return frmts

# custom exit code block to write out logs
def terminate(objBase, exit_code, log_category = False):
    if (objBase):
        success = 'OK'
        if (exit_code != 0):
            success = 'Failed!'
        objBase.message('[{}]'.format(success), objBase.const_status_text)
        if (log_category == True):
            log.CloseCategory()
        objBase.close() #persist information/errors collected.
    return (exit_code)
# ends

def fn_collect_input_files(src):    # collect input files to support (resume) support.
    if (not src):
        return False
    if (not g_is_generate_report or
        not g_rpt):
        return False
    try:
        _type = 'local'
        _src = str(src)     # input (src) could be an object
        if (_src.startswith('<Key')):
            _type = 'cloud'
            _brk = _src.split(',')
            _src = _brk[1].replace('>', '')
        g_rpt.addFile(_src);
        return True
    except:
        pass
    return False

def fn_pre_process_copy_default (src, dst, arg):
    if (fn_collect_input_files(src)):
        return False             # just gathering information for the report either (op=report). Do not proceed with (Copying/e.t.c)
    if (not src):
        return False
    if (til):
        if (src.lower().endswith(CTIL_EXTENSION_)):
            til.setOutputPath(src, dst)
    return True

def fn_copy_temp_dst(input_source, cb_args, *args):
    fn_cpy_ = Copy()
    file_lst = fn_cpy_.get_group_filelist(input_source)
    if (len(file_lst) == 0):
        return False    # no copying.
    files = []
    for file in file_lst:
        (p, f) = os.path.split(file.replace('\\', '/'))
        if (args is not None):
            if (isinstance(args[0], dict) == True):
                if (('cfg' in args[0]) == True):
                    if (getBooleanValue(args[0]['cfg'].getValue('istempoutput')) == False):
                        return False    # no copying..
                    p += '/'
                    t = args[0]['cfg'].getValue('tempoutput', False).replace('\\', '/')    # safety check
                    if (t.endswith('/') == False): # making sure, replace will work fine.
                        t += '/'
                    o = args[0]['cfg'].getValue(CCFG_PRIVATE_OUTPUT, False).replace('\\', '/') # safety check
                    if (o.endswith('/') == False):
                        o += '/'
                    dst = (p.replace(t, o))
                    files.append({'src' : p, 'dst' : dst, 'f' : f})
    if (len(files) != 0):
        fn_cpy_.batch(files, {'mode' : 'move'}, None)
    return True

class Args:
    def __init__(self):
        pass
    def __setattr__(self, name, value):
        self.__dict__[name] = value
    def __getattr__(self, name):
        try:
            return self.__dict__[name]
        except KeyError:
            return None
    def __str__(self):
        _return_str = ''
        for k in self.__dict__:
            _return_str += '{}={},'.format(k, self.__getattr__(k))
        if (_return_str[-1:] == ','):
            _return_str = _return_str[:len(_return_str) -1]
        return _return_str

class Application(object):
    __program_ver__ = 'v1.6p'
    __program_name__ = 'OptimizeRasters.py %s' % __program_ver__
    __program_desc__ = 'Convert raster formats to a valid output format through GDAL_Translate.\n' + \
    '\nPlease Note:\nOptimizeRasters.py is entirely case-sensitive, extensions/paths in the config ' + \
    'file are case-sensitive and the program will fail if the correct path/case is not ' + \
    'entered at the cmd-line or in the config file.\n'
    def __init__(self, args):
        self._usr_args = args
        self._msg_callback = None
        self._log_path = None
        self._base = None
        self._postMessagesToArcGIS = False
    def __load_config__ (self, config):
        global cfg
        if (self._args is None):
            return False
        # read in the config file.
        if (not self._args.config):
            self._args.config = os.path.abspath(os.path.join(os.path.dirname(__file__), CCFG_FILE))
        config_ = self._args.config
        if (self._args.input and            # Pick up the config file name from a (resume) job file.
            self._args.input.lower().endswith(Report.CJOB_EXT)):
            _r = Report(Base());
            if (not _r.init(self._args.input)):
                self.writeToConsole('Err. ({})/init'.format(self._args.input))
                return False
            if (not _r.read()):
                self.writeToConsole('Err. ({})/read'.format(self._args.input))
                return False
            if (CRPT_HEADER_KEY in _r._header):
                config_ = _r._header[CRPT_HEADER_KEY]
            _r = None
        self._args.config = os.path.abspath(config_)         # replace/force the original path to abspath.
        cfg  = Config()
        ret = cfg.init(config_, 'Defaults')
        if (not ret):
            msg = 'Err. Unable to read-in settings from ({})'.format(config_)
            self.writeToConsole(msg, const_critical_text)   # log file is not up yet, write to (console)
            return False
        # ends
        # deal with cfg extensions (rasters/exclude list)
        rasters_ext_ = cfg.getValue(CCFG_RASTERS_NODE, False)
        if (rasters_ext_ is None):
            rasters_ext_ = 'tif,mrf'        # defaults: in-code if defaults are missing in cfg file.

        exclude_ext_ = cfg.getValue(CCFG_EXCLUDE_NODE, False)
        if (exclude_ext_ is None):
            exclude_ext_ = 'ovr,rrd,aux.xml,idx,lrc,mrf_cache,pjp,ppng,pft,pzp,pjg' # defaults: in-code if defaults are missing in cfg file.

        cfg.setValue(CCFG_RASTERS_NODE, formatExtensions(rasters_ext_))
        cfg.setValue(CCFG_EXCLUDE_NODE, formatExtensions(exclude_ext_))
        # ends
        return True
    def __setup_log_support(self):
        log = None
        try:
            solutionLib_path = os.path.realpath(__file__)
            if (os.path.isdir(solutionLib_path) == False):
                solutionLib_path = os.path.dirname(solutionLib_path)
            _CLOG_FOLDER = 'logs'
            self._log_path  = os.path.join(solutionLib_path, _CLOG_FOLDER)
            sys.path.append(os.path.join(solutionLib_path, 'solutionsLog'))
            import logger
            log = logger.Logger();
            log.Project ('OptimizeRasters')
            log.LogNamePrefix('OR')
            log.StartLog()
            cfg_log_path = cfg.getValue('LogPath')
            if (cfg_log_path):
                if (os.path.isdir(cfg_log_path) == False):
                    Message ('Invalid log-path (%s). Resetting to (%s)' % (cfg_log_path, self._log_path));
                    cfg_log_path = None
            if (cfg_log_path):
                self._log_path = os.path.join(cfg_log_path, _CLOG_FOLDER)
            log.SetLogFolder(self._log_path)
            print ('Log-path set to ({})'.format(self._log_path))
        except Exception as e:
            print ('Warning: External logging support disabled! ({})'.format(str(e)));
        # ends
        # let's write to log (input config file content plus all cmd-line args)
        if (log):
            # inject cmd-line
            log.CreateCategory('Cmd-line')
            cmd_line  = []
            _args_text = str(self._args).lower().replace('namespace(', '').replace('\\\\', '/')
            _args_text_len = len(_args_text)
            _args = _args_text[:_args_text_len - 1 if _args_text[-1:] == ')' else _args_text_len].split(',')
            for arg in _args:
                try:
                    (k, v) = arg.split('=')
                except:
                    log.Message('Invalid arg at cmd-line (%s)' % (arg.strip()), const_critical_text)
                    continue
                if (v != 'none'):
                    cmd_line.append('-{}'.format(arg.replace('\'', '"').strip()))

            log.Message(' '.join(cmd_line), const_general_text);
            log.CloseCategory()
            # ends
            # inject cfg content
            log.CreateCategory('Input-config-values')
            for v in cfg.m_cfgs:
                log.Message('%s=%s' % (v, cfg.m_cfgs[v]), const_general_text)
            log.CloseCategory()
            # ends
        return Base(log, self._msg_callback, cfg)
    def writeToConsole(self, msg, status = const_general_text):
        if (self._msg_callback):
            return (self._msg_callback(msg, status))
        print (msg)          # log file is not up yet, write to (console)
        return True
    def getReport(self):
        global _rpt
        return _rpt if _rpt else None
    def init(self):
        global _rpt, \
        cfg, \
        til
        self.writeToConsole(self.__program_name__)
        self.writeToConsole(self.__program_desc__)
        _rpt = cfg = til = None     #by (default) til extensions or its associated files aren't processed differently.
        if (not self._usr_args):
            return False
        if (isinstance(self._usr_args, argparse.Namespace)):
            self._args = self._usr_args
        else:
            self._args = Args()
            for i in self._usr_args:
                try:
                    self._args.__setattr__(i, self._usr_args[i])
                except:
                    pass
            if (self._args.__getattr__(CRESUME_ARG) is None):
                self._args.__setattr__(CRESUME_ARG, True)
        if (not self.__load_config__(self._args)):
            return False
        self._base = self.__setup_log_support()          # initialize log support.
        if (not self._base.init()):
            self._base.message('Unable to initialize the (Base) module', self._base.const_critical_text)
            return CRET_ERROR
        if (self._args.input and
            os.path.isfile (self._args.input)):
            _rpt = Report(self._base);
            if (not _rpt.init(self._args.input)):        # not checked for return.
                self._base.message('Unable to init (Report/job)', self._base.const_critical_text)
                return False
            for arg in vars(self._args):
                if (arg == CRESUME_HDR_INPUT):
                    continue
                setattr(self._args, arg, None)      # any other cmd-line args will be ignored/nullified.
            if (not _rpt.read(self.__jobContentCallback)):
                self._base.message('Unable to read the -input job file.', self._base.const_critical_text)
                return False
        self._base.getUserConfiguration.setValue('handler_resume_reporter', _rpt)
        # do we need to process (til) files?
        for x in self._base.getUserConfiguration.getValue(CCFG_RASTERS_NODE):
            if (x.lower() == 'til'):
                til = TIL()
                break
        # ends
        return True
    def registerMessageCallback(self, fnptr):
        if (not fnptr):
            return False
        self._msg_callback = fnptr
    @property
    def postMessagesToArcGIS(self):
        return self._postMessagesToArcGIS
    @postMessagesToArcGIS.setter
    def postMessagesToArcGIS(self, value):
        if (not self._base or
            not self._base._m_log or
            not hasattr(self._base._m_log, 'isGPRun')):
            return
        self._postMessagesToArcGIS = self._base.getBooleanValue(value)
        self._base._m_log.isGPRun = self.postMessagesToArcGIS
    def __jobContentCallback(self, line):
        if (cfg):
            if (cfg.getValue(CLOAD_RESTORE_POINT)):      # ignore if not called from main()
                return True
        lns = line.strip().split(',')
        _fname = lns[0].strip().replace('\\', '/')
        if (_fname.startswith(Report.CHEADER_PREFIX)):
            _hdr = _fname.replace(Report.CHEADER_PREFIX, '').split('=')
            if (len(_hdr) > 1):
                _key = _hdr[0].strip()
                if (_key == CRESUME_HDR_INPUT):
                    return True
                setattr (self._args, _key, _hdr[1].strip())
        return True
    def run(self):
        global raster_buff, \
        til, \
        cfg, \
        _rpt, \
        g_rpt, \
        g_is_generate_report, \
        user_args_Callback, \
        S3_storage, \
        azure_storage

        S3_storage = None
        azure_storage = None

        g_rpt = None
        raster_buff = []
        g_is_generate_report = False

        CRESUME_CREATE_JOB_TEXT = '[Resume] Creating job ({})'

        # is resume?
        if (self._args.input and
            os.path.isfile (self._args.input)):
            _rpt = Report(self._base);
            if (not _rpt.init(self._args.input)):        # not checked for return.
                self._base.message ('Unable to init (Reporter/obj)', self._base.const_critical_text)
                return(terminate(self._base, eFAIL))
            if (not _rpt.read()):
                self._base.message ('Unable to read the -input report file ({})'.format(self._args.input), self._base.const_critical_text)
                return(terminate(self._base, eFAIL))
            self._args.job = os.path.basename(self._args.input)
            self._base.getUserConfiguration.setValue('handler_resume_reporter', _rpt)
        # ends

        # Get the default (project name)
        project_name = self._args.job
        if (project_name and
            project_name.lower().endswith(Report.CJOB_EXT)):
            project_name = project_name[:len(project_name) - len(Report.CJOB_EXT)]
        if (not project_name):
            project_name = cfg.getValue(CPRJ_NAME, False)
        if (not project_name):      # is the project still null?
            project_name = Report.getUniqueFileName();     #'OptimizeRasters'
        if (self._base.getMessageHandler):
            self._base.getMessageHandler.LogNamePrefix(project_name)           # update (log) file name prefix.
        cfg.setValue(CPRJ_NAME, project_name)
        _project_path = '{}{}'.format(os.path.join(os.path.dirname(self._args.input if self._args.input and self._args.input.lower().endswith(Report.CJOB_EXT) else __file__), project_name), Report.CJOB_EXT)
        if (not cfg.getValue(CLOAD_RESTORE_POINT)):
            if (os.path.exists(_project_path)):
                self._args.op = None
                self._args.input = _project_path
                cfg.setValue(CLOAD_RESTORE_POINT, True)
                self._base.message ('{} Using job ({})'.format (CRESUME_MSG_PREFIX, _project_path))
                _status = self.run()
                return
        # ends
        # detect input cloud type
        inAmazon = CCLOUD_AMAZON
        dn_cloud_type = self._args.clouddownloadtype;
        if (not dn_cloud_type):
            dn_cloud_type = cfg.getValue(CIN_CLOUD_TYPE, True)
        inAmazon = dn_cloud_type == CCLOUD_AMAZON or not dn_cloud_type
        # ends
        # let's create a restore point
        if (not self._args.input or        # assume it's a folder from s3/azure
            (self._args.input and
            not os.path.isfile(self._args.input))):
            if (not self._args.op):
                self._args.op = COP_RPT
        # valid (op/utility) commands
        _utility = {
        COP_UPL : None,
        COP_DNL : None,
        COP_RPT : None,
        COP_NOCONVERT : None
        }
        # ends
        if (self._args.op):
            self._args.op = self._args.op.lower()
            if (not self._args.op in _utility):
                self._base.message ('Invalid utility operation mode ({})'.format(self._args.op), self._base.const_critical_text)
                return(terminate(self._base, eFAIL))
            if(self._args.op == COP_RPT or
                self._args.op == COP_UPL or
                self._args.op == COP_NOCONVERT):
                g_rpt = Report(self._base);
                if (not g_rpt.init(_project_path, self._args.input if self._args.input else cfg.getValue(CIN_S3_PARENTFOLDER if inAmazon else CIN_AZURE_PARENTFOLDER, False))):
                    self._base.message ('Unable to init (Report)', self._base.const_critical_text)
                    return(terminate(self._base, eFAIL))
                g_is_generate_report = True
                if (self._args.op == COP_UPL):
                    self._args.cloudupload = 'true'
                    self._args.tempoutput = self._args.input if os.path.isdir(self._args.input) else os.path.dirname(self._args.input)
                    if (cfg.getValue(CLOAD_RESTORE_POINT) and
                        _rpt):
                        if (not 'input' in _rpt._header):
                            return(terminate(self._base, eFAIL))
                        self._args.tempoutput = _rpt._header['input']
        # fix the slashes to force a convention
        if (self._args.input):
            self._args.input = self._base.convertToForwardSlash(self._args.input, not self._args.input.lower().endswith(Report.CJOB_EXT))
        if (self._args.output):
            self._args.output = self._base.convertToForwardSlash(self._args.output)
        if (self._args.cache):
            self._args.cache = self._base.convertToForwardSlash(self._args.cache)
        # ends
        # read in (interleave)
        if (cfg.getValue(CCFG_INTERLEAVE) is None):
            cfg.setValue(CCFG_INTERLEAVE, 'PIXEL');
        # ends
        # overwrite (Out_CloudUpload, IncludeSubdirectories) with cmd-line args if defined.
        if (self._args.cloudupload or self._args.s3output):
            cfg.setValue(CCLOUD_UPLOAD, getBooleanValue(self._args.cloudupload) if self._args.cloudupload else getBooleanValue(self._args.s3output))
            cfg.setValue(CCLOUD_UPLOAD_OLD_KEY, cfg.getValue(CCLOUD_UPLOAD))
            if (self._args.clouduploadtype):
                self._args.clouduploadtype = self._args.clouduploadtype.lower()
                cfg.setValue(COUT_CLOUD_TYPE, self._args.clouduploadtype)
        is_cloud_upload = getBooleanValue(cfg.getValue(CCLOUD_UPLOAD)) if cfg.getValue(CCLOUD_UPLOAD) else getBooleanValue(cfg.getValue(CCLOUD_UPLOAD_OLD_KEY))
        # for backward compatibility (-s3output)
        if (not cfg.getValue(CCLOUD_UPLOAD)):
            cfg.setValue(CCLOUD_UPLOAD, is_cloud_upload)
        if (not cfg.getValue(COUT_CLOUD_TYPE)):
            cfg.setValue(COUT_CLOUD_TYPE, CCLOUD_AMAZON)
        # ends
        if (self._args.subs):
            cfg.setValue('IncludeSubdirectories', getBooleanValue(self._args.subs))
        # ends
        # do we have -tempinput path to copy rasters first before conversion.
        is_input_temp = False
        if (self._args.tempinput):
            self._args.tempinput = self._base.convertToForwardSlash(self._args.tempinput)
            if (not os.path.isdir(self._args.tempinput)):
                try:
                    os.makedirs(self._args.tempinput)
                except Exception as exp:
                    self._base.message('Unable to create the -tempinput path (%s) [%s]' % (self._args.tempinput, str(exp)), self._base.const_critical_text)
                    return(terminate(self._base, eFAIL))
            is_input_temp = True         # flag flows to deal with -tempinput
            cfg.setValue('istempinput', is_input_temp)
            cfg.setValue(CUSR_TEMPINPUT, self._args.tempinput)
        # ends
        # let's setup -tempoutput
        is_output_temp = False
        if (self._args.tempoutput):
            self._args.tempoutput = self._base.convertToForwardSlash(self._args.tempoutput)
            if (not os.path.isdir(self._args.tempoutput)):
                # attempt to create the -tempoutput
                try:
                    if (not self._args.op or
                        (self._args.op and
                         self._args.op != COP_UPL)):
                        os.makedirs(self._args.tempoutput)
                except Exception as exp:
                    self._base.message ('Unable to create the -tempoutput path (%s)\n[%s]' % (self._args.tempoutput, str(exp)), self._base.const_critical_text)
                    return(terminate(self._base, eFAIL))
                # ends
            is_output_temp = True
            cfg.setValue('istempoutput', is_output_temp)
            cfg.setValue('tempoutput', self._args.tempoutput)
        # ends
        # are we doing input from S3|Azure?
        err_init_msg = 'Unable to initialize the ({}) upload module! Check module setup/credentials. Quitting..'
        isinput_s3 = getBooleanValue(self._args.s3input);
        if (self._args.clouddownload):
            isinput_s3 = getBooleanValue(self._args.clouddownload);
        # import boto modules only when required. This allows users to run the program for only local file operations.
        if ((inAmazon and
             isinput_s3) or
            (getBooleanValue(cfg.getValue(CCLOUD_UPLOAD)) and
             cfg.getValue(COUT_CLOUD_TYPE) == CCLOUD_AMAZON)):
            cfg.setValue(CCFG_PRIVATE_INC_BOTO, True)
            try:
                global boto
                import boto
                from boto.s3.key import Key
                from boto.s3.connection import OrdinaryCallingFormat
            except:
                self._base.message ('\n%s requires the (boto) module to run its S3 specific operations. Please install (boto) for python.' % (self.__program_name__), self._base.const_critical_text)
                return(terminate(self._base, eFAIL))
        # ends
        # take care of missing -input and -output if -clouddownload==True
        # Note/Warning: S3/Azure inputs/outputs are case-sensitive hence wrong (case) could mean no files found on S3/Azure
        if (isinput_s3 == True):
            _cloudInput = self._args.input
            if (not _cloudInput):
                _cloudInput = cfg.getValue(CIN_S3_PARENTFOLDER if inAmazon else CIN_AZURE_PARENTFOLDER, False);
            if (_cloudInput):
                self._args.input = _cloudInput = _cloudInput.strip().replace('\\', '/')
            cfg.setValue(CIN_S3_PARENTFOLDER, _cloudInput)
        if (is_cloud_upload):
            if (not is_output_temp):
                if ((self._args.op and self._args.op != COP_UPL) or
                    not self._args.op and
                    (_rpt and
                    _rpt.operation != COP_UPL)):
                    self._base.message ('-tempoutput must be specified if -cloudupload=true', self._base.const_critical_text)
                    return(terminate(self._base, eFAIL))
            _access = cfg.getValue(COUT_AZURE_ACCESS)
            if (_access):
                if (not _access in ('private', 'blob', 'container')):
                        self._base.message ('Invalid value for ({})'.format(COUT_AZURE_ACCESS), self._base.const_critical_text);
                        return(terminate(self._base, eFAIL))
                if (_access == 'private'):      # private is not recognized by Azure, used internally only for clarity
                    cfg.setValue(COUT_AZURE_ACCESS, None)       # None == private container

            if (self._args.output is None):
                _cloud_upload_type = cfg.getValue(COUT_CLOUD_TYPE, True)
                if (_cloud_upload_type == CCLOUD_AMAZON):
                    self._args.output = cfg.getValue(COUT_S3_PARENTFOLDER, False);
                elif (_cloud_upload_type == CCLOUD_AZURE):
                    self._args.output = cfg.getValue(COUT_AZURE_PARENTFOLDER, False);
                else:
                    self._base.message ('Invalid value for ({})'.format(COUT_CLOUD_TYPE), self._base.const_critical_text)
                    return(terminate (self._base, eFAIL))
                if (self._args.output):
                    self._args.output = self._args.output.strip().replace('\\', '/')
                cfg.setValue(COUT_S3_PARENTFOLDER, self._args.output)
        # ends
        if (not self._args.output or
            not self._args.input):
            if ((not self._args.op and
                not self._args.input) or
                (self._args.op and
                not self._args.input)):
                self._base.message ('-input/-output is not specified!', self._base.const_critical_text)
                return(terminate(self._base, eFAIL))

        # set output in cfg.
        dst_ = self._args.output
        if (dst_ and
            dst_[-1:] != '/'):
            dst_ += '/'

        cfg.setValue(CCFG_PRIVATE_OUTPUT, dst_ if dst_ else '')
        # ends

        # cfg-init-valid modes
        cfg_modes = {
        'tif',
        'tif_lzw',
        'tif_jpeg',
        'tif_mix',
        'tif_dg',
        'tiff_landsat',
        'mrf',
        'mrf_jpeg',
        'mrf_mix',
        'mrf_dg',
        'mrf_landsat',
        'cachingmrf',
        'clonemrf',
        'splitmrf'
        }
        # ends

        # read-in (-mode)
        cfg_mode = self._args.mode     # cmd-line -mode overrides the cfg value.
        if (cfg_mode is None):
            cfg_mode = cfg.getValue('Mode')
        if (cfg_mode is None or
            (cfg_mode.lower() in cfg_modes) == False):
            self._base.message('<Mode> value not set/illegal', self._base.const_critical_text);
            return(terminate(self._base, eFAIL))
        cfg_mode = cfg_mode.lower()
        cfg.setValue('Mode', cfg_mode)
        # ends

        # is clonepath defined?
        if (self._args.clonepath and
            cfg_mode.startswith('mrf')):
            self._args.clonepath = self._args.clonepath.replace('\\', '/')
            if (not self._args.clonepath.endswith('/')):
                self._args.clonepath += '/'
            cfg.setValue(CCLONE_PATH, self._args.clonepath)
        # ends

        # cache path
        if (self._args.cache):
            cfg.setValue(CCACHE_PATH, self._args.cache)
        # ends

        # read in build pyramids value
        do_pyramids = 'true'
        if (not self._args.pyramids):
            self._args.pyramids = cfg.getValue('BuildPyramids')
        if (self._args.pyramids):
            do_pyramids = self._args.pyramids = str(self._args.pyramids).lower()
        # ends

        # set jpeg_quality from cmd to override cfg value. Must be set before compression->init()
        if (self._args.quality):
            cfg.setValue('Quality', self._args.quality)
        if (self._args.prec):
            cfg.setValue('LERCPrecision', self._args.prec)
        if (self._args.pyramids):
            if (self._args.pyramids == CCMD_PYRAMIDS_ONLY):
                if (not cfg.getValue(CLOAD_RESTORE_POINT)):     # -input, -output path check isn't done if -input points to a job (.orjob) file
                    if (self._args.input != self._args.output):
                        if (isinput_s3 == True):    # in case of input s3, output is used as a temp folder locally.
                            if (getBooleanValue(cfg.getValue(CCLOUD_UPLOAD)) == True):
                                if (cfg.getValue(COUT_S3_PARENTFOLDER, False) != cfg.getValue(CIN_S3_PARENTFOLDER, False)):
                                    self._base.message ('<%s> and <%s> must be the same if the -pyramids=only' % (CIN_S3_PARENTFOLDER, COUT_S3_PARENTFOLDER), const_critical_text)
                                    return(terminate(self._base, eFAIL))
                        else:
                            self._base.message ('-input and -output paths must be the same if the -pyramids=only', const_critical_text);
                            return(terminate(self._base, eFAIL))
        if (getBooleanValue(do_pyramids) == False and
            do_pyramids != CCMD_PYRAMIDS_ONLY and
            do_pyramids != CCMD_PYRAMIDS_EXTERNAL):
                do_pyramids = 'false'
        cfg.setValue('Pyramids', do_pyramids)
        cfg.setValue('isuniformscale', True if do_pyramids == CCMD_PYRAMIDS_ONLY else getBooleanValue(do_pyramids))
        # ends

        # read in the gdal_path from config.
        gdal_path = cfg.getValue(CCFG_GDAL_PATH)      # note: validity is checked within (compression-mod)
        # ends

        comp = compression(gdal_path, base = self._base)
        ret = comp.init(0)      # warning/error messages get printed within .init()
        if (not ret):
            self._base.message('Unable to initialize/compression module', self._base.const_critical_text);
            return(terminate(self._base, eFAIL))

        # s3 upload settings.
        out_s3_profile_name = self._args.outputprofile
        if (not out_s3_profile_name):
            out_s3_profile_name = cfg.getValue('Out_S3_AWS_ProfileName', False)
        if (out_s3_profile_name):
            cfg.setValue ('Out_S3_AWS_ProfileName', out_s3_profile_name)

        s3_output = cfg.getValue(COUT_S3_PARENTFOLDER, False)
        s3_id = cfg.getValue('Out_S3_ID', False)
        s3_secret = cfg.getValue('Out_S3_Secret', False)

        if (getBooleanValue(cfg.getValue(CCLOUD_UPLOAD))):
            if (cfg.getValue(COUT_CLOUD_TYPE, True) == CCLOUD_AMAZON):
                if ((s3_output is None and self._args.output is None)):
                        self._base.message ('Empty/Invalid values detected for keys in the ({}) beginning with (Out_S3|Out_S3_ID|Out_S3_Secret|Out_S3_AWS_ProfileName) or values for command-line args (-outputprofile)'.format(self._args.config), self._base.const_critical_text)
                        return(terminate(self._base, eFAIL))
                # instance of upload storage.
                S3_storage = S3Storage(self._base)
                if (self._args.output):
                    s3_output = self._args.output
                    cfg.setValue(COUT_S3_PARENTFOLDER, s3_output)
                # do we overwrite the output_bucekt_name with cmd-line?
                if (self._args.outputbucket):
                    cfg.setValue('Out_S3_Bucket', self._args.outputbucket)
                # end
                ret =  S3_storage.init(s3_output, s3_id, s3_secret, CS3STORAGE_OUT)
                if (ret == False):
                    self._base.message (err_init_msg.format('S3'), const_critical_text);
                    return(terminate(self._base, eFAIL))
                S3_storage.inputPath = self._args.output
                cfg.setValue(COUT_VSICURL_PREFIX, '/vsicurl/{}{}'.format(S3_storage.bucketupload.generate_url(0).split('?')[0].replace('https', 'http'),
                cfg.getValue(COUT_S3_PARENTFOLDER, False)) if not S3_storage._isBucketPublic else
                '/vsicurl/http://{}.{}/{}'.format(S3_storage.m_bucketname, CINOUT_S3_DEFAULT_DOMAIN, cfg.getValue(COUT_S3_PARENTFOLDER, False)))
                # ends
            elif (cfg.getValue(COUT_CLOUD_TYPE, True) == CCLOUD_AZURE):
                _account_name = cfg.getValue(COUT_AZURE_ACCOUNTNAME, False);
                _account_key = cfg.getValue(COUT_AZURE_ACCOUNTKEY, False);
                _container = cfg.getValue(COUT_AZURE_CONTAINER);        #  Azure container names will be lowercased.
                _out_profile = cfg.getValue(COUT_AZURE_PROFILENAME, False)
                if (self._args.outputbucket):
                    _container = self._args.outputbucket
                    cfg.setValue(COUT_AZURE_CONTAINER, self._args.outputbucket.lower())     # lowercased
                if (self._args.outputprofile):
                    _out_profile = self._args.outputprofile
                    cfg.setValue(COUT_AZURE_PROFILENAME, _out_profile)
                if (((not _account_name or
                    not _account_key) and
                    not _out_profile) or
                    not _container):
                    self._base.message ('Empty/Invalid values detected for keys ({}/{}/{}/{})'.format(COUT_AZURE_ACCOUNTNAME, COUT_AZURE_ACCOUNTKEY, COUT_AZURE_CONTAINER, COUT_AZURE_PROFILENAME), self._base.const_critical_text);
                    return(terminate (self._base, eFAIL));
                azure_storage = Azure(_account_name, _account_key, _out_profile, self._base);
                if (not azure_storage.init()):
                    self._base.message (err_init_msg.format(CCLOUD_AZURE.capitalize()), self._base.const_critical_text);
                    return(terminate(self._base, eFAIL))
                cfg.setValue(COUT_VSICURL_PREFIX, '/vsicurl/{}{}'.format('http://{}.blob.core.windows.net/{}/'.format(azure_storage.getAccountName, _container),
                self._args.output if self._args.output else cfg.getValue(COUT_S3_PARENTFOLDER, False)))
            else:
                self._base.message ('Invalid value for ({})'.format(COUT_CLOUD_TYPE), const_critical_text)
                return(terminate(self._base, eFAIL))

        user_args_Callback = {
        USR_ARG_UPLOAD : getBooleanValue(cfg.getValue(CCLOUD_UPLOAD)),
        USR_ARG_DEL : getBooleanValue(cfg.getValue('Out_S3_DeleteAfterUpload'))
        }
        # ends
        cpy = Copy(self._base)
        list = {
        'copy' : {'*'},
        'exclude' : {}
        }
        for i in cfg.getValue(CCFG_RASTERS_NODE) + cfg.getValue(CCFG_EXCLUDE_NODE):
            list['exclude'][i] = ''

        is_caching = False
        if (cfg_mode == 'clonemrf' or
            cfg_mode == 'splitmrf' or
            cfg_mode == 'cachingmrf'):
            is_caching = True

        if (is_caching == True):
            cfg.setValue('istempinput', False)
            cfg.setValue('Pyramids', False)

        callbacks = {
        #'copy' : copy_callback,
        'exclude'  : exclude_callback
        }

        callbacks_for_meta = {
        'exclude'  : exclude_callback_for_meta
        }

        CONST_CPY_ERR_0 = 'Unable to initialize (Copy) module!'
        CONST_CPY_ERR_1 = 'Unable to process input data/(Copy) module!'

        CONST_OUTPUT_EXT = '.%s' % ('mrf')

        # keep original-source-ext
        cfg_keep_original_ext = getBooleanValue(cfg.getValue('KeepExtension'))
        cfg_threads = cfg.getValue('Threads')
        msg_threads = 'Thread-count invalid/undefined, resetting to default'
        try:
            cfg_threads = int(cfg_threads)   # (None) value is expected
        except:
            cfg_threads = -1
        if (cfg_threads <= 0 or
            cfg_threads > CCFG_THREADS):
            cfg_threads = CCFG_THREADS
            self._base.message('%s(%s)' % (msg_threads, CCFG_THREADS), self._base.const_warning_text)
        # ends
        # let's deal with copying when -input is on s3
        if (isinput_s3 == True):
            cfg.setValue('iss3', True);
            in_s3_parent = cfg.getValue(CIN_S3_PARENTFOLDER, False)
            in_s3_profile_name = self._args.inputprofile
            if (not in_s3_profile_name):
                in_s3_profile_name = cfg.getValue('In_S3_AWS_ProfileName' if inAmazon else 'In_Azure_ProfileName', False)
            if (in_s3_profile_name):
                cfg.setValue ('In_S3_AWS_ProfileName', in_s3_profile_name)
            in_s3_id = cfg.getValue('In_S3_ID' if inAmazon else 'In_Azure_AccountName', False)
            in_s3_secret = cfg.getValue('In_S3_Secret' if inAmazon else 'In_Azure_AccountKey', False)
            in_s3_bucket = self._args.inputbucket
            if (not in_s3_bucket):
                in_s3_bucket = cfg.getValue('In_S3_Bucket' if inAmazon else 'In_Azure_Container', False)
            if (in_s3_parent is None or
                in_s3_bucket is None):
                    self._base.message ('Invalid/empty value(s) found in node(s) [In_S3_ParentFodler, In_S3_Bucket]', self._base.const_critical_text)
                    return(terminate(self._base, eFAIL))
            cfg.setValue('In_S3_Bucket', in_s3_bucket)          # update (in s3 bucket name in config)
            in_s3_parent = in_s3_parent.replace('\\', '/')
            if (in_s3_parent[:1] == '/'):
                in_s3_parent = in_s3_parent[1:]
                cfg.setValue(CIN_S3_PARENTFOLDER, in_s3_parent)
            if (inAmazon):
                o_S3_storage = S3Storage(self._base)
                ret =  o_S3_storage.init(in_s3_parent, in_s3_id, in_s3_secret, CS3STORAGE_IN)
                if (ret == False):
                    self._base.message ('Unable to initialize S3-storage! Quitting..', self._base.const_critical_text);
                    return(terminate(self._base, eFAIL))
                if (str(o_S3_storage.bucketupload.connection).lower().endswith('.ecstestdrive.com')):   # handles EMC namespace cloud urls differently
                    cfg.setValue(CIN_S3_PREFIX, '/vsicurl/http://{}.public.ecstestdrive.com/{}/'.format(
                    o_S3_storage.bucketupload.connection.aws_access_key_id.split('@')[0], o_S3_storage.m_bucketname))
                else:   # for all other standard cloud urls
                    cfg.setValue(CIN_S3_PREFIX, '/vsicurl/{}'.format(o_S3_storage.bucketupload.generate_url(0).split('?')[0]).replace('https', 'http') if not o_S3_storage._isBucketPublic else
                    '/vsicurl/http://{}.{}/'.format(o_S3_storage.m_bucketname, CINOUT_S3_DEFAULT_DOMAIN)) # vsicurl doesn't like 'https'
                o_S3_storage.inputPath = self._args.output
                if (o_S3_storage.getS3Content(o_S3_storage.remote_path, o_S3_storage.S3_copy_to_local, exclude_callback) == False):
                    self._base.message ('Unable to read S3-Content', self._base.const_critical_text);
                    return(terminate(self._base, eFAIL))
            else:
                # let's do (Azure) init
                in_azure_storage = Azure(in_s3_id, in_s3_secret, in_s3_profile_name, self._base);
                if (not in_azure_storage.init() or
                    not in_azure_storage.getAccountName):
                    self._base.message ('({}) download initialization error. Check input credentials/profile name. Quitting..'.format(CCLOUD_AZURE.capitalize()), self._base.const_critical_text);
                    return(terminate(self._base, eFAIL))
                in_azure_storage._include_subFolders = getBooleanValue(cfg.getValue('IncludeSubdirectories'))
                _restored = cfg.getValue(CLOAD_RESTORE_POINT)
                if (not _restored):
                    in_azure_storage._mode = in_azure_storage.CMODE_SCAN_ONLY
                    cfg.setValue(CIN_AZURE_PARENTFOLDER, in_s3_parent if in_s3_parent.endswith('/') else in_s3_parent + '/')
                _azureParentFolder = _azParent = cfg.getValue(CIN_AZURE_PARENTFOLDER, False) if not _rpt else _rpt.root
                if (_azureParentFolder == '/'):
                    _azureParentFolder = ''
                cfg.setValue(CIN_S3_PREFIX, '/vsicurl/{}'.format('http://{}.blob.core.windows.net/{}/'.format(in_azure_storage.getAccountName, cfg.getValue('In_S3_Bucket'))))
                if (not in_azure_storage.browseContent(in_s3_bucket, _azParent, in_azure_storage.copyToLocal)):
                    return(terminate(self._base, eFAIL))
                if (not _restored):
                    _files = in_azure_storage.getBrowseContent()
                    if (_files):
                        for f in _files:
                            fn_collect_input_files(f)
                # ends
        # ends
        # control flow if conversions required.
        if (is_caching == False):
            if (isinput_s3 == False and
                not self._args.input.lower().startswith('http')):
                ret = cpy.init(self._args.input, self._args.tempoutput if is_output_temp and getBooleanValue(cfg.getValue(CCLOUD_UPLOAD)) else self._args.output, list, callbacks, cfg)
                if  (ret == False):
                    self._base.message(CONST_CPY_ERR_0, self._base.const_critical_text);
                    return(terminate(self._base, eFAIL))
                ret = cpy.processs(self._base.S3Upl if is_cloud_upload == True else None, user_args_Callback, fn_pre_process_copy_default)
                if (ret == False):
                    self._base.message(CONST_CPY_ERR_1, self._base.const_critical_text);
                    return(terminate(self._base, eFAIL))
                if (is_input_temp == True):
                    pass        # no post custom code yet for non-rasters
            files = raster_buff
            files_len = len(files)
            if (files_len):
                if (is_input_temp == True and
                    isinput_s3 == False and
                    not cfg.getValue(CLOAD_RESTORE_POINT)):
                    # if the -tempinput path is defined, we first copy rasters from the source path to -tempinput before any conversion.
                    self._base.message ('Copying files to -tempinput path (%s)' % (cfg.getValue(CUSR_TEMPINPUT, False)))
                    cpy_files_ = []
                    for i in range(0, len(files)):
                        get_dst_path = files[i]['dst'].replace(self._args.output if cfg.getValue('tempoutput', False) is None else cfg.getValue('tempoutput', False), cfg.getValue(CUSR_TEMPINPUT, False))
                        cpy_files_.append(
                        {
                        'src' : files[i]['src'],
                        'dst' : get_dst_path,
                        'f' : files[i]['f']
                        })
                        files[i]['src'] = get_dst_path
                    cpy.batch(cpy_files_, None)
                self._base.message('Converting..');
            # collect all the input raster files.
            if (g_is_generate_report and
                g_rpt):
                for req in files:
                    _src = '{}{}{}'.format(req['src'], '/' if not req['src'].replace('\\', '/').endswith('/') else '', req['f'])
                    if (getBooleanValue(cfg.getValue('istempinput'))):
                        _tempinput = cfg.getValue(CUSR_TEMPINPUT, False)
                        _tempinput = _tempinput[:-1] if _tempinput.endswith('/') and not self._args.input.endswith('/') else _tempinput
                        _src = _src.replace(_tempinput, self._args.input)
                    g_rpt.addFile(_src)     # prior to this point, rasters get added to g_rpt during the (pull/copy) process if -clouddownload=true && -tempinput is defined.
                self._base.message ('{}'.format(CRESUME_CREATE_JOB_TEXT).format (_project_path))
                for arg in vars(self._args):
                    g_rpt.addHeader(arg, getattr(self._args, arg))
                g_rpt.write();
                self._args.op = None
                self._args.input = _project_path
                cfg.setValue(CLOAD_RESTORE_POINT, True)
                self.run()
                return
            # ends
            a = []
            threads = []
            batch = cfg_threads
            s = 0
            while 1:
                m = s + batch
                if (m >= files_len):
                    m =  files_len
                threads = []
                for i in range(s, m):
                    req = files[i]
                    (input_file, output_file) = getInputOutput(req['src'], req['dst'], req['f'], isinput_s3)
                    f, e = os.path.splitext(output_file)
                    if (cfg_keep_original_ext == False):
                        output_file = output_file.replace(e, '.{}'.format(cfg_mode.split('_')[0]))
                    _build_pyramids = True
                    if (til):
                        if (til.find(req['f'])):
                            til.addFileToProcessed(req['f'])    # increment the process counter if the raster belongs to a (til) file.
                            _build_pyramids = False     # build pyramids is always turned off for rasters that belong to (.til) files.
                    t = threading.Thread(target = comp.compress, args = (input_file, output_file, args_Callback, _build_pyramids, self._base.S3Upl if is_cloud_upload == True else fn_copy_temp_dst if is_output_temp == True and isinput_s3 == False else None, user_args_Callback))
                    t.daemon = True
                    t.start()
                    threads.append(t)
                for t in threads:
                    t.join()
                # process til file if all the associate files have been processed
                if (til):
                    for _til in til:
                        if (not til.isAllFilesProcessed(_til)):
                            self._base.message ('TIL> Not yet completed for ({})'.format(_til));
                        if (til.isAllFilesProcessed(_til)):
                            _doPostProcessing = True
                            if (cfg.getValue(CLOAD_RESTORE_POINT)):
                                if (_rpt.getRecordStatus(_til, CRPT_PROCESSED) == CRPT_YES):
                                    self._base.message ('{} {}'.format(CRESUME_MSG_PREFIX, _til))
                                    _doPostProcessing = False
                            til_output_path = til.getOutputPath(_til)
                            if (_doPostProcessing):
                                if (not til_output_path):
                                    self._base.message ('TIL output-path returned empty/Internal error', self._base.const_warning_text);
                                    continue
                                ret = comp.createaOverview(til_output_path)
                                if (not ret):
                                    self._base.message ('Unable to build pyramids on ({})'.format(til_output_path), self._base.const_warning_text);
                                    continue
                                ret = comp.compress('{}.ovr'.format(til_output_path), '{}.mrf'.format(til_output_path), args_Callback)
                                if (not ret):
                                    self._base.message ('Unable to convert (til.ovr=>til.mrf) for file ({}.ovr)'.format(til_output_path), self._base.const_warning_text)
                                    continue
                                # let's rename (.mrf) => (.ovr)
                                try:
                                    os.remove('{}.ovr'.format(til_output_path))
                                    os.rename('{}.mrf'.format(til_output_path), '{}.ovr'.format(til_output_path))
                                except Exception as e:
                                    self._base.message ('({})'.format(str(e)), self._base.const_warning_text)
                                    continue
                                # update .ovr file updates at -clonepath
                                try:
                                    if (self._args.clonepath):
                                        _clonePath = til_output_path.replace(self._args.output, '')
                                        _mk_input_path = os.path.join(self._args.clonepath, '{}.mrf'.format(_clonePath))
                                        doc = minidom.parse(_mk_input_path)
                                        xmlString = doc.toxml()
                                        xmlString = xmlString.replace('.mrf<', '.ovr<')
                                        xmlString = xmlString.replace('.{}'.format(CCACHE_EXT), '.ovr.{}'.format(CCACHE_EXT))
                                        _indx = xmlString.find ('<{}>'.format(CMRF_DOC_ROOT))
                                        if (_indx == -1):
                                            raise Exception('Err. Invalid MRF/header')
                                        xmlString = xmlString[_indx:]
                                        _mk_save_path = '{}{}.ovr'.format(self._args.clonepath, _clonePath.replace('.mrf', ''))
                                        with open (_mk_save_path, 'w+') as _fpOvr:
                                            _fpOvr.write(xmlString)
                                except Exception as e:
                                    self._base.message ('Unable to update .ovr for [{}] ({})'.format(til_output_path, str(e)), self._base.const_warning_text)
                                    continue
                                # ends
                            # upload (til) related files (.idx, .ovr, .lrc)
                            if (is_cloud_upload and
                                S3_storage):
                                ret = S3_storage.upload_group('{}.CHS'.format(til_output_path))
                                retry_failed_lst  = []
                                failed_upl_lst = S3_storage.getFailedUploadList()
                                if (failed_upl_lst):
                                    [retry_failed_lst.append(_x['local']) for _x in failed_upl_lst['upl']]
                                # let's delete all the associate files related to (TIL) files.
                                (p, n) = os.path.split(til_output_path)
                                for r,d,f in os.walk(p):
                                    for file in f:
                                        if (r != p):
                                            continue
                                        mk_filename = os.path.join(r, file).replace('\\', '/')
                                        if (til.fileTILRelated(mk_filename)):
                                            if (mk_filename in retry_failed_lst):        # Don't delete files included in the (failed upload list)
                                                continue
                                            try:
                                                self._base.message ('[Del] {}'.format(mk_filename))
                                                os.remove(mk_filename)
                                            except Exception as e:
                                                self._base.message ('[Del] Err. {} ({})'.format(mk_filename, str(e)), self._base.const_critical_text)
                                # ends
                            # ends
                # ends
                s = m
                if s == files_len or s == 0:
                    break
        # ends
        # block to deal with caching ops.
        if (is_caching == True and
            do_pyramids != CCMD_PYRAMIDS_ONLY):
            if (not g_is_generate_report):
                self._base.message ('\nProcessing caching operations...')
            if (isinput_s3 == False):
                raster_buff = []
                if (cfg_mode == 'splitmrf'):        # set explicit (exclude list) for mode (splitmrf)
                    list['exclude']['idx'] = ''
                ret = cpy.init(self._args.input, self._args.output, list, callbacks_for_meta, cfg)
                if  (ret == False):
                    self._base.message(CONST_CPY_ERR_0, self._base.const_critical_text);
                    return(terminate(self._base, eFAIL))
                ret = cpy.processs(pre_processing_callback = fn_pre_process_copy_default)
                if (ret == False):
                    self._base.message(CONST_CPY_ERR_1, self._base.const_critical_text);
                    return(terminate(self._base, eFAIL))
            if (g_is_generate_report and
                g_rpt):
                for req in raster_buff:
                    (input_file, output_file) = getInputOutput(req['src'], req['dst'], req['f'], isinput_s3)
                    _src = '{}{}{}'.format(req['src'], '/' if not req['src'].replace('\\', '/').endswith('/') else '', req['f'])
                    g_rpt.addFile(_src)
                self._base.message ('{}'.format(CRESUME_CREATE_JOB_TEXT).format (_project_path))
                self._args.cloudupload = 'false'    # Uploading is disabled for modes related to caching.
                for arg in vars(self._args):
                    g_rpt.addHeader(arg, getattr(self._args, arg))
                g_rpt.write();
                self._args.op = None
                cfg.setValue(CCMD_ARG_INPUT, self._args.input)      # preserve the original -input path
                self._args.input = _project_path
                cfg.setValue(CLOAD_RESTORE_POINT, True)
                self.run()
                return
            for req in raster_buff:
                (input_file, output_file) = getInputOutput(req['src'], req['dst'], req['f'], isinput_s3)
                (f, ext) = os.path.splitext(req['f'])
                ext = ext.lower()
                if (cfg_keep_original_ext == False):
                    output_file = output_file.replace(ext, CONST_OUTPUT_EXT)
                finalPath = output_file
                if (is_output_temp):
                    finalPath = output_file.replace(self._args.tempoutput, self._args.output)
                if (cfg_mode != 'splitmrf'):     # uses GDAL utilities
                    ret = comp.compress(input_file, output_file, args_Callback_for_meta,
                    post_processing_callback = fn_copy_temp_dst if is_output_temp else None)    # and not isinput_s3
                else:
                    try:
                        shutil.copyfile(input_file, finalPath)
                    except Exception as e:
                        self._base.message ('[CPY] {} ({})'.format(input_file, str(e)), self._base.const_critical_text)
                        continue
                if (not os.path.exists(finalPath)):
                    continue
                # update .mrf.
                updateMRF = UpdateMRF(self._base)
                _output_home_path = self._args.output
                if (updateMRF.init(finalPath, _output_home_path, cfg.getValue('Mode'),
                    self._args.cache, _output_home_path, cfg.getValue(COUT_VSICURL_PREFIX, False))):
                    if (not updateMRF.update(finalPath)):
                        self._base.message ('Updating ({}) was not successful!'.format(finalPath), self._base.const_critical_text);
                        continue
                # ends
        # do we have failed upload files on list?
        if (is_cloud_upload and
            S3_storage):
            if (cfg.getValue(COUT_CLOUD_TYPE) == CCLOUD_AMAZON):
                failed_upl_lst = S3_storage.getFailedUploadList()
                if (failed_upl_lst):
                    self._base.message ('Retry - Failed upload list.', const_general_text);
                    _fptr = None
                    if (self._log_path):
                        try:
                            if (not os.path.isdir(self._log_path)):
                                os.makedirs(self._log_path)
                            ousr_date =  datetime.datetime.now()
                            err_upl_file = os.path.join(self._log_path, '%s_UPL_ERRORS_%04d%02d%02dT%02d%02d%02d.txt' % (cfg.getValue(CPRJ_NAME, False), ousr_date.year, ousr_date.month, ousr_date.day, \
                            ousr_date.hour, ousr_date.minute, ousr_date.second))
                            _fptr = open(err_upl_file, 'w+')
                        except:
                            pass
                    for v in failed_upl_lst['upl']:
                        self._base.message ('%s' % (v['local']), const_general_text)
                        ret = S3_storage.upload_group(v['local'])
                        # the following files will be logged as unsuccessful uploads to output cloud
                        if (not ret):
                            if (_fptr): _fptr.write('{}\n'.format(v['local']))
                            if (_rpt):      # Do we have an input file list?
                                if ('local' in v):
                                    _local = v['local']
                                    if (_local):
                                        setUploadRecordStatus (_local, CRPT_NO)
                        # ends
                        for r in ret:
                            try:
                                self._base.message ('[Del] {}'.format(r))
                                try:
                                    os.remove(r)
                                except:
                                    time.sleep(CDEL_DELAY_SECS)
                                    os.remove(r)
                            except Exception as e:
                                self._base.message ('[Del] {} ({})'.format(r, str(e)))
                    if (_fptr):
                        _fptr.close()
                        _fptr = None
        # ends
        # let's clean-up rasters @ -tempinput path
        dbg_delete = True
        if (dbg_delete == True):
            if (is_input_temp == True and
                is_caching == False):        # if caching is (True), -tempinput is ignored and no deletion of source @ -input takes place.
                if (len(raster_buff) != 0):
                    self._base.message ('Removing input rasters at ({})'.format(cfg.getValue(CUSR_TEMPINPUT, False)))
                    for req in raster_buff:
                        (input_file , output_file) = getInputOutput(req['src'], req['dst'], req['f'], isinput_s3)
                        try:
                            if (os.path.exists(input_file)):
                                self._base.message ('[Del] {}'.format(input_file))
                                os.remove(input_file )
                        except Exception as exp:
                            self._base.message ('[Del] {} ({})'.format(input_file, str(exp)), self._base.const_warning_text)
                    self._base.message ('Done.')
        # ends
        if (not raster_buff):
            self._base.message ('No input rasters to process..', self._base.const_warning_text);
        # ends
        _status = eOK
        # write out the (job file) with updated status.
        if (_rpt):
            if (not _rpt.write() or
                _rpt.hasFailures()):
                _status = eFAIL
            if (_status == eOK):
                if (self._base.getMessageHandler and
                    not _rpt.moveJobFileToPath(self._base.getMessageHandler.logFolder)):
                    _status = eFAIL
        # ends
        self._base.message ('Done..\n')
        return(terminate(self._base, _status))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-mode', help='Processing mode/output format', dest='mode');
    parser.add_argument('-input', help='Input raster files directory/job file to resume', dest='input');
    parser.add_argument('-output', help='Output directory', dest='output');
    parser.add_argument('-subs', help='Include sub-directories in -input? [true/false]', dest='subs');
    parser.add_argument('-cache', help='cache output directory', dest='cache');
    parser.add_argument('-config', help='Configuration file with default settings', dest='config');
    parser.add_argument('-quality', help='JPEG quality if compression is jpeg', dest='quality');
    parser.add_argument('-prec', help='LERC precision', dest='prec');
    parser.add_argument('-pyramids', help='Generate pyramids? [true/false/only/external]', dest='pyramids');
    parser.add_argument('-tempinput', help='Path to copy -input raters before conversion', dest=CUSR_TEMPINPUT);
    parser.add_argument('-tempoutput', help='Path to output converted rasters before moving to (-output) path', dest='tempoutput');
    parser.add_argument('-clouddownload', help='Is -input a cloud storage? [true/false: default:false]', dest='clouddownload');
    parser.add_argument('-cloudupload', help='Is -output a cloud storage? [true/false]', dest='cloudupload');
    parser.add_argument('-clouduploadtype', choices=['amazon', 'azure'], help='Upload Cloud Type [amazon/azure]', dest='clouduploadtype');
    parser.add_argument('-clouddownloadtype', choices=['amazon', 'azure'], help='Download Cloud Type [amazon/azure]', dest='clouddownloadtype');
    parser.add_argument('-inputprofile', help='Input cloud profile name with credentials', dest='inputprofile');
    parser.add_argument('-outputprofile', help='Output cloud profile name with credentials', dest='outputprofile');
    parser.add_argument('-inputbucket', help='Input cloud bucket/container name', dest='inputbucket');
    parser.add_argument('-outputbucket', help='Output cloud bucket/container name', dest='outputbucket');
    parser.add_argument('-op', help='Utility operation mode [upload/noconvert]', dest='op');
    parser.add_argument('-job', help='job/log-prefix file name', dest='job');
    parser.add_argument('-clonepath', help='Path to auto-generate cloneMRF files during the conversion process', dest='clonepath');
    parser.add_argument('-s3input', help='Deprecated. Use (-clouddownload)', dest='s3input');
    parser.add_argument('-s3output', help='Deprecated. Use (-cloudupload)', dest='s3output');

    args = parser.parse_args()
    app = Application(args)
    #app.registerMessageCallback(messageDebug)
    if (not app.init()):
        return eFAIL
    return app.run()

if __name__ == '__main__':
    ret = main()
    print ('\nDone..')
    exit(ret)

