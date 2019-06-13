# ------------------------------------------------------------------------------
# Copyright 2019 Esri
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
# Name: OptimizeRasters.py
# Description: Optimizes rasters via gdal_translate/gdaladdo
# Version: 20190613
# Requirements: Python
# Required Arguments: -input -output
# Optional Arguments: -mode -cache -config -quality -prec -pyramids
# -tempinput -tempoutput -subs -clouddownload -cloudupload
# -inputprofile -outputprofile -op -job -inputprofile -outputprofile
# -inputbucket -outputbucket -rasterproxypath -clouddownloadtype -clouduploadtype
# -usetoken
# Usage: python.exe OptimizeRasters.py <arguments>
# Note: OptimizeRasters.xml (config) file is placed alongside OptimizeRasters.py
# OptimizeRasters.py is entirely case-sensitive, extensions/paths in the config
# file are case-sensitive and the program will fail if the correct paths are not
# entered at the cmd-line/UI or in the config file.
# Author: Esri Imagery Workflows team
# ------------------------------------------------------------------------------
# !/usr/bin/env python

CRUN_IN_AWSLAMBDA = False    # IMPORTANT> Set (CRUN_IN_AWSLAMBDA) to (True) when the OptimizeRasters.py is used within the (lambda_function.zip) to act as a lambda function.

import sys
import os
import base64

import mmap
import threading
import time

from xml.dom import minidom
import subprocess
import shutil

import argparse
import math
import ctypes
if (sys.version_info[0] < 3):
    import ConfigParser
    from urllib import urlopen, urlencode
else:
    import configparser as ConfigParser
    from urllib.request import urlopen
    from urllib.parse import urlencode
import json
import hashlib
import binascii
from datetime import datetime, timedelta
import fnmatch
# ends

# enum error codes
eOK = 0
eFAIL = 1
# ends

CEXEEXT = '.exe'
CONST_OUTPUT_EXT = '.%s' % ('mrf')
CloudOGTIFFExt = '.cogtiff'
COGTIFFAuxFile = '.tif.cogtiff.aux.xml'
UpdateOrjobStatus = 'updateOrjobStatus'
CreateOverviews = 'createOverviews'
DefJpegQuality = 85

# const related to (Reporter) class
CRPT_SOURCE = 'SOURCE'
CRPT_COPIED = 'COPIED'
CRPT_PROCESSED = 'PROCESSED'
CRPT_UPLOADED = 'UPLOADED'
CRPT_HEADER_KEY = 'config'
CPRT_HANDLER = 'handler_resume_reporter'

CRPT_YES = 'yes'
CRPT_NO = 'no'
CRPT_UNDEFINED = ''
# ends

# user hsh const
USR_ARG_UPLOAD = 'upload'
USR_ARG_DEL = 'del'
# ends

# PL
CPLANET_IDENTIFY = 'api.planet.com'
SigAlibaba = 'aliyuncs.com'

# Del delay
CDEL_DELAY_SECS = 20
# ends

CPRJ_NAME = 'ProjectName'
CLOAD_RESTORE_POINT = '__LOAD_RESTORE_POINT__'
CCMD_ARG_INPUT = '__CMD_ARG_INPUT__'

CVSICURL_PREFIX = '/vsicurl/'

# utility const
CSIN_UPL = 'SIN_UPL'
CINC_SUB = 'INC_SUB'

COP_UPL = 'upload'
COP_DNL = 'download'
COP_RPT = 'report'
COP_NOCONVERT = 'noconvert'
COP_LAMBDA = 'lambda'
COP_COPYONLY = 'copyonly'
COP_CREATEJOB = 'createjob'
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
CRESUME_ARG_VAL_RETRYALL = 'retryall'
CRESUME_HDR_INPUT = 'input'
CRESUME_HDR_OUTPUT = 'output'
InputProfile = 'inputprofile'
OutputProfile = 'outputprofile'
# ends

CINPUT_PARENT_FOLDER = 'Input_ParentFolder'
CUSR_TEXT_IN_PATH = 'hashkey'
CRASTERPROXYPATH = 'rasterproxypath'
CTEMPOUTPUT = 'tempoutput'
CTEMPINPUT = 'tempinput'
CISTEMPOUTPUT = 'istempoutput'
CISTEMPINPUT = 'istempinput'
CHASH_DEF_INSERT_POS = 2
CHASH_DEF_CHAR = '#'
CHASH_DEF_SPLIT_CHAR = '@'
UseToken = 'usetoken'
UseTokenOnOuput = 'usetokenonoutput'
CTimeIt = 'timeit'

# const node-names in the config file
CCLOUD_AMAZON = 'amazon'
CCLOUD_AZURE = 'azure'
CCLOUD_GOOGLE = 'google'
CDEFAULT_TIL_PROCESSING = 'DefaultTILProcessing'

# Azure constants
COUT_AZURE_PARENTFOLDER = 'Out_Azure_ParentFolder'
COUT_AZURE_ACCOUNTNAME = 'Out_Azure_AccountName'
COUT_AZURE_ACCOUNTKEY = 'Out_Azure_AccountKey'
COUT_AZURE_CONTAINER = 'Out_Azure_Container'
COUT_AZURE_ACCESS = 'Out_Azure_Access'
COUT_AZURE_PROFILENAME = 'Out_Azure_ProfileName'
CIN_AZURE_PARENTFOLDER = 'In_Azure_ParentFolder'
CIN_AZURE_CONTAINER = 'In_Azure_Container'
COP = 'Op'
# ends

# google constants
COUT_GOOGLE_BUCKET = 'Out_Google_Bucket'
COUT_GOOGLE_PROFILENAME = 'Out_Google_ProfileName'
CIN_GOOGLE_PARENTFOLDER = 'In_Google_ParentFolder'
COUT_GOOGLE_PARENTFOLDER = 'Out_Google_ParentFolder'
# ends

CCLOUD_UPLOAD_THREADS = 20          # applies to both (azure and amazon/s3)
CCLOUD_UPLOAD = 'CloudUpload'
CCLOUD_UPLOAD_OLD_KEY = 'Out_S3_Upload'
COUT_CLOUD_TYPE = 'Out_Cloud_Type'
COUT_S3_PARENTFOLDER = 'Out_S3_ParentFolder'
COUT_S3_ACL = 'Out_S3_ACL'
CIN_S3_PARENTFOLDER = 'In_S3_ParentFolder'
CIN_S3_PREFIX = 'In_S3_Prefix'
CIN_CLOUD_TYPE = 'In_Cloud_Type'
COUT_VSICURL_PREFIX = 'Out_VSICURL_Prefix'
CINOUT_S3_DEFAULT_DOMAIN = 's3.amazonaws.com'
DefS3Region = 'us-east-1'
COUT_DELETE_AFTER_UPLOAD_OBSOLETE = 'Out_S3_DeleteAfterUpload'
COUT_DELETE_AFTER_UPLOAD = 'DeleteAfterUpload'
# ends

# const
CCFG_FILE = 'OptimizeRasters.xml'
CCFG_GDAL_PATH = 'GDALPATH'
# ends

# til related
CTIL_EXTENSION_ = '.til'
# ends

CCACHE_EXT = '.mrf_cache'
CMRF_DOC_ROOT = 'MRF_META'  # <{CMRF_DOC_ROOT}>  mrf XML root node
CMRF_DOC_ROOT_LEN = len(CMRF_DOC_ROOT) + 2  # includes '<' and '>' in XML node.

# global dbg flags
CS3_MSG_DETAIL = False
CS3_UPLOAD_RETRIES = 3
# ends

# S3Storage direction
CS3STORAGE_IN = 0
CS3STORAGE_OUT = 1
# ends


class TimeIt(object):
    Name = 'Name'
    Conversion = 'Conversion'
    Overview = 'Overview'
    Download = 'Download'
    Upload = 'Upload'

    def __init__(self):
        pass

    @staticmethod
    def timeOperation(func):
        def wrapper(*args, **kwargs):
            sTime = time.time()
            result = func(*args, **kwargs)
            if (not result):
                return result
            eTime = time.time()
            if ('name' in kwargs):
                if (kwargs['name'] is None):
                    return result
                prevIndex = -1
                for i in range(0, len(kwargs['store'].timedInfo['files'])):
                    if kwargs['name'] in kwargs['store'].timedInfo['files'][i][TimeIt.Name]:
                        prevIndex = i
                        break
                if (prevIndex == -1):
                    kwargs['store'].timedInfo['files'].append({TimeIt.Name: kwargs['name']})
                    prevIndex = len(kwargs['store'].timedInfo['files']) - 1
                method = 'processing'   # default method
                if ('method' in kwargs):
                    method = kwargs['method']
                if ('store' in kwargs):
                    kwargs['store'].timedInfo['files'][prevIndex][method] = '%.3f' % (eTime - sTime)
            return result
        return wrapper


class UI(object):

    def __init__(self, profileName=None):
        self._profileName = profileName
        self._errorText = []
        self._availableBuckets = []

    @property
    def errors(self):
        return iter(self._errorText)


class ProfileEditorUI(UI):
    TypeAmazon = 'amazon'
    TypeAzure = 'azure'
    TypeGoogle = 'google'
    TypeAlibaba = 'alibaba'

    def __init__(self, profileName, storageType, accessKey, secretkey, credentialProfile=None, **kwargs):
        super(ProfileEditorUI, self).__init__(profileName)
        self._accessKey = accessKey
        self._secretKey = secretkey
        self._storageType = storageType
        self._credentialProfile = credentialProfile
        self._properties = kwargs

    def validateCredentials(self):
        try:
            azure_storage = None
            if (self._storageType == self.TypeAmazon or
                    self._storageType == self.TypeAlibaba):
                import boto3
                import botocore.config
                import botocore
                session = boto3.Session(self._accessKey, self._secretKey,
                                        profile_name=self._credentialProfile if self._credentialProfile else None)
                awsCredentials = ConfigParser.RawConfigParser()
                rootPath = '.aws'
                AwsEndpoint = 'aws_endpoint_url'
                if (self._credentialProfile):
                    if (self._storageType == self.TypeAlibaba):
                        rootPath = '.OptimizeRasters/Alibaba'
                    userHome = '{}/{}/{}'.format(os.path.expanduser('~').replace('\\', '/'), rootPath, 'credentials')
                    awsCredentials.read(userHome)
                    if (not awsCredentials.has_section(self._credentialProfile)):
                        return False
                    endPoint = awsCredentials.get(self._credentialProfile, AwsEndpoint) if awsCredentials.has_option(self._credentialProfile, AwsEndpoint) else None
                if (AwsEndpoint in self._properties):
                    endPoint = self._properties[AwsEndpoint]
                useAlibaba = endPoint and endPoint.lower().find(SigAlibaba) != -1
                con = session.resource('s3', endpoint_url=endPoint, config=botocore.config.Config(s3={'addressing_style': 'virtual'}) if useAlibaba else None)
                [self._availableBuckets.append(i.name) for i in con.buckets.all()]  # this will throw if credentials are invalid.
            elif(self._storageType == self.TypeAzure):
                azure_storage = Azure(self._accessKey, self._secretKey, self._credentialProfile, None)
                azure_storage.init()
                [self._availableBuckets.append(i.name) for i in azure_storage._blob_service.list_containers()]  # this will throw.
            elif(self._storageType == self.TypeGoogle):
                with open(self._profileName, 'r') as reader:
                    serviceJson = json.load(reader)
                    Project_Id = 'project_id'
                    if (Project_Id not in serviceJson):
                        raise Exception('(Project_Id) key isn\'t found in file ({})'.format(self._profileName))
                os.environ['GCLOUD_PROJECT'] = serviceJson[Project_Id]
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self._profileName
                try:
                    from google.cloud import storage
                    gs = storage.Client()
                except Exception as e:
                    self._errorText.append(str(e))
                    return False
                [self._availableBuckets.append(bucket.name) for bucket in gs.list_buckets()]
            else:
                raise Exception('Invalid storage type')
        except Exception as e:
            MsgInvalidCredentials = 'Invalid Credentials>'
            if (self._storageType == self.TypeAmazon or
                    self._storageType == self.TypeAlibaba):
                try:
                    from botocore.exceptions import ClientError
                except ImportError as e:
                    self._errorText.append(str(e))
                    return False
                if (isinstance(e, ClientError)):
                    exCode = e.response['Error']['Code'].lower()
                    if (exCode not in ['invalidaccesskeyid', 'signaturedoesnotmatch']):
                        return True  # the user may not have the access rights to list buckets but the bucket keys/contents could be accessed if the bucket name is known.
                    elif(exCode in ['accessdenied']):
                        return True  # the user has valid credentials but without the bucketlist permission.
            elif(self._storageType == self.TypeAzure):
                if (azure_storage):
                    if (azure_storage._SASToken):   # It's assumed, SAS string credentials aren't allowed to list buckets and the bucket name is picked from the SAS string.
                        self._availableBuckets.append(azure_storage._SASBucket)
                        return True
            self._errorText.append(MsgInvalidCredentials)
            self._errorText.append(str(e))
            return False
        return True


class OptimizeRastersUI(ProfileEditorUI):

    def __init__(self, profileName, storageType):
        super(OptimizeRastersUI, self).__init__(profileName, storageType, None, None, profileName)

    def getAvailableBuckets(self):
        ret = self.validateCredentials()
        response = {'response': {'results': ret, 'buckets': []}}
        if (not ret):
            return response
        response['response']['buckets'] = self._availableBuckets
        return response


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
        self.isLinux = os.name == 'posix'
        self.CMINSIZEALLOWED = 5242880

    def memoryStatus(self):
        if (not self.isLinux):
            ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(self))
        return self

    def getFreeMem(self):
        if (self.isLinux):
            try:
                return int(int(os.popen("free -b").readlines()[1].split()[2]) * .01)
            except Exception as e:
                return self.CMINSIZEALLOWED
        return int(self.memoryStatus().ullAvailPhys * .01)  # download file isn't split in chunks, for now usage is set to 0.01

    def memoryPerDownloadChunk(self):
        return self.getFreeMem()

    def memoryPerUploadChunk(self, totalThreads):   # get upload payload size per thread for the total cloud upload threads required.
        memPerChunk = self.getFreeMem() / totalThreads
        if (memPerChunk < self.CMINSIZEALLOWED):
            memPerChunk = self.CMINSIZEALLOWED
        return memPerChunk


class Lambda:
    account_name = 'aws_access_key_id'
    account_key = 'aws_secret_access_key'
    account_region = 'region'
    account_sns = 'sns_arn'
    queue_length = 'queuelength'

    def __init__(self, base=None):
        self._sns_aws_access_key = \
            self._sns_aws_secret_access_key = None
        self._sns_region = 'us-east-1'
        self._sns_ARN = None
        self._sns_connection = None
        self._aws_credentials = None    # aws credential file.
        self._base = base

    def initSNS(self, keyProfileName):
        if (not keyProfileName):
            return False
        self._aws_credentials = ConfigParser.RawConfigParser()
        userHome = '{}/{}/{}'.format(os.path.expanduser('~').replace('\\', '/'), '.aws', 'credentials')
        with open(userHome) as fptr:
            self._aws_credentials.readfp(fptr)
        if (not self._aws_credentials.has_section(keyProfileName)):
            return False
        self._sns_aws_access_key = self._aws_credentials.get(keyProfileName, self.account_name) if self._aws_credentials.has_option(keyProfileName, self.account_name) else None
        self._sns_aws_secret_access_key = self._aws_credentials.get(keyProfileName, self.account_key) if self._aws_credentials.has_option(keyProfileName, self.account_key) else None
        if (self._aws_credentials.has_option(keyProfileName, self.account_region)):
            self._sns_region = self._aws_credentials.get(keyProfileName, self.account_region)
        self._sns_ARN = self._aws_credentials.get(keyProfileName, self.account_sns) if self._aws_credentials.has_option(keyProfileName, self.account_sns) else None
        if (not self._sns_aws_access_key or
                not self._sns_aws_secret_access_key):
            return False
        try:
            import boto3
            session = boto3.Session(aws_access_key_id=self._sns_aws_access_key, aws_secret_access_key=self._sns_aws_secret_access_key, region_name=self._sns_region)
            self._sns_connection = session.resource('sns')
            self._sns_connection.meta.client.get_topic_attributes(TopicArn=self._sns_ARN)
        except ImportError as e:
            self._base.message('({})/Lambda'.format(str(e)), self._base.const_critical_text)
            return False
        except Exception as e:
            self._base.message('SNS/init\n{}'.format(str(e)), self._base.const_critical_text)
            return False
        return True

    def _updateCredentials(self, doc, direction='In'):
        inProfileNode = doc.getElementsByTagName('{}_S3_AWS_ProfileName'.format(direction))
        inKeyIDNode = doc.getElementsByTagName('{}_S3_ID'.format(direction))
        inKeySecretNode = doc.getElementsByTagName('{}_S3_Secret'.format(direction))
        rptProfile = self._base.getUserConfiguration.getValue('{}_S3_AWS_ProfileName'.format(direction))
        _resumeReporter = self._base.getUserConfiguration.getValue(CPRT_HANDLER)    # gives a chance to overwrite the profile name in the parameter file with the orjob one.
        if (_resumeReporter):                                                       # unless the orjob was edited manually, the profile name on both would be the same.
            selectProfile = InputProfile if direction == 'In' else OutputProfile
            if (selectProfile in _resumeReporter._header):
                rptProfile = _resumeReporter._header[selectProfile]
        CERR_MSG = 'Credential keys don\'t exist/invalid'
        if ((not len(inProfileNode) or
             not inProfileNode[0].hasChildNodes() or
                not inProfileNode[0].firstChild) and
                not rptProfile):
            if (not len(inKeyIDNode) or
                    not len(inKeySecretNode)):
                self._base.message(CERR_MSG, self._base.const_critical_text)
                return False
            if (not inKeyIDNode[0].hasChildNodes() or
                    not inKeyIDNode[0].firstChild):
                self._base.message(CERR_MSG, self._base.const_critical_text)
                return False
        else:
            keyProfileName = rptProfile if rptProfile else inProfileNode[0].firstChild.nodeValue
            if (not self._aws_credentials.has_section(keyProfileName)):
                return False
            parentNode = doc.getElementsByTagName('Defaults')
            if (not len(parentNode)):
                self._base.message('Unable to update credentials', self._base.const_critical_text)
                return False
            _sns_aws_access_key = self._aws_credentials.get(keyProfileName, self.account_name) if self._aws_credentials.has_option(keyProfileName, self.account_name) else None
            _sns_aws_secret_access_key = self._aws_credentials.get(keyProfileName, self.account_key) if self._aws_credentials.has_option(keyProfileName, self.account_key) else None
            if (len(inKeyIDNode)):
                if (inKeyIDNode[0].hasChildNodes() and
                        inKeyIDNode[0].firstChild.nodeValue):
                    _sns_aws_access_key = inKeyIDNode[0].firstChild.nodeValue
                parentNode[0].removeChild(inKeyIDNode[0])
            inKeyIDNode = doc.createElement('{}_S3_ID'.format(direction))
            inKeyIDNode.appendChild(doc.createTextNode(str(_sns_aws_access_key)))
            parentNode[0].appendChild(inKeyIDNode)
            if (len(inKeySecretNode)):
                if (inKeySecretNode[0].hasChildNodes() and
                        inKeySecretNode[0].firstChild.nodeValue):
                    _sns_aws_secret_access_key = inKeySecretNode[0].firstChild.nodeValue
                parentNode[0].removeChild(inKeySecretNode[0])
            inKeySecretNode = doc.createElement('{}_S3_Secret'.format(direction))
            inKeySecretNode.appendChild(doc.createTextNode(str(_sns_aws_secret_access_key)))
            parentNode[0].appendChild(inKeySecretNode)
            if (inProfileNode.length):
                parentNode[0].removeChild(inProfileNode[0])
            if (not _sns_aws_access_key or
                    not _sns_aws_secret_access_key):
                self._base.message(CERR_MSG, self._base.const_critical_text)
                return False
        return True

    def submitJob(self, orjob):
        if (not self._sns_connection or
                not orjob):
            return False
        _orjob = Report(Base())
        if (not _orjob.init(orjob) or
                not _orjob.read()):
            self._base.message('Job file read error', self._base.const_critical_text)
            return False
        orjobName = os.path.basename(orjob)
        orjobWOExt = orjobName.lower().replace(Report.CJOB_EXT, '')
        configPath = _orjob._header['config']
        configName = '{}.xml'.format(orjobWOExt)
        if (CTEMPINPUT in _orjob._header):
            _orjob._header[CTEMPINPUT] = '/tmp/{}/tempinput'.format(orjobWOExt)
        if (CTEMPOUTPUT in _orjob._header):
            _orjob._header[CTEMPOUTPUT] = '/tmp/{}/tempoutput'.format(orjobWOExt)
        if (CRASTERPROXYPATH in _orjob._header):
            _orjob._header['store{}'.format(CRASTERPROXYPATH)] = _orjob._header[CRASTERPROXYPATH]
            _orjob._header[CRASTERPROXYPATH] = '/tmp/{}/{}'.format(orjobWOExt, CRASTERPROXYPATH)
        if ('config' in _orjob._header):
            _orjob._header['config'] = '/tmp/{}'.format(configName)
        configContent = ''
        try:
            with open(configPath, 'rb') as f:
                configContent = f.read()
        except Exception as e:
            self._base.message('{}'.format(str(e)), self._base.const_critical_text)
            return False
        try:
            doc = minidom.parseString(configContent)
            if (not _orjob._isInputHTTP):   # skip looking into the parameter file for credentials if the input is a direct HTTP link with no reqruirement to pre-download the raster/file before processing.
                if (not self._updateCredentials(doc, 'In')):
                    return False
            if (not self._updateCredentials(doc, 'Out')):
                return False
            configContent = doc.toprettyxml()
        except Exception as e:
            self._base.message(str(e), self._base.const_critical_text)
            return False
        orjobHeader = ''
        for hdr in _orjob._header:
            if (hdr in [InputProfile, OutputProfile]):  # lambda works with AWS key pairs and not profile names.
                continue
            orjobHeader += '# {}={}\n'.format(hdr, _orjob._header[hdr])
        length = len(_orjob._input_list)
        jobQueue = self._base.getUserConfiguration.getValue(self.queue_length)
        if (not jobQueue):
            jobQueue = _orjob._header[self.queue_length] if self.queue_length in _orjob._header else None   # read from orjob/reporter if not in at cmd-line
        if (not jobQueue or
            jobQueue and
            (jobQueue <= 0 or
             jobQueue > length)):
            jobQueue = length
        i = 0
        errLambda = False
        functionJobs = []
        functionName = None
        useLambdaFunction = False
        lambdaArgs = _orjob.operation.split(':')
        if (len(lambdaArgs) > 2):
            if (lambdaArgs[1].lower() == 'function'):
                functionName = lambdaArgs[2]  # preserve case in lambda functions.
                useLambdaFunction = True
        self._base.message('Invoke using ({})'.format('Function' if useLambdaFunction else 'SNS'))
        while(i < length):
            orjobContent = ''
            for j in range(i, i + jobQueue):
                if (j == length):
                    break
                f = _orjob._input_list[j]
                if (f.endswith('/')):
                    i += 1
                    continue    # skip folder entries
                if (not orjobContent):
                    orjobContent += orjobHeader
                orjobContent += '{}\n'.format(f)
                i += 1
            if (not orjobContent):
                continue
            store = {'orjob': {'file': '{}_{}{}'.format(orjobWOExt, i, Report.CJOB_EXT), 'content': orjobContent}, 'config': {'file': configName, 'content': configContent}}
            message = json.dumps(store)
            if (useLambdaFunction):
                functionJobs.append(message)
            else:
                if (not self.invokeSNS(message)):
                    errLambda = True
        if (useLambdaFunction and
                not self.invokeFunction(functionName, functionJobs)):
            errLambda = True
        return not errLambda

    def invokeSNS(self, message):
        publish = None
        try:
            publish = self._sns_connection.meta.client.publish(TopicArn=self._sns_ARN, Message=message, Subject='OR')
            CPUBLISH_META = 'ResponseMetadata'
            if (CPUBLISH_META in publish and
                    'RequestId' in publish[CPUBLISH_META]):
                self._base.message('Lambda working on the (RequestID) [{}]...'.format(publish[CPUBLISH_META]['RequestId']))
        except Exception as e:
            self._base.message('{}'.format(str(e)), self._base.const_critical_text)
            return False
        return True

    def invokeFunction(self, functionName, message):
        if (not functionName or
                not message):
            return False
        try:
            payloads = []
            MaxJobs = len(message)
            for i in range(0, MaxJobs):
                payload = {'Records': [{'Sns': {'Message': message[i]}}]}
                payloads.append(payload)
            timeStart = datetime.now()
            pool = ThreadPool(LambdaFunction, base=self._base, function_name=functionName, aws_access_key_id=self._sns_aws_access_key, aws_secret_access_key=self._sns_aws_secret_access_key)
            pool.init(maxWorkers=100)
            for i in range(0, len(payloads)):
                pool.addWorker(payloads[i], i)
            pool.run()
            self._base.getUserConfiguration.getValue(CPRT_HANDLER).write()    # update .orjob status
            self._base.message('duration> {}s'.format((datetime.now() - timeStart).total_seconds()))
            if (pool.isErrorDetected):
                return False
        except Exception as e:
            self._base.message('{}'.format(str(e)), self._base.const_critical_text)
            return False
        return True


class LambdaFunction(threading.Thread):
    Base = 'base'

    def __init__(self, kwargs):
        threading.Thread.__init__(self)
        self.daemon = True
        self.function = None
        self.kwargs = kwargs
        self.result = None
        self.base = None
        if (self.Base in kwargs and
                isinstance(kwargs[self.Base], Base)):
            self.base = kwargs[self.Base]
        pass

    def init(self, payload, jobID=0):
        FunctionName = 'function_name'
        if (FunctionName in self.kwargs):
            self.function = self.kwargs[FunctionName]
        if (self.function is None):
            return False
        self.payload = payload
        self.jobID = jobID
        return True

    @property
    def response(self):
        return self.result

    def message(self, message, messageType=0):
        if (self.base is not None):
            if (hasattr(self.base, 'message')):
                return self.base.message(message, messageType)
        print (message)

    def run(self):
        try:
            import boto3
            import boto3.session
            session = boto3.session.Session()
            client = session.client('lambda', aws_access_key_id=self.kwargs['aws_access_key_id'] if 'aws_access_key_id' in self.kwargs else None,
                                    aws_secret_access_key=self.kwargs['aws_secret_access_key'] if 'aws_secret_access_key' in self.kwargs else None)
            self.result = client.invoke(FunctionName=self.function, InvocationType='RequestResponse',
                                        Payload=json.dumps(self.payload))
            respJSON = json.loads(self.result['Payload'].read())
            if (not respJSON):
                return None
            respStatus = respJSON['status'] if 'status' in respJSON else None
            if (self.base is not None):
                report = self.base.getUserConfiguration.getValue(CPRT_HANDLER)
                report.syncRemoteToLocal(respJSON)
            self.message('Completed/{}/Status [{}]'.format(self.jobID, str(respStatus)))
        except Exception as e:
            self.message('{}'.format(e), self.base.const_critical_text if self.base else 2)    # 2 for critical
            if (self.base is not None):
                self.base.getUserConfiguration.setValue(CCFG_LAMBDA_INVOCATION_ERR, True)
            return False
        return True


class ThreadPool(object):
    DefMaxWorkers = 1
    Job = 'job'
    JobID = 'jobID'
    Base = 'base'

    def __init__(self, function, **kwargs):
        self.maxWorkers = self.DefMaxWorkers
        self.function = function
        self.kwargs = kwargs
        self.base = None
        if (self.Base in kwargs and
                isinstance(kwargs[self.Base], Base)):
            self.base = kwargs[self.Base]
        self.work = []
        self._isErrorDetected = False

    def init(self, maxWorkers=DefMaxWorkers):
        try:
            self.maxWorkers = int(maxWorkers)
            if (self.maxWorkers < 1):
                self.maxWorkers = self.DefMaxWorkers
        except BaseException:
            self.maxWorkers = self.DefMaxWorkers

    def addWorker(self, job, jobID=None):
        self.work.append({self.Job: job, self.JobID: jobID})

    def message(self, message, messageType=0):
        if (self.base is not None):
            if (hasattr(self.base, 'message')):
                return self.base.message(message, messageType)
        print (message)

    @property
    def isErrorDetected(self):
        return self._isErrorDetected

    def run(self):
        lenBuffer = self.maxWorkers
        threads = []
        workers = 0
        maxWorkers = len(self.work)
        while(1):
            len_threads = len(threads)
            while(len_threads):
                alive = [t.isAlive() for t in threads]
                countDead = sum(not x for x in alive)
                if (countDead):
                    lenBuffer = countDead
                    threads = [t for t in threads if t.isAlive()]
                    break
            buffer = []
            for i in range(0, lenBuffer):
                if (workers == maxWorkers):
                    break
                buffer.append(self.work[workers])
                workers += 1
            if (not buffer and
                    not threads):
                break
            for f in buffer:
                try:
                    t = self.function(self.kwargs)
                    isJobID = self.JobID in f
                    if (not t.init(f[self.Job], f[self.JobID] if isJobID else 0)):
                        return False
                    t.daemon = True
                    if (isJobID):
                        self.message('Started/{}'.format(f[self.JobID]))
                    t.start()
                    threads.append(t)
                except Exception as e:
                    self.message(str(e))
                    continue
        if (self.base is not None):
            if (self.base.getUserConfiguration.getValue(CCFG_LAMBDA_INVOCATION_ERR)):
                self._isErrorDetected = True
                return False
        return True


class RasterAssociates(object):
    RasterAuxExtensions = ['.lrc', '.idx', '.pjg', '.ppng', '.pft', '.pjp', '.pzp', '.tif.cog.pzp', '.tif.cog.idx', '.tif.cogtiff.aux.xml']

    def __init__(self):
        self._info = {}

    def _stripExtensions(self, relatedExts):
        return ';'.join([x.strip() for x in relatedExts.split(';') if x.strip()])

    def addRelatedExtensions(self, primaryExt, relatedExts):    # relatedExts can be a ';' delimited list.
        if (not primaryExt or
            not primaryExt.strip() or
                not relatedExts):
            return False
        for p in primaryExt.split(';'):
            p = p.strip()
            if (not p):
                continue
            if (p in self._info):
                self._info[p] += ';{}'.format(self._stripExtensions(relatedExts))
                continue
            self._info[p] = self._stripExtensions(relatedExts)
        return True

    @staticmethod
    def removeRasterProxyAncillaryFiles(inputPath):
        # remove ancillary extension files that are no longer required for (rasterproxy) files on the client side.
        refBasePath = inputPath[:-len(CONST_OUTPUT_EXT)]
        errorEntries = []
        for ext in RasterAssociates.RasterAuxExtensions:
            try:
                path = refBasePath + ext
                if (os.path.exists(path)):
                    if (path.endswith(COGTIFFAuxFile)):
                        os.rename(path, path.replace(CloudOGTIFFExt, ''))
                        continue
                    os.remove(path)
            except Exception as e:
                errorEntries.append('{}'.format(str(e)))
        return errorEntries

    @staticmethod
    def findExtension(path):
        if (not path):
            return False
        pos = path.rfind('.')
        ext = None
        while(pos != -1):
            ext = path[pos + 1:]
            pos = path[:pos].rfind('.')
        return ext

    def findPrimaryExtension(self, relatedExt):
        _relatedExt = self.findExtension(relatedExt)
        if (not _relatedExt):
            return False
        for primaryExt in self._info:
            if (self._info[primaryExt].find(_relatedExt) != -1):
                splt = self._info[primaryExt].split(';')
                if (_relatedExt in splt):
                    return primaryExt
        return None

    def getInfo(self):
        return self._info


class Base(object):
    # log status types enums
    const_general_text = 0
    const_warning_text = 1
    const_critical_text = 2
    const_status_text = 3
    # ends

    def __init__(self, msgHandler=None, msgCallback=None, userConfig=None):
        self._m_log = msgHandler
        self._m_msg_callback = msgCallback
        self._m_user_config = userConfig
        if (self._m_msg_callback):
            if (self._m_log):
                self._m_log.isPrint = False

    def init(self):
        self.hashInfo = {}
        self.timedInfo = {'files': []}
        self._modifiedProxies = []
        return True

    def message(self, msg, status=const_general_text):
        if (self._m_log):
            self._m_log.Message(msg, status)
        if (self._m_msg_callback):
            self._m_msg_callback(msg, status)

    def isLinux(self):
        return sys.platform.lower().startswith(('linux', 'darwin'))

    def convertToTokenPath(self, inputPath):
        if (not inputPath):
            return None
        tokenPath = None
        if (self.getBooleanValue(self.getUserConfiguration.getValue(UseToken)) and
                self.getBooleanValue(self.getUserConfiguration.getValue('iss3'))):
            cloudHandler = self.getSecuredCloudHandlerPrefix(CS3STORAGE_IN)
            if (not cloudHandler):
                return None
            tokenPath = inputPath.replace(self.getUserConfiguration.getValue(CIN_S3_PREFIX, False),
                                          '/{}/{}/'.format(cloudHandler, self.getUserConfiguration.getValue('In_S3_Bucket', False)))
        return tokenPath

    def copyBinaryToTmp(self, binarySrc, binaryDst):
        if (not os.path.exists(binaryDst)):
            try:
                shutil.copyfile(binarySrc, binaryDst)
                os.chmod(binaryDst, 0o777)   # set (rwx) to make lambda work.
                self.message('**LAMBDA** Copied -> {}'.format(binarySrc))
            except Exception as e:
                self.message(str(e), self.const_critical_text)
                return False
        return True

    def convertToForwardSlash(self, input, endSlash=True):
        if (not input):
            return None
        _input = input.replace('\\', '/').strip()
        if (_input[-4:].lower().endswith('.csv')):
            endSlash = False
        f, e = os.path.splitext(_input)
        if (endSlash and
            not _input.endswith('/') and
                not _input.lower().startswith('http') and
                len(e) == 0):
            _input += '/'
        return _input

    def insertUserTextToOutputPath(self, path, text, pos):
        if (not path):
            return None
        if (not text):
            return path
        try:
            _pos = int(pos)
        except BaseException:
            _pos = CHASH_DEF_INSERT_POS
        _path = path.split('/')
        _pos -= 1
        lenPath = len(_path)
        if (_pos >= lenPath):
            _pos = lenPath - 1
        p = os.path.dirname(path)
        if (p not in self.hashInfo):
            if (text == CHASH_DEF_CHAR):
                text = binascii.hexlify(os.urandom(4))
            else:
                m = hashlib.md5()
                m.update('{}/{}'.format(p, text))
                text = m.hexdigest()
            text = '{}_@'.format(text[:8])    # take only the fist 8 chars
            self.hashInfo[p] = text
        else:
            text = self.hashInfo[p]
        _path.insert(_pos, text)
        return '/'.join(_path)

    def urlEncode(self, url):
        if (not url):
            return ''
        _url = url.strip().replace('\\', '/')
        _storePaths = []
        for path in _url.split('/'):
            if (path.find(':') != -1):
                _storePaths.append(path)
                continue
            data = {'url': path}
            encoded = urlencode(data)
            _storePaths.append(encoded.split('=')[1])
        return '/'.join(_storePaths)

    def getBooleanValue(self, value):        # helper function
        if (value is None):
            return False
        if (isinstance(value, bool)):
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
            if (not CRUN_IN_AWSLAMBDA):
                self._m_log.WriteLog('#all')   # persist information/errors collected.

    def renameMetaFileToMatchRasterExtension(self, metaFile):
        updatedMetaFile = metaFile
        if (self.getUserConfiguration and
                not self.getBooleanValue(self.getUserConfiguration.getValue('KeepExtension'))):
            rasterExtension = RasterAssociates().findExtension(updatedMetaFile)
            if (not rasterExtension):
                return metaFile
            inputExtensions = rasterExtension.split('.')
            firstExtension = inputExtensions[0]
            if (len(inputExtensions) == 1):  # no changes to extension if the input has only one extension.
                return metaFile
            if (True in [firstExtension.endswith(x) for x in self.getUserConfiguration.getValue(CCFG_RASTERS_NODE)]):
                updatedMetaFile = updatedMetaFile.replace('.{}'.format(firstExtension), '.mrf')
        return updatedMetaFile

    def _isRasterProxyFormat(self, uFormat):
        if (not uFormat):
            return False
        rpFormat = self.getUserConfiguration.getValue('rpformat')
        return rpFormat == uFormat.lower()

    def copyMetadataToClonePath(self, sourcePath):
        if (not self.getUserConfiguration):
            return False
        _clonePath = self.getUserConfiguration.getValue(CCLONE_PATH, False)
        if (not _clonePath):
            return True     # not an error.
        if (self._isRasterProxyFormat('csv')):
            return True     # not an error.
        presentMetaLocation = self.getUserConfiguration.getValue(CCFG_PRIVATE_OUTPUT, False)
        if (self.getUserConfiguration.getValue(CTEMPOUTPUT) and
                self.getBooleanValue(self.getUserConfiguration.getValue(CCLOUD_UPLOAD))):
            presentMetaLocation = self.getUserConfiguration.getValue(CTEMPOUTPUT, False)
        _cloneDstFile = sourcePath.replace(presentMetaLocation, _clonePath)
        _cloneDirs = os.path.dirname(_cloneDstFile)
        try:
            if (not os.path.exists(_cloneDirs)):
                makedirs(_cloneDirs)
            if (sourcePath != _cloneDstFile):
                shutil.copyfile(sourcePath, _cloneDstFile)
        except Exception as e:
            self.message(str(e), self.const_critical_text)
            return False
        return True

    def S3Upl(self, input_file, user_args, **kwargs):
        global _rpt
        internal_err_msg = 'Internal error at [S3Upl]'
        if (not self._m_user_config or
            (user_args and
             not isinstance(user_args, dict))):
            self.message(internal_err_msg, self.const_critical_text)
            return False
        _source_path = None
        if (_rpt):
            _source_path = getSourcePathUsingTempOutput(input_file)
            if (_source_path):
                _ret_val = _rpt.getRecordStatus(_source_path, CRPT_UPLOADED)
                if (_ret_val == CRPT_YES):
                    return True
        ret_buff = []
        upload_cloud_type = self._m_user_config.getValue(COUT_CLOUD_TYPE, True)
        if (upload_cloud_type == CCLOUD_AMAZON):
            if (S3_storage is None):    # globally declared: S3_storage
                self.message(internal_err_msg, self.const_critical_text)
                return False
            _single_upload = _include_subs = False    # def
            if (user_args):
                if (CSIN_UPL in user_args):
                    _single_upload = self.getBooleanValue(user_args[CSIN_UPL])
                if (CINC_SUB in user_args):
                    _include_subs = self.getBooleanValue(user_args[CINC_SUB])
            ret_buff = S3_storage.upload_group(input_file, single_upload=_single_upload, include_subs=_include_subs)
            if (len(ret_buff) == 0):
                return False
        elif (upload_cloud_type == CCLOUD_AZURE):
            if(azure_storage is None):
                self.message(internal_err_msg, self.const_critical_text)
                return False
            properties = {
                CTEMPOUTPUT: self._m_user_config.getValue(CTEMPOUTPUT, False),
                'access': self._m_user_config.getValue(COUT_AZURE_ACCESS, True)
            }
            _input_file = input_file.replace('\\', '/')
            (p, n) = os.path.split(_input_file)
            indx = n.find('.')
            file_name_prefix = n
            if (indx >= 0):
                file_name_prefix = file_name_prefix[:indx]
            input_folder = os.path.dirname(_input_file)
            for r, d, f in os.walk(input_folder):
                r = r.replace('\\', '/')
                if (r == input_folder):
                    for _file in f:
                        if (_file.startswith('{}.'.format(file_name_prefix))):
                            file_to_upload = os.path.join(r, _file)
                            if (azure_storage.upload(
                                file_to_upload,
                                self._m_user_config.getValue(COUT_AZURE_CONTAINER, False),
                                self._m_user_config.getValue(CCFG_PRIVATE_OUTPUT, False),
                                properties, name=_source_path, method=TimeIt.Upload, store=self
                            )):
                                ret_buff.append(file_to_upload)
                    break
        elif (upload_cloud_type == Store.TypeGoogle):
            if(google_storage is None):
                self.message(internal_err_msg, self.const_critical_text)
                return False
            properties = {
                CTEMPOUTPUT: self._m_user_config.getValue(CTEMPOUTPUT, False),
                'access': self._m_user_config.getValue(COUT_AZURE_ACCESS, True)
            }
            _input_file = input_file.replace('\\', '/')
            (p, n) = os.path.split(_input_file)
            indx = n.find('.')
            file_name_prefix = n
            if (indx >= 0):
                file_name_prefix = file_name_prefix[:indx]
            input_folder = os.path.dirname(_input_file)
            for r, d, f in os.walk(input_folder):
                r = r.replace('\\', '/')
                if (r == input_folder):
                    for _file in f:
                        if (_file.startswith('{}.'.format(file_name_prefix))):
                            file_to_upload = self.convertToForwardSlash(os.path.join(r, _file), False)
                            if (google_storage.upload(
                                file_to_upload,
                                self._m_user_config.getValue(COUT_GOOGLE_BUCKET, False),
                                self._m_user_config.getValue(CCFG_PRIVATE_OUTPUT, False),
                                properties
                            )):
                                ret_buff.append(file_to_upload)
                    break
        if (CS3_MSG_DETAIL):
            self.message('Following file(s) uploaded to ({})'.format(upload_cloud_type.capitalize()))
            [self.message('{}'.format(f)) for f in ret_buff]
        if (user_args):
            if (USR_ARG_DEL in user_args):
                if (user_args[USR_ARG_DEL] and
                        user_args[USR_ARG_DEL]):
                    for f in ret_buff:
                        try:
                            _is_remove = True
                            if (til):
                                if (til.fileTILRelated(f)):
                                    _is_remove = False
                            if (_is_remove):
                                try:
                                    os.remove(f)
                                except BaseException:
                                    time.sleep(CDEL_DELAY_SECS)
                                    os.remove(f)
                                self.message('[Del] %s' % (f))
                        except Exception as e:
                            self.message('[Del] Err. (%s)' % (str(e)), self.const_critical_text)
        if (ret_buff):
            Input = 'input'
            setUploadRecordStatus(kwargs[Input] if kwargs and Input in kwargs else input_file, CRPT_YES)
        return (len(ret_buff) > 0)

    def getSecuredCloudHandlerPrefix(self, direction):
        warningMsg = 'getSecuredCloudHandlerPrefix/{} is false'.format('-usetoken' if direction == CS3STORAGE_IN else 'internal/usetokenonoutput')
        if (direction == CS3STORAGE_IN and
                not self.getBooleanValue(self.getUserConfiguration.getValue(UseToken))):
            self.message(warningMsg, self.const_warning_text)
            return None
        if (direction == CS3STORAGE_OUT and
                not self.getBooleanValue(self.getUserConfiguration.getValue(UseTokenOnOuput))):
            self.message(warningMsg, self.const_warning_text)
            return None
        storageType = self.getUserConfiguration.getValue(COUT_CLOUD_TYPE if direction == CS3STORAGE_OUT else CIN_CLOUD_TYPE, True)
        prefix = 'vsis3'
        usingOSSDomain = self.getUserConfiguration.getValue('{}oss'.format('in' if direction == CS3STORAGE_IN else 'out'))  # alibaba?
        if (usingOSSDomain):
            prefix = 'vsioss'
        elif (storageType == CCLOUD_AZURE):
            prefix = 'vsiaz'
        elif (storageType == CCLOUD_GOOGLE):
            prefix = 'vsigs'
        return prefix


class GDALInfo(object):
    CGDAL_INFO_EXE = 'gdalinfo'
    CW = 'width'
    CH = 'height'

    def __init__(self, base, msgCallback=None):
        self._GDALPath = None
        self._GDALInfo = []
        self._propertyNames = [self.CW, self.CH]
        self._base = base
        self._m_msg_callback = msgCallback

    def init(self, GDALPath):
        if (not GDALPath):
            return False
        if (self._base and
                not isinstance(self._base, Base)):
            return False
        if (not self._base.isLinux()):
            self.CGDAL_INFO_EXE += CEXEEXT
        self._GDALPath = GDALPath.replace('\\', '/')
        if (not self._GDALPath.endswith('/{}'.format(self.CGDAL_INFO_EXE))):
            self._GDALPath = os.path.join(self._GDALPath, self.CGDAL_INFO_EXE).replace('\\', '/')
        # check for path existence / e.t.c
        if (not os.path.exists(self._GDALPath)):
            self.message('Invalid GDALInfo/Path ({})'.format(self._GDALPath), self._base.const_critical_text)
            return False
        if (CRUN_IN_AWSLAMBDA):
            _gdalinfo = '/tmp/{}'.format(self.CGDAL_INFO_EXE)
            if (not self._base.copyBinaryToTmp(self._GDALPath, _gdalinfo)):
                return False
            self._GDALPath = _gdalinfo
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
        args.append('"{}"'.format(input))
        self.message('Using GDALInfo ({})..'.format(input), self._base.const_general_text)
        return self._call_external(args)

    def message(self, msg, status=0):
        self._m_msg_callback(msg, status) if self._m_msg_callback else self._base.message(msg, status)

    @property
    def bandInfo(self):
        if (not len(self._GDALInfo)):
            return None
        retInfo = []
        for v in self._GDALInfo:
            if (v.startswith('Band ')):
                retInfo.append(v)
        return retInfo

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
                except BaseException:
                    pass
        _value = (_max / _BS)
        if (_value <= 0):
            return ''
        _levels = int(2 ** math.ceil(math.log(_value, 2)))
        CDEFPYRAMID_LEV = '2'
        _steps = ''
        while (_levels >= 2):
            _steps = '{} {}'.format(_levels, _steps)
            _levels >>= 1
        _steps = _steps.strip()
        if (not _steps):
            _steps = CDEFPYRAMID_LEV
        self.message('<PyramidFactor> set to ({})'.format(_steps), self._base.const_general_text)
        return _steps

    def _call_external(self, args):
        p = subprocess.Popen(' '.join(args), shell=True, stdout=subprocess.PIPE)
        message = '/'
        CSIZE_PREFIX = b'Size is'
        while (message):
            message = p.stdout.readline()
            if (message):
                _strip = message.strip()
                if (_strip.find(CSIZE_PREFIX) != -1):
                    wh = _strip.split(CSIZE_PREFIX)
                    if (len(wh) > 1):
                        wh = wh[1].split(b',')
                        if (self.CW in self._propertyNames):
                            self.width = int(wh[0].strip())
                        if (self.CH in self._propertyNames):
                            self.height = int(wh[1].strip())
                self._GDALInfo.append(_strip)
        return len(self._GDALInfo) > 0


class UpdateMRF:

    def __init__(self, base=None):
        self._mode = \
            self._cachePath = \
            self._input = \
            self._output = \
            self._homePath = \
            self._outputURLPrefix = None
        self._base = base

    def init(self, input, output, mode=None,
             cachePath=None, homePath=None, outputURLPrefix=None):
        if (not input or
                not output):
            return False
        if (not os.path.exists(output)):
            try:
                makedirs(output)
            except Exception as e:
                self._base.message(str(e), self._base.const_critical_text)
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

    def copyInputMRFFilesToOutput(self, doUpdate=True):
        if (not self._input or
                not self._output):
            if (self._base):
                self._base.message('Not initialized!', self._base.const_critical_text)
            return False
        _prefix = self._input[:self._input.rfind('.')]
        input_folder = os.path.dirname(self._input)
        _resumeReporter = self._base.getUserConfiguration.getValue(CPRT_HANDLER)
        if (_resumeReporter and
                CRESUME_HDR_OUTPUT not in _resumeReporter._header):
            _resumeReporter = None
        rpformat = self._base.getUserConfiguration.getValue('rpformat')
        rpCSV = self._base._isRasterProxyFormat(rpformat)
        for r, d, f in os.walk(input_folder):
            r = r.replace('\\', '/')
            if (r == input_folder):
                for _file in f:
                    if (True in [_file.lower().endswith(x) for x in RasterAssociates.RasterAuxExtensions]):
                        continue
                    _mk_path = r + '/' + _file
                    if (_mk_path.startswith(_prefix)):
                        try:
                            _output_path = self._output
                            if (self._homePath):
                                userInput = self._homePath
                                if (_resumeReporter):
                                    userInput = _resumeReporter._header[CRESUME_HDR_OUTPUT]
                                _output_path = os.path.join(self._output, os.path.dirname(self._input.replace(self._homePath if self._input.startswith(self._homePath) else userInput, '')))    #
                            if (not os.path.exists(_output_path)):
                                if (not rpCSV):
                                    makedirs(_output_path)
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
                                if (not rpCSV):
                                    shutil.copy(_mk_path, _mk_copy_path)
                        except Exception as e:
                            if (self._base):
                                self._base.message('-rasterproxypath/{}'.format(str(e)), self._base.const_critical_text)
                            continue

    def update(self, output, **kwargs):
        try:
            _CCACHE_EXT = '.mrf_cache'
            _CDOC_ROOT = 'MRF_META'
            comp_val = None         # for (splitmrf)
            doc = minidom.parse(self._input)
            _rasterSource = self._input
            isCOGTIFF = self._base.getUserConfiguration.getValue('cog')
            autoCreateRasterProxy = False
            if (self._mode):
                autoCreateRasterProxy = not self._mode.endswith('mrf')
            if (self._outputURLPrefix and   # -cloudupload?
                    self._homePath):
                usrPath = self._base.getUserConfiguration.getValue(CUSR_TEXT_IN_PATH, False)
                usrPathPos = CHASH_DEF_INSERT_POS  # default insert pos (sub-folder loc) for user text in output path
                if (usrPath):
                    (usrPath, usrPathPos) = usrPath.split(CHASH_DEF_SPLIT_CHAR)
                _rasterSource = '{}{}'.format(self._outputURLPrefix, _rasterSource.replace(self._homePath, ''))
                if (_rasterSource.startswith('/vsicurl/')):
                    isOutContainerSAS = False
                    if (self._base.getBooleanValue(self._base.getUserConfiguration.getValue(UseTokenOnOuput)) and
                            not self._base.getBooleanValue(self._base.getUserConfiguration.getValue('iss3'))):
                        cloudHandler = self._base.getSecuredCloudHandlerPrefix(CS3STORAGE_OUT)
                        if (cloudHandler):
                            outContainer = self._base.getUserConfiguration.getValue('Out_S3_Bucket', False)
                            proxyURL = self._base.getUserConfiguration.getValue(CCLONE_PATH, False)
                            proxySubfolders = output.replace(proxyURL, '')
                            proxyFileURL = os.path.join(self._base.getUserConfiguration.getValue(CCFG_PRIVATE_OUTPUT, False), proxySubfolders)
                            isOutContainerSAS = (self._base.getUserConfiguration.getValue(COUT_CLOUD_TYPE, True) == CCLOUD_AZURE and
                                                 azure_storage is not None and
                                                 azure_storage._SASToken is not None)
                            _rasterSource = '/vsicurl/{}'.format(azure_storage._blob_service.make_blob_url(outContainer, proxyFileURL)) if isOutContainerSAS else '/{}/{}/{}'.format(cloudHandler, outContainer, proxyFileURL)
                    if (not isOutContainerSAS):
                        _rasterSource = self._base.urlEncode(_rasterSource)
                if (usrPath):
                    _idx = _rasterSource.find(self._base.getUserConfiguration.getValue(CCFG_PRIVATE_OUTPUT, False))
                    if (_idx != -1):
                        suffix = self._base.insertUserTextToOutputPath(_rasterSource[_idx:], usrPath, usrPathPos)
                        _rasterSource = _rasterSource[:_idx] + suffix
            else:   # if -tempoutput is set, readjust the CachedSource/Source path to point to -output.
                if (self._base.getUserConfiguration.getValue(CTEMPOUTPUT) or
                        autoCreateRasterProxy):
                    _output = self._base.getUserConfiguration.getValue(CCFG_PRIVATE_OUTPUT)
                    if (_output):
                        _rasterSource = _rasterSource.replace(self._homePath, _output)
            nodeMeta = doc.getElementsByTagName(_CDOC_ROOT)
            nodeRaster = doc.getElementsByTagName('Raster')
            if (not nodeMeta or
                    not nodeRaster):
                raise Exception('Err. Invalid header')
            cachedNode = doc.getElementsByTagName('CachedSource')
            if (not cachedNode):
                cachedNode.append(doc.createElement('CachedSource'))
                nodeSource = doc.createElement('Source')
                azSAS = self._base.getUserConfiguration.getValue(CFGAZSASW, False)
                trueInput = _rasterSource
                if ('trueInput' in kwargs):
                    trueInput = kwargs['trueInput']
                nodeSource.appendChild(doc.createTextNode('{}{}'.format(trueInput, '?' + azSAS if azSAS else '')))
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
            # make sure the 'CacheSource/Source' is pointing at the processed raster output
            if (autoCreateRasterProxy):
                node = doc.getElementsByTagName('Source')
                if (node):
                    sourceVal = os.path.join(_rasterSource, os.path.basename(self._input).split(CloudOGTIFFExt)[0])
                    node[0].firstChild.nodeValue = sourceVal
            # ends
            if (self._cachePath):
                cache_output = self._cachePath
            if (not self._base.getUserConfiguration):
                raise Exception('Err/Internal. UpdateMRF/getUserConfiguration')
            cacheSubFolders = ''
            if (self._cachePath):
                cacheSubFolders = self._base.convertToForwardSlash(os.path.dirname(output)).replace(self._output if self._cachePath else self._homePath, '')
            (f, ext) = os.path.splitext(os.path.basename(self._input))
            rep_data_file = rep_indx_file = os.path.abspath('{}{}{}{}'.format(cache_output, cacheSubFolders, f, _CCACHE_EXT)).replace('\\', '/')  # Get abs path in case the -output was relative for cache to function properly.
            nodeData = nodeIndex = None
            if (comp_val):
                extensions_lup = {
                    'lerc': {'data': '.lrc', 'index': '.idx'}
                }
            useTokenPath = self._base.convertToTokenPath(doc.getElementsByTagName('Source')[0].firstChild.nodeValue)
            if (useTokenPath is not None):
                doc.getElementsByTagName('Source')[0].firstChild.nodeValue = useTokenPath
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
            _mrfBody = doc.toxml().replace('&quot;', '"')       # GDAL mrf driver can't handle XML entity names.
            _indx = _mrfBody.find('<{}>'.format(_CDOC_ROOT))
            if (_indx == -1):
                raise Exception('Err. Invalid MRF/header')
            _mrfBody = _mrfBody[_indx:]
            rpCSV = self._base._isRasterProxyFormat(self._base.getUserConfiguration.getValue('rpformat'))
            if (rpCSV):
                _mrfBody = _mrfBody.replace('\n', '') + '\n'
                self._base._modifiedProxies.append(_mrfBody)
                if (self._or_mode == 'rasterproxy' or
                        self._base.getUserConfiguration.getValue(CCLONE_PATH)):  # if using the template 'CreateRasterProxy', keep only the .csv file.
                    try:
                        if (not base._isRasterProxyFormat('csv')):
                            os.remove(self._input)
                            os.remove('{}.aux.xml'.format(self._input))
                    except BaseException:
                        pass    # not an error
            else:
                with open(output.split(CloudOGTIFFExt)[0] if isCOGTIFF else output, 'w') as c:
                    c.write(_mrfBody)
                if (isCOGTIFF):
                    os.remove(output)
        except Exception as e:
            if (self._base):
                self._base.message('Updating ({}) was not successful!\nPlease make sure the input is (MRF) format.\n{}'.format(output, str(e)), self._base.const_critical_text)
            return False
        return True


class Report:
    CHEADER_PREFIX = '#'
    CJOB_EXT = '.orjob'
    CVSCHAR = '\t'
    CRPT_URL_TRUENAME = 'URL_NAME'
    CHDR_TEMPOUTPUT = CTEMPOUTPUT
    CHDR_CLOUDUPLOAD = 'cloudupload'
    CHDR_CLOUD_DWNLOAD = 'clouddownload'
    CHDR_MODE = 'mode'
    CHDR_OP = 'op'
    SnapshotDelay = 20  # Delay in secs before the partial status of the .orjob gets written to the local disk.

    def __init__(self, base):
        self._input_list = []
        self._input_list_info = {}
        self._input_list_info_ex = {}
        self._header = {
            'version': '{}/{}'.format(Application.__program_ver__, Application.__program_date__)
        }
        self._base = base
        self._isInputHTTP = False
        self._m_rasterAssociates = RasterAssociates()
        self._m_rasterAssociates.addRelatedExtensions('img;IMG', 'ige;IGE')  # To copy files required by raster formats to the primary raster copy location (-tempinput?) before any conversion could take place.
        self._m_rasterAssociates.addRelatedExtensions('ntf;NTF;tif;TIF', 'RPB;rpb')  # certain associated files need to be present alongside rasters for GDAL to work successfully.
        self._m_skipExtentions = ('til.ovr')   # status report for these extensions will be skipped. Case insensitive comparison.
        self._rptPreviousTime = datetime.now()

    def init(self, report_file, root=None):
        if (not self._base or
                not isinstance(self._base, Base)):
            return False
        if (not report_file):
            return False
        if (not report_file.lower().endswith(self.CJOB_EXT)):
            return False
        self._report_file = report_file
        if (root):
            f, e = os.path.splitext(root)
            _root = root.replace('\\', '/')
            if ((self._base.getUserConfiguration and
                 self._base.getUserConfiguration.getValue('Mode') == BundleMaker.CMODE) or
                root.lower().startswith('http://') or
                    root.lower().startswith('https://') or
                    len(e) != 0):
                self._input_list.append(_root)
                return True
            if (_root[-1:] != '/'):
                _root += '/'
            self._input_list.append(_root)          # first element in the report is the -input path to source
        return True

    @property
    def header(self):
        return self._header

    @header.setter
    def header(self, value):
        self._header = value

    def getRecordStatus(self, input, type):         # returns (true or false)
        if (input is None or
                type is None):
            return CRPT_UNDEFINED
        try:
            return (self._input_list_info[input][type.upper()])
        except BaseException:
            pass
        return CRPT_UNDEFINED

    @staticmethod
    def getUniqueFileName():
        _dt = datetime.now()
        _prefix = 'OR'
        _jobName = _prefix + "_%04d%02d%02dT%02d%02d%02d%06d" % (_dt.year, _dt.month, _dt.day,
                                                                 _dt.hour, _dt.minute, _dt.second, _dt.microsecond)
        return _jobName

    def _createSnapshot(self):  # take snapshot/updates .job file partially.
        rptCurrentTime = datetime.now()
        rptDuration = (rptCurrentTime - self._rptPreviousTime).total_seconds()
        if (rptDuration > self.SnapshotDelay):
            result = self.write()
            self._base.message('Orjob/Snapshot/Status>{}@{}'.format(str(result), str(str(datetime.utcnow()))))
            self._rptPreviousTime = rptCurrentTime

    def updateRecordStatus(self, input, type, value):  # input is the (src) path name which is case sensitive.
        if (input is None or
            type is None or
                value is None):
            return False
        self._createSnapshot()
        _input = input.strip().split('?')[0]
        if (_input.lower().endswith(self._m_skipExtentions)):
            return True     # not flagged as an err
        if (CTEMPINPUT in self._header):
            if (_input.startswith(self._header[CTEMPINPUT])):
                _input = _input.replace(self._header[CTEMPINPUT], self.root)
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
        if (CRESUME_HDR_OUTPUT in self._header and
                _path == self._header[CRESUME_HDR_OUTPUT]):
            _input = _input.replace(_path, self._header[CRESUME_HDR_INPUT])
        (p, e) = os.path.splitext(_input)
        while(e):
            _input = '{}{}'.format(p, e)
            if (_input in self._input_list_info):
                break
            (p, e) = os.path.splitext(p)
        _type = type.upper()
        if (_type not in [CRPT_COPIED, CRPT_PROCESSED, CRPT_UPLOADED]):
            self._base.message('Invalid type ({}) at (Reporter)'.format(type), self._base.const_critical_text)
            return False
        _value = value.lower()
        if (_value not in [CRPT_YES, CRPT_NO]):
            self._base.message('Invalid value ({}) at (Reporter)'.format(_value), self._base.const_critical_text)
            return False
        if (not e):
            if (_input in self._input_list_info and
                    self.CRPT_URL_TRUENAME in self._input_list_info[_input]):
                (p, e) = os.path.splitext(self._input_list_info[_input][self.CRPT_URL_TRUENAME])
            if (not e):  # still no extension?
                self._base.message('Invalid input/no extension for ({})/Reporter'.format(_input), self._base.const_warning_text)
                self._input_list_info[_input][_type] = _value
                return False
        self._input_list_info[_input][_type] = _value
        return True

    def addHeader(self, key, value):
        if (not key or
                value is None):
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
        _get_store = self.findWith(_file)
        if (_get_store and
                _get_store == _file):
            return False        # no duplicate entries allowed.
        self._input_list.append(_file)
        return True

    @property
    def items(self):
        return self._input_list

    @property
    def operation(self):
        Operation = COP.lower()
        if (Operation not in self._header):
            return None
        op = self._header[Operation]
        if (not op):
            return None
        if (op.lower().startswith(COP_LAMBDA)):
            return op   # lambda op values are case-sensitive.
        return op.lower()   # lower case values for all other operations.

    @property
    def root(self):
        if (not self._input_list):
            return ''
        _root = self._input_list[0]
        if (CRESUME_HDR_INPUT in self._header):
            _root = self._header[CRESUME_HDR_INPUT]
            if (_root.lower().startswith('http')):
                if (not _root.endswith('/')):
                    _root += '/'
        return _root

    def read(self, readCallback=None):
        try:
            with open(self._report_file, 'r') as _fptr:
                ln = _fptr.readline()
                hdr_skipped = False
                retryAll = False    # If 'resume=='retryall', files will be copied/processed/uploaded regardless of the individual file status.
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
                            _hdr_key = _hdr[0].strip()
                            _hdr.pop(0)
                            _hdr_val = '='.join(_hdr).strip()
                            if (_hdr_key == CTEMPINPUT or
                                    _hdr_key == CTEMPOUTPUT):
                                if (not _hdr_val.endswith('/')):
                                    _hdr_val += '/'
                            elif (_hdr_key == Lambda.queue_length):
                                if (not str.isdigit(_hdr_val)):
                                    ln = _fptr.readline()
                                    continue
                                _hdr_val = int(_hdr_val)    # filter {Lambda.queuelength}
                            elif (_hdr_key == self.CHDR_MODE):
                                _hdr_val = _hdr_val.lower()  # lower case (mode)
                            self.addHeader(_hdr_key, _hdr_val)
                            ln = _fptr.readline()
                            continue
                    if (not _fname or
                            not hdr_skipped):        # do not accept empty lines.
                        if (ln.find(CRPT_SOURCE) >= 0 and
                                ln.find(CRPT_COPIED)):      # skip line if it's the column header without the '#' prefix?
                            ln = _fptr.readline()
                        if (_fname):
                            hdr_skipped = True
                            if (CRESUME_HDR_INPUT in self._header):
                                _input = self._header[CRESUME_HDR_INPUT].lower()
                                if (_input.startswith('http://') or
                                        _input.startswith('https://')):
                                    self._isInputHTTP = True
                            if (not retryAll and
                                    CRESUME_ARG in self._header):
                                if (self._header[CRESUME_ARG].lower() == CRESUME_ARG_VAL_RETRYALL):
                                    retryAll = True
                        continue
                    _copied = '' if len(lns) <= 1 else lns[1].strip()       # for now, previously stored status values aren't used.
                    _processed = '' if len(lns) <= 2 else lns[2].strip()
                    _uploaded = '' if len(lns) <= 3 else lns[3].strip()
                    if (retryAll):
                        _copied = _processed = _uploaded = ''   # reset all status
                    if (self.addFile(_fname)):
                        self._input_list_info[_fname] = {
                            CRPT_COPIED: _copied,
                            CRPT_PROCESSED: _processed,
                            CRPT_UPLOADED: _uploaded
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
                makedirs(path)
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
            with open(self._report_file, 'w+') as _fptr:
                for key in self._header:
                    if (self.CHDR_OP == key):
                        # op==createjob header is not written out into the output .orjob file.
                        # This allows the .orjob file to be used with the -input arg to process the data separately.
                        if (self._header[key] == COP_CREATEJOB):
                            continue
                    _fptr.write('{} {}={}\n'.format(self.CHEADER_PREFIX, key, self._header[key]))
                _fptr.write(_frmt.format(CRPT_SOURCE, CRPT_COPIED, CRPT_PROCESSED, CRPT_UPLOADED))
                for f in self._input_list:
                    _fptr.write(_frmt.format(f,
                                             self._input_list_info[f][CRPT_COPIED] if f in self._input_list_info else '',
                                             self._input_list_info[f][CRPT_PROCESSED] if f in self._input_list_info else '',
                                             self._input_list_info[f][CRPT_UPLOADED] if f in self._input_list_info else ''
                                             ))
        except Exception as exp:
            self._base.message('{}'.format(str(exp)), self._base.const_critical_text)
            return False
        return True

    def writeTimeItReport(self, reportFile):
        import csv
        try:
            with open(reportFile, 'wb') as csvfile:
                fieldnames = [TimeIt.Name, TimeIt.Conversion, TimeIt.Overview, TimeIt.Download, TimeIt.Upload]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for f in self._base.timedInfo['files']:
                    writer.writerow(f)
        except Exception as e:
            self._base.message('TimeIt> {}'.format(str(e)), self._base.const_critical_text)
            return False
        return True

    def walk(self):
        walk_tree = []
        for f in self:
            (d, f) = os.path.split(f)
            walk_tree.append(('{}/'.format(d), (), (f.strip(),)))
        return walk_tree

    def __iter__(self):
        return iter(self._input_list)

    def syncRemoteToLocal(self, statusInfo):
        if (not statusInfo):
            return False
        InputListInfo = 'input_list_info'
        if (InputListInfo not in statusInfo):
            return False
        for entry in statusInfo[InputListInfo]:
            status = statusInfo[InputListInfo][entry]
            for _type in status:
                if (not status[_type]):
                    continue
                self.updateRecordStatus(entry, _type, status[_type])
        return True

    def addMetadata(self, file, key, value):
        if (file is None or
            key is None or
                value is None):
            self._base.message('addMetadata/null', self._base.const_critical_text)
            return False
        _file = file.replace('\\', '/')
        srchIndex = -1
        try:
            srchIndex = list(self._input_list_info.keys()).index(_file)
        except Exception as e:
            return False
        if (srchIndex not in self._input_list_info_ex):
            self._input_list_info_ex[srchIndex] = {}
        self._input_list_info_ex[srchIndex][key] = value
        return True

    def getMetadata(self, file, key):
        if (file is None or
                key is None):
            self._base.message('getMetadata/null', self._base.const_critical_text)
            return None
        _file = file.replace('\\', '/')
        srchIndex = -1
        try:
            srchIndex = list(self._input_list_info.keys()).index(_file)
        except Exception as e:
            return None
        if (key not in self._input_list_info_ex[srchIndex]):
            return None
        return self._input_list_info_ex[srchIndex][key]

    def __len__(self):
        return len(self._input_list)

    def __getitem__(self, index):
        return self._input_list[index]

# class to read/gather info on til files.


class TIL:
    CRELATED_FILE_COUNT = 'related_file_count'
    CPROCESSED_FILE_COUNT = 'processed_file_count'
    CKEY_FILES = 'files'
    CRASTER_EXT_IN_TIL = 'rasterExtension'

    def __init__(self):
        self._rasters = []
        self._tils = []
        self._tils_info = {}
        self._output_path = {}
        self._defaultTILProcessing = False

    @property
    def defaultTILProcessing(self):
        return self._defaultTILProcessing

    @defaultTILProcessing.setter
    def defaultTILProcessing(self, value):
        self._defaultTILProcessing = value

    @property
    def TILCount(self):
        return len(self._tils)

    def addTIL(self, input):        # add (til) files to process later via (fnc: process).
                                    # This when the (til) files are found before the associated (files) could be not found at the (til) location because they may not have been downloaded yet.
        _input = input.replace('\\', '/')
        if (_input not in self._tils):
            self._tils.append(_input)
        if (not input.lower() in self._tils_info):
            self._tils_info[_input.lower()] = {
                self.CRELATED_FILE_COUNT: 0,
                self.CPROCESSED_FILE_COUNT: 0,
                self.CKEY_FILES: [],
                self.CRASTER_EXT_IN_TIL: None
            }
        return True

    def findOriginalSourcePath(self, processPath):
        for path in self._output_path:
            if (self._output_path[path] == processPath):
                return path
        return None

    def fileTILRelated(self, input):
        idx = input.split('.')
        f = idx[0]
        f = f.replace('\\', '/').split('/')
        f = f[len(f) - 1]
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

    def _processContent(self, fileName, line):
        if (not line or
                not fileName):
            return False
        _line = line
        ln = _line.strip()
        CBREAK = 'filename ='
        if (ln.find(CBREAK) == -1):
            return True
        splt = ln.replace('"', '').replace(';', '').split(CBREAK)
        if (len(splt) == 2):
            file_name = splt[1].strip()
            if (file_name not in self._rasters):
                self._rasters.append(file_name)
                _key_til_info = fileName.lower()
                if (not self._tils_info[_key_til_info][self.CRASTER_EXT_IN_TIL]):
                    rasterExtension = RasterAssociates.findExtension(file_name)
                    if (rasterExtension):
                        self._tils_info[_key_til_info][self.CRASTER_EXT_IN_TIL] = rasterExtension
                if (_key_til_info in self._tils_info):
                    self._tils_info[_key_til_info][self.CRELATED_FILE_COUNT] += 1
                    self._tils_info[_key_til_info][self.CKEY_FILES].append(file_name)
        return True

    def processInMemoryTILContent(self, fileName, content):
        if (content is None):
            return False
        lines = content.split('\n')
        for line in lines:
            self._processContent(fileName, line)
        return True

    def process(self, input):
        if (not input or
                len(input) == 0):
            return False
        if (not os.path.exists(input)):
            return False
        with open(input, 'r') as _fp:
            _line = _fp.readline()
            while (_line):
                self._processContent(input, _line)
                _line = _fp.readline()
        return True

    def setOutputPath(self, input, output):  # set the output path for each til entry on list.
        if (input not in self._output_path):
            self._output_path[input] = output

    def getOutputPath(self, input):
        if (input not in self._output_path):
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


class ProgressPercentage(object):

    def __init__(self, base, filename):
        self._base = base       # base
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            message = "%s / %d  (%.2f%%)" % (
                self._seen_so_far, self._size,
                percentage)
            if (self._base is not None):
                if (hasattr(self._base, 'message')):
                    self._base.message(message, self._base.const_general_text)
                    return True
            sys.stdout.write(message)
            sys.stdout.flush()


class S3Upload:

    def __init__(self, base, s3_bucket, s3_path, local_file, acl_policy='private'):
        self._base = base       # base
        self.m_s3_path = s3_path
        self.m_local_file = local_file
        self.m_s3_bucket = s3_bucket
        self.m_acl_policy = 'private' if acl_policy is None or acl_policy.strip() == '' else acl_policy
        self.mp = None
        pass

    def init(self):
        try:
            from boto3.s3.transfer import S3Transfer, TransferConfig
            self.mp = S3Transfer(self.m_s3_bucket.meta.client)
        except Exception as e:
            self._base.message('({})'.format(str(e)), self._base.const_critical_text)
            return False
        return True

    @TimeIt.timeOperation
    def upload(self, **kwargs):
        # if (self.m_local_file.endswith('.lrc')):        # debug. Must be removed before release.
        # return True                                 # "
        self._base.message('[S3-Push] {}'.format(self.m_local_file))
        try:
            self.mp.upload_file(self.m_local_file, self.m_s3_bucket.name, self.m_s3_path, extra_args={'ACL': self.m_acl_policy}, callback=ProgressPercentage(self._base, self.m_local_file))
        except Exception as e:  # trap any connection issues.
            self._base.message('({})'.format(str(e)), self._base.const_critical_text)
            return False
        return True

    def __del__(self):
        if (self.mp):
            self.mp = None


class SlnTMStringIO:

    def __init__(self, size, buf=''):
        self.m_size = size
        self.m_buff = mmap.mmap(-1, self.m_size)
        self.m_spos = self.m_fsize = 0

    def close(self):
        self.m_buff.close()
        del self.m_buff
        pass

    def next(self):
        pass

    def seek(self, pos, mode=0):
        if mode == 1:
            pos += self.m_spos
        elif mode == 2:
            pos += len(self.m_buff)
        self.m_spos = max(0, pos)

    def tell(self):
        return self.m_spos

    def read(self, n=-1):
        buff_len = self.m_fsize
        nRead = (self.m_spos + n)
        if (nRead > buff_len):
            n = n - (nRead - buff_len)
        self.m_buff.seek(self.m_spos, 0)
        self.m_spos += n
        return self.m_buff.read(n)

    def readline(self, length=None):
        pass

    def readlines(self, sizehint=0):
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
    # cloud-type
    TypeAmazon = 'amazon'
    TypeAzure = 'azure'
    TypeGoogle = 'google'
    TypeAlibaba = 'alibaba'
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

    def upload(self, file_path, container_name, parent_folder, properties=None):
        self._input_file_path = file_path
        self._upl_container_name = container_name
        self._upl_parent_folder = parent_folder
        self._upl_properties = properties
        return True

    def setSource(self, container_name, parent_folder, properties=None):
        self._dn_container_name = container_name
        self._dn_parent_folder = parent_folder
        self._dn_properties = properties
        return True

    def readProfile(self, account_name, account_key):
        config = ConfigParser.RawConfigParser()
        userHome = '{}/{}/{}'.format(os.path.expanduser('~').replace('\\', '/'), '.OptimizeRasters/Microsoft', 'azure_credentials')
        with open(userHome) as fptr:
            config.readfp(fptr)
        if (not config.has_section(self._profile_name)):
            return (None, None)
        azure_account_name = config.get(self._profile_name, account_name) if config.has_option(self._profile_name, account_name) else None
        azure_account_key = config.get(self._profile_name, account_key) if config.has_option(self._profile_name, account_key) else None
        return (azure_account_name, azure_account_key)

    def message(self, msg, status=0):     # type (0: general, 1: warning, 2: critical, 3: statusText)
        if (self._base):
            self._base.message(msg, status)
            return
        status_text = ''
        if (status == 1):
            status_text = 'Warning'
        elif (status == 2):
            status_text = 'Err'
        print ('{}{}{}'.format(status_text, '. ' if status_text else '', msg))


class Google(Store):
    DafaultStorageDomain = 'http://storage.googleapis.com/'

    def __init__(self, project_name, client_id, client_secret, profile_name=None, base=None):
        super(Google, self).__init__(client_id, client_secret, profile_name, base)
        self._browsecontent = []
        self._projectName = project_name
        self._client = None
        self._bucket = None

    def init(self, bucketName):
        try:
            if (self._profile_name is None or
                    not bucketName):
                return False
            if (not self._projectName):
                with open(self._profile_name, 'r') as reader:
                    serviceJson = json.load(reader)
                    Project_Id = 'project_id'
                    if (Project_Id not in serviceJson):
                        raise Exception('(Project_Id) key isn\'t found in file ({})'.format(self._profile_name))
                self._projectName = serviceJson[Project_Id]
            os.environ['GCLOUD_PROJECT'] = self._projectName
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self._profile_name
            from google.cloud import storage
            self._client = storage.Client()
            self._bucket = self._client.lookup_bucket(bucketName)
            if (self._bucket is None):
                raise Exception('Bucket ({}) isn\'t found!'.format(bucketName))
        except Exception as e:
            self.message(str(e), self.const_critical_text)
            return False
        return True

    @property
    def id(self):
        return 'gs'  # short for google-storage

    def _addBrowseContent(self, blobName):
        if (not blobName):
            return False
        if (self._mode == self.CMODE_SCAN_ONLY):
            self._browsecontent.append(blobName)
            return True
        return False

    def getBrowseContent(self):
        return self._browsecontent

    def browseContent(self, bucketName, parentFolder, cb=None, precb=None):
        url = parentFolder
        if (url == '/' or
                url is None):
            url = ''    # defaults to bucket root.
        super(Google, self).setSource(bucketName, url)
        for item in self._bucket.list_blobs(prefix=url, delimiter='/{}'.format('*' if self._include_subFolders else '')):
            self._addBrowseContent(item.name)
            if (precb and
                    self._base.getUserConfiguration):
                _resumeReporter = self._base.getUserConfiguration.getValue(CPRT_HANDLER)
                if (_resumeReporter):
                    remotePath = _resumeReporter._header[CRESUME_HDR_INPUT]
                    precb(item.name if remotePath == '/' else item.name.replace(remotePath, ''), remotePath, _resumeReporter._header[CRESUME_HDR_OUTPUT])
            if (cb and
                    self._mode != self.CMODE_SCAN_ONLY):
                cb(item.name)
        return True

    def copyToLocal(self, blob_source):
        if (not blob_source or
                self._dn_parent_folder is None):    # note> empty value in parent path is allowed but not (None)
            self.message('{}> Not initialized'.format(self.id), self.const_critical_text)
            return False
        try:
            _user_config = self._base.getUserConfiguration
            _resumeReporter = _user_config.getValue(CPRT_HANDLER)
            # what does the restore point say about the (blob_source) status?
            if (_resumeReporter):
                if (blob_source not in _resumeReporter._input_list_info):   # if -subs=true but not on .orjob/internal list, bail out early
                    return True
                if (blob_source.endswith('/')):  # skip folders.
                    return True
                _get_rstr_val = _resumeReporter.getRecordStatus(blob_source, CRPT_COPIED)
                if (_get_rstr_val == CRPT_YES):
                    self.message('{} {}'.format(CRESUME_MSG_PREFIX, blob_source))
                    return True
            # ends
            _googleParentFolder = _user_config.getValue(CIN_GOOGLE_PARENTFOLDER, False)
            _googlePath = blob_source if _googleParentFolder == '/' else blob_source.replace(_googleParentFolder, '')
            output_path = _user_config.getValue(CCFG_PRIVATE_OUTPUT, False) + _googlePath
            isUpload = self._base.getBooleanValue(_user_config.getValue(CCLOUD_UPLOAD))
            if (_user_config.getValue(CISTEMPOUTPUT) and
                    isUpload):
                output_path = _user_config.getValue(CTEMPOUTPUT, False) + _googlePath
            if (not output_path):
                return False
            is_raster = False
            is_tmp_input = self._base.getBooleanValue(_user_config.getValue(CISTEMPINPUT))
            primaryRaster = None
            if (_resumeReporter and
                    is_tmp_input):
                primaryRaster = _resumeReporter._m_rasterAssociates.findPrimaryExtension(_googlePath)
            if (filterPaths(blob_source, _user_config.getValue(CCFG_EXCLUDE_NODE))):
                return False
            elif (primaryRaster or  # if the blob_source is an associated raster file, consider it as a raster.
                  filterPaths(blob_source, _user_config.getValue(CCFG_RASTERS_NODE))):
                isTIL = output_path.lower().endswith(CTIL_EXTENSION_)
                if (is_tmp_input):
                    if (not isTIL):
                        output_path = _user_config.getValue(CTEMPINPUT, False) + _googlePath
                is_raster = not isTIL
            if (_user_config.getValue('Pyramids') == CCMD_PYRAMIDS_ONLY):
                return False
            flr = os.path.dirname(output_path)
            if (not os.path.exists(flr)):
                try:
                    makedirs(flr)
                except Exception as e:
                    raise
            if (is_raster):
                if (not is_tmp_input):
                    return True
            writeTo = output_path
            self.message('[{}-Pull] {}'.format(self.id, blob_source))
            if (not is_raster):
                writeTo = self._base.renameMetaFileToMatchRasterExtension(writeTo)
            blob = self._bucket.get_blob(blob_source)
            blob.download_to_filename(writeTo)
            if (self._event_postCopyToLocal):
                self._event_postCopyToLocal(writeTo)
            # take care of (til) inputs.
            if (til):
                if (writeTo.lower().endswith(CTIL_EXTENSION_)):
                    if (til.addTIL(writeTo)):
                        til.setOutputPath(writeTo, writeTo)
            # ends
            # mark download/copy status
            if (_resumeReporter):
                _resumeReporter.updateRecordStatus(blob_source, CRPT_COPIED, CRPT_YES)
            # ends
            # copy metadata files to -clonepath if set
            if (not is_raster):  # do not copy raster associated files to clone path.
                self._base.copyMetadataToClonePath(output_path)
            # ends
            # Handle any post-processing, if the final destination is to S3, upload right away.
            if (isUpload):
                if (self._base.getBooleanValue(_user_config.getValue(CISTEMPINPUT))):
                    if (is_raster):
                        return True
                _is_success = self._base.S3Upl(writeTo, user_args_Callback)
                if (not _is_success):
                    return False
            # ends
        except Exception as e:
            self.message('({})'.format(str(e)), self.const_critical_text)
            if (_resumeReporter):
                _resumeReporter.updateRecordStatus(blob_source, CRPT_COPIED, CRPT_NO)
            return False
        return True

    def upload(self, input_path, container_name, parent_folder, properties=None):
        if (not input_path or
            not container_name or
                parent_folder is None):
            return False
        _parent_folder = parent_folder
        if (not _parent_folder):
            if (self._base.getUserConfiguration):
                _parent_folder = self._base.getUserConfiguration.getValue(CIN_GOOGLE_PARENTFOLDER)
        if (_parent_folder == '/' or
                _parent_folder is None):
            _parent_folder = ''
        if (properties):
            if (CTEMPOUTPUT in properties):
                _tempoutput = properties[CTEMPOUTPUT]
                _parent_folder = os.path.dirname(input_path.replace('\\', '/').replace(_tempoutput, _parent_folder))
        usrPath = self._base.getUserConfiguration.getValue(CUSR_TEXT_IN_PATH, False)
        usrPathPos = CHASH_DEF_INSERT_POS  # default insert pos (sub-folder loc) for user text in output path
        if (usrPath):
            (usrPath, usrPathPos) = usrPath.split(CHASH_DEF_SPLIT_CHAR)
            _parent_folder = self._base.insertUserTextToOutputPath('{}{}'.format(_parent_folder, '/' if not _parent_folder.endswith('/') else ''), usrPath, usrPathPos)
        super(Google, self).upload(input_path, container_name, _parent_folder, properties)
        localPath = self._input_file_path
        cloudPath = self._base.convertToForwardSlash(os.path.join(self._upl_parent_folder, os.path.basename(localPath)), False)
        try:
            self.message('[{}-Push] {}'.format(self.id, cloudPath))
            from google.cloud import storage
            client = storage.Client()   # has to use a new client,bucket object/upload_from_filename api has issues in a threaded environment.
            bucket = client.get_bucket(self._bucket.name)
            blob = bucket.blob(cloudPath)
            blob.upload_from_filename(localPath)
        except Exception as e:
            self.message(str(e), self.const_critical_text)
            return False
        return True


class Azure(Store):
    CHUNK_MIN_SIZE = 4 * 1024 * 1024
    COUT_AZURE_ACCOUNTNAME_INFILE = 'azure_account_name'
    COUT_AZURE_ACCOUNTKEY_INFILE = 'azure_account_key'
    DefaultDomain = 'blob.core.windows.net'

    def __init__(self, account_name, account_key, profile_name=None, base=None):
        super(Azure, self).__init__(account_name, account_key, profile_name, base)
        self._browsecontent = []

    def init(self, direction=CS3STORAGE_IN):
        try:
            if (not self._account_name):
                (self._account_name, self._account_key) = self.readProfile(self.COUT_AZURE_ACCOUNTNAME_INFILE, self.COUT_AZURE_ACCOUNTKEY_INFILE)
            self._SASToken = self._SASBucket = None
            if (self._account_name and
                    self._account_name.lower().startswith('http')):
                breakSAS = self._account_name.split('?')
                if (len(breakSAS) == 2):
                    self._SASToken = breakSAS[-1]
                    self._SASBucket = breakSAS[0].split('/')[-1]  # get bucket name from the SAS string.
                if (self._base):
                    self._base.getUserConfiguration.setValue(CFGAZSAS if direction == CS3STORAGE_IN else CFGAZSASW, self._SASToken)
                self._account_name = self._account_name.split('.')[0].split('//')[1]
            if (not self._account_name and
                    not self._SASToken):
                return False
            from azure.storage.blob import BlockBlobService, BlobPermissions
            self._blob_service = BlockBlobService(account_name=self._account_name, account_key=self._account_key, sas_token=self._SASToken)
            ACL = None
            try:
                ACL = self._blob_service.get_container_acl(self._base.getUserConfiguration.getValue(CIN_AZURE_CONTAINER if direction == CS3STORAGE_IN else COUT_AZURE_CONTAINER))
            except Exception as e:
                pass
            if (ACL is None or
                    ACL.public_access is None):  # internally access rights get checked on the input/output containers.
                if (self._base):
                    if (direction == CS3STORAGE_IN):
                        self._base.getUserConfiguration.setValue(UseToken, True if self._base.getBooleanValue(self._base.getUserConfiguration.getValue(UseToken)) else False)
                    else:
                        self._base.getUserConfiguration.setValue(UseTokenOnOuput, True)
            if (ACL is not None and
                    self._base):
                if (ACL.public_access is None or
                        self._base.getBooleanValue(self._base.getUserConfiguration.getValue(UseToken if direction == CS3STORAGE_IN else UseTokenOnOuput))):
                    os.environ['AZURE_STORAGE_ACCOUNT'] = self._account_name
                    os.environ['AZURE_STORAGE_ACCESS_KEY'] = self._account_key
        except Exception as e:
            self.message(str(e), self.const_critical_text)
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
            self._browsecontent.append(blobName)
            return True
        return False

    def getBrowseContent(self):
        return self._browsecontent

    def browseContent(self, container_name, parent_folder, cb=None, precb=None):
        super(Azure, self).setSource(container_name, parent_folder)
        blobs = []
        marker = None
        while (True):
            try:
                batch = self._blob_service.list_blobs(self._dn_container_name, marker=marker, prefix=parent_folder)
                blobs.extend(batch)
                if not batch.next_marker:
                    break
                marker = batch.next_marker
            except BaseException:
                self._base.message('Unable to read from ({}). Check container name ({})/credentials.'.format(CCLOUD_AZURE.capitalize(),
                                                                                                             self._dn_container_name),
                                   self._base.const_critical_text)
                return False
        for blob in blobs:
            levels = blob.name.split('/')
            if (not self._include_subFolders):
                if (len(levels) > 2):
                    continue
            blob_name = blob.name
            self._addBrowseContent(blob_name)
            if (precb and
                    self._base.getUserConfiguration):
                _resumeReporter = self._base.getUserConfiguration.getValue(CPRT_HANDLER)
                if (_resumeReporter):
                    remotePath = _resumeReporter._header[CRESUME_HDR_INPUT]
                    precb(blob_name if remotePath == '/' else blob_name.replace(remotePath, ''), remotePath, _resumeReporter._header[CRESUME_HDR_OUTPUT])
            if (cb and
                    self._mode != self.CMODE_SCAN_ONLY):
                cb(blob_name)
        return True

    @TimeIt.timeOperation
    def __copyRemoteToLocal(self, blob_source, writeTo, **kwargs):
        try:
            self._blob_service.get_blob_to_path(self._dn_container_name, blob_source, writeTo)
        except Exception as e:
            self._base.message('({})'.format(str(e)), self._base.const_critical_text)
            _resumeReporter = self._base.getUserConfiguration.getValue(CPRT_HANDLER)
            if (_resumeReporter):
                _resumeReporter.updateRecordStatus(blob_source, CRPT_COPIED, CRPT_NO)
            return False
        return True

    def copyToLocal(self, blob_source, **kwargs):
        try:
            if (not blob_source):
                return False
            _user_config = self._base.getUserConfiguration
            _resumeReporter = _user_config.getValue(CPRT_HANDLER)
            # what does the restore point say about the (blob_source) status?
            if (_resumeReporter):
                if (blob_source not in _resumeReporter._input_list_info):   # if -subs=true but not on .orjob/internal list, bail out early
                    return True
                _get_rstr_val = _resumeReporter.getRecordStatus(blob_source, CRPT_COPIED)
                if (_get_rstr_val == CRPT_YES):
                    self._base.message('{} {}'.format(CRESUME_MSG_PREFIX, blob_source))
                    return True
            # ends
            _azureParentFolder = _user_config.getValue(CIN_AZURE_PARENTFOLDER, False)
            _azurePath = blob_source if _azureParentFolder == '/' else blob_source.replace(_azureParentFolder, '')
            output_path = _user_config.getValue(CCFG_PRIVATE_OUTPUT, False) + _azurePath
            isUpload = self._base.getBooleanValue(_user_config.getValue(CCLOUD_UPLOAD))
            if (_user_config.getValue(CISTEMPOUTPUT) and
                    isUpload):
                output_path = _user_config.getValue(CTEMPOUTPUT, False) + _azurePath
            is_raster = False
            is_tmp_input = self._base.getBooleanValue(_user_config.getValue(CISTEMPINPUT))
            primaryRaster = None
            if (_resumeReporter and
                    is_tmp_input):
                primaryRaster = _resumeReporter._m_rasterAssociates.findPrimaryExtension(_azurePath)
            if (filterPaths(blob_source, _user_config.getValue(CCFG_EXCLUDE_NODE))):
                return False
            elif (primaryRaster or  # if the blob_source is an associated raster file, consider it as a raster.
                  filterPaths(blob_source, _user_config.getValue(CCFG_RASTERS_NODE))):
                isTIL = output_path.lower().endswith(CTIL_EXTENSION_)
                if (is_tmp_input):
                    if (not isTIL):
                        output_path = _user_config.getValue(CTEMPINPUT, False) + _azurePath
                is_raster = not isTIL
            if (_user_config.getValue('Pyramids') == CCMD_PYRAMIDS_ONLY):
                return False
            if (not blob_source or
                not output_path or
                    not self._dn_parent_folder):
                self._base.message('Azure> Not initialized', self._base.const_critical_text)
                return False
            flr = os.path.dirname(output_path)
            if (not os.path.exists(flr)):
                try:
                    makedirs(flr)
                except Exception as e:
                    raise
            if (is_raster):
                if (not is_tmp_input):
                    return True
            writeTo = output_path
            self._base.message('[Azure-Pull] {}'.format(blob_source))
            if (not is_raster):
                writeTo = self._base.renameMetaFileToMatchRasterExtension(writeTo)
            result = self.__copyRemoteToLocal(blob_source, writeTo, name=blob_source, method=TimeIt.Download, store=self._base)
            if (not result):
                return False
            if (self._event_postCopyToLocal):
                self._event_postCopyToLocal(writeTo)
            # take care of (til) inputs.
            if (til):
                if (writeTo.lower().endswith(CTIL_EXTENSION_)):
                    if (til.addTIL(writeTo)):
                        til.setOutputPath(writeTo, writeTo)
            # ends
            # mark download/copy status
            if (_resumeReporter):
                _resumeReporter.updateRecordStatus(blob_source, CRPT_COPIED, CRPT_YES)
            # ends
            # copy metadata files to -clonepath if set
            if (not is_raster):  # do not copy raster associated files to clone path.
                self._base.copyMetadataToClonePath(output_path)
            # ends
            # Handle any post-processing, if the final destination is to S3, upload right away.
            if (isUpload):
                if (getBooleanValue(_user_config.getValue(CISTEMPINPUT))):
                    if (is_raster):
                        return True
                _is_success = self._base.S3Upl(writeTo, user_args_Callback)
                if (not _is_success):
                    return False
            # ends
        except Exception as e:
            self._base.message('({})'.format(str(e)), self._base.const_critical_text)
            if (_resumeReporter):
                _resumeReporter.updateRecordStatus(blob_source, CRPT_COPIED, CRPT_NO)
            return False
        return True

    @TimeIt.timeOperation
    def upload(self, input_path, container_name, parent_folder, properties=None, **kwargs):
        if (not input_path or
            not container_name or
                parent_folder is None):
            return False
        _parent_folder = parent_folder
        if (not _parent_folder):
            if (self._base.getUserConfiguration):
                _parent_folder = self._base.getUserConfiguration.getValue(CIN_AZURE_PARENTFOLDER)
        if (_parent_folder == '/' or
                _parent_folder is None):
            _parent_folder = ''
        if (properties):
            if (CTEMPOUTPUT in properties):
                _tempoutput = properties[CTEMPOUTPUT]
                _parent_folder = os.path.dirname(input_path.replace('\\', '/').replace(_tempoutput, _parent_folder))
        usrPath = self._base.getUserConfiguration.getValue(CUSR_TEXT_IN_PATH, False)
        usrPathPos = CHASH_DEF_INSERT_POS  # default insert pos (sub-folder loc) for user text in output path
        if (usrPath):
            (usrPath, usrPathPos) = usrPath.split(CHASH_DEF_SPLIT_CHAR)
            _parent_folder = self._base.insertUserTextToOutputPath('{}{}'.format(_parent_folder, '/' if not _parent_folder.endswith('/') else ''), usrPath, usrPathPos)
        super(Azure, self).upload(input_path, container_name, _parent_folder, properties)
        blob_path = self._input_file_path
        blob_name = os.path.join(self._upl_parent_folder, os.path.basename(blob_path))
        # if (blob_name.endswith('.lrc')):         # debug. Must be removed before release.
        #   return True                          #  "
        # return True     # debug. Must be removed before release.
        isContainerCreated = False
        t0 = datetime.now()
        time_to_wait_before_retry = 3
        max_time_to_wait = 60
        self.message('Accessing container ({})..'.format(self._upl_container_name))
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
                tm_pre = datetime.now()
                while(True):
                    time_delta = datetime.now() - tm_pre
                    if (time_delta.seconds > time_to_wait_before_retry):
                        break
                t1 = datetime.now() - t0
                if (t1.seconds > max_time_to_wait):
                    self.message('Timed out to create container.', self.const_critical_text)
                    break
        if (not isContainerCreated):
            self.message('Unable to create the container ({})'.format(self._upl_container_name), self.const_critical_text)
            exit(1)
        self.message('Done.')
        f = None
        try:         # see if we can open it
            f = open(blob_path, 'rb')
            f_size = os.path.getsize(blob_path)
        except Exception as e:
            self.message('File open/upload: ({})'.format(str(e)), self.const_critical_text)
            if (f):
                f.close()
            return False
        threads = []
        block_ids = []
        pos_buffer = upl_blocks = 0
        len_buffer = CCLOUD_UPLOAD_THREADS     # set this to no of parallel (chunk) uploads at once.
        tot_blocks = int((f_size / Azure.CHUNK_MIN_SIZE) + 1)
        idx = 1
        self.message('Uploading ({})'.format(blob_path))
        self.message('Total blocks to upload ({})'.format(tot_blocks))
        st = datetime.now()
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
                if (not chunk):
                    break
                buffer.append(SlnTMStringIO(len(chunk)))
                buffer[len(buffer) - 1].write(chunk)
            if (len(buffer) == 0 and
                    len(threads) == 0):
                break
            for e in buffer:
                try:
                    block_id = base64.b64encode(b'%06d' % idx).decode('utf-8')
                    self.message('Adding block-id ({}/{})'.format(idx, tot_blocks))
                    t = threading.Thread(target=self._runBlock,
                                         args=(self._blob_service, e, self._upl_container_name, blob_name, block_id))
                    t.daemon = True
                    t.start()
                    threads.append(t)
                    block_ids.append(block_id)
                    idx += 1
                except Exception as e:
                    self.message(str(e), self.const_critical_text)
                    if (f):
                        f.close()
                    return False
        try:
            self.message('Finalizing uploads..')
            from azure.storage.blob.models import BlobBlock, BlobBlockList
            blockList = BlobBlockList().uncommitted_blocks
            [blockList.append(BlobBlock(id=block)) for block in block_ids]
            ret = self._blob_service.put_block_list(self._upl_container_name, blob_name, blockList)
        except Exception as e:
            Message(str(e), self.const_critical_text)
            return False
        finally:
            if (f):
                f.close()
        self.message('Duration. ({} sec)'.format((datetime.now() - st).seconds))
        self.message('Done.')
        return True


class S3Storage:
    RoleAccessKeyId = 'AccessKeyId'
    RoleSecretAccessKey = 'SecretAccessKey'
    RoleToken = 'Token'

    def __init__(self, base):
        self._base = base
        self._isBucketPublic = False
        self._isRequesterPay = False
        self._isNoAccessToListBuckets = False

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
            if (self.m_user_config.getValue(CCFG_PRIVATE_INC_BOTO)):    # return type is a boolean hence no need to explicitly convert.
                try:
                    awsSessionToken = None
                    sessionProfile = _profile_name
                    if (_profile_name and
                            _profile_name.lower().startswith('using_')):
                        roleInfo = self.getIamRoleInfo()
                        if (roleInfo is None):
                            return False
                        sessionProfile = None
                        self.CAWS_ACCESS_KEY_ID = roleInfo[self.RoleAccessKeyId]
                        self.CAWS_ACCESS_KEY_SECRET = roleInfo[self.RoleSecretAccessKey]
                        awsSessionToken = roleInfo[self.RoleToken]
                        # let's initialize the AWS env variables to allow GDAL to work when invoked externally.
                        os.environ['AWS_ACCESS_KEY_ID'] = self.CAWS_ACCESS_KEY_ID
                        os.environ['AWS_SECRET_ACCESS_KEY'] = self.CAWS_ACCESS_KEY_SECRET
                        os.environ['AWS_SESSION_TOKEN'] = awsSessionToken
                        # ends
                    self._isBucketPublic = self.CAWS_ACCESS_KEY_ID is None and self.CAWS_ACCESS_KEY_SECRET is None and _profile_name is None
                    session = boto3.Session(self.CAWS_ACCESS_KEY_ID if not sessionProfile else None, self.CAWS_ACCESS_KEY_SECRET if not sessionProfile else None,
                                            profile_name=_profile_name if not awsSessionToken else None, aws_session_token=awsSessionToken if awsSessionToken else None)
                    endpointURL = None
                    AWSEndpointURL = 'aws_endpoint_url'
                    SessionProfile = 'profiles'
                    if (_profile_name and
                        SessionProfile in session._session.full_config and
                        _profile_name in session._session.full_config[SessionProfile] and
                            AWSEndpointURL in session._session.full_config[SessionProfile][_profile_name]):
                        endpointURL = session._session.full_config[SessionProfile][_profile_name][AWSEndpointURL]
                        self._base.message('Using {} endpoint> {}'.format('output' if direction == CS3STORAGE_OUT else 'input', endpointURL))
                        self.CAWS_ACCESS_KEY_ID = session.get_credentials().access_key  # initialize access_key, secret_key using the profile.
                        self.CAWS_ACCESS_KEY_SECRET = session.get_credentials().secret_key
                    import botocore
                    useAlibaba = endpointURL and endpointURL.lower().find(SigAlibaba) != -1
                    if (self._base.getUserConfiguration.getValue(UseToken)):
                        os.environ['OSS_ACCESS_KEY_ID'] = session.get_credentials().access_key
                        os.environ['OSS_SECRET_ACCESS_KEY'] = session.get_credentials().secret_key
                    self.m_user_config.setValue('{}oss'.format('in' if direction == CS3STORAGE_IN else 'out'), useAlibaba)
                    bucketCon = session.client('s3')
                    region = DefS3Region
                    try:
                        loc = bucketCon.get_bucket_location(Bucket=self.m_bucketname)['LocationConstraint']
                        if (loc):
                            region = loc
                    except Exception as e:
                        self._base.message('get/bucket/region ({})'.format(str(e)), self._base.const_warning_text)
                    self.con = session.resource('s3', region, endpoint_url=endpointURL if endpointURL else None, config=botocore.config.Config(s3={'addressing_style': 'virtual' if useAlibaba else 'path'}))
                    if (self._isBucketPublic):
                        self.con.meta.client.meta.events.register('choose-signer.s3.*', botocore.handlers.disable_signing)
                except Exception as e:
                    self._base.message(str(e), self._base.const_critical_text)
                    return False
                try:
                    self.bucketupload = self.con.Bucket(self.m_bucketname)
                    self.con.meta.client.head_bucket(Bucket=self.m_bucketname)
                except botocore.exceptions.ClientError as e:
                    if (int(e.response['Error']['Code']) == 403):
                        try:
                            fetchMeta = self.con.meta.client.head_object(Bucket=self.m_bucketname, RequestPayer='requester', Key='_*CHS')
                        except Exception as e:
                            if (int(e.response['Error']['Code']) == 404):
                                self._isRequesterPay = True
                                os.environ['AWS_REQUEST_PAYER'] = 'requester'
                                self._base.getUserConfiguration.setValue(UseToken, True)    # overrides, the cmd-line -usetoken plus the <UseToken> node value in the parameter file.
                            elif(int(e.response['Error']['Code']) == 403):
                                self._isNoAccessToListBuckets = True
                    if (not self._isRequesterPay and
                            not self._isNoAccessToListBuckets):
                        self._base.message('Invalid {} S3 bucket ({})/credentials.'.format(
                            CRESUME_HDR_OUTPUT if direction == CS3STORAGE_OUT else CRESUME_HDR_INPUT,
                            self.m_bucketname),
                            self._base.const_critical_text)
                        return False
                    os.environ['AWS_ACCESS_KEY_ID'] = session.get_credentials().access_key
                    os.environ['AWS_SECRET_ACCESS_KEY'] = session.get_credentials().secret_key
                except Exception as e:
                    self._base.message(str(e), self._base.const_critical_text)
                    return False
        _remote_path = remote_path
        if (_remote_path and
                os.path.isfile(_remote_path)):      # are we reading a file list?
            self._input_flist = _remote_path
            try:
                global _rpt
                _remote_path = _rpt.root
            except Exception as e:
                self._base.message('Report ({})'.format(str(e)), self._base.const_critical_text)
                return False
        self.remote_path = self._base.convertToForwardSlash(_remote_path)
        if (not self.remote_path):
            self.remote_path = ''   # defaults to bucket root.
        return True

    def getIamRoleInfo(self):
        roleMetaUrl = 'http://169.254.169.254/latest/meta-data/iam/security-credentials'
        urlResponse = None
        try:
            urlResponse = urlopen(roleMetaUrl)
            IamRole = urlResponse.read().decode('utf-8')
            if (IamRole.find('404') != -1):
                return None
            urlResponse.close()
            urlResponse = urlopen('{}/{}'.format(roleMetaUrl, IamRole))
            roleInfo = json.loads(urlResponse.read())
        except Exception as e:
            self._base.message('IAM Role not found.\n{}'.format(str(e)), self._base.const_critical_text)
            return None
        finally:
            if (urlResponse):
                urlResponse.close()
        if (self.RoleAccessKeyId in roleInfo and
            self.RoleSecretAccessKey in roleInfo and
                self.RoleToken in roleInfo):
            return roleInfo
        return None

    def getEndPoint(self, domain):
        redirectEndPoint = domain
        try:
            urlResponse = urlopen(domain)
            doc = minidom.parseString(urlResponse.read())
            endPoint = doc.getElementsByTagName('Endpoint')
            redirectEndPoint = 'http://{}/'.format(endPoint[0].firstChild.nodeValue)
        except Exception as e:
            pass
        return redirectEndPoint

    @property
    def inputPath(self):
        return self.__m_input_path

    @inputPath.setter
    def inputPath(self, value):
        self.__m_input_path = value

    def getFailedUploadList(self):
        return self.__m_failed_upl_lst

    def list(self, connection, bucket, prefix, includeSubFolders=False, keys=[], marker=''):
        try:   # requires/ListObjects access.
            result = connection.meta.client.list_objects(Bucket=bucket, Prefix=prefix, Delimiter='/', Marker=marker, RequestPayer='requester' if self._isRequesterPay else '')
        except Exception as e:
            self._base.message(e.message, self._base.const_critical_text)
            return False
        Contents = 'Contents'
        NextMarker = 'NextMarker'
        if (Contents in result):
            for k in result[Contents]:
                keys.append(k['Key'])
        for item in result.get('CommonPrefixes', []):
            if (not includeSubFolders):
                if (item['Prefix'].endswith('/')):
                    continue
            self.list(connection, bucket, item.get('Prefix'), includeSubFolders, keys, marker)
        if (NextMarker in result):
            self.list(connection, bucket, prefix, includeSubFolders, keys, result[NextMarker])
        return keys

    def getS3Content(self, prefix, cb=None, precb=None):
        isLink = self._input_flist is not None
        subs = True
        if (self.m_user_config):
            root_only_ = self.m_user_config.getValue('IncludeSubdirectories')
            if (subs is not None):    # if there's a value, take it else defaults to (True)
                subs = self._base.getBooleanValue(root_only_)
        keys = self.list(self.con, self.m_bucketname, prefix, subs) if not isLink else _rpt
        if (not keys):
            return False
        isRoot = self.remote_path == '/'
        # get the til files first
        if (til):
            if (not til.TILCount):
                try:
                    for key in keys:
                        if (not key or
                                key.endswith('/')):
                            continue
                        if (key.lower().endswith(CTIL_EXTENSION_)):
                            S3_path = key.replace(self.remote_path if not isRoot else '', '')    # remote path following the input folder/.
                            cb(key, S3_path)       # callback on the client-side
                            outputPath = self.m_user_config.getValue(CCFG_PRIVATE_OUTPUT, False) + S3_path
                            isCloudUpload = self._base.getBooleanValue(self.m_user_config.getValue(CCLOUD_UPLOAD))
                            if ((self.m_user_config.getValue(CISTEMPOUTPUT)) and
                                    isCloudUpload):
                                outputPath = self.m_user_config.getValue(CTEMPOUTPUT, False) + S3_path  # -tempoutput must be set with -cloudoutput=true
                            til.addTIL(key)
                            til.setOutputPath(key, outputPath)
                            tilObj = self.con.meta.client.get_object(Bucket=self.m_bucketname, Key=key)
                            tilContentsAsString = tilObj['Body'].read().decode('utf-8')
                            til.processInMemoryTILContent(key, tilContentsAsString)
                except Exception as e:
                    self._base.message(e.message, self._base.const_critical_text)
                    return False
        # ends
        try:
            threads = []
            keysIndx = 0
            nBuffer = CCFG_THREADS
            while(1):
                nThreads = len(threads)
                while(nThreads > 0):
                    alive = [t.isAlive() for t in threads]
                    nDead = sum(not x for x in alive)
                    if (nDead):
                        nBuffer = nDead
                        threads = [t for t in threads if t.isAlive()]
                        break
                buffer = []
                if (keysIndx == 0):
                    if (keys[keysIndx].endswith('/')):
                        keysIndx += 1
                for i in range(keysIndx, keysIndx + nBuffer):
                    if (i >= len(keys)):
                        break
                    buffer.append(keys[i])
                keysIndx += nBuffer
                if (len(buffer) == 0 and
                        len(threads) == 0):
                    break
                for key in buffer:
                    try:
                        remotePath = key.replace(self.remote_path if not isRoot else '', '')    # remote path following the input folder/.
                        if (not key or
                                key.endswith('/')):
                            continue
                        if (cb):
                            if (precb):
                                if (precb(remotePath, self.remote_path, self.inputPath)):     # is raster/exclude list?
                                    copyRemoteRaster = False
                                    if (til and
                                        til.defaultTILProcessing and
                                            til.fileTILRelated(os.path.basename(key))):
                                        copyRemoteRaster = True  # copy ancillary TIL files if the default TIL processing is set to (true)
                                    if (not copyRemoteRaster and
                                            not key.lower().endswith(CTIL_EXTENSION_)):  # TIL is a raster but we need to copy it locally.
                                        if (not self._base.getBooleanValue(self.m_user_config.getValue(CISTEMPINPUT))):
                                            continue
                        t = threading.Thread(target=cb,
                                             args=(key, remotePath))
                        t.daemon = True
                        t.start()
                        threads.append(t)
                    except Exception as e:
                        self.message(str(e), self.const_critical_text)
                        return False
        except Exception as e:
            self._base.message(e.message, self._base.const_critical_text)
            return False
        return True

    @TimeIt.timeOperation
    def __copyRemoteToLocal(self, S3_key, mk_path, **kwargs):
        try:
            self.con.meta.client.download_file(self.m_bucketname, S3_key, mk_path, ExtraArgs={'RequestPayer': 'requester'} if self._isRequesterPay else {})
        except Exception as e:
            self._base.message('({}\n{})'.format(str(e), mk_path), self._base.const_critical_text)
            if (_rpt):
                _rpt.updateRecordStatus(S3_key, CRPT_COPIED, CRPT_NO)
            return False
        return True

    def S3_copy_to_local(self, S3_key, S3_path):
        err_msg_0 = 'S3/Local path is invalid'
        if (S3_key is None):   # get rid of invalid args.
            self._base.message(err_msg_0)
            return False
        # what does the restore point say about the (S3_key) status?
        if (_rpt):
            _get_rstr_val = _rpt.getRecordStatus(S3_key, CRPT_COPIED)
            if (_get_rstr_val == CRPT_YES):
                self._base.message('{} {}'.format(CRESUME_MSG_PREFIX, S3_key))
                return True
        # ends
        if (self.m_user_config is None):     # shouldn't happen
            self._base.message('Internal/User config not initialized.', self._base.const_critical_text)
            return False
        output_path = self.m_user_config.getValue(CCFG_PRIVATE_OUTPUT, False) + S3_path
        is_cpy_to_s3 = self._base.getBooleanValue(self.m_user_config.getValue(CCLOUD_UPLOAD))
        if ((self.m_user_config.getValue(CISTEMPOUTPUT)) and
                is_cpy_to_s3):
            output_path = self.m_user_config.getValue(CTEMPOUTPUT, False) + S3_path  # -tempoutput must be set with -cloudoutput=true
        is_raster = False
        is_tmp_input = self._base.getBooleanValue(self.m_user_config.getValue(CISTEMPINPUT))
        primaryRaster = None
        if (_rpt and
                is_tmp_input):
            primaryRaster = _rpt._m_rasterAssociates.findPrimaryExtension(S3_path)
        if (filterPaths(S3_key, self.m_user_config.getValue(CCFG_EXCLUDE_NODE))):
            return False
        elif (primaryRaster or  # if the S3_key is an associated raster file, consider it as a raster.
              filterPaths(S3_key, self.m_user_config.getValue(CCFG_RASTERS_NODE))):
            isTIL = output_path.lower().endswith(CTIL_EXTENSION_)
            if (is_tmp_input):
                if (not isTIL):
                    useTempInputPath = True
                    if (til and
                        til.fileTILRelated(S3_path) and
                            til.defaultTILProcessing):
                        useTempInputPath = False
                    if (useTempInputPath):
                        output_path = self.m_user_config.getValue(CTEMPINPUT, False) + S3_path
            is_raster = not isTIL
        if (self.m_user_config.getValue('Pyramids') == CCMD_PYRAMIDS_ONLY):
            return False
        # collect input file names.
        if (fn_collect_input_files(S3_key)):
            return False
        # ends
        mk_path = output_path
        self._base.message('[S3-Pull] %s' % (mk_path))
        mk_path = self._base.renameMetaFileToMatchRasterExtension(mk_path)
        flr = os.path.dirname(mk_path)
        if (not os.path.exists(flr)):
            try:
                makedirs(flr)
            except Exception as e:
                self._base.message('(%s)' % (str(e)), self._base.const_critical_text)
                if (_rpt):
                    _rpt.updateRecordStatus(S3_key, CRPT_COPIED, CRPT_NO)
                return False
        # let's write remote to local
        result = self.__copyRemoteToLocal(S3_key, mk_path, name=S3_key, method=TimeIt.Download, store=self._base)
        if (not result):
            return False
        # ends
        # mark download/copy status
        if (_rpt):
            _rpt.updateRecordStatus(S3_key, CRPT_COPIED, CRPT_YES)
        # ends
        # copy metadata files to -clonepath if set
        if (not is_raster):  # do not copy raster associated files to clone path.
            self._base.copyMetadataToClonePath(mk_path)
        # ends
        # Handle any post-processing, if the final destination is to S3, upload right away.
        if (is_cpy_to_s3):
            if (getBooleanValue(self.m_user_config.getValue(CISTEMPINPUT))):
                if (is_raster):
                    return True
            if (til and
                til.defaultTILProcessing and
                is_raster and
                    til.fileTILRelated(mk_path)):
                return True
            _is_success = self._base.S3Upl(mk_path, user_args_Callback)
            if (not _is_success):
                return False
        # ends
        return True
    # ends

    def upload(self):
        self._base.message('[S3-Push]..')
        for r, d, f in os.walk(self.inputPath):
            for file in f:
                lcl_file = os.path.join(r, file).replace('\\', '/')
                upl_file = lcl_file.replace(self.inputPath, self.remote_path)
                self._base.message(upl_file)
                try:
                    S3 = S3Upload(self.bucketupload, upl_file, lcl_file, self.m_user_config.getValue(COUT_S3_ACL) if self.m_user_config else None)
                    if (not S3.init()):
                        self._base.message('Unable to initialize [S3-Push] for (%s=%s)' % (lcl_file, upl_file), self._base.const_warning_text)
                        continue
                    ret = S3.upload()
                    if (not ret):
                        self._base.message('[S3-Push] (%s)' % (upl_file), self._base.const_warning_text)
                        continue
                except Exception as e:
                    self._base.message('(%s)' % (str(e)), self._base.const_warning_text)
                finally:
                    if (S3 is not None):
                        del S3
        return True

    def _addToFailedList(self, localPath, remotePath):
        if ('upl' not in self.getFailedUploadList()):
            self.__m_failed_upl_lst['upl'] = []
        _exists = False
        for v in self.__m_failed_upl_lst['upl']:
            if (v['local'] == localPath):
                _exists = True
                break
        if (not _exists):
            self.__m_failed_upl_lst['upl'].append({'local': localPath, 'remote': remotePath})
        return True

    def upload_group(self, input_source, single_upload=False, include_subs=False):
        global _rpt
        m_input_source = input_source.replace('\\', '/')
        input_path = os.path.dirname(m_input_source)
        upload_buff = []
        usrPath = self.m_user_config.getValue(CUSR_TEXT_IN_PATH, False)
        usrPathPos = CHASH_DEF_INSERT_POS  # default insert pos (sub-folder loc) for user text in output path
        if (usrPath):
            (usrPath, usrPathPos) = usrPath.split(CHASH_DEF_SPLIT_CHAR)
        (p, e) = os.path.splitext(m_input_source)
        for r, d, f in os.walk(input_path):
            for file in f:
                mk_path = os.path.join(r, file).replace('\\', '/')
                if ((single_upload and
                     (mk_path == m_input_source)) or
                        mk_path.startswith('{}.'.format(p))):
                    try:
                        S3 = _source_path = None
                        if (_rpt):
                            _source_path = getSourcePathUsingTempOutput(mk_path)
                            if (_source_path):
                                _ret_val = _rpt.getRecordStatus(_source_path, CRPT_UPLOADED)
                                if (_ret_val == CRPT_YES):
                                    continue
                        upl_file = mk_path.replace(self.inputPath, self.remote_path)
                        if (getBooleanValue(self.m_user_config.getValue(CCLOUD_UPLOAD))):
                            rep = self.inputPath
                            if (not rep.endswith('/')):
                                rep += '/'
                            if (getBooleanValue(self.m_user_config.getValue(CISTEMPOUTPUT))):
                                rep = self.m_user_config.getValue(CTEMPOUTPUT, False)
                            upl_file = mk_path.replace(rep, self.remote_path if self.m_user_config.getValue('iss3') else self.m_user_config.getValue(CCFG_PRIVATE_OUTPUT, False))
                        if (usrPath):
                            upl_file = self._base.insertUserTextToOutputPath(upl_file, usrPath, usrPathPos)
                        S3 = S3Upload(self._base, self.bucketupload, upl_file, mk_path, self.m_user_config.getValue(COUT_S3_ACL) if self.m_user_config else None)
                        if (not S3.init()):
                            self._base.message('Unable to initialize S3-Upload for (%s=>%s)' % (mk_path, upl_file), self._base.const_warning_text)
                            self._addToFailedList(mk_path, upl_file)
                            continue
                        upl_retries = CS3_UPLOAD_RETRIES
                        ret = False
                        while(upl_retries and not ret):
                            ret = S3.upload(name=_source_path, method=TimeIt.Upload, store=self._base)
                            if (not ret):
                                time.sleep(10)   # let's sleep for a while until s3 kick-starts
                                upl_retries -= 1
                                self._base.message('[S3-Push] (%s), retries-left (%d)' % (upl_file, upl_retries), self._base.const_warning_text)
                        if (not ret):
                            self._addToFailedList(mk_path, upl_file)
                            if (S3 is not None):
                                del S3
                                S3 = None
                            continue
                    except Exception as inf:
                        self._base.message('(%s)' % (str(inf)), self._base.const_critical_text)
                    finally:
                        if (S3 is not None):
                            del S3
                            S3 = None
                    upload_buff.append(mk_path)    # successful entries to return.
                    if (single_upload):
                        return upload_buff
            if (not include_subs):
                return upload_buff
        return upload_buff       # this could be empty.
# ends


CIDX_USER_INPUTFILE = 0
CIDX_USER_CONFIG = 2
CIDX_USER_CLSBASE = 3
CCFG_BLOCK_SIZE = 512
CCMD_PYRAMIDS_ONLY = 'only'
CCMD_PYRAMIDS_EXTERNAL = 'external'
CCMD_PYRAMIDS_SOURCE = 'source'  # Used by CreateRasterProxy
CCFG_THREADS = 10
CCFG_RASTERS_NODE = 'RasterFormatFilter'
CCFG_EXCLUDE_NODE = 'ExcludeFilter'
CCFG_PRIVATE_INC_BOTO = '__inc_boto__'
CCFG_PRIVATE_OUTPUT = '__output__'
CFGAZSAS = '__szsas__'
CFGAZSASW = '__szsasw__'
CCFG_LAMBDA_INVOCATION_ERR = '__LAMBDA_INV_ERR__'
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


def args_Callback(args, user_data=None):
    _LERC = 'lerc'
    _LERC2 = 'lerc2'
    _JPEG = 'jpeg'
    _JPEG12 = 'jpeg12'
    m_compression = _LERC    # default if external config is faulty
    m_lerc_prec = None
    m_compression_quality = DefJpegQuality
    m_bsize = CCFG_BLOCK_SIZE
    m_mode = 'chs'
    m_nodata_value = None
    m_predictor = 1
    m_interleave = 'PIXELS'
    if (user_data):
        try:
            userParameters = user_data[CIDX_USER_CONFIG].getValue('GDAL_Translate_UserParameters')
            if (userParameters):
                [args.append(i) for i in userParameters.split()]
            compression_ = user_data[CIDX_USER_CONFIG].getValue('Compression')
            useCOGTIFF = user_data[CIDX_USER_CONFIG].getValue('cog') == True
            if (useCOGTIFF):
                compression_ = 'Deflate'
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
                args.append('-co')
                args.append('BIGTIFF=IF_NEEDED')
                args.append('-co')
                args.append('TILED=YES')
                m_mode = 'GTiff'   # so that gdal_translate can understand.
                if (m_interleave == 'PIXEL' and
                        m_compression.startswith(_JPEG)):
                    _base = user_data[CIDX_USER_CLSBASE]
                    if (_base):
                        gdalInfo = GDALInfo(_base)
                        gdalInfo.init(user_data[CIDX_USER_CONFIG].getValue(CCFG_GDAL_PATH, False))
                        if (gdalInfo.process(user_data[CIDX_USER_INPUTFILE])):
                            ret = gdalInfo.bandInfo
                            if (ret and
                                    len(ret) != 1):
                                args.append('-co')
                                args.append('PHOTOMETRIC=YCBCR')
                    if (m_compression == _JPEG12):
                        args.append('-co')
                        args.append('NBITS=12')
                    m_compression = _JPEG
                if (m_interleave == 'PIXEL' and
                        m_compression == 'deflate'):
                    args.append('-co')
                    args.append(' predictor={}'.format(m_predictor))
        except BaseException:     # could throw if index isn't found
            pass    # ingnore with defaults.
    args.append('-of')
    args.append(m_mode)
    args.append('-co')
    args.append('COMPRESS=%s' % (_LERC if m_compression == _LERC2 else m_compression))
    if (m_nodata_value):
        args.append('-a_nodata')
        args.append(str(m_nodata_value))
    if (m_compression == _JPEG):
        args.append('-co')
        if (m_mode == 'mrf'):   # if the output is (mrf)
            args.append('QUALITY=%s' % (m_compression_quality))
            if (m_ignorealphaband):
                args.append('-co')
                args.append('OPTIONS="MULTISPECTRAL:1"')
        else:
            args.append('JPEG_QUALITY=%s' % (m_compression_quality))
        args.append('-co')
        args.append('INTERLEAVE=%s' % (m_interleave))
    if (m_compression.startswith(_LERC)):
        if (m_lerc_prec or
            m_compression == _LERC2 or
                m_compression == _LERC):
            args.append('-co')
            args.append('OPTIONS="{}{}"'.format('' if not m_lerc_prec else 'LERC_PREC={}'.format(m_lerc_prec), '{}V2=ON'.format(' ' if m_lerc_prec else '') if m_compression == _LERC2 or m_compression == _LERC else ''))
    args.append('-co')
    if (m_mode.lower() == 'gtiff'):
        args.append('{}={}'.format('BLOCKXSIZE', m_bsize))
        args.append('-co')
        args.append('{}={}'.format('BLOCKYSIZE', m_bsize))
    else:
        args.append('{}={}'.format('BLOCKSIZE', m_bsize))
    return args


def args_Callback_for_meta(args, user_data=None):
    _LERC = 'lerc'
    _LERC2 = 'lerc2'
    m_scale = 2
    m_bsize = CCFG_BLOCK_SIZE
    m_pyramid = True
    m_comp = _LERC
    m_lerc_prec = None
    m_compression_quality = DefJpegQuality
    if (user_data):
        try:
            scale_ = user_data[CIDX_USER_CONFIG].getValue('Scale')
            if (scale_):
                m_scale = scale_
            bsize_ = user_data[CIDX_USER_CONFIG].getValue('BlockSize')
            if (bsize_):
                m_bsize = bsize_
            ovrpyramid = user_data[CIDX_USER_CONFIG].getValue('isuniformscale')
            if (ovrpyramid is not None):
                m_pyramid = ovrpyramid
                if (m_pyramid == 'source'):
                    rpt = user_data[CIDX_USER_CLSBASE].getUserConfiguration.getValue(CPRT_HANDLER)
                    if (rpt):
                        cldInput = user_data[CIDX_USER_CONFIG].getValue(CIN_S3_PREFIX, False)
                        rptName = user_data[0].replace(cldInput, '') if cldInput is not None else user_data[0]
                        ovrpyramid = rpt.getMetadata(rptName, 'isuniformscale')
                        m_pyramid = None if ovrpyramid is None else ovrpyramid
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
        except BaseException:     # could throw if index isn't found
            pass    # ingnore with defaults.
    args.append('-of')
    args.append('MRF')
    args.append('-co')
    args.append('COMPRESS=%s' % (_LERC if m_comp == _LERC2 else m_comp))
    if (m_comp.startswith(_LERC)):
        if (m_lerc_prec or
            m_comp == _LERC2 or
                m_comp == _LERC):
            args.append('-co')
            args.append('OPTIONS="{}{}"'.format('' if not m_lerc_prec else 'LERC_PREC={}'.format(m_lerc_prec), '{}V2=ON'.format(' ' if m_lerc_prec else '') if m_comp == _LERC2 or m_comp == _LERC else ''))
    elif(m_comp == 'jpeg'):
        args.append('-co')
        args.append('QUALITY=%s' % (m_compression_quality))
        args.append('-co')
        args.append('INTERLEAVE=%s' % (m_interleave))
    args.append('-co')
    args.append('NOCOPY=True')
    if (m_pyramid):
        args.append('-co')
        args.append('UNIFORM_SCALE=%s' % (m_scale))
    args.append('-co')
    args.append('BLOCKSIZE=%s' % (m_bsize))
    args.append('-co')
    # let's fix the cache extension
    cache_source = user_data[0]
    isQuotes = cache_source[0] == '"' and cache_source[-1] == '"'
    quoteChar = '' if isQuotes else '"'
    args.append('CACHEDSOURCE={}{}{}'.format(quoteChar, cache_source, quoteChar))
    # ends
    return args


def copy_callback(file, src, dst):
    Message(file)
    return True


def exclude_callback(file, src, dst):
    if (file is None):
        return False
    (f, e) = os.path.splitext(file)
    if (filterPaths(os.path.join(src, file), cfg.getValue(CCFG_RASTERS_NODE)) or
            src.lower().startswith('http')):
        if (file.lower().endswith(CTIL_EXTENSION_)):
            return True
        raster_buff.append({'f': file, 'src': '' if src == '/' else src, 'dst': dst if dst else ''})
        return True
    return False


def exclude_callback_for_meta(file, src, dst):
    exclude_callback(file, src, dst)


def getSourcePathUsingTempOutput(input):
    # cfg, _rpt are global vars.
    if (not _rpt or
            not getBooleanValue(cfg.getValue(CISTEMPOUTPUT))):
        return None
    _mk_path = input.replace(cfg.getValue(CTEMPOUTPUT, False), '')
    _indx = -1
    if (True in [_mk_path.lower().endswith(i) for i in ['.idx', '.lrc', '.pjg', '.pzp', '.pft', '.ppng', '.pjp', '.aux.xml']]):       # if any one of these extensions fails,
        _indx = _mk_path.rfind('.')                                                          # the main (raster) file upload entry in (Reporter) would be set to (no) denoting a failure in one of its associated files.
    if (_indx == -1):
        return (_rpt.findExact('{}{}'.format(_rpt.root, _mk_path)))
    for i in _rpt:
        if (i.find(_mk_path[:_indx + 1]) != -1):
            if (True in [i.endswith(x) for x in cfg.getValue(CCFG_RASTERS_NODE)]):
                return i
    return None


def setUploadRecordStatus(input, rpt_status):
    _rpt_src = getSourcePathUsingTempOutput(input)
    if (_rpt_src and
            _rpt.updateRecordStatus(_rpt_src, CRPT_UPLOADED, rpt_status)):
        return True
    return False


def filterPaths(file, patterns):
    global cfg
    if (not file and
            not cfg):
        print ('Internal/Empty args/filterPaths()')
        return False
    filePatterns = patterns  # cfg.getValue(CCFG_RASTERS_NODE)
    matched = False
    if (filePatterns):
        for pattern in filePatterns:
            firstChar = pattern[0]
            if (firstChar != '?' and
                firstChar != '*' and
                    firstChar != '['):
                pattern = '*' + pattern      # force to match the ending.
            if (fnmatch.fnmatchcase(file, pattern)):
                matched = True
                break
    return matched


class Copy:

    def __init__(self, base=None):
        self._base = base

    def init(self, src, dst, copy_list, cb_list, user_config=None):
        if (not dst or
                not src):
            return False
        self.src = src.replace('\\', '/')
        self._input_flist = None
        if (not os.path.isdir(self.src)):
            if (not os.path.exists(self.src)):
                self.message('Invalid -input report file ({})'.format(self.src), const_critical_text)
                return False
            self._input_flist = self.src
            try:
                global _rpt
                self.src = _rpt.root
            except Exception as e:
                self.message('Report ({})'.format(str(e)), const_critical_text)
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
        if (user_config):
            self.m_user_config = user_config
            include_subs = self.m_user_config.getValue('IncludeSubdirectories')
            if (include_subs is not None):    # if there's a value either (!None), take it else defaults to (True)
                self.__m_include_subs = getBooleanValue(include_subs)
        return True

    def message(self, msg, msgType=None):
        if (self._base):
            return (self._base.message(msg, msgType))
        print (msg)

    def processs(self, post_processing_callback=None, post_processing_callback_args=None, pre_processing_callback=None):
        log = None
        if (self._base):
            log = self._base.getMessageHandler
        if (log):
            log.CreateCategory('Copy')
        self.message('Copying non rasters/aux files (%s=>%s)..' % (self.src, self.dst))
        # init - TIL files
        is_link = self._input_flist is not None
        if (til):
            for r, d, f in _rpt.walk() if is_link else os.walk(self.src):
                for file in f:
                    if (not file):
                        continue
                    if (not self.__m_include_subs):
                        if ((r[:-1] if r[-1:] == '/' else r) != os.path.dirname(self.src)):     # note: first arg to walk (self.src) has a trailing '/'
                            continue
                    if (file.lower().endswith(CTIL_EXTENSION_)):
                        _til_filename = os.path.join(r, file)
                        if (til):
                            til.addTIL(_til_filename)
            for _til in til:
                til.process(_til)
        # ends
        for r, d, f in _rpt.walk() if is_link else os.walk(self.src):
            for file in f:
                if (not file):
                    continue
                if (not self.__m_include_subs):
                    if ((r[:-1] if r[-1:] == '/' else r) != os.path.dirname(self.src)):     # note: first arg to walk (self.src) has a trailing '/'
                        continue
                free_pass = False
                dst_path = r.replace(self.src, self.dst)
                if (('*' in self.format['copy'])):
                    free_pass = True
                if (not free_pass):
                    _isCpy = False
                    for _p in self.format['copy']:
                        if (file.endswith(_p)):
                            _isCpy = True
                            break
                    if (not _isCpy):
                        continue
                isInputWebAPI = isInputHttp = False
                if (_rpt and
                        _rpt._isInputHTTP):
                    isInputHttp = True
                    (f, e) = os.path.splitext(file)
                    if (not e):     # if no file extension at the end of URL, it's assumed we're talking to a web service endpoint which in turn returns a raster.
                        isInputWebAPI = True
                isPlanet = self.src.find(CPLANET_IDENTIFY) != -1
                if (filterPaths(os.path.join(r, file), self.format['exclude']) and
                    not file.lower().endswith(CTIL_EXTENSION_) or       # skip 'exclude' list items and always copy (.til) files to destination.
                        isInputWebAPI or
                        isPlanet or
                        isInputHttp):
                    if (('exclude' in self.cb_list)):
                        if (self.cb_list['exclude'] is not None):
                            if (self.m_user_config is not None):
                                if (getBooleanValue(self.m_user_config.getValue(CISTEMPOUTPUT))):
                                    dst_path = r.replace(self.src, self.m_user_config.getValue(CTEMPOUTPUT, False))    # no checks on temp-output validty done here. It's assumed it has been prechecked at the time of its assignment.
                            _r = r
                            if (self.m_user_config):
                                if (self.m_user_config.getValue(CLOAD_RESTORE_POINT)):
                                    if (getBooleanValue(self.m_user_config.getValue(CISTEMPINPUT))):
                                        r = r.replace(self.src, self.m_user_config.getValue(CTEMPINPUT, False))
                            if (_rpt and
                                    _rpt._isInputHTTP and
                                    (Report.CHDR_MODE in _rpt._header and
                                     _rpt._header[Report.CHDR_MODE] != 'cachingmrf' and
                                     _rpt._header[Report.CHDR_MODE] != 'rasterproxy')):
                                _mkRemoteURL = os.path.join(_r, file)
                                try:
                                    file_url = urlopen(_mkRemoteURL if not isInputWebAPI else os.path.splitext(_mkRemoteURL)[0])
                                    respHeaders = []
                                    if (sys.version_info[0] < 3):
                                        respHeaders = file_url.headers.headers
                                    else:
                                        for hdr in file_url.getheaders():
                                            respHeaders.append('{}: {}'.format(hdr[0], hdr[1]))
                                    isFileNameInHeader = False
                                    for v in respHeaders:
                                        if (v.startswith('Content-Disposition')):
                                            token = 'filename='
                                            if (isPlanet):
                                                if (_mkRemoteURL in _rpt._input_list_info):
                                                    _rpt._input_list_info[_mkRemoteURL][Report.CRPT_URL_TRUENAME] = v.split(':')[1].strip()
                                                isFileNameInHeader = True
                                                if (v.find(token) == -1):
                                                    break
                                            f = v.find(token)
                                            if (f != -1):
                                                e = v.find('\r', f + len(token))
                                                if (_mkRemoteURL in _rpt._input_list_info):
                                                    _rpt._input_list_info[_mkRemoteURL][Report.CRPT_URL_TRUENAME] = v[f + len(token): e].strip().replace('"', '').replace('?', '_')
                                                isFileNameInHeader = True
                                            break
                                    localPath = self.m_user_config.getValue(CTEMPINPUT)
                                    if (localPath is None):
                                        if (self.m_user_config.getValue(COP) == COP_COPYONLY):
                                            localPath = self.m_user_config.getValue(CCFG_PRIVATE_OUTPUT)
                                    if (localPath):    # we've to download the file first and save to the name requested.
                                        r = r.replace(self.src, localPath)
                                        if (not os.path.exists(r)):
                                            makedirs(r)
                                        file = _rpt._input_list_info[_mkRemoteURL][Report.CRPT_URL_TRUENAME] if isFileNameInHeader else file
                                        self._base.message('{}'.format(file_url.geturl()))
                                        with open(os.path.join(r, file), 'wb') as fp:
                                            buff = 2024 * 1024
                                            while True:
                                                chunk = file_url.read(buff)
                                                if (not chunk):
                                                    break
                                                fp.write(chunk)
                                        # mark download/copy status
                                        if (_rpt):
                                            _rpt.updateRecordStatus(_mkRemoteURL, CRPT_COPIED, CRPT_YES)
                                        # ends
                                except Exception as e:
                                    self._base.message('{}'.format(str(e), self._base.const_critical_text))
                            if (not self.cb_list['exclude'](file, r, dst_path)):       # skip fruther processing if 'false' returned from the callback fnc
                                continue
                    continue
                try:
                    if (('copy' in self.cb_list)):
                        if (self.cb_list['copy'] is not None):
                            if (not self.cb_list['copy'](file, r, dst_path)):       # skip fruther processing if 'false' returned
                                continue
                    if (not g_is_generate_report):              # do not create folders for op==reporting only.
                        if (not os.path.exists(dst_path)):
                            if (not self._base._isRasterProxyFormat('csv')):
                                makedirs(dst_path)
                    dst_file = os.path.join(dst_path, file)
                    src_file = os.path.join(r, file)
                    do_post_processing_cb = do_copy = True
                    if (os.path.dirname(src_file.replace('\\', '/')) != os.path.dirname(dst_path.replace('\\', '/')) or
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
                                primaryRaster = _rpt._m_rasterAssociates.findPrimaryExtension(src_file)
                                if (primaryRaster):
                                    _ext = _rpt._m_rasterAssociates.findExtension(src_file)
                                    if (_ext):
                                        _mkPrimaryRaster = '{}{}'.format(src_file[:len(src_file) - len(_ext)], primaryRaster)
                                        if (_mkPrimaryRaster in _rpt._input_list_info):
                                            if (CTEMPINPUT in _rpt._header):
                                                dst_file = dst_file.replace(_rpt._header[CRESUME_HDR_OUTPUT], _rpt._header[CTEMPINPUT])
                                dst_file = self._base.renameMetaFileToMatchRasterExtension(dst_file)
                                if (not self._base._isRasterProxyFormat('csv')):
                                    shutil.copyfile(src_file, dst_file)
                                # Clone folder will get all the metadata files by default.
                                if (not primaryRaster):  # do not copy raster associated files to clone path.
                                    self._base.copyMetadataToClonePath(dst_file)
                                # ends
                            if (self._input_flist):
                                _rpt.updateRecordStatus(src_file, CRPT_COPIED, CRPT_YES)
                            self.message('{} {}'.format(CRESUME_MSG_PREFIX if not do_copy else '[CPY]', src_file.replace(self.src, '')))
                    # copy post-processing
                    if (do_post_processing_cb):
                        if (post_processing_callback):
                            ret = post_processing_callback(dst_file, post_processing_callback_args)    # ignore errors from the callback
                    # ends
                except Exception as e:
                    if (self._input_flist):
                        _rpt.updateRecordStatus(os.path.join(r, file), CRPT_COPIED, CRPT_NO)
                    self.message('(%s)' % (str(e)), self._base.const_critical_text)
                    continue
        self.message('Done.')
        if (log):
            log.CloseCategory()
        return True

    def get_group_filelist(self, input_source):          # static
        m_input_source = input_source.replace('\\', '/')
        input_path = os.path.dirname(m_input_source)
        file_buff = []
        (p, e) = os.path.splitext(m_input_source)
        for r, d, f in os.walk(input_path):
            for file in f:
                mk_path = os.path.join(r, file).replace('\\', '/')
                if (mk_path.startswith(p)):
                    file_buff.append(mk_path)
        return file_buff

    def batch(self, file_lst, args=None, pre_copy_callback=None):
        threads = []
        files_len = len(file_lst)
        batch = 1
        s = 0
        while True:
            m = s + batch
            if (m >= files_len):
                m = files_len
            threads = []
            for i in range(s, m):
                req = file_lst[i]
                (input_file, output_file) = getInputOutput(req['src'], req['dst'], req['f'], False)
                dst_path = os.path.dirname(output_file)
                if (not os.path.exists(dst_path)):
                    makedirs(dst_path)
                CCOPY = 0
                CMOVE = 1
                mode_ = CCOPY        # 0 = copy, 1 = move
                if (args is not None):
                    if (isinstance(args, dict)):
                        if (('mode' in args)):
                            if (args['mode'].lower() == 'move'):
                                mode_ = CMOVE
                if (mode_ == CCOPY):
                    self.message('[CPY] %s' % (output_file))
                    shutil.copyfile(input_file, output_file)
                elif (mode_ == CMOVE):
                    self.message('[MV] %s' % (output_file))
                    try:
                        shutil.move(input_file, output_file)
                    except Exception as e:
                        self.message(str(e))
            s = m
            if s == files_len or s == 0:
                break
                pass
                # ends
        return True


class Compression(object):

    def __init__(self, gdal_path, base):
        self.m_gdal_path = gdal_path
        self.CGDAL_TRANSLATE_EXE = 'gdal_translate'
        self.CGDAL_BUILDVRT_EXE = 'gdalbuildvrt'
        self.CGDAL_ADDO_EXE = 'gdaladdo'
        self.m_id = None
        self.m_user_config = None
        self._base = base

    def init(self, id=None):
        if (id):
            self.m_id = id
        if (not self._base or
            not isinstance(self._base, Base) or
                not isinstance(self._base.getUserConfiguration, Config)):
            Message('Err/Internal. (Compression) instance is not initialized with a valid (Base) instance.', const_critical_text)
            return False
        if (not self._base.isLinux()):
            self.CGDAL_TRANSLATE_EXE += CEXEEXT
            self.CGDAL_ADDO_EXE += CEXEEXT
            self.CGDAL_BUILDVRT_EXE += CEXEEXT
        self.m_user_config = self._base.getUserConfiguration
        # internal gdal_path could get modified here.
        if (not self.m_gdal_path or
                not os.path.isdir(self.m_gdal_path)):
            if (self.m_gdal_path):
                self.message('Invalid GDAL path ({}) in paramter file. Using default location.'.format(self.m_gdal_path), const_warning_text)
            self.m_gdal_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), r'GDAL/bin')
            if (not os.path.isdir(self.m_gdal_path)):
                self.message('GDAL not found at ({}).'.format(self.m_gdal_path), self._base.const_critical_text)
                return False
            self.m_user_config.setValue(CCFG_GDAL_PATH, self.m_gdal_path)
        # ends
        # set gdal_data enviornment path
        os.environ['GDAL_DATA'] = os.path.join(os.path.dirname(self.m_gdal_path), 'data')
        os.environ['GDAL_HTTP_UNSAFESSL'] = 'true'  # disable CURL SSL certificate problem
        # ends
        msg_text = '(%s) is not found at (%s)'
        _gdal_translate = os.path.join(self.m_gdal_path, self.CGDAL_TRANSLATE_EXE)
        if (not os.path.isfile(_gdal_translate)):
            self.message(msg_text % (self.CGDAL_TRANSLATE_EXE, self.m_gdal_path), self._base.const_critical_text)
            return False
        if (CRUN_IN_AWSLAMBDA):
            if (not self._base.copyBinaryToTmp(_gdal_translate, '/tmp/{}'.format(self.CGDAL_TRANSLATE_EXE))):
                return False
        _gdaladdo = os.path.join(self.m_gdal_path, self.CGDAL_ADDO_EXE)
        if (not os.path.isfile(_gdaladdo)):
            self.message(msg_text % (self.CGDAL_ADDO_EXE, self.m_gdal_path), self._base.const_critical_text)
            return False
        if (CRUN_IN_AWSLAMBDA):
            if (not self._base.copyBinaryToTmp(_gdaladdo, '/tmp/{}'.format(self.CGDAL_ADDO_EXE))):
                return False
            # copy shared so binaries. Note> libcurl.so.4 support is installed on Lambda by default.
            if (not self._lambdaCopySharedSO('libgdal.so.20')):
                return False
        return True

    def _lambdaCopySharedSO(self, sharedLib):
        try:
            self.message('# pre-sudo {}'.format(sharedLib))
            _so = os.path.join(self.m_gdal_path, sharedLib)
            p = subprocess.Popen(' '.join(['sudo', 'cp', _so, '/var/task']), shell=True)
            self.message('# post-sudo {}'.format(sharedLib))
        except Exception as e:
            self.message('Err. lambda>{}'.format(str(e)))
            return False
        return True

    def message(self, msg, status=const_general_text):
        write = msg
        if (self.m_id):
            write = '[{}] {}'.format(threading.current_thread().name, msg)
        self._base.message(write, status)
        return True

    def buildMultibandVRT(self, input_files, output_file):
        if (len(input_files) == 0):
            return False
        args = [os.path.join(self.m_gdal_path, self.CGDAL_BUILDVRT_EXE)]
        args.append(output_file)
        for f in (input_files):
            args.append(f)
        self.message('Creating VRT output file (%s)' % (output_file))
        return self._call_external(args)

    def compress(self, input_file, output_file, args_callback=None, build_pyramids=True, post_processing_callback=None, post_processing_callback_args=None, **kwargs):
        isRasterProxyCaller = False
        if (UpdateOrjobStatus in kwargs):
            if (not kwargs[UpdateOrjobStatus]):
                isRasterProxyCaller = True
        if (_rpt):
            if (input_file in _rpt._input_list_info and
                    Report.CRPT_URL_TRUENAME in _rpt._input_list_info[input_file]):
                output_file = '{}/{}'.format(os.path.dirname(output_file), _rpt._input_list_info[input_file][Report.CRPT_URL_TRUENAME])
        _vsicurl_input = self.m_user_config.getValue(CIN_S3_PREFIX, False)
        _input_file = input_file.replace(_vsicurl_input, '') if _vsicurl_input else input_file
        isTempInput = self._base.getBooleanValue(self.m_user_config.getValue(CISTEMPINPUT))
        if (isTempInput):
            if (_rpt):
                if (not _rpt._isInputHTTP):
                    _input_file = _input_file.replace(self.m_user_config.getValue(CTEMPINPUT, False), '' if _rpt.root == '/' else _rpt.root)
        _do_process = ret = True
        # get restore point snapshot
        if (self.m_user_config.getValue(CLOAD_RESTORE_POINT)):
            _get_rstr_val = _rpt.getRecordStatus(_input_file, CRPT_PROCESSED)
            if (_get_rstr_val == CRPT_YES or
                    _rpt.operation == COP_UPL):
                if (_rpt.operation != COP_UPL):
                    self.message('{} {}'.format(CRESUME_MSG_PREFIX, _input_file))
                _do_process = False
        # ends
        breakInputPath = input_file.split('/')
        if (breakInputPath[-1].lower().endswith('.adf')):
            breakOututPath = output_file.split('/')
            fileTitle = breakOututPath[-1]
            breakInputPath.pop()
            breakOututPath.pop()
            if (breakInputPath[-1] == breakOututPath[-1]):
                breakOututPath.pop()
            output_file = '/'.join(breakOututPath) + '/{}.{}'.format(breakInputPath[-1], self.m_user_config.getValue('Mode'))
        post_process_output = output_file
        if (_do_process):
            out_dir_path = os.path.dirname(output_file)
            if (not os.path.exists(out_dir_path)):
                try:
                    makedirs(os.path.dirname(output_file))   # let's try to make the output dir-tree else GDAL would fail
                except Exception as exp:
                    time.sleep(2)    # let's try to sleep for few seconds and see if any other thread has created it.
                    if (not os.path.exists(out_dir_path)):
                        self.message('(%s)' % str(exp), self._base.const_critical_text)
                        if (_rpt):
                            _rpt.updateRecordStatus(_input_file, CRPT_PROCESSED, CRPT_NO)
                        return False
            # ends
            isModeClone = self.m_user_config.getValue('Mode') == 'clonemrf'
            do_process = (_rpt and _rpt.operation != COP_NOCONVERT) and not isModeClone
            if (not do_process):
                self.message('[CPY] {}'.format(_input_file))
                if (input_file.startswith('/vsicurl/')):
                    try:
                        _dn_vsicurl_ = input_file.split('/vsicurl/')[1]
                        file_url = urlopen(_dn_vsicurl_)
                        validateForClone = isModeClone
                        with open(output_file, 'wb') as fp:
                            buff = 2024 * 1024
                            while True:
                                chunk = file_url.read(buff)
                                if (validateForClone):
                                    validateForClone = False
                                    if (chunk[:CMRF_DOC_ROOT_LEN] != '<{}>'.format(CMRF_DOC_ROOT)):
                                        self.message('Invalid MRF ({})'.format(_dn_vsicurl_), self._base.const_critical_text)
                                        raise Exception
                                if (not chunk):
                                    break
                                fp.write(chunk)
                    except Exception as e:
                        if (_rpt):
                            _rpt.updateRecordStatus(_input_file, CRPT_PROCESSED, CRPT_NO)
                            return False
                else:
                    if (isTempInput or
                            not self._base.getBooleanValue(self.m_user_config.getValue('iss3'))):
                        shutil.copyfile(input_file, output_file)
                if (isModeClone):
                    # Simulate the MRF file update (to include the CachedSource) which was earlier done via the GDAL_Translate->MRF driver.
                    try:
                        _CDOC_ROOT = CMRF_DOC_ROOT
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
                        with open(output_file, "w") as c:
                            _mrfBody = doc.toxml().replace('&quot;', '"')       # GDAL mrf driver can't handle XML entity names.
                            _indx = _mrfBody.find('<{}>'.format(_CDOC_ROOT))
                            if (_indx == -1):
                                raise Exception()
                            _mrfBody = _mrfBody[_indx:]
                            c.write(_mrfBody)
                    except BaseException:
                        self.message('Invalid MRF ({})'.format(input_file), self._base.const_critical_text)
                        if (_rpt):
                            _rpt.updateRecordStatus(_input_file, CRPT_PROCESSED, CRPT_NO)
                        return False
                    # ends
            do_pyramids = self.m_user_config.getValue('Pyramids')
            timeIt = kwargs['name'] if 'name' in kwargs else None
            azSAS = self.m_user_config.getValue(CFGAZSAS, False)
            inputRaster = self._base.urlEncode(input_file) if _vsicurl_input and input_file.find(CPLANET_IDENTIFY) == -1 and not azSAS and not isTempInput else '"{}"'.format(input_file)
            useTokenPath = self._base.convertToTokenPath(inputRaster)
            if (useTokenPath is not None):
                inputRaster = useTokenPath
            if (do_pyramids != CCMD_PYRAMIDS_ONLY and
                    do_process):
                args = [os.path.join(self.m_gdal_path, self.CGDAL_TRANSLATE_EXE)]
                if (args_callback is None):      # defaults
                    args.append('-of')
                    args.append('MRF')
                    args.append('-co')
                    args.append('COMPRESS=LERC')
                    args.append('-co')
                    args.append('BLOCKSIZE=512')
                else:
                    args = args_callback(args, [inputRaster if useTokenPath else input_file, output_file, self.m_user_config, self._base])      # callback user function to get arguments.
                if (_rpt):
                    if (input_file.startswith('/vsicurl/')):
                        trueFile = input_file.replace('/vsicurl/', '')
                        if (trueFile in _rpt._input_list_info and
                                Report.CRPT_URL_TRUENAME in _rpt._input_list_info[trueFile]):
                            (urlFileName, urlExt) = os.path.splitext(os.path.join(output_file.split('?')[0], _rpt._input_list_info[trueFile][Report.CRPT_URL_TRUENAME]))
                            if (not self._base.getBooleanValue(self.m_user_config.getValue('KeepExtension')) and
                                    args[1] == '-of'):
                                urlExt = args[2]
                            post_process_output = output_file = '{}{}{}'.format(urlFileName, '' if urlExt.startswith('.') else '.', urlExt)
                            try:
                                createPath = os.path.dirname(output_file)
                                if (not os.path.exists(createPath)):
                                    makedirs(createPath)
                            except Exception as e:
                                self.message(str(e), self._base.const_critical_text)
                args.append(inputRaster)
                useCOGTIFF = self.m_user_config.getValue('cog') == True
                if (useCOGTIFF):
                    output_file += CloudOGTIFFExt
                useVsimem = self._base.getBooleanValue(self.m_user_config.getValue('vsimem'))
                args.append('"{}{}"'.format('/vsimem/' if useVsimem else '', output_file))
                self.message('Applying compression (%s)' % (useTokenPath if useTokenPath else input_file))
                ret = self._call_external(args, name=timeIt, method=TimeIt.Conversion, store=self._base)
                self.message('Status: (%s).' % ('OK' if ret else 'FAILED'))
                if (not ret):
                    if (_rpt):
                        _rpt.updateRecordStatus(_input_file, CRPT_PROCESSED, CRPT_NO)
                    return ret
            if (build_pyramids):        # build pyramids is always turned off for rasters that belong to (.til) files.
                if (self._base.getBooleanValue(do_pyramids) or     # accept any valid boolean value.
                    do_pyramids == CCMD_PYRAMIDS_ONLY or
                        do_pyramids == CCMD_PYRAMIDS_EXTERNAL):
                    iss3 = self.m_user_config.getValue('iss3')
                    if (iss3 and do_pyramids == CCMD_PYRAMIDS_ONLY):
                        if (do_pyramids != CCMD_PYRAMIDS_ONLY):     # s3->(local)->.ovr
                            input_file = output_file
                        output_file = output_file + '.__vrt__'
                        self.message('BuildVrt (%s=>%s)' % (input_file, output_file))
                        ret = self.buildMultibandVRT([input_file], output_file)
                        self.message('Status: (%s).' % ('OK' if ret else 'FAILED'))
                        if (not ret):
                            if (_rpt):
                                _rpt.updateRecordStatus(_input_file, CRPT_PROCESSED, CRPT_NO)
                            return ret  # we can't proceed if vrt couldn't be built successfully.
                    kwargs['source'] = timeIt  # input_file
                    ret = self.createaOverview('"{}"'.format(output_file), **kwargs)
                    self.message('Status: (%s).' % ('OK' if ret else 'FAILED'), self._base.const_general_text if ret else self._base.const_critical_text)
                    if (not ret):
                        if (_rpt):
                            _rpt.updateRecordStatus(_input_file, CRPT_PROCESSED, CRPT_NO)
                        return False
                    if (iss3 and
                            do_pyramids == CCMD_PYRAMIDS_ONLY):
                        try:
                            os.remove(output_file)      # *.ext__or__ temp vrt file.
                            in_ = output_file + '.ovr'
                            out_ = in_.replace('.__vrt__' + '.ovr', '.ovr')
                            if (os.path.exists(out_)):
                                os.remove(out_)         # probably leftover from a previous instance.
                            self.message('rename (%s=>%s)' % (in_, out_))
                            os.rename(in_, out_)
                        except BaseException:
                            self.message('Unable to rename/remove (%s)' % (output_file), self._base.const_warning_text)
                            if (_rpt):
                                _rpt.updateRecordStatus(_input_file, CRPT_PROCESSED, CRPT_NO)
                            return False
                    if (useCOGTIFF and
                            not isRasterProxyCaller):
                        inputDeflated = output_file
                        output_file = output_file.replace(CloudOGTIFFExt, '')
                        compression = self.m_user_config.getValue('Compression')
                        CompressPrefix = 'COMPRESS='
                        x = [x.startswith(CompressPrefix) for x in args]
                        posCompression = x.index(True)
                        args[posCompression] = '{}{}'.format(CompressPrefix, compression)
                        args.pop()  # prev / output
                        args.pop()  # prev / input
                        if (compression == 'jpeg'):
                            args.append('-co')
                            args.append('PHOTOMETRIC=YCBCR')
                            QualityPrefix = 'JPEG_QUALITY='
                            x = [x.startswith(QualityPrefix) for x in args]
                            posQuality = -1
                            cfgJpegQuality = self.m_user_config.getValue('Quality')
                            if (cfgJpegQuality is None):
                                cfgJpegQuality = DefJpegQuality
                            if (x and
                                    True in x):
                                posQuality = x.index(True)
                            if (posQuality == -1):
                                args.append('-co')
                                args.append('{}{}'.format(QualityPrefix, cfgJpegQuality))
                            else:
                                args[posQuality] = '{}{}'.format(QualityPrefix, cfgJpegQuality)
                        args.append('-co')
                        args.append('COPY_SRC_OVERVIEWS=YES')
                        args.append(inputDeflated)
                        args.append(output_file)
                        self.message('Creating cloud optimized GeoTIFF (%s)' % (output_file))
                        # remove any user defined GDAL translate parameters when calling GDAL_Translate for the second time to generate COG rasters.
                        jstr = ' '.join(args)
                        userGdalParameters = self.m_user_config.getValue('GDAL_Translate_UserParameters')
                        if (userGdalParameters):
                            x = jstr.find(userGdalParameters)
                            if (x != -1):
                                jstr = jstr.replace(userGdalParameters, '')
                        # ends
                        args = jstr.split()
                        ret = self._call_external(args)
                        try:
                            os.remove(inputDeflated)
                        except BaseException:
                            self.message('Unable to delete the temporary file at ({})'.format(inputDeflated), self._base.const_warning_text)
                        self.message('Status: (%s).' % ('OK' if ret else 'FAILED'))
                        if (not ret):
                            if (_rpt):
                                _rpt.updateRecordStatus(_input_file, CRPT_PROCESSED, CRPT_NO)
                            return ret
        # Do we auto generate raster proxy files as part of the raster conversion process?
        if (self.m_user_config.getValue(CCLONE_PATH)):
            mode = self.m_user_config.getValue('Mode')
            modifyProxy = True
            RecursiveCall = 'recursiveCall'
            if (not mode.endswith('mrf') and
                    RecursiveCall not in kwargs):
                rasterProxyPath = os.path.join(self.m_user_config.getValue(CCLONE_PATH, False), os.path.basename(output_file))
                ret = self.compress(output_file, rasterProxyPath, args_Callback_for_meta,
                                    post_processing_callback=None, updateOrjobStatus=False, createOverviews=False, recursiveCall=True, **kwargs)
                errorEntries = RasterAssociates.removeRasterProxyAncillaryFiles(rasterProxyPath)
                if (errorEntries):
                    for err in errorEntries:
                        self.message('Unable to delete ({})'.format(err), self._base.const_warning_text)
                modifyProxy = False
            if (modifyProxy):
                updateMRF = UpdateMRF(self._base)
                _output_home_path = self.m_user_config.getValue(CCFG_PRIVATE_OUTPUT, False)  # cmdline arg -output
                _tempOutputPath = self.m_user_config.getValue(CTEMPOUTPUT, False)
                if (_tempOutputPath):
                    _output_home_path = _tempOutputPath
                if (RecursiveCall in kwargs):
                    _output_home_path = output_file = os.path.join(self.m_user_config.getValue(CCLONE_PATH, False), os.path.basename(output_file))
                if (updateMRF.init(output_file, self.m_user_config.getValue(CCLONE_PATH, False), mode,
                                   self.m_user_config.getValue(CCACHE_PATH, False), _output_home_path, self.m_user_config.getValue(COUT_VSICURL_PREFIX, False))):
                    updateMRF.copyInputMRFFilesToOutput()
        # ends
        # call any user-defined fnc for any post-processings.
        if (post_processing_callback):
            if (self._base.getBooleanValue(self.m_user_config.getValue(CCLOUD_UPLOAD))):
                self.message('[{}-Push]..'.format(self.m_user_config.getValue(COUT_CLOUD_TYPE).capitalize()))
            ret = post_processing_callback(post_process_output, post_processing_callback_args, input=os.path.basename(input_file), f=post_process_output, cfg=self.m_user_config)
            self.message('Status: (%s).' % ('OK' if ret else 'FAILED'))
        # ends
        if (_rpt and
                _rpt.operation != COP_UPL):
            if (til and
                    _input_file.lower().endswith(CTIL_EXTENSION_)):
                originalSourcePath = til.findOriginalSourcePath(_input_file)
                if (originalSourcePath is not None):
                    _rpt.updateRecordStatus(originalSourcePath, CRPT_PROCESSED, CRPT_YES)
                    for TilRaster in til._tils_info[originalSourcePath.lower()][TIL.CKEY_FILES]:
                        _rpt.updateRecordStatus(self._base.convertToForwardSlash(os.path.dirname(originalSourcePath), True) + TilRaster, CRPT_PROCESSED, CRPT_YES)
                return ret
            if (isRasterProxyCaller):
                return ret
            _rpt.updateRecordStatus(_input_file, CRPT_PROCESSED, CRPT_YES)
        return ret

    def createaOverview(self, input_file, isBQA=False, **kwargs):
        if (CreateOverviews in kwargs):
            if (not kwargs[CreateOverviews]):
                return True  # skip if called by a create raster proxy operation.
        pyFactor = '2'
        pySampling = 'average'
        mode = self.m_user_config.getValue('Mode')
        if (mode):
            if (mode == 'cachingmrf' or
                mode == 'clonemrf' or
                    mode == 'rasterproxy' or
                    mode == 'splitmrf'):
                return True
        # skip pyramid creation on (tiffs) related to (til) files.
        if (til):
            (p, n) = os.path.split(input_file)
            if (til.find(n)):
                return True
        # ends
        # skip pyramid creation for (.ecw) files.
        if (input_file.lower().endswith('.ecw')):
            return True
        # ends
        self.message('Creating pyramid ({})'.format(input_file))
        # let's input cfg values..
        pyFactor_ = self.m_user_config.getValue('PyramidFactor')
        if (pyFactor_ and
                pyFactor_.strip()):
            pyFactor = pyFactor_.replace(',', ' ')  # can be commna sep vals in the cfg file.
        else:
            gdalInfo = GDALInfo(self._base, self.message)
            gdalInfo.init(self.m_gdal_path)
            if (gdalInfo.process(input_file)):
                pyFactor = gdalInfo.pyramidLevels
                if (not pyFactor):
                    self.message('Pyramid creation skipped for file ({}). Image size too small.'.format(input_file), const_warning_text)
                    return True
        pySampling_ = self.m_user_config.getValue('PyramidSampling')
        if (pySampling_):
            pySampling = pySampling_
            if (pySampling.lower() == 'avg' and
                    input_file.lower().endswith(CTIL_EXTENSION_)):
                pySampling = 'average'
        pyCompression = self.m_user_config.getValue('PyramidCompression')
        args = [os.path.join(self.m_gdal_path, self.CGDAL_ADDO_EXE)]
        args.append('-r')
        args.append('nearest' if isBQA else pySampling)
        pyQuality = self.m_user_config.getValue('Quality')
        pyInterleave = self.m_user_config.getValue(CCFG_INTERLEAVE)
        if (pyCompression == 'jpeg' or
                pyCompression == 'png'):
            if (not mode.startswith('mrf')):
                pyExternal = False
                pyExternal_ = self.m_user_config.getValue('Pyramids')
                if (pyExternal_):
                    pyExternal = pyExternal_ == CCMD_PYRAMIDS_EXTERNAL
                if (pyExternal):
                    args.append('-ro')
                if (mode.startswith('tif') and
                    pyCompression == 'jpeg' and
                        pyInterleave == 'pixel'):
                    args.append('--config')
                    args.append('PHOTOMETRIC_OVERVIEW')
                    args.append('YCBCR')
            args.append('--config')
            args.append('COMPRESS_OVERVIEW')
            args.append(pyCompression)
            args.append('--config')
            args.append('INTERLEAVE_OVERVIEW')
            args.append(pyInterleave)
            args.append('--config')
            args.append('JPEG_QUALITY_OVERVIEW')
            args.append(pyQuality)
        args.append(input_file)
        pyFactors = pyFactor.split()
        for f in pyFactors:
            args.append(f)
        sourcePath = input_file
        if ('source' in kwargs):
            sourcePath = kwargs['source']
        return self._call_external(args, name=sourcePath, method=TimeIt.Overview, store=self._base)

    @TimeIt.timeOperation
    def _call_external(self, args, messageCallback=None, **kwargs):
        if (CRUN_IN_AWSLAMBDA):
            tmpELF = '/tmp/{}'.format(os.path.basename(args[0]))
            args[0] = tmpELF
        p = subprocess.Popen(' '.join(args), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        message = ''
        messages = []
        val = p.poll()
        while (val is None):
            time.sleep(0.5)
            val = p.poll()
            message = p.stdout.readline()
            if (message):
                messages.append(message.strip())
        if (messages):
            self.message('messages:')
            for m in messages:
                self.message(m)
        if (not p.stderr):
            return True
        warnings = p.stderr.readlines()
        if (warnings):
            self.message('warnings/errors:')
            is_error = False
            for w in warnings:
                w = w.strip()
                if (isinstance(w, bytes)):
                    w = bytes.decode(w)
                if (not is_error):
                    if (w.find('ERROR') >= 0):
                        is_error = True
                        if (w.find('ECW') >= 0 and
                                self._base.isLinux()):   # temp fix to get rid of (no version information available) warnings for .so under linux
                            is_error = False
                self.message(w)
                if (messageCallback):
                    messageCallback(w)
            if (is_error):
                return False
        return True


class BundleMaker(Compression):

    CBUNDLEMAKER_BIN = 'BundleMaker'
    CPROJ4SO = 'libproj.so'
    CMODE = 'bundle'
    CLEVEL = 'level'

    def __init__(self, inputRaster, *args, **kwargs):
        super(BundleMaker, self).__init__(*args, **kwargs)
        self.inputRaster = inputRaster
        self.bundleName = None  # 'Rae80C11080'
        self.level = None

    def init(self):
        if (not super(BundleMaker, self).init()):
            return False
        self.homePath = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'BundleMaker')
        if (CRUN_IN_AWSLAMBDA):
            _tmp = '/tmp/{}'.format(self.CBUNDLEMAKER_BIN)
            if (not self._base.copyBinaryToTmp(os.path.join(self.homePath, self.CBUNDLEMAKER_BIN), _tmp)):
                return False
            if (not self._lambdaCopySharedSO(self.CPROJ4SO)):
                return False
            self.homePath = _tmp
        return True

    def _messageCallback(self, msg):
        if (self.level is None and
                msg.startswith('Output at level')):
            self.level = msg[msg.rfind(' ') + 1:]

    def run(self):
        _resumeReporter = self._base.getUserConfiguration.getValue(CPRT_HANDLER)
        if (not _resumeReporter or
            (_resumeReporter and
                (Report.CHDR_TEMPOUTPUT not in _resumeReporter._header or
                 self.CMODE not in _resumeReporter._header or
                 self.CLEVEL not in _resumeReporter._header))):
            return False
        self.bundleName = _resumeReporter._header[self.CMODE]
        self.level = _resumeReporter._header[self.CLEVEL]
        args = [self.homePath if CRUN_IN_AWSLAMBDA else os.path.join(self.homePath, '{}.exe'.format(self.CBUNDLEMAKER_BIN, '.exe')),
                '-level', self.level, '-bundle', self.bundleName, self.inputRaster, _resumeReporter._header[Report.CHDR_TEMPOUTPUT]]
        self.message('BundleMaker> ({})'.format(self.inputRaster))
        ret = self._call_external(args)
        if (not ret or
                not self.level):
            return ret
        if (self._base.getBooleanValue(_resumeReporter._header[Report.CHDR_CLOUDUPLOAD])):
            self._base.S3Upl(os.path.join(_resumeReporter._header[Report.CHDR_TEMPOUTPUT], '_alllayers/L{}/{}.mrf'.format(self.level, self.bundleName)), None)
        return True


class Config:

    def __init__(self):
        pass

    def init(self, config, root):
        try:
            self.m_doc = minidom.parse(config)
        except BaseException:
            return False
        nodes = self.m_doc.getElementsByTagName(root)
        if (len(nodes) == 0):
            return False
        node = nodes[0].firstChild
        self.m_cfgs = {}
        while (node):
            if (not node.hasChildNodes()):
                node = node.nextSibling
                continue
            if (not (node.nodeName in self.m_cfgs)):
                self.m_cfgs[node.nodeName] = node.firstChild.nodeValue
            node = node.nextSibling
            pass
        return True

    def getValue(self, key, toLower=True):  # returns (value) or None
        if (key in self.m_cfgs):
            if (toLower):
                try:    # trap any non-strings
                    return self.m_cfgs[key].lower()
                except BaseException:
                    pass
            return self.m_cfgs[key]
        return None

    def setValue(self, key, value):
        if (key in self.m_cfgs):
            if (hasattr(self.m_cfgs[key], '__setitem__')):
                self.m_cfgs[key].append(value)
                return
        self.m_cfgs[key] = value


def getInputOutput(inputfldr, outputfldr, file, isinput_s3):
    input_file = os.path.join(inputfldr, file)
    output_file = os.path.join(outputfldr, file)
    ifile_toLower = input_file.lower()
    if (ifile_toLower.startswith('http://') or
            ifile_toLower.startswith('https://')):
        cfg.setValue(CIN_S3_PREFIX, '/vsicurl/')
        input_file = input_file.replace('\\', '/')
        isinput_s3 = True
    if (isinput_s3):
        azSAS = cfg.getValue(CFGAZSAS, False)
        input_file = '{}{}{}'.format(cfg.getValue(CIN_S3_PREFIX, False), input_file, '?' + azSAS if azSAS else '')
        output_file = outputfldr
        if (getBooleanValue(cfg.getValue(CISTEMPINPUT)) or
                getBooleanValue(cfg.getValue(CISTEMPOUTPUT))):
            output_file = os.path.join(output_file, file)
            if (getBooleanValue(cfg.getValue(CISTEMPINPUT))):
                input_file = os.path.join(cfg.getValue(CTEMPINPUT, False), file)
            if (getBooleanValue(cfg.getValue(CISTEMPOUTPUT))):
                tempOutput = cfg.getValue(CTEMPOUTPUT, False)
                _file = file
                if (output_file.startswith(tempOutput)):     # http source raster entries without -tempinput will have subfolder info in (output_file)
                    _file = output_file.replace(tempOutput, '')
                output_file = os.path.join(cfg.getValue(CTEMPOUTPUT, False), _file)
            return (input_file, output_file)
        output_file = os.path.join(output_file, file)
    return (input_file, output_file)


def getBooleanValue(value):
    if (value is None):
        return False
    if (isinstance(value, bool)):
        return value
    val = value.lower()
    if (val == 'true' or
        val == 'yes' or
        val == 't' or
        val == '1' or
            val == 'y'):
        return True
    return False


def formatExtensions(value):
    if (value is None or
            len(value.strip()) == 0):
        return []
    frmts = value.split(',')
    for i in range(0, len(frmts)):
        frmts[i] = frmts[i].strip()
    return frmts

# custom exit code block to write out logs


def terminate(objBase, exit_code, log_category=False):
    if (objBase):
        success = 'OK'
        if (exit_code != 0):
            success = 'Failed!'
        objBase.message('[{}]'.format(success), objBase.const_status_text)
        if (log_category):
            log.CloseCategory()
        objBase.close()  # persist information/errors collected.
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
        g_rpt.addFile(_src)
        return True
    except BaseException:
        pass
    return False


def fn_pre_process_copy_default(src, dst, arg):
    if (fn_collect_input_files(src)):
        return False             # just gathering information for the report either (op=report). Do not proceed with (Copying/e.t.c)
    if (not src):
        return False
    if (til):
        if (src.lower().endswith(CTIL_EXTENSION_)):
            til.setOutputPath(src, dst)
    return True


def fn_copy_temp_dst(input_source, cb_args, **kwargs):
    fn_cpy_ = Copy()
    file_lst = fn_cpy_.get_group_filelist(input_source)
    if (len(file_lst) == 0):
        return False    # no copying.
    files = []
    for file in file_lst:
        (p, f) = os.path.split(file.replace('\\', '/'))
        if (kwargs is not None):
            if (isinstance(kwargs, dict)):
                if (('cfg' in kwargs)):
                    if (not getBooleanValue(kwargs['cfg'].getValue(CISTEMPOUTPUT))):
                        return False    # no copying..
                    p += '/'
                    t = kwargs['cfg'].getValue(CTEMPOUTPUT, False).replace('\\', '/')    # safety check
                    if (not t.endswith('/')):  # making sure, replace will work fine.
                        t += '/'
                    o = kwargs['cfg'].getValue(CCFG_PRIVATE_OUTPUT, False).replace('\\', '/')  # safety check
                    if (not o.endswith('/')):
                        o += '/'
                    dst = (p.replace(t, o))
                    files.append({'src': p, 'dst': dst, 'f': f})
    if (len(files) != 0):
        fn_cpy_.batch(files, {'mode': 'move'}, None)
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
            _return_str = _return_str[:len(_return_str) - 1]
        return _return_str


def makedirs(filepath):
    try:
        os.makedirs(filepath)
    except Exception as e:
        if (e.errno == os.errno.EEXIST):     # filepath already exists
            return
        raise


class Application(object):
    __program_ver__ = 'v2.0.5f'
    __program_date__ = '20190613'
    __program_name__ = 'OptimizeRasters.py {}/{}'.format(__program_ver__, __program_date__)
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

    def __load_config__(self, config):
        global cfg
        if (self._args is None):
            return False
        # read in the config file.
        if (not self._args.config):
            self._args.config = os.path.abspath(os.path.join(os.path.dirname(__file__), CCFG_FILE))
        config_ = self._args.config
        if (self._args.input and            # Pick up the config file name from a (resume) job file.
                self._args.input.lower().endswith(Report.CJOB_EXT)):
            _r = Report(Base())
            if (not _r.init(self._args.input)):
                self.writeToConsole('Err. ({})/init'.format(self._args.input))
                return False
            self.writeToConsole('ORJob> Reading/Preparing data. Please wait..')  # Big .orjob files can take some time.
            if (not _r.read()):
                self.writeToConsole('Err. ({})/read'.format(self._args.input))
                return False
            self.writeToConsole('ORJob> Done.')  # msg end of read.
            if (CRPT_HEADER_KEY in _r._header):
                config_ = _r._header[CRPT_HEADER_KEY]
            if (Report.CHDR_MODE in _r._header):
                self._args.mode = _r._header[Report.CHDR_MODE]  # mode in .orjob has priority over the template <Mode> value.
            if (Report.CHDR_OP in _r._header):
                self._args.op = _r._header[Report.CHDR_OP]
            _r = None
        self._args.config = os.path.abspath(config_)         # replace/force the original path to abspath.
        cfg = Config()
        ret = cfg.init(config_, 'Defaults')
        if (not ret):
            msg = 'Err. Unable to read-in settings from ({})'.format(config_)
            self.writeToConsole(msg, const_critical_text)   # log file is not up yet, write to (console)
            return False
        # ends
        # deal with cfg extensions (rasters/exclude list)
        opCopyOnly = False
        operation = cfg.getValue(COP)
        if (not operation):
            operation = self._args.op
        if (operation):
            opCopyOnly = operation == COP_COPYONLY  # no defaults for (CCFG_RASTERS_NODE, CCFG_EXCLUDE_NODE) if op={COP_COPYONLY}
        rasters_ext_ = cfg.getValue(CCFG_RASTERS_NODE, False)
        if (rasters_ext_ is None and
                not opCopyOnly):
            rasters_ext_ = 'tif,mrf'        # defaults: in-code if defaults are missing in cfg file.
        exclude_ext_ = cfg.getValue(CCFG_EXCLUDE_NODE, False)
        if (exclude_ext_ is None and
                not opCopyOnly):
            exclude_ext_ = 'ovr,rrd,aux.xml,idx,lrc,mrf_cache,pjp,ppng,pft,pzp,pjg'  # defaults: in-code if defaults are missing in cfg file.
        cfg.setValue(CCFG_RASTERS_NODE, [] if opCopyOnly else formatExtensions(rasters_ext_))   # {CCFG_RASTERS_NODE} entries not allowed for op={COP_COPYONLY}
        cfg.setValue(CCFG_EXCLUDE_NODE, formatExtensions(exclude_ext_))
        cfg.setValue('cmdline', self._args)
        # ends
        # init -mode
        # cfg-init-valid modes
        cfg_modes = {
            'tif',
            'tif_lzw',
            'tif_jpeg',
            'tif_cog',
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
            'rasterproxy',
            'splitmrf',
            BundleMaker.CMODE
        }
        # ends
        # read-in (-mode)
        cfg_mode = self._args.mode     # cmd-line -mode overrides the cfg value.
        if (cfg_mode is None):
            cfg_mode = cfg.getValue('Mode')
        if (cfg_mode is None or
                (not cfg_mode.lower() in cfg_modes)):
            Message('<Mode> value not set/illegal ({})'.format(str(cfg_mode)), Base.const_critical_text)
            return False
        cfg_mode = cfg_mode.lower()
        if (cfg_mode == 'tif_cog'):  # suffix for creating cloud optimized geoTiffs
            cfg.setValue('cog', True)
            cfg_mode = 'tif'    # reset mode
        cfg.setValue('Mode', cfg_mode)
        # ends
        return True

    def __setup_log_support(self):
        log = None
        try:
            solutionLib_path = os.path.realpath(__file__)
            if (not os.path.isdir(solutionLib_path)):
                solutionLib_path = os.path.dirname(solutionLib_path)
            _CLOG_FOLDER = 'logs'
            self._log_path = os.path.join(solutionLib_path, _CLOG_FOLDER)
            sys.path.append(os.path.join(solutionLib_path, 'SolutionsLog'))
            import logger
            log = logger.Logger()
            log.Project('OptimizeRasters')
            log.LogNamePrefix('OR')
            log.StartLog()
            cfg_log_path = cfg.getValue('LogPath')
            if (cfg_log_path):
                if (not os.path.isdir(cfg_log_path)):
                    Message('Invalid log-path (%s). Resetting to (%s)' % (cfg_log_path, self._log_path))
                    cfg_log_path = None
            if (cfg_log_path):
                self._log_path = os.path.join(cfg_log_path, _CLOG_FOLDER)
            log.SetLogFolder(self._log_path)
            print ('Log-path set to ({})'.format(self._log_path))
        except Exception as e:
            print ('Warning: External logging support disabled! ({})'.format(str(e)))
        # ends
        # let's write to log (input config file content plus all cmd-line args)
        if (log):
            log.Message('version={}/{}'.format(Application.__program_ver__, Application.__program_date__), const_general_text)
            # inject cmd-line
            log.CreateCategory('Cmd-line')
            cmd_line = []
            _args_text = str(self._args).replace('Namespace(', '').replace('\\\\', '/')
            _args_text_len = len(_args_text)
            _args = _args_text[:_args_text_len - 1 if _args_text[-1:] == ')' else _args_text_len].split(',')
            for arg in _args:
                try:
                    (k, v) = arg.split('=')
                except BaseException:
                    log.Message('Invalid arg at cmd-line (%s)' % (arg.strip()), const_critical_text)
                    continue
                if (v != 'None'):
                    cmd_line.append('-{}'.format(arg.replace('\'', '"').strip()))
            log.Message(' '.join(cmd_line), const_general_text)
            log.CloseCategory()
            # ends
            # inject cfg content
            log.CreateCategory('Input-config-values')
            for v in cfg.m_cfgs:
                if (v == 'cmdline'):
                    continue
                log.Message('%s=%s' % (v, cfg.m_cfgs[v]), const_general_text)
            log.CloseCategory()
            # ends
        return Base(log, self._msg_callback, cfg)

    def writeToConsole(self, msg, status=const_general_text):
        if (self._msg_callback):
            return (self._msg_callback(msg, status))
        print (msg)          # log file is not up yet, write to (console)
        return True

    @property
    def configuration(self):
        if (self._base is None):
            return None
        return self._base.getUserConfiguration.m_cfgs

    @configuration.setter
    def configuration(self, value):
        self._base.getUserConfiguration.m_cfgs = value
        if (_rpt):
            if (COP_RPT in value):
                _rpt._header = value[COP_RPT]._header
                _rpt.write()

    def getReport(self):
        global _rpt
        if (_rpt):
            return _rpt
        storeOp = self._args.op
        self._args.op = COP_CREATEJOB
        result = self.run()
        if (not result):
            self._args.op = storeOp
            return None
        newOrJobFile = os.path.join(os.path.dirname(__file__), cfg.getValue(CPRJ_NAME, False)) + Report.CJOB_EXT
        self._args.input = newOrJobFile     # skip reinitialiaztion, change the input to point the newly created .orjob file.
        self._args.op = storeOp
        return _rpt if _rpt else None

    def init(self):
        global _rpt, \
            cfg, \
            til
        self.writeToConsole(self.__program_name__)
        self.writeToConsole(self.__program_desc__)
        _rpt = cfg = til = None
        if (not self._usr_args):
            return False
        if (isinstance(self._usr_args, argparse.Namespace)):
            self._args = self._usr_args
        else:
            self._args = Args()
            for i in self._usr_args:
                try:
                    self._args.__setattr__(i, self._usr_args[i])
                except BaseException:
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
            self._args.input.lower().endswith(Report.CJOB_EXT) and
                os.path.isfile(self._args.input)):
            _rpt = Report(self._base)
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
            if (CRESUME_HDR_OUTPUT in self._usr_args):
                # override the output path in the .orjob file if a custom 'output' path exists.
                if (isinstance(self._usr_args, dict)):  # do only if called by user code. self._usr_args type is 'argparse' when called by cmd-line
                    userOutput = self._base.convertToForwardSlash(self._usr_args[CRESUME_HDR_OUTPUT])
                    self._base.getUserConfiguration.setValue(CCFG_PRIVATE_OUTPUT, userOutput)
                    self._args.output = userOutput
                # ends
        self._base.getUserConfiguration.setValue(CPRT_HANDLER, _rpt)
        # verify user defined text for cloud output path
        usrPath = self._args.hashkey
        if (usrPath):
            usrPathPos = CHASH_DEF_INSERT_POS  # default insert pos
            _s = usrPath.split(CHASH_DEF_SPLIT_CHAR)
            if (len(_s) == 2):
                if (not _s[0]):
                    _s[0] = CHASH_DEF_CHAR
                usrPath = _s[0]
                if (len(_s) > 1):
                    try:
                        usrPathPos = int(_s[1])
                        if (int(_s[1]) < CHASH_DEF_INSERT_POS):
                            usrPathPos = CHASH_DEF_INSERT_POS
                    except BaseException:
                        pass
            self._base.getUserConfiguration.setValue(CUSR_TEXT_IN_PATH, '{}{}{}'.format(usrPath, CHASH_DEF_SPLIT_CHAR, usrPathPos))
        # ends
        # do we need to process (til) files?
        if ('til' in [x.lower() for x in self._base.getUserConfiguration.getValue(CCFG_RASTERS_NODE)]):
            til = TIL()
            if (self._base.getBooleanValue(self._base.getUserConfiguration.getValue(CDEFAULT_TIL_PROCESSING))):
                til.defaultTILProcessing = True
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
                _hdr.pop(0)
                _val = '='.join(_hdr).strip()
                if (_key == CRESUME_HDR_INPUT):
                    return True
                setattr(self._args, _key, _val)
        return True

    def __initOperationCreateJob(self):
        global _rpt
        _rpt = Report(self._base)
        createdOrjob = cfg.getValue(CPRJ_NAME, False)
        if (not createdOrjob.lower().endswith(Report.CJOB_EXT)):
            createdOrjob += Report.CJOB_EXT
        if (not _rpt.init(os.path.join(os.path.dirname(os.path.abspath(__file__)), createdOrjob)) or
                not _rpt.read()):        # not checked for return.
            self._base.message('Unable to init/read (Report/job/op/createJob)', self._base.const_critical_text)
            return False
        return True

    @property
    def isOperationCreateJob(self):
        if (self._args.op and
                self._args.op == COP_CREATEJOB):    # note (op=={COP_CREATEJOB} is ignored if resume == {CRESUME_ARG_VAL_RETRYALL}
            if (_rpt):
                if (CRESUME_ARG in _rpt._header and
                        _rpt._header[CRESUME_ARG].lower() == CRESUME_ARG_VAL_RETRYALL):
                    return False
            return True
        return False

    def _isLambdaJob(self):
        if (CRUN_IN_AWSLAMBDA):
            return False
        if (self._args.op and
                self._args.op.startswith(COP_LAMBDA)):
            if (not self._base.getBooleanValue(self._args.clouddownload)):
                _resumeReporter = self._base.getUserConfiguration.getValue(CPRT_HANDLER)
                if (_resumeReporter and
                        not _resumeReporter._isInputHTTP):
                    return False
            if (self._base.getBooleanValue(self._args.cloudupload)):
                return True
        return False

    def _runLambdaJob(self, jobFile):
        # process @ lambda
        self._base.message('Using AWS Lambda..')
        sns = Lambda(self._base)
        if (not sns.initSNS('aws_lambda')):
            self._base.message('Unable to initialize', self._base.const_critical_text)
            return False
        if (not sns.submitJob(jobFile)):
            self._base.message('Unable to submit job.', self._base.const_critical_text)
            return False
        return True
        # ends

    def run(self):
        global raster_buff, \
            til, \
            cfg, \
            _rpt, \
            g_rpt, \
            g_is_generate_report, \
            user_args_Callback, \
            S3_storage, \
            azure_storage, \
            google_storage

        S3_storage = None
        azure_storage = None
        google_storage = None

        g_rpt = None
        raster_buff = []
        g_is_generate_report = False

        CRESUME_CREATE_JOB_TEXT = '[Resume] Creating job ({})'

        # is resume?
        if (self._args.input and
            self._args.input.lower().endswith(Report.CJOB_EXT) and
                os.path.isfile(self._args.input)):
            _rpt = Report(self._base)
            if (not _rpt.init(self._args.input)):        # not checked for return.
                self._base.message('Unable to init (Reporter/obj)', self._base.const_critical_text)
                return(terminate(self._base, eFAIL))
            if (not _rpt.read()):
                self._base.message('Unable to read the -input report file ({})'.format(self._args.input), self._base.const_critical_text)
                return(terminate(self._base, eFAIL))
            self._args.job = os.path.basename(self._args.input)
            self._base.getUserConfiguration.setValue(CPRT_HANDLER, _rpt)
        # ends

        # Get the default (project name)
        project_name = self._args.job
        if (project_name and
                project_name.lower().endswith(Report.CJOB_EXT)):
            project_name = project_name[:len(project_name) - len(Report.CJOB_EXT)]
        if (not project_name):
            project_name = cfg.getValue(CPRJ_NAME, False)
        if (not project_name):      # is the project still null?
            project_name = Report.getUniqueFileName()  # 'OptimizeRasters'
        if (self._base.getMessageHandler):
            self._base.getMessageHandler.LogNamePrefix(project_name)           # update (log) file name prefix.
        cfg.setValue(CPRJ_NAME, project_name)
        _project_path = '{}{}'.format(os.path.join(os.path.dirname(self._args.input if self._args.input and self._args.input.lower().endswith(Report.CJOB_EXT) else __file__), project_name), Report.CJOB_EXT)
        if (not cfg.getValue(CLOAD_RESTORE_POINT)):
            if (os.path.exists(_project_path)):
                if (self.isOperationCreateJob):  # .orobs with -op={createJob} can't be run.
                    self._base.message('{} Job ({}) already exists!'.format(CRESUME_MSG_PREFIX, _project_path))
                    return True
                # process @ lambda?
                if (self._isLambdaJob()):
                    return(terminate(self._base, eOK if self._runLambdaJob(_project_path) else eFAIL))
                # ends
                self._args.op = None
                self._args.input = _project_path
                cfg.setValue(CLOAD_RESTORE_POINT, True)
                self._base.message('{} Using job ({})'.format(CRESUME_MSG_PREFIX, _project_path))
                _status = self.run()
                return
        # ends
        # detect input cloud type
        cloudDownloadType = self._args.clouddownloadtype
        if (not cloudDownloadType):
            cloudDownloadType = cfg.getValue(CIN_CLOUD_TYPE, True)
        inAmazon = cloudDownloadType == CCLOUD_AMAZON or not cloudDownloadType
        if (inAmazon):
            cloudDownloadType = Store.TypeAmazon
        cfg.setValue(CIN_CLOUD_TYPE, cloudDownloadType)
        # ends
        # are we doing input from S3|Azure?
        isinput_s3 = self._base.getBooleanValue(self._args.s3input)
        if (self._args.clouddownload):
            isinput_s3 = self._base.getBooleanValue(self._args.clouddownload)
        # ends
        # let's create a restore point
        if (not self._args.input or        # assume it's a folder from s3/azure
            (self._args.input and
             not os.path.isfile(self._args.input))):
            if (not self._args.op):
                self._args.op = COP_RPT
        # valid (op/utility) commands
        _utility = {
            COP_UPL: None,
            COP_DNL: None,
            COP_RPT: None,
            COP_NOCONVERT: None,
            COP_LAMBDA: None,
            COP_COPYONLY: None,
            COP_CREATEJOB: None
        }
        # ends
        # op={COP_COPYONLY} check
        if (self._args.op == COP_RPT):
            opKey = cfg.getValue(COP)
            if (opKey == COP_COPYONLY):
                if (self._args.cloudupload or
                        self._args.s3output):
                    self._args.op = COP_UPL  # conditions will enable local->local copy if -cloudupload is (false)
                else:
                    self._args.tempoutput = None  # -tempoutput is disabled if -cloudupload=false and -op={COP_COPYONLY}
                if (isinput_s3):
                    self._args.op = COP_NOCONVERT
                    cfg.setValue(COUT_DELETE_AFTER_UPLOAD, True)    # Delete temporary files in (local) transit for (op={COP_COPYONLY}) if the input source is from (cloud).
                    # However, If the input (source) path is from the local machine, the config value in (COUT_DELETE_AFTER_UPLOAD) is used.
        # ends
        if (self._args.op):
            splt = self._args.op.split(':')
            splt[0] = splt[0].lower()
            self._args.op = ':'.join(splt)
            if (splt[0] not in _utility):   # -op arg can have multiple init values separated by ':', e.g. -op lambda:function:xyz
                self._base.message('Invalid utility operation mode ({})'.format(self._args.op), self._base.const_critical_text)
                return(terminate(self._base, eFAIL))
            if(self._args.op == COP_RPT or
                    self._args.op == COP_UPL or
                    self._args.op == COP_NOCONVERT or
                    self._args.op == COP_COPYONLY or
                    self._args.op == COP_CREATEJOB or
                    self._args.op.startswith(COP_LAMBDA)):
                if (self._args.op.startswith(COP_LAMBDA)):
                    isinput_s3 = self._args.clouddownload = self._args.cloudupload = True     # make these cmd-line args (optional) to type at the cmd-line for op={COP_LAMBDA}
                    cfg.setValue(Lambda.queue_length, self._args.queuelength)
                g_rpt = Report(self._base)
                if (not g_rpt.init(_project_path, self._args.input if self._args.input else cfg.getValue(CIN_S3_PARENTFOLDER if inAmazon else CIN_AZURE_PARENTFOLDER, False))):
                    self._base.message('Unable to init (Report)', self._base.const_critical_text)
                    return(terminate(self._base, eFAIL))
                g_is_generate_report = True
                if (self._args.op == COP_UPL):
                    self._args.cloudupload = 'true'
                    self._args.tempoutput = self._args.input if os.path.isdir(self._args.input) else os.path.dirname(self._args.input)
                    if (cfg.getValue(CLOAD_RESTORE_POINT) and
                            _rpt):
                        if (CRESUME_HDR_INPUT not in _rpt._header):
                            return(terminate(self._base, eFAIL))
                        self._args.tempoutput = _rpt._header[CRESUME_HDR_INPUT]
        # read-in <Mode>
        cfg_mode = cfg.getValue('Mode')
        # fix the slashes to force a convention
        if (self._args.input):
            self._args.input = self._base.convertToForwardSlash(self._args.input,
                                                                not (self._args.input.lower().endswith(Report.CJOB_EXT) or cfg_mode == BundleMaker.CMODE))
        if (self._args.output):
            self._args.output = self._base.convertToForwardSlash(self._args.output)
        if (self._args.cache):
            self._args.cache = self._base.convertToForwardSlash(self._args.cache)
        # ends
        # read in (interleave)
        if (cfg.getValue(CCFG_INTERLEAVE) is None):
            cfg.setValue(CCFG_INTERLEAVE, 'PIXEL')
        # ends
        # overwrite (Out_CloudUpload, IncludeSubdirectories) with cmd-line args if defined.
        if (self._args.cloudupload or self._args.s3output):
            cfg.setValue(CCLOUD_UPLOAD, self._base.getBooleanValue(self._args.cloudupload) if self._args.cloudupload else self._base.getBooleanValue(self._args.s3output))
            cfg.setValue(CCLOUD_UPLOAD_OLD_KEY, cfg.getValue(CCLOUD_UPLOAD))
            if (self._args.clouduploadtype):
                self._args.clouduploadtype = self._args.clouduploadtype.lower()
                cfg.setValue(COUT_CLOUD_TYPE, self._args.clouduploadtype)
        is_cloud_upload = self._base.getBooleanValue(cfg.getValue(CCLOUD_UPLOAD)) if cfg.getValue(CCLOUD_UPLOAD) else self._base.getBooleanValue(cfg.getValue(CCLOUD_UPLOAD_OLD_KEY))
        if (is_cloud_upload):
            if (self._args.output and
                    self._args.output.startswith('/')):  # remove any leading '/' for http -output
                self._args.output = self._args.output[1:]
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
                    makedirs(self._args.tempinput)
                except Exception as exp:
                    self._base.message('Unable to create the -tempinput path (%s) [%s]' % (self._args.tempinput, str(exp)), self._base.const_critical_text)
                    return(terminate(self._base, eFAIL))
            is_input_temp = True         # flag flows to deal with -tempinput
            cfg.setValue(CISTEMPINPUT, is_input_temp)
            cfg.setValue(CTEMPINPUT, self._args.tempinput)
        # ends
        # let's setup -tempoutput
        is_output_temp = False
        if (not self._args.tempoutput):
            if (self._args.op and
                    self._args.op.startswith(COP_LAMBDA)):
                self._args.tempoutput = '/tmp/'  # -tempoutput is not required when -cloudupload=true with -op=lambda.
                # This is to suppress warnings or false alarms when reusing the .orjob file without the # -tempoutput key in header with the -clouduplaod=true.
        if (self._args.tempoutput):
            self._args.tempoutput = self._base.convertToForwardSlash(self._args.tempoutput)
            if (not os.path.isdir(self._args.tempoutput)):
                # attempt to create the -tempoutput
                try:
                    if (not self._args.op or
                        (self._args.op and
                         self._args.op != COP_UPL) and
                        self._args.op and
                            not self._args.op.startswith(COP_LAMBDA)):
                        makedirs(self._args.tempoutput)
                except Exception as exp:
                    self._base.message('Unable to create the -tempoutput path (%s)\n[%s]' % (self._args.tempoutput, str(exp)), self._base.const_critical_text)
                    return(terminate(self._base, eFAIL))
                # ends
            is_output_temp = True
            cfg.setValue(CISTEMPOUTPUT, is_output_temp)
            cfg.setValue(CTEMPOUTPUT, self._args.tempoutput)
        # ends
        # import boto modules only when required. This allows users to run the program for only local file operations.
        if ((inAmazon and
             isinput_s3) or
            (getBooleanValue(cfg.getValue(CCLOUD_UPLOAD)) and
             cfg.getValue(COUT_CLOUD_TYPE) == CCLOUD_AMAZON)):
            cfg.setValue(CCFG_PRIVATE_INC_BOTO, True)
            try:
                global boto3
                import boto3
            except BaseException:
                self._base.message('\n%s requires the (boto3) module to run its S3 specific operations. Please install (boto3) for python.' % (self.__program_name__), self._base.const_critical_text)
                return(terminate(self._base, eFAIL))
        # ends
        # take care of missing -input and -output if -clouddownload==True
        # Note/Warning: S3/Azure inputs/outputs are case-sensitive hence wrong (case) could mean no files found on S3/Azure
        if (isinput_s3):
            _cloudInput = self._args.input
            if (not _cloudInput):
                _cloudInput = cfg.getValue(CIN_S3_PARENTFOLDER if inAmazon else CIN_AZURE_PARENTFOLDER, False)
            if (_cloudInput):
                self._args.input = _cloudInput = _cloudInput.strip().replace('\\', '/')
            cfg.setValue(CIN_S3_PARENTFOLDER, _cloudInput)
        if (is_cloud_upload):
            if (not is_output_temp):
                if ((self._args.op and self._args.op != COP_UPL) or
                    not self._args.op and
                    (_rpt and
                     _rpt.operation != COP_UPL)):
                    self._base.message('-tempoutput must be specified if -cloudupload=true', self._base.const_critical_text)
                    return(terminate(self._base, eFAIL))
            _access = cfg.getValue(COUT_AZURE_ACCESS)
            if (_access):
                if (_access not in ('private', 'blob', 'container')):
                    self._base.message('Invalid value for ({})'.format(COUT_AZURE_ACCESS), self._base.const_critical_text)
                    return(terminate(self._base, eFAIL))
                if (_access == 'private'):      # private is not recognized by Azure, used internally only for clarity
                    cfg.setValue(COUT_AZURE_ACCESS, None)       # None == private container
            if (self._args.output is None):
                _cloud_upload_type = cfg.getValue(COUT_CLOUD_TYPE, True)
                if (_cloud_upload_type == CCLOUD_AMAZON):
                    self._args.output = cfg.getValue(COUT_S3_PARENTFOLDER, False)
                elif (_cloud_upload_type == CCLOUD_AZURE):
                    self._args.output = cfg.getValue(COUT_AZURE_PARENTFOLDER, False)
                else:
                    self._base.message('Invalid value for ({})'.format(COUT_CLOUD_TYPE), self._base.const_critical_text)
                    return(terminate(self._base, eFAIL))
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
                self._base.message('-input/-output is not specified!', self._base.const_critical_text)
                return(terminate(self._base, eFAIL))
        # set output in cfg.
        dst_ = self._base.convertToForwardSlash(self._args.output)
        cfg.setValue(CCFG_PRIVATE_OUTPUT, dst_ if dst_ else '')
        # ends
        # is -rasterproxypath/-clonepath defined at the cmd-line?
        if (self._args.rasterproxypath):
            self._args.clonepath = self._args.rasterproxypath   # -rasterproxypath takes precedence. -clonepath is now deprecated.
        if (self._args.clonepath or
                cfg_mode == 'rasterproxy'):
            rpPath = self._args.clonepath if self._args.clonepath else self._args.output
            if (rpPath[-4:].lower().endswith('.csv')):
                cfg.setValue('rpformat', 'csv')
                cfg.setValue('rpfname', self._base.convertToForwardSlash(rpPath, False))
                if (self._args.clonepath):
                    self._args.clonepath = os.path.dirname(rpPath)
                else:   # if createrasterproxy template is used, -output is the -rasterproxypath
                    self._args.output = os.path.dirname(rpPath)
            if (self._args.clonepath):
                self._args.clonepath = self._base.convertToForwardSlash(self._args.clonepath)
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
                        if (isinput_s3):    # in case of input s3, output is used as a temp folder locally.
                            if (getBooleanValue(cfg.getValue(CCLOUD_UPLOAD))):
                                if (cfg.getValue(COUT_S3_PARENTFOLDER, False) != cfg.getValue(CIN_S3_PARENTFOLDER, False)):
                                    self._base.message('<%s> and <%s> must be the same if the -pyramids=only' % (CIN_S3_PARENTFOLDER, COUT_S3_PARENTFOLDER), const_critical_text)
                                    return(terminate(self._base, eFAIL))
                        else:
                            self._base.message('-input and -output paths must be the same if the -pyramids=only', const_critical_text)
                            return(terminate(self._base, eFAIL))
        if (not getBooleanValue(do_pyramids) and
            do_pyramids != CCMD_PYRAMIDS_ONLY and
                do_pyramids != CCMD_PYRAMIDS_EXTERNAL and
                do_pyramids != CCMD_PYRAMIDS_SOURCE):
            do_pyramids = 'false'
        cfg.setValue('Pyramids', do_pyramids)
        cfg.setValue('isuniformscale', True if do_pyramids == CCMD_PYRAMIDS_ONLY else getBooleanValue(do_pyramids) if do_pyramids != CCMD_PYRAMIDS_SOURCE else CCMD_PYRAMIDS_SOURCE)
        # ends
        # read in the gdal_path from config.
        gdal_path = cfg.getValue(CCFG_GDAL_PATH, False)      # note: validity is checked within (compression-mod)
        # ends
        comp = Compression(gdal_path, base=self._base)
        ret = comp.init(0)      # warning/error messages get printed within .init()
        if (not ret):
            self._base.message('Unable to initialize/compression module', self._base.const_critical_text)
            return(terminate(self._base, eFAIL))
        # s3 upload settings.
        out_s3_profile_name = self._args.outputprofile
        if (not out_s3_profile_name):
            out_s3_profile_name = cfg.getValue('Out_S3_AWS_ProfileName', False)
        if (out_s3_profile_name):
            cfg.setValue('Out_S3_AWS_ProfileName', out_s3_profile_name)
        s3_output = cfg.getValue(COUT_S3_PARENTFOLDER, False)
        s3_id = cfg.getValue('Out_S3_ID', False)
        s3_secret = cfg.getValue('Out_S3_Secret', False)

        err_init_msg = 'Unable to initialize the ({}) upload module! Check module setup/credentials. Quitting..'
        if (self._base.getBooleanValue(cfg.getValue(CCLOUD_UPLOAD))):
            if (cfg.getValue(COUT_CLOUD_TYPE, True) == CCLOUD_AMAZON):
                if ((s3_output is None and self._args.output is None)):
                    self._base.message('Empty/Invalid values detected for keys in the ({}) beginning with (Out_S3|Out_S3_ID|Out_S3_Secret|Out_S3_AWS_ProfileName) or values for command-line args (-outputprofile)'.format(self._args.config), self._base.const_critical_text)
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
                ret = S3_storage.init(s3_output, s3_id, s3_secret, CS3STORAGE_OUT)
                if (not ret):
                    self._base.message(err_init_msg.format('S3'), const_critical_text)
                    return(terminate(self._base, eFAIL))
                S3_storage.inputPath = self._args.output
                domain = S3_storage.con.meta.client.generate_presigned_url('get_object', Params={'Bucket': S3_storage.m_bucketname, 'Key': ' '}).split('%20?')[0]
                cfg.setValue(COUT_VSICURL_PREFIX, '/vsicurl/{}{}'.format(domain.replace('https', 'http'),
                                                                         cfg.getValue(COUT_S3_PARENTFOLDER, False)) if not S3_storage._isBucketPublic else
                             '/vsicurl/http://{}.{}/{}'.format(S3_storage.m_bucketname, CINOUT_S3_DEFAULT_DOMAIN, cfg.getValue(COUT_S3_PARENTFOLDER, False)))
                # ends
            elif (cfg.getValue(COUT_CLOUD_TYPE, True) == CCLOUD_AZURE):
                _account_name = cfg.getValue(COUT_AZURE_ACCOUNTNAME, False)
                _account_key = cfg.getValue(COUT_AZURE_ACCOUNTKEY, False)
                _container = cfg.getValue(COUT_AZURE_CONTAINER)
                _out_profile = cfg.getValue(COUT_AZURE_PROFILENAME, False)
                if (self._args.outputbucket):
                    _container = self._args.outputbucket
                    outBucket = self._args.outputbucket.lower()
                    cfg.setValue(COUT_AZURE_CONTAINER, outBucket)     # lowercased
                    cfg.setValue('Out_S3_Bucket', outBucket)    # UpdateMRF/update uses 'Out_S3_Bucket'/Generic key name to read in the output bucket name.
                if (self._args.outputprofile):
                    _out_profile = self._args.outputprofile
                    cfg.setValue(COUT_AZURE_PROFILENAME, _out_profile)
                if (((not _account_name or
                      not _account_key) and
                     not _out_profile) or
                        not _container):
                    self._base.message('Empty/Invalid values detected for keys ({}/{}/{}/{})'.format(COUT_AZURE_ACCOUNTNAME, COUT_AZURE_ACCOUNTKEY, COUT_AZURE_CONTAINER, COUT_AZURE_PROFILENAME), self._base.const_critical_text)
                    return(terminate(self._base, eFAIL))
                azure_storage = Azure(_account_name, _account_key, _out_profile, self._base)
                if (not azure_storage.init(CS3STORAGE_OUT)):
                    self._base.message(err_init_msg.format(CCLOUD_AZURE.capitalize()), self._base.const_critical_text)
                    return(terminate(self._base, eFAIL))
                cfg.setValue(COUT_VSICURL_PREFIX, '/vsicurl/{}{}'.format('http://{}.{}/{}/'.format(azure_storage.getAccountName, Azure.DefaultDomain, _container),
                                                                         self._args.output if self._args.output else cfg.getValue(COUT_S3_PARENTFOLDER, False)))
            elif (cfg.getValue(COUT_CLOUD_TYPE, True) == Store.TypeGoogle):
                _bucket = cfg.getValue(COUT_GOOGLE_BUCKET)  # bucket name
                _out_profile = cfg.getValue(COUT_GOOGLE_PROFILENAME, False)
                if (self._args.outputbucket):
                    _bucket = self._args.outputbucket
                    cfg.setValue(COUT_GOOGLE_BUCKET, self._args.outputbucket.lower())     # lowercased
                if (self._args.outputprofile):
                    _out_profile = self._args.outputprofile
                    cfg.setValue(COUT_GOOGLE_PROFILENAME, _out_profile)
                if (not _out_profile or
                        not _bucket):
                    self._base.message('Empty/Invalid values detected for keys ({}/{})'.format(COUT_GOOGLE_BUCKET, COUT_GOOGLE_PROFILENAME), self._base.const_critical_text)
                    return(terminate(self._base, eFAIL))
                google_storage = Google(None, '', '', _out_profile, self._base)
                if (not google_storage.init(_bucket)):
                    self._base.message(err_init_msg.format(Store.TypeGoogle.capitalize()), self._base.const_critical_text)
                    return(terminate(self._base, eFAIL))
                cfg.setValue(COUT_VSICURL_PREFIX, '/vsicurl/{}/{}'.format('{}{}'.format(Google.DafaultStorageDomain, _bucket), self._args.output if self._args.output else cfg.getValue(COUT_GOOGLE_PARENTFOLDER, False)))
            else:
                self._base.message('Invalid value for ({})'.format(COUT_CLOUD_TYPE), self._base.const_critical_text)
                return(terminate(self._base, eFAIL))
        isDeleteAfterUpload = cfg.getValue(COUT_DELETE_AFTER_UPLOAD)
        if (isDeleteAfterUpload is None):
            isDeleteAfterUpload = cfg.getValue(COUT_DELETE_AFTER_UPLOAD_OBSOLETE)
        isDeleteAfterUpload = self._base.getBooleanValue(isDeleteAfterUpload)
        user_args_Callback = {
            USR_ARG_UPLOAD: self._base.getBooleanValue(cfg.getValue(CCLOUD_UPLOAD)),
            USR_ARG_DEL: isDeleteAfterUpload
        }
        # ends
        cpy = Copy(self._base)
        list = {
            'copy': {'*'},
            'exclude': {}
        }
        for i in cfg.getValue(CCFG_RASTERS_NODE) + cfg.getValue(CCFG_EXCLUDE_NODE):
            list['exclude'][i] = ''

        is_caching = False
        if (cfg_mode == 'clonemrf' or
            cfg_mode == 'splitmrf' or
                cfg_mode == 'cachingmrf' or
                cfg_mode == 'rasterproxy'):
            is_caching = True

        if (is_caching):
            cfg.setValue(CISTEMPINPUT, False)
            cfg.setValue('Pyramids', False)

        callbacks = {
            # 'copy' : copy_callback,
            'exclude': exclude_callback
        }

        callbacks_for_meta = {
            'exclude': exclude_callback_for_meta
        }

        CONST_CPY_ERR_0 = 'Unable to initialize (Copy) module!'
        CONST_CPY_ERR_1 = 'Unable to process input data/(Copy) module!'

        # keep original-source-ext
        cfg_keep_original_ext = self._base.getBooleanValue(cfg.getValue('KeepExtension'))
        cfg_threads = cfg.getValue('Threads')
        msg_threads = 'Thread-count invalid/undefined, resetting to default'
        try:
            cfg_threads = int(cfg_threads)   # (None) value is expected
        except BaseException:
            cfg_threads = -1
        if (cfg_threads <= 0 or
                (cfg_threads > CCFG_THREADS and
                 not is_caching)):
            cfg_threads = CCFG_THREADS
            self._base.message('%s(%s)' % (msg_threads, CCFG_THREADS), self._base.const_warning_text)
        # ends
        # let's deal with copying when -input is on s3
        storeUseToken = cfg.getValue('UseToken')
        isUseToken = self._args.usetoken if self._args.usetoken else storeUseToken
        if (not isUseToken):
            isUseToken = self._base.getUserConfiguration.getValue(UseToken)
        cfg.setValue(UseToken, self._base.getBooleanValue(isUseToken))
        if (self._args.rasterproxypath and
                cfg.getValue(UseToken)):
            cfg.setValue(UseTokenOnOuput, True)
        if (isinput_s3):
            cfg.setValue('iss3', True)
            in_s3_parent = cfg.getValue(CIN_S3_PARENTFOLDER, False)
            in_s3_profile_name = self._args.inputprofile
            if (not in_s3_profile_name):
                inputProfileKeyToRead = {
                    Store.TypeAmazon: 'In_S3_AWS_ProfileName',
                    Store.TypeAzure: 'In_Azure_ProfileName',
                    Store.TypeGoogle: 'In_Google_ProfileName'
                }
                in_s3_profile_name = cfg.getValue(inputProfileKeyToRead[cloudDownloadType], False)
            if (in_s3_profile_name):
                cfg.setValue('In_S3_AWS_ProfileName', in_s3_profile_name)
            inputClientIdKeyToRead = {
                Store.TypeAmazon: 'In_S3_ID',
                Store.TypeAzure: 'In_Azure_AccountName',
                Store.TypeGoogle: None
            }
            inputClientSecretKeyToRead = {
                Store.TypeAmazon: 'In_S3_Secret',
                Store.TypeAzure: 'In_Azure_AccountKey',
                Store.TypeGoogle: None
            }
            in_s3_id = cfg.getValue(inputClientIdKeyToRead[cloudDownloadType], False)
            in_s3_secret = cfg.getValue(inputClientSecretKeyToRead[cloudDownloadType], False)
            in_s3_bucket = self._args.inputbucket
            if (not in_s3_bucket):
                inputBucketKeyToRead = {
                    Store.TypeAmazon: 'In_S3_Bucket',
                    Store.TypeAzure: 'In_Azure_Container',
                    Store.TypeGoogle: 'In_Google_Bucket'
                }
                in_s3_bucket = cfg.getValue(inputBucketKeyToRead[cloudDownloadType], False)
            if (in_s3_parent is None or
                    in_s3_bucket is None):
                self._base.message('Invalid/empty value(s) found in node(s) [In_S3_ParentFolder, In_S3_Bucket]', self._base.const_critical_text)
                return(terminate(self._base, eFAIL))
            cfg.setValue('In_S3_Bucket', in_s3_bucket)          # update (in s3 bucket name in config)
            in_s3_parent = in_s3_parent.replace('\\', '/')
            if (in_s3_parent[:1] == '/' and
                    not in_s3_parent.lower().endswith(Report.CJOB_EXT)):
                in_s3_parent = in_s3_parent[1:]
                cfg.setValue(CIN_S3_PARENTFOLDER, in_s3_parent)
            if (cloudDownloadType == Store.TypeAmazon):
                o_S3_storage = S3Storage(self._base)
                ret = o_S3_storage.init(in_s3_parent, in_s3_id, in_s3_secret, CS3STORAGE_IN)
                if (not ret):
                    self._base.message('Unable to initialize S3-storage! Quitting..', self._base.const_critical_text)
                    return(terminate(self._base, eFAIL))
                if (str(o_S3_storage.con.meta.client._endpoint.host).lower().endswith('.ecstestdrive.com')):   # handles EMC namespace cloud urls differently
                    cfg.setValue(CIN_S3_PREFIX, '/vsicurl/http://{}.public.ecstestdrive.com/{}/'.format(
                        o_S3_storage.CAWS_ACCESS_KEY_ID.split('@')[0], o_S3_storage.m_bucketname))
                else:   # for all other standard cloud urls
                    domain = o_S3_storage.con.meta.client.generate_presigned_url('get_object', Params={'Bucket': o_S3_storage.m_bucketname, 'Key': ' '}).split('%20?')[0]
                    cfg.setValue(CIN_S3_PREFIX, '/vsicurl/{}'.format(domain.replace('https', 'http')) if not o_S3_storage._isBucketPublic else
                                 '/vsicurl/http://{}.{}/'.format(o_S3_storage.m_bucketname, CINOUT_S3_DEFAULT_DOMAIN))  # vsicurl doesn't like 'https'
                o_S3_storage.inputPath = self._args.output
                if (not o_S3_storage.getS3Content(o_S3_storage.remote_path, o_S3_storage.S3_copy_to_local, exclude_callback)):
                    self._base.message('Unable to read S3-Content', self._base.const_critical_text)
                    return(terminate(self._base, eFAIL))
            elif (cloudDownloadType == Store.TypeAzure):
                # let's do (Azure) init
                self._base.getUserConfiguration.setValue(CIN_AZURE_CONTAINER, in_s3_bucket)
                in_azure_storage = Azure(in_s3_id, in_s3_secret, in_s3_profile_name, self._base)
                if (not in_azure_storage.init() or
                        not in_azure_storage.getAccountName):
                    self._base.message('({}) download initialization error. Check input credentials/profile name. Quitting..'.format(CCLOUD_AZURE.capitalize()), self._base.const_critical_text)
                    return(terminate(self._base, eFAIL))
                in_azure_storage._include_subFolders = self._base.getBooleanValue(cfg.getValue('IncludeSubdirectories'))
                _restored = cfg.getValue(CLOAD_RESTORE_POINT)
                _azParent = self._args.input
                if (not _restored):
                    in_azure_storage._mode = in_azure_storage.CMODE_SCAN_ONLY
                else:
                    _azParent = '/' if not _rpt else _rpt.root
                if (not _azParent.endswith('/')):
                    _azParent += '/'
                cfg.setValue(CIN_AZURE_PARENTFOLDER, _azParent)
                cfg.setValue(CIN_S3_PREFIX, '/vsicurl/{}'.format('http{}://{}.{}/{}/'.format('s' if in_azure_storage._SASToken else '', in_azure_storage.getAccountName, Azure.DefaultDomain, cfg.getValue('In_S3_Bucket'))))
                if (not in_azure_storage.browseContent(in_s3_bucket, _azParent, in_azure_storage.copyToLocal, exclude_callback)):
                    return(terminate(self._base, eFAIL))
                if (not _restored):
                    _files = in_azure_storage.getBrowseContent()
                    if (_files):
                        for f in _files:
                            fn_collect_input_files(f)
            elif (cloudDownloadType == Store.TypeGoogle):
                inGoogleStorage = Google(None, in_s3_id, in_s3_secret, in_s3_profile_name, self._base)
                if (not inGoogleStorage.init(in_s3_bucket)):
                    self._base.message('({}) download initialization error. Check input credentials/profile name. Quitting..'.format(Store.TypeGoogle.capitalize()), self._base.const_critical_text)
                    return(terminate(self._base, eFAIL))
                inGoogleStorage._include_subFolders = self._base.getBooleanValue(cfg.getValue('IncludeSubdirectories'))
                restored = cfg.getValue(CLOAD_RESTORE_POINT)
                gsParent = self._args.input
                if (not restored):
                    inGoogleStorage._mode = inGoogleStorage.CMODE_SCAN_ONLY
                else:
                    gsParent = '/' if not _rpt else _rpt.root
                if (not gsParent.endswith('/')):
                    gsParent += '/'
                cfg.setValue(CIN_GOOGLE_PARENTFOLDER, gsParent)
                cfg.setValue(CIN_S3_PREFIX, '/vsicurl/{}'.format('{}{}/'.format(Google.DafaultStorageDomain, self._args.inputbucket)))
                if (not inGoogleStorage.browseContent(in_s3_bucket, gsParent, inGoogleStorage.copyToLocal, exclude_callback)):
                    return(terminate(self._base, eFAIL))
                if (not restored):
                    _files = inGoogleStorage.getBrowseContent()
                    if (_files):
                        for f in _files:
                            fn_collect_input_files(f)
                pass
                # ends
        # ends
        # control flow if conversions required.
        if (not is_caching):
            isDirectInput = filterPaths(self._args.input, cfg.getValue(CCFG_RASTERS_NODE))
            if (not isinput_s3 and
                    not cfg_mode == BundleMaker.CMODE and
                    not isDirectInput):
                ret = cpy.init(self._args.input, self._args.tempoutput if is_output_temp and self._base.getBooleanValue(cfg.getValue(CCLOUD_UPLOAD)) else self._args.output, list, callbacks, cfg)
                if (not ret):
                    self._base.message(CONST_CPY_ERR_0, self._base.const_critical_text)
                    return(terminate(self._base, eFAIL))
                ret = cpy.processs(self._base.S3Upl if is_cloud_upload else None, user_args_Callback, fn_pre_process_copy_default)
                if (not ret):
                    self._base.message(CONST_CPY_ERR_1, self._base.const_critical_text)
                    return(terminate(self._base, eFAIL))
                if (is_input_temp):
                    pass        # no post custom code yet for non-rasters
            if (cfg_mode == BundleMaker.CMODE):
                p, f = os.path.split(self._args.input if not cfg.getValue(CLOAD_RESTORE_POINT) else self._base.convertToForwardSlash(_rpt._input_list[0], False))
                raster_buff = [{'dst': self._args.output,
                                'f': f,
                                'src': p}]
            files = raster_buff
            files_len = len(files)
            if (files_len):
                if (is_input_temp and
                    not isinput_s3 and
                        not cfg.getValue(CLOAD_RESTORE_POINT)):
                    # if the -tempinput path is defined, we first copy rasters from the source path to -tempinput before any conversion.
                    self._base.message('Copying files to -tempinput path (%s)' % (cfg.getValue(CTEMPINPUT, False)))
                    cpy_files_ = []
                    for i in range(0, len(files)):
                        get_dst_path = files[i]['dst'].replace(self._args.output if cfg.getValue(CTEMPOUTPUT, False) is None else cfg.getValue(CTEMPOUTPUT, False), cfg.getValue(CTEMPINPUT, False))
                        cpy_files_.append(
                            {
                                'src': files[i]['src'],
                                'dst': get_dst_path,
                                'f': files[i]['f']
                            })
                        files[i]['src'] = get_dst_path
                    cpy.batch(cpy_files_, None)
                self._base.message('Converting..')
            # collect all the input raster files.
            if (g_is_generate_report and
                    g_rpt):
                for req in files:
                    _src = '{}{}{}'.format(req['src'], '/' if not req['src'].replace('\\', '/').endswith('/') and req['src'] else '', req['f'])
                    if (self._base.getBooleanValue(cfg.getValue(CISTEMPINPUT))):
                        _tempinput = cfg.getValue(CTEMPINPUT, False)
                        _tempinput = _tempinput[:-1] if _tempinput.endswith('/') and not self._args.input.endswith('/') else _tempinput
                        _src = _src.replace(_tempinput, self._args.input)
                    g_rpt.addFile(_src)     # prior to this point, rasters get added to g_rpt during the (pull/copy) process if -clouddownload=true && -tempinput is defined.
                self._base.message('{}'.format(CRESUME_CREATE_JOB_TEXT).format(_project_path))
                for arg in vars(self._args):
                    val = getattr(self._args, arg)
                    if (arg == CRESUME_HDR_INPUT):
                        f, e = os.path.splitext(val)
                        if (len(e) != 0):
                            val = val[:val.rindex('/') + 1]
                    g_rpt.addHeader(arg, val)
                g_rpt.write()
                if (self.isOperationCreateJob):
                    return self.__initOperationCreateJob()
                # process @ lambda?
                if (self._isLambdaJob()):
                    _rpt = Report(self._base)
                    if (not _rpt.init(_project_path) or
                            not _rpt.read()):
                        self._base.message('Unable to read the -input report file ({})'.format(self._args.input), self._base.const_critical_text)
                        return(terminate(self._base, eFAIL))
                    self._base.getUserConfiguration.setValue(CPRT_HANDLER, _rpt)
                    ret = eOK if self._runLambdaJob(g_rpt._report_file) else eFAIL
                    if (ret == eOK):
                        if (self._args.op != COP_LAMBDA):   # synchronous call.
                            self._moveJobFileToLogPath()
                    return(terminate(self._base, ret))
                # ends
                self._args.op = None
                self._args.input = _project_path
                cfg.setValue(CLOAD_RESTORE_POINT, True)
                self.run()
                return
            # ends
            raster_buff = files
            len_buffer = cfg_threads
            threads = []
            store_files_indx = 0
            store_files_len = len(raster_buff)
            doRasterProxy = cfg.getValue(CCLONE_PATH) and not cfg_mode.startswith('mrf')
            while(1):
                len_threads = len(threads)
                while(len_threads):
                    alive = [t.isAlive() for t in threads]
                    cnt_dead = sum(not x for x in alive)
                    if (cnt_dead):
                        len_buffer = cnt_dead
                        threads = [t for t in threads if t.isAlive()]
                        break
                buffer = []
                for i in range(0, len_buffer):
                    if (store_files_indx == store_files_len):
                        break
                    buffer.append(raster_buff[store_files_indx])
                    store_files_indx += 1
                if (not buffer and
                        not threads):
                    break
                for req in buffer:
                    (input_file, output_file) = getInputOutput(req['src'], req['dst'], req['f'], isinput_s3)
                    f, e = os.path.splitext(output_file)
                    if (not cfg_keep_original_ext):
                        modeExtension = cfg_mode.split('_')[0]
                        if (modeExtension.lower() == e[1:].lower()):
                            modeExtension = e[1:]   # keep the input extension case. This will ensure the file status gets updated properly in the orjob file.
                        output_file = output_file.replace(e, '.{}'.format(modeExtension))
                    _build_pyramids = True
                    if (til):
                        if (til.find(req['f'])):
                            til.addFileToProcessed(req['f'])    # increment the process counter if the raster belongs to a (til) file.
                            _build_pyramids = False     # build pyramids is always turned off for rasters that belong to (.til) files.
                    useBundleMaker = cfg_mode == BundleMaker.CMODE
                    if (useBundleMaker):
                        bundleMaker = BundleMaker(input_file, gdal_path, base=self._base)
                        if (not bundleMaker.init()):
                            continue
                        t = threading.Thread(target=bundleMaker.run)
                    else:
                        doProcessRaster = True
                        if (til is not None and
                            til.defaultTILProcessing and
                                til.fileTILRelated(os.path.basename(input_file))):
                            doProcessRaster = False  # skip processing individual rasters/tiffs referenced by the .til files. Ask GDAL to process .til without any custom OR logic involved.
                            if (not isinput_s3):
                                processedPath = output_file
                                if (self._base.getBooleanValue(cfg.getValue(CISTEMPOUTPUT))):
                                    if (not is_cloud_upload):
                                        processedPath = processedPath.replace(req['dst'], self._args.output)
                                if (self._base.getBooleanValue(cfg.getValue(CISTEMPINPUT))):
                                    try:
                                        shutil.move(input_file, processedPath)
                                    except Exception as e:
                                        self._base.message('TIL/[MV] ({})->({})\n{}'.format(input_file, processedPath, str(e)), self._base.const_critical_text)
                                else:
                                    try:
                                        shutil.copy(input_file, processedPath)
                                    except Exception as e:
                                        self._base.message('TIL/[CPY] ({})->({})\n{}'.format(input_file, processedPath, str(e)), self._base.const_critical_text)
                        if (doProcessRaster):
                            t = threading.Thread(target=comp.compress,
                                                 args=(input_file, output_file, args_Callback, _build_pyramids, self._base.S3Upl if is_cloud_upload else fn_copy_temp_dst if is_output_temp and not is_cloud_upload else None, user_args_Callback), kwargs={'name': os.path.join(req['src'], req['f'])})
                        t.daemon = True
                        t.start()
                        threads.append(t)
            # til work
            if (til):
                for _til in til:
                    _doPostProcessing = True
                    if (cfg.getValue(CLOAD_RESTORE_POINT)):
                        if (_rpt.getRecordStatus(_til, CRPT_PROCESSED) == CRPT_YES):
                            self._base.message('{} {}'.format(CRESUME_MSG_PREFIX, _til))
                            _doPostProcessing = False
                    if (not til.isAllFilesProcessed(_til)):
                        if (_doPostProcessing):
                            self._base.message('TIL> Not yet completed for ({})'.format(_til))
                    if (til.isAllFilesProcessed(_til)):
                        til_output_path = til.getOutputPath(_til)
                        if (_doPostProcessing):
                            if (not til_output_path):
                                self._base.message('TIL output-path returned empty/Internal error', self._base.const_warning_text)
                                continue
                            if (not til.defaultTILProcessing):
                                ret = comp.createaOverview(til_output_path)
                                if (not ret):
                                    self._base.message('Unable to build pyramids on ({})'.format(til_output_path), self._base.const_warning_text)
                                    continue
                            tilOutputExtension = 'mrf'
                            tilsInfoKey = _til.lower()  # keys in TIL._tils_info are in lowercase.
                            ret = comp.compress('{}{}'.format(til_output_path, '.ovr' if not til.defaultTILProcessing else ''), '{}.{}'.format(til_output_path, tilOutputExtension), args_Callback, name=til_output_path)
                            if (not ret):
                                self._base.message('Unable to convert (til.ovr=>til.mrf) for file ({}.ovr)'.format(til_output_path), self._base.const_warning_text)
                                continue
                            try:
                                if (til.defaultTILProcessing):   # remove all the internally referenced (raster/tiff) files by the .TIL file that are no longer needed post conversion.
                                    for associate in til._tils_info[tilsInfoKey][TIL.CKEY_FILES]:
                                        processedPath = os.path.join(os.path.dirname(til_output_path), associate)
                                        try:
                                            os.remove(processedPath)
                                        except Exception as e:
                                            self._base.message(str(e), self._base.const_critical_text)
                                            continue
                                else:
                                    # let's rename (.mrf) => (.ovr)
                                    os.remove('{}.ovr'.format(til_output_path))
                                    os.rename('{}.mrf'.format(til_output_path), '{}.ovr'.format(til_output_path))
                            except Exception as e:
                                self._base.message('({})'.format(str(e)), self._base.const_warning_text)
                                continue
                            # update .ovr file updates at -clonepath
                            try:
                                if (self._args.clonepath):
                                    _clonePath = til_output_path.replace(self._args.output if not self._args.tempoutput or (self._args.tempoutput and not self._base.getBooleanValue(self._args.cloudupload)) else self._args.tempoutput, '')
                                    _mk_input_path = os.path.join(self._args.clonepath, '{}.mrf'.format(_clonePath))
                                    doc = minidom.parse(_mk_input_path)
                                    xmlString = doc.toxml()
                                    xmlString = xmlString.replace('.mrf<', '.ovr<')
                                    xmlString = xmlString.replace('.{}'.format(CCACHE_EXT), '.ovr.{}'.format(CCACHE_EXT))
                                    _indx = xmlString.find('<{}>'.format(CMRF_DOC_ROOT))
                                    if (_indx == -1):
                                        raise Exception('Err. Invalid MRF/header')
                                    xmlString = xmlString[_indx:]
                                    _mk_save_path = '{}{}.ovr'.format(self._args.clonepath, _clonePath.replace('.mrf', ''))
                                    with open(_mk_save_path, 'w+') as _fpOvr:
                                        _fpOvr.write(xmlString)
                            except Exception as e:
                                self._base.message('Unable to update .ovr for [{}] ({})'.format(til_output_path, str(e)), self._base.const_warning_text)
                                continue
                            # ends
                        # upload (til) related files (.idx, .ovr, .lrc)
                        if (is_cloud_upload and
                                S3_storage):
                            ret = S3_storage.upload_group('{}.CHS'.format(til_output_path))
                            retry_failed_lst = []
                            failed_upl_lst = S3_storage.getFailedUploadList()
                            if (failed_upl_lst):
                                [retry_failed_lst.append(_x['local']) for _x in failed_upl_lst['upl']]
                            # let's delete all the associate files related to (TIL) files.
                            if (self._base.getBooleanValue(cfg.getValue(COUT_DELETE_AFTER_UPLOAD))):
                                (p, n) = os.path.split(til_output_path)
                                for r, d, f in os.walk(p):
                                    for file in f:
                                        if (r != p):
                                            continue
                                        mk_filename = os.path.join(r, file).replace('\\', '/')
                                        if (til.fileTILRelated(mk_filename)):
                                            if (mk_filename in retry_failed_lst):        # Don't delete files included in the (failed upload list)
                                                continue
                                            try:
                                                self._base.message('[Del] {}'.format(mk_filename))
                                                os.remove(mk_filename)
                                            except Exception as e:
                                                self._base.message('[Del] Err. {} ({})'.format(mk_filename, str(e)), self._base.const_critical_text)
                            # ends
                        # ends
            # ends
        # block to deal with caching ops.
        if (is_caching and
                do_pyramids != CCMD_PYRAMIDS_ONLY):
            if (not g_is_generate_report):
                self._base.message('\nProcessing caching operations...')
            if (not isinput_s3):
                raster_buff = []
                if (cfg_mode == 'splitmrf'):        # set explicit (exclude list) for mode (splitmrf)
                    list['exclude']['idx'] = ''
                ret = cpy.init(self._args.input, self._args.output, list, callbacks_for_meta, cfg)
                if (not ret):
                    self._base.message(CONST_CPY_ERR_0, self._base.const_critical_text)
                    return(terminate(self._base, eFAIL))
                ret = cpy.processs(pre_processing_callback=fn_pre_process_copy_default)
                if (not ret):
                    self._base.message(CONST_CPY_ERR_1, self._base.const_critical_text)
                    return(terminate(self._base, eFAIL))
            if (g_is_generate_report and
                    g_rpt):
                for req in raster_buff:
                    (input_file, output_file) = getInputOutput(req['src'], req['dst'], req['f'], isinput_s3)
                    _src = '{}{}{}'.format(req['src'], '/' if not req['src'].replace('\\', '/').endswith('/') and req['src'] != '' else '', req['f'])
                    g_rpt.addFile(_src)
                self._base.message('{}'.format(CRESUME_CREATE_JOB_TEXT).format(_project_path))
                self._args.cloudupload = 'false'    # Uploading is disabled for modes related to caching.
                for arg in vars(self._args):
                    g_rpt.addHeader(arg, getattr(self._args, arg))
                g_rpt.write()
                if (self.isOperationCreateJob):
                    return self.__initOperationCreateJob()
                self._args.op = None
                cfg.setValue(CCMD_ARG_INPUT, self._args.input)      # preserve the original -input path
                self._args.input = _project_path
                cfg.setValue(CLOAD_RESTORE_POINT, True)
                self.run()
                return
            len_buffer = cfg_threads
            threads = []
            store_files_indx = 0
            store_files_len = len(raster_buff)
            while(1):
                len_threads = len(threads)
                while(len_threads):
                    alive = [t.isAlive() for t in threads]
                    cnt_dead = sum(not x for x in alive)
                    if (cnt_dead):
                        len_buffer = cnt_dead
                        threads = [t for t in threads if t.isAlive()]
                        break
                buffer = []
                for i in range(0, len_buffer):
                    if (store_files_indx == store_files_len):
                        break
                    buffer.append(raster_buff[store_files_indx])
                    store_files_indx += 1
                if (not buffer and
                        not threads):
                    break
                preSignedURL = None
                setPreAssignedURL = False
                if (cloudDownloadType == Store.TypeAmazon):  # enabled only for 'amazon' for now.
                    if (isinput_s3 and
                            o_S3_storage is not None):
                        setPreAssignedURL = True
                elif(cloudDownloadType == Store.TypeAzure):
                    if (isinput_s3 and
                            in_azure_storage is not None):
                        setPreAssignedURL = True
                for f in buffer:
                    try:
                        if (setPreAssignedURL):
                            preAkey = '{}{}'.format(f['src'], f['f'])
                            if (cloudDownloadType == Store.TypeAmazon):
                                self._args.preAssignedURL = o_S3_storage.con.meta.client.generate_presigned_url('get_object', Params={'Bucket': o_S3_storage.m_bucketname, 'Key': preAkey})
                            else:
                                if (not cfg.getValue(CFGAZSAS)):
                                    from azure.storage.blob import BlobPermissions
                                    SAS = in_azure_storage._blob_service.generate_blob_shared_access_signature(in_s3_bucket, preAkey, BlobPermissions.READ, datetime.utcnow() + timedelta(hours=1))
                                    self._args.preAssignedURL = in_azure_storage._blob_service.make_blob_url(in_s3_bucket, preAkey, sas_token=SAS)
                        t = threading.Thread(target=threadProxyRaster, args=(f, self._base, comp, self._args))
                        t.daemon = True
                        t.start()
                        threads.append(t)
                    except Exception as e:
                        self._base.message('Err. {}'.format(str(e)), self._base.const_critical_text)
                        continue
        # do we have failed upload files on list?
        if (is_cloud_upload and
                S3_storage):
            if (cfg.getValue(COUT_CLOUD_TYPE) == CCLOUD_AMAZON):
                failed_upl_lst = S3_storage.getFailedUploadList()
                if (failed_upl_lst):
                    self._base.message('Retry - Failed upload list.', const_general_text)
                    _fptr = None
                    if (self._log_path):
                        try:
                            if (not os.path.isdir(self._log_path)):
                                makedirs(self._log_path)
                            ousr_date = datetime.now()
                            err_upl_file = os.path.join(self._log_path, '%s_UPL_ERRORS_%04d%02d%02dT%02d%02d%02d.txt' % (cfg.getValue(CPRJ_NAME, False), ousr_date.year, ousr_date.month, ousr_date.day,
                                                                                                                         ousr_date.hour, ousr_date.minute, ousr_date.second))
                            _fptr = open(err_upl_file, 'w+')
                        except BaseException:
                            pass
                    for v in failed_upl_lst['upl']:
                        self._base.message('%s' % (v['local']), const_general_text)
                        ret = S3_storage.upload_group(v['local'])
                        # the following files will be logged as unsuccessful uploads to output cloud
                        if (not ret):
                            if (_fptr):
                                _fptr.write('{}\n'.format(v['local']))
                            if (_rpt):      # Do we have an input file list?
                                if ('local' in v):
                                    _local = v['local']
                                    if (_local):
                                        setUploadRecordStatus(_local, CRPT_NO)
                        # ends
                        for r in ret:
                            try:
                                self._base.message('[Del] {}'.format(r))
                                try:
                                    os.remove(r)
                                except BaseException:
                                    time.sleep(CDEL_DELAY_SECS)
                                    os.remove(r)
                            except Exception as e:
                                self._base.message('[Del] {} ({})'.format(r, str(e)))
                    if (_fptr):
                        _fptr.close()
                        _fptr = None
        # ends
        # let's clean-up rasters @ -tempinput path
        if (is_input_temp and
                not is_caching):        # if caching is (True), -tempinput is ignored and no deletion of source @ -input takes place.
            if (len(raster_buff) != 0):
                self._base.message('Removing input rasters at ({})'.format(cfg.getValue(CTEMPINPUT, False)))
                for req in raster_buff:
                    doRemove = True
                    (input_file, output_file) = getInputOutput(req['src'], req['dst'], req['f'], isinput_s3)
                    try:
                        if (_rpt):
                            if (_rpt.getRecordStatus('{}{}'.format(req['src'], req['f']), CRPT_PROCESSED) == CRPT_NO):
                                doRemove = False
                        if (doRemove and
                                os.path.exists(input_file)):
                            self._base.message('[Del] {}'.format(input_file))
                            os.remove(input_file)
                    except Exception as e:
                        self._base.message('[Del] {} ({})'.format(input_file, str(e)), self._base.const_warning_text)
                    if (_rpt and
                            doRemove):
                        primaryExt = _rpt._m_rasterAssociates.findExtension(input_file)
                        if (primaryExt):
                            raInfo = _rpt._m_rasterAssociates.getInfo()
                            if (raInfo and
                                    primaryExt in raInfo):
                                self._base.message('Removing associated files for ({})'.format(input_file))
                                for relatedExt in raInfo[primaryExt].split(';'):
                                    try:
                                        _mkPrimaryRaster = '{}{}'.format(input_file[:len(input_file) - len(primaryExt)], relatedExt)
                                        if (os.path.exists(_mkPrimaryRaster)):
                                            self._base.message('[Del] {}'.format(_mkPrimaryRaster))
                                            os.remove(_mkPrimaryRaster)
                                    except Exception as e:
                                        self._base.message('[Del] {} ({})'.format(_mkPrimaryRaster, str(e)), self._base.const_warning_text)
                self._base.message('Done.')
        # ends
        if (not raster_buff):
            if (len(cfg.getValue(CCFG_RASTERS_NODE))):  # it's possible to have empty {CCFG_RASTERS_NODE} raster extensions. e.g configs for op=copyonly
                self._base.message('No input rasters to process..', self._base.const_warning_text)
        # ends
        _status = eOK
        # write out the (job file) with updated status.
        if (_rpt):
            if (not _rpt.write() or
                    _rpt.hasFailures()):
                _status = eFAIL
            if (_status == eOK):
                if (not CRUN_IN_AWSLAMBDA):
                    _status = self._moveJobFileToLogPath()
            timeReport = self._args.timeit
            if (timeReport):
                _rpt.writeTimeItReport(timeReport)  # write the execution time details report
        # ends
        # write out the raster proxy .csv file
        if (self._base._isRasterProxyFormat('csv')):
            pfname = cfg.getValue('rpfname', False)
            if (pfname):
                with open(pfname, 'a') as rpWriter:
                    rpWriter.write('ObjectID;Raster\n')
                    for i in range(0, len(self._base._modifiedProxies)):
                        proxyStr = self._base._modifiedProxies[i]
                        proxyStr = ' '.join(proxyStr.split()).replace('"', '\'')
                        proxyStr = '><'.join(proxyStr.split('> <'))
                        rpWriter.write('{};{}\n'.format(i + 1, proxyStr))
        # ends
        self._base.message('Done..\n')
        return(terminate(self._base, _status))

    def _moveJobFileToLogPath(self):
        global _rpt
        global cfg
        if (self._base is None or
                _rpt is None):
            return eFAIL
        status = eOK
        txtInConfig = 'KeepLogFile'
        txtInRPT = txtInConfig.lower()
        if (self._base.getMessageHandler is None):
            return status
        if (self._base.getBooleanValue(cfg.getValue(txtInConfig))):
            return status
        if (txtInRPT in _rpt._header):
            if (self._base.getBooleanValue(_rpt._header[txtInRPT])):
                return status
        if (not _rpt.moveJobFileToPath(self._base.getMessageHandler.logFolder)):
            self._base.message('Unable to move the .orjob file to the log path.', self._base.const_warning_text)
            status = eFAIL
        return status


def threadProxyRaster(req, base, comp, args):
    usrConfig = base.getUserConfiguration
    (inputFile, outputFile) = getInputOutput(req['src'], req['dst'], req['f'], args.clouddownload)
    (f, ext) = os.path.splitext(req['f'])
    rptName = os.path.join(req['src'], req['f'])
    if (not base.getBooleanValue(usrConfig.getValue('KeepExtension'))):
        outputFile = outputFile.replace(ext, CONST_OUTPUT_EXT)
    finalPath = outputFile
    isTempOut = base.getBooleanValue(usrConfig.getValue(CISTEMPOUTPUT))
    if (isTempOut):
        finalPath = outputFile.replace(args.tempoutput, args.output)
    mode = usrConfig.getValue('Mode')
    bytesAtHeader = None
    isInputMRF = False
    if (mode != 'splitmrf'):
        if (mode == 'rasterproxy'):
            # Determine file type by reading few bytes off its header.
            sigMRF = '<{}>'.format(CMRF_DOC_ROOT.lower())[:4]   # mrf XML root node
            sigMRFLength = len(sigMRF)  # reading as small as possble to determine the correct type to avoid large data transfers for bigger .orjob files.
            bytesAtHeader = None
            remoteURL = None
            if (inputFile.startswith(CVSICURL_PREFIX)):
                dnVSICURL = inputFile.split(CVSICURL_PREFIX)[1]
                remoteReader = None
                remoteURL = args.preAssignedURL if (hasattr(args, 'preAssignedURL') and args.preAssignedURL is not None) else dnVSICURL
                try:
                    remoteReader = urlopen(remoteURL)
                    bytesAtHeader = remoteReader.read(sigMRFLength)
                except Exception as e:
                    base.message(str(e), base.const_critical_text)
                    if (_rpt):
                        _rpt.updateRecordStatus(rptName, CRPT_PROCESSED, CRPT_NO)
                    return False
                finally:
                    if (remoteReader):
                        remoteReader.close()
            else:
                try:
                    with open(inputFile, 'rb') as fptrProxy:
                        bytesAtHeader = fptrProxy.read(sigMRFLength)
                except Exception as e:
                    base.message(str(e), base.const_critical_text)
                    if (_rpt):
                        _rpt.updateRecordStatus(rptName, CRPT_PROCESSED, CRPT_NO)
                    return False
            if (bytesAtHeader):
                mode = 'cachingmrf'
                if (isinstance(bytesAtHeader, bytes)):
                    try:
                        bytesAtHeader = bytesAtHeader.decode('utf-8')
                    except BaseException:
                        pass  # ignore any invalid start byte issues.
                if (bytesAtHeader.lower() == sigMRF):
                    isInputMRF = True
                    mode = 'clonemrf'
                    contents = None
                    if (inputFile.startswith(CVSICURL_PREFIX)):
                        remoteReader = None
                        try:
                            remoteReader = urlopen(remoteURL)
                            contents = remoteReader.read()
                            if (not base._isRasterProxyFormat('csv')):
                                with open(outputFile, 'wb') as writer:
                                    writer.write(contents)
                            srcPyramids = contents.find(b'<Rsets') != -1
                            if (_rpt):
                                ret = _rpt.addMetadata(rptName, 'isuniformscale', srcPyramids)
                        except Exception as e:
                            base.message(str(e), base.const_critical_text)
                            if (_rpt):
                                _rpt.updateRecordStatus(rptName, CRPT_PROCESSED, CRPT_NO)
                            return False
                        finally:
                            if (remoteReader):
                                remoteReader.close()
                    else:
                        try:
                            with open(inputFile, 'rb') as proxyReader:
                                contents = proxyReader.read()
                                if (contents is not None):
                                    if (isInputMRF):
                                        if (not base._isRasterProxyFormat('csv')):
                                            with open(outputFile, 'wb') as writer:
                                                writer.write(contents)
                                    srcPyramids = contents.find(b'<Rsets') != -1
                                    if (_rpt):
                                        ret = _rpt.addMetadata(inputFile, 'isuniformscale', srcPyramids)
                        except Exception as e:
                            base.message(str(e), base.const_critical_text)
                            if (_rpt):
                                _rpt.updateRecordStatus(rptName, CRPT_PROCESSED, CRPT_NO)
                            return False
                    if (contents is not None):
                        if (contents.find(b'<Compression>LERC') == -1):
                            mode = 'cachingmrf'
            # ends
        if (not isInputMRF):
            ret = comp.compress(inputFile, outputFile, args_Callback_for_meta,
                                post_processing_callback=fn_copy_temp_dst if isTempOut else None, name=rptName)
    else:
        try:
            shutil.copyfile(inputFile, finalPath)
        except Exception as e:
            base.message('[CPY] {} ({})'.format(inputFile, str(e)), base.const_critical_text)
            return False
    if (not os.path.exists(finalPath)):
        if (not base._isRasterProxyFormat('csv')):
            return False
    # update .mrf.
    updateMRF = UpdateMRF(base)
    homePath = args.output
    inputMRF = finalPath
    if (isInputMRF):
        homePath = req['src']
        if (not inputFile.startswith(CVSICURL_PREFIX)):
            inputMRF = inputFile
    if (updateMRF.init(inputMRF, args.output, mode,
                       args.cache, homePath, usrConfig.getValue(COUT_VSICURL_PREFIX, False))):
        if (not updateMRF.update(finalPath, trueInput=inputFile)):
            base.message('Updating ({}) was not successful!'.format(finalPath), base.const_critical_text)
            return False
    # ends
    # remove ancillary extension files that are no longer required for (rasterproxy) files on the client side.
    errorEntries = RasterAssociates.removeRasterProxyAncillaryFiles(finalPath)
    if (errorEntries):
        for err in errorEntries:
            base.message('Unable to delete ({})'.format(err), base.const_warning_text)
    # ends
    return True


def main():
    optional = '[Optional]'
    parser = argparse.ArgumentParser()
    parser.add_argument('-mode', help='Processing mode/output format', dest='mode')
    parser.add_argument('-input', help='Input raster files directory/job file to resume', dest=CRESUME_HDR_INPUT)
    parser.add_argument('-output', help='Output directory', dest=CRESUME_HDR_OUTPUT)
    parser.add_argument('-subs', help='Include sub-directories in -input? [true/false]', dest='subs')
    parser.add_argument('-cache', help='cache output directory', dest='cache')
    parser.add_argument('-config', help='Configuration file with default settings', dest='config')
    parser.add_argument('-quality', help='JPEG quality if compression is jpeg', dest='quality')
    parser.add_argument('-prec', help='LERC precision', dest='prec')
    parser.add_argument('-pyramids', help='Generate pyramids? [true/false/only/external]', dest='pyramids')
    parser.add_argument('-tempinput', help='{} Path to copy -input raters before conversion'.format(optional), dest=CTEMPINPUT)
    parser.add_argument('-tempoutput', help='Path to output converted rasters before moving to (-output) path. {} This is only required if -cloudupload is (true)'.format(optional), dest=CTEMPOUTPUT)
    parser.add_argument('-clouddownload', help='Is -input a cloud storage? [true/false: default:false]', dest='clouddownload')
    parser.add_argument('-cloudupload', help='Is -output a cloud storage? [true/false]', dest='cloudupload')
    parser.add_argument('-clouduploadtype', choices=['amazon', 'azure', 'google'], help='Upload Cloud Type [amazon/azure]', dest='clouduploadtype')
    parser.add_argument('-clouddownloadtype', choices=['amazon', 'azure', 'google'], help='Download Cloud Type [amazon/azure/google]', dest='clouddownloadtype')
    parser.add_argument('-inputprofile', help='Input cloud profile name with credentials', dest=InputProfile)
    parser.add_argument('-outputprofile', help='Output cloud profile name with credentials', dest=OutputProfile)
    parser.add_argument('-inputbucket', help='Input cloud bucket/container name', dest='inputbucket')
    parser.add_argument('-outputbucket', help='Output cloud bucket/container name', dest='outputbucket')
    parser.add_argument('-op', help='Utility operation mode [{}/{}/{}/{}/{}]'.format(COP_UPL, COP_NOCONVERT, COP_LAMBDA, COP_COPYONLY, COP_CREATEJOB), dest=Report.CHDR_OP)
    parser.add_argument('-job', help='Name output job/log-prefix file name', dest='job')
    parser.add_argument('-hashkey', help='Hashkey for encryption to use in output paths for cloud storage. e.g. -hashkey=random@1. This will insert the encrypted text using the -hashkey (\'random\') as the first folder name for the output path', dest=CUSR_TEXT_IN_PATH)
    parser.add_argument('-rasterproxypath', help='{} Path to auto-generate raster proxy files during the conversion process'.format(optional), dest='rasterproxypath')
    parser.add_argument('-clonepath', help='Deprecated. Use (-rasterproxypath)', dest='clonepath')
    parser.add_argument('-s3input', help='Deprecated. Use (-clouddownload)', dest='s3input')
    parser.add_argument('-s3output', help='Deprecated. Use (-cloudupload)', dest='s3output')
    parser.add_argument('-queuelength', type=int, help='No of simultaneous rasters to process in lambda function. To use with -op=lambda', dest=Lambda.queue_length)
    parser.add_argument('-usetoken', help='Use token to access cloud data? [true/false: default:false]', dest=UseToken)
    parser.add_argument('-timeit', help='Execution time details report', dest=CTimeIt)

    args = parser.parse_args()
    app = Application(args)
    # app.registerMessageCallback(messageDebug)
    if (not app.init()):
        return eFAIL
    jobStart = datetime.now()
    status = app.run()
    duration = (datetime.now() - jobStart).total_seconds()
    print ('Time taken> {}s'.format(duration))
    return status


if __name__ == '__main__':
    ret = main()
    print ('\nDone..')
    exit(ret)
