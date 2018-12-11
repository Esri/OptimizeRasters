# ------------------------------------------------------------------------------
# Copyright 2018 Esri
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
# Name: OptimizeRasters.pyt
# Description: UI for OptimizeRasters
# Version: 20181104
# Requirements: ArcMap / gdal_translate / gdaladdo
# Required Arguments:optTemplates, inType, inprofiles, inBucket, inPath, outType
# outprofiles, outBucket, outPath
# Optional Arguments:intempFolder, outtempFolder, cloneMRFFolder, cacheMRFFolder
# Usage: To load within ArcMap
# Author: Esri Imagery Workflows team
# ------------------------------------------------------------------------------

import arcpy
from arcpy import env
import sys
import os
import subprocess
import time
import io
if (sys.version_info[0] < 3):
    import ConfigParser
else:
    import configparser as ConfigParser
from xml.dom import minidom
from datetime import datetime

templateinUse = None
AzureRoot = '.OptimizeRasters/Microsoft'
GoogleRoot = '.OptimizeRasters/Google'
AwsRoot = '.aws'
PublicBucket = 'Public Bucket'
EC2WIAMRole = 'Using_EC2 Instance_with_IAM_Role'


def returnDate():
    sDate = str(datetime.date(datetime.today())).replace('-', '')
    sTime = str(datetime.time(datetime.today())).split('.')[0].replace(':', '')
    return sDate + sTime


def setXMLXPathValue(doc, xPath, key, value):
    if (not doc or
        not xPath or
        not key or
            not value):
        return False
    nodes = doc.getElementsByTagName(key)
    if (not nodes):
        userNode = doc.createElement(key)
        userNode.appendChild(doc.createTextNode(value))
        defNodes = doc.getElementsByTagName('Defaults')
        if (not defNodes):  # shouldn't happen
            return False
        defNodes[0].appendChild(userNode)
        nodes = doc.getElementsByTagName(key)
    for node in nodes:
        parents = []
        c = node
        while(c.parentNode):
            parents.insert(0, c.nodeName)
            c = c.parentNode
        p = '/'.join(parents)
        if (p == xPath):
            if (not node.hasChildNodes()):
                node.appendChild(doc.createTextNode(value))
                return True
            node.firstChild.data = str(value)
            return True
    return False


def returntemplatefiles():
    selfscriptpath = os.path.dirname(__file__)
    templateloc = os.path.join(selfscriptpath, 'templates')
    templatefilelist = os.listdir(templateloc)
    global allactualxmlFiles
    allactualxmlFiles = []
    allxmlFiles = []
    for ft in templatefilelist:
        if ft.endswith('.xml'):
            allactualxmlFiles.append(ft)
            ft = ft.replace('.xml', '')
            allxmlFiles.append(ft)

    userTempLoc = os.path.join(selfscriptpath, 'UserTemplates')
    if os.path.exists(userTempLoc) == True:
        userTempLoclist = os.listdir(userTempLoc)
        for ft in userTempLoclist:
            if ft.endswith('.xml'):
                allactualxmlFiles.append(ft)
                ft = ft.replace('.xml', '')
                allxmlFiles.append(ft)

    return allxmlFiles


def returnjobFiles():
    selfscriptpath = os.path.dirname(__file__)

    jobfileList = os.listdir(selfscriptpath)
    allactualjobfiles = []
    alljobFiles = []

    for ft in jobfileList:
        if ft.endswith('.orjob'):
            allactualjobfiles.append(ft)
            ft = ft.replace('.orjob', '')
            alljobFiles.append(ft)

    return alljobFiles


def setPaths(xFname, values):
    overExisting = True
    rootPath = 'OptimizeRasters/Defaults/'
    xfName2 = os.path.normpath(xFname)
    doc = minidom.parse(xfName2)
    for keyValueList in values:
        aKey = keyValueList[0]
        aVal = keyValueList[1]
        pathtoreplace = rootPath + aKey
        setXMLXPathValue(doc, pathtoreplace, aKey, aVal)
    if 'UserTemplates' in xFname:
        if overExisting == True:
            fnToWrite = xfName2
        else:
            asuffix = returnDate()
            fnToWrite = xfName2.replace('.xml', '_' + asuffix + '.xml')
    else:
        selfscriptpath = os.path.dirname(__file__)
        userLoc = os.path.join(selfscriptpath, 'UserTemplates')
        if os.path.exists(userLoc) == False:
            os.mkdir(userLoc)
        baseName = os.path.basename(xFname)
        asuffix = returnDate()
        baseName = baseName.replace('.xml', '_' + asuffix + '.xml')
        fnToWrite = os.path.join(userLoc, baseName)
    with io.open(fnToWrite, 'w', encoding='utf-8') as writer:
        writer.write(doc.toxml())
    return fnToWrite


def returnPaths(xFname):
    keyList = ['Mode', 'RasterFormatFilter', 'ExcludeFilter', 'IncludeSubdirectories', 'Compression', 'Quality', 'LERCPrecision', 'BuildPyramids', 'PyramidFactor', 'PyramidSampling', 'PyramidCompression', 'NoDataValue', 'BlockSize', 'Scale', 'KeepExtension', 'Threads', 'Op', 'GDAL_Translate_UserParameters']
    xfName2 = os.path.normpath(xFname)
    if (not os.path.exists(xfName2)):
        return None
    doc = minidom.parse(xfName2)
    valueList = []
    for key in keyList:
        nodes = doc.getElementsByTagName(key)
        if (nodes.length == 0):
            valueList.append([key, ''])
            continue
        for node in nodes:
            if node.firstChild is not None:
                aVal = node.firstChild.data
            else:
                aVal = ''
            valueList.append([key, aVal])
    return ([keyList, valueList])


def attchValues(toolcontrol, allValues):
    keylist = allValues[0]
    valList = allValues[1]
    toolcontrol.value = valList
    return


def returnTempFolder():
    templiist = []
    templiist.append('TMP')
    templiist.append('TEMP')
    for tt in templiist:
        tempVal = os.getenv(tt)
        if tempVal is not None:
            return tempVal
            break


def config_Init(parentfolder, filename):
    if (not parentfolder or
            not filename):
        return None
    global config
    global awsfile
    config = ConfigParser.RawConfigParser()
    homedrive = os.getenv('HOMEDRIVE')
    homepath = os.getenv('HOMEPATH')
    homefolder = os.path.join(homedrive, homepath)
    awsfolder = os.path.join(homefolder, parentfolder)
    if (filename == '*.json'):  # google cs filter
        for r, d, f in os.walk(awsfolder):
            for service in f:
                config.add_section(os.path.join(r, service).replace('\\', '/'))
            break
        return config
    awsfile = os.path.join(awsfolder, filename)
    if os.path.exists(awsfile) == True:
        try:
            config.read(awsfile)
            if (not config.has_section(PublicBucket)):
                config.add_section(PublicBucket)
            if (not config.has_section(EC2WIAMRole)):
                config.add_section(EC2WIAMRole)
        except BaseException:
            pass
        return config
    else:
        if os.path.exists(os.path.dirname(awsfile)) == False:
            os.makedirs(os.path.dirname(awsfile))
        mode = 'w+'
        tmpFile = open(awsfile, mode)
        with open(awsfile, mode) as tmpFIle:
            tmpFIle.close
        return config


def config_writeSections(configfileName, peAction, section, option1, value1, option2, value2, option3, value3):
    peAction_ = peAction.lower()
    if peAction_ == 'overwrite existing':
        appConfig = config
        appConfig.remove_section(section)
        mode = 'w'
    elif peAction_ == 'delete existing':
        config.remove_section(section)
        mode = 'w'
        with open(configfileName, mode) as configfile:
            config.write(configfile)
        return True
    else:
        appConfig = ConfigParser.RawConfigParser()
        mode = 'a'
    isIAMRole = section.lower().startswith(EC2WIAMRole.lower())
    # let's validate the credentials before writing out.
    if (not isIAMRole and
            (peAction_.startswith('overwrite') or     # update existing or add new but ignore for del.
             mode == 'a')):
        try:
            import OptimizeRasters
        except Exception as e:
            arcpy.AddError(str(e))
            return False
        storageType = OptimizeRasters.CCLOUD_AMAZON
        if (option1):
            if (option1.lower().startswith('azure')):
                storageType = OptimizeRasters.CCLOUD_AZURE
            elif (option1.lower().startswith('alibaba')):
                storageType = OptimizeRasters.ProfileEditorUI.TypeAlibaba
        profileEditorUI = OptimizeRasters.ProfileEditorUI(section, storageType, value1, value2, aws_endpoint_url=value3)
        ret = profileEditorUI.validateCredentials()
        if (not ret):
            [arcpy.AddError(i) for i in profileEditorUI.errors]
            return False
    # ends
    appConfig.add_section(section)
    if (not isIAMRole):  # if not IAM role, write out the credential key pair
        appConfig.set(section, option1, value1)
        appConfig.set(section, option2, value2)
    if (value3):
        appConfig.set(section, option3, value3.strip())
    with open(configfileName, mode) as configfile:
        appConfig.write(configfile)
    return True


def getAvailableBuckets(ctlProfileType, ctlProfileName):
    response = {'response': {'results': False, 'buckets': []}}
    try:
        import OptimizeRasters
        if (ctlProfileType.valueAsText):
            inputSourceType = ctlProfileType.valueAsText.lower()
            storageType = OptimizeRasters.Store.TypeAmazon
            if (inputSourceType.startswith('local')):
                return response
            elif (inputSourceType.find('azure') != -1):
                storageType = OptimizeRasters.Store.TypeAzure
            elif (inputSourceType.find('google') != -1):
                storageType = OptimizeRasters.Store.TypeGoogle
            ORUI = OptimizeRasters.OptimizeRastersUI(ctlProfileName.value, storageType)
            if (not ORUI):
                raise Exception()
            return ORUI.getAvailableBuckets()
    except BaseException:
        pass
    return response


def checkPrerequisites(parameters, cloudType, ctrlIndexPos):
    cType = 'boto3'
    try:
        if (cloudType.startswith('Amazon')):
            import boto3
        elif (cloudType.startswith('Microsoft')):
            cType = 'azure'
            from azure.storage.blob import BlockBlobService
        elif (cloudType.startswith('Google')):
            cType = 'google-cloud'
            from google.cloud import storage
    except ImportError as e:
        parameters[ctrlIndexPos].setErrorMessage('{}\nTo fix, please install the python module ({}).'.format(str(e), cType))
        return False
    return True


class Toolbox(object):

    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the
        .pyt file)."""
        self.label = "Toolbox"
        self.alias = ""
        # List of tool classes associated with this toolbox
        self.tools = [OptimizeRasters, ProfileEditor, ResumeJobs]


class ResumeJobs(object):

    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Resume Jobs"
        self.description = ""
        self.canRunInBackground = True
        self.tool = 'ProfileEditor'

    def getParameterInfo(self):
        pendingJobs = arcpy.Parameter(
            displayName="Pending Jobs",
            name="pendingJobs",
            datatype="GPString",
            parameterType="Required",
            direction="Input")
        pendingJobs.filter.type = "ValueList"

        pendingJobs.filter.list = returnjobFiles()
        parameters = [pendingJobs]
        return parameters
        pass

    def updateParameters(self, parameters):
        # let's remove the entry if successful executed thru (def execute).
        pendingJobs = parameters[0]
        pendingJobs.filter.list = returnjobFiles()
        parameters[0] = [pendingJobs]
        # ends

    def updateMessages(self, parameters):
        pass

    def isLicensed(parameters):
        """Set whether tool is licensed to execute."""
        return True

    def execute(self, parameters, messages):
        CORJOB = '.orjob'
        args = {}
        aJob = parameters[0].valueAsText
        if (not aJob.lower().endswith(CORJOB)):
            aJob += CORJOB
        template_path = os.path.realpath(__file__)
        configFN = '{}/{}'.format(os.path.dirname(template_path), os.path.basename(aJob)).replace('\\', '/')
        if (not os.path.exists(configFN)):      # detect errors early.
            arcpy.AddError('Err. OptimizeRasters job file ({}) is not found!'.format(configFN))
            return False
        args['input'] = configFN
       # let's run (OptimizeRasters)
        import OptimizeRasters
        app = OptimizeRasters.Application(args)
        if (not app.init()):
            arcpy.AddError('Err. Unable to initialize (OptimizeRasters module)')
            return False
        app.postMessagesToArcGIS = True
        return app.run()
        # ends


class ProfileEditor(object):

    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Profile Editor"
        self.description = ""
        self.canRunInBackground = True
        self.tool = 'ProfileEditor'

    def getParameterInfo(self):
        profileType = arcpy.Parameter(
            displayName="Profile Type",
            name="profileType",
            datatype="GPString",
            parameterType="Required",
            direction="Input")
        profileType.filter.type = "ValueList"
        profileType.filter.list = ['Amazon S3', 'Microsoft Azure']
        profileType.value = 'Amazon S3'

        profileName = arcpy.Parameter(
            displayName="Profile Name",
            name="profileName",
            datatype="GPString",
            parameterType="Required",
            direction="Input")
        #profileName.value = 'or_public_in'

        iAmRolePara = arcpy.Parameter(
            displayName="IAM Role Profile",
            name="iAmRolePara",
            datatype="GPBoolean",
            parameterType="Optional",
            direction="Input")

        accessKey = arcpy.Parameter(
            displayName="Access/Account Key ID",
            name="accessKey",
            datatype="GPString",
            parameterType="Required",
            direction="Input")

        secretAccessKey = arcpy.Parameter(
            displayName="Secret Access/Account Key",
            name="secretAccessKey",
            datatype="GPString",
            parameterType="Required",
            direction="Input")

        imRoleURL = arcpy.Parameter(
            displayName="Endpoint URL",
            name="imRoleURL",
            datatype="GPString",
            parameterType="Optional",
            direction="Input")

        action = arcpy.Parameter(
            displayName="Editor Option",
            name="action",
            datatype="GPString",
            parameterType="Optional",
            direction="Input")

        action.filter.type = "ValueList"
        action.filter.list = ['Overwrite Existing', 'Delete Existing']
        action.value = 'Overwrite Existing'
        action.enabled = False

        iAmRolePara.value = False
        parameters = [profileType, profileName, accessKey, secretAccessKey, imRoleURL, action]
        return parameters

    def updateParameters(self, parameters):
        if parameters[0].altered == True:
            pType = parameters[0].valueAsText
            if pType == 'Amazon S3':
                pFolder = AwsRoot
                pfileName = 'credentials'
            elif pType == 'Microsoft Azure':
                pFolder = AzureRoot
                pfileName = 'azure_credentials'
            config_Init(pFolder, pfileName)
            if parameters[1].altered == True:
                pName = parameters[1].valueAsText
                if (config.has_section(pName)):
                    parameters[5].enabled = True
                else:
                    parameters[5].enabled = False
        if parameters[5].enabled == True:
            pass
            if parameters[2].value is None:
                parameters[2].value = 'None'
            if parameters[3].value is None:
                parameters[3].value = 'None'
        else:
            pass
            if parameters[2].value == 'None':
                parameters[2].value = ''
            if parameters[3].value == 'None':
                parameters[3].value = ''

    def updateMessages(self, parameters):
        if parameters[0].altered == True:
            pType = parameters[0].valueAsText
            if (pType != 'Amazon S3') and (pType != 'Microsoft Azure'):
                parameters[0].setErrorMessage('Invalid Value. Pick from List only.')
                return
            else:
                if (not checkPrerequisites(parameters, pType, 0)):
                    return False
                parameters[0].clearMessage()
            if parameters[1].altered == True:
                pType = parameters[0].valueAsText
                pName = parameters[1].valueAsText
                if (config.has_section(pName)):
                    parameters[1].setWarningMessage('Profile name already exists. Select the appropriate action.')
                else:
                    parameters[1].clearMessage()

    def isLicensed(parameters):
        """Set whether tool is licensed to execute."""
        return True

    def execute(self, parameters, messages):
        pType = parameters[0].valueAsText
        if pType == 'Amazon S3':
            pFolder = AwsRoot
            pfileName = 'credentials'
            option1 = 'aws_access_key_id'
            option2 = 'aws_secret_access_key'
            option3 = 'aws_endpoint_url'
        elif pType == 'Microsoft Azure':
            pFolder = AzureRoot
            pfileName = 'azure_credentials'
            option1 = 'azure_account_name'
            option2 = 'azure_account_key'
            option3 = 'azure_endpoint_url'
        pName = parameters[1].valueAsText
        if parameters[5].enabled == False:
            peAction = ''
        else:
            peAction = parameters[5].valueAsText
        accessKeyID = parameters[2].valueAsText
        accessSeceretKey = parameters[3].valueAsText
        endPointURL = parameters[4].valueAsText
        config_writeSections(awsfile, peAction, pName, option1, accessKeyID, option2, accessSeceretKey, option3, endPointURL)


class OptimizeRasters(object):

    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "OptimizeRasters"
        self.description = ""
        self.canRunInBackground = True
        self.tool = 'ConvertFiles'

    def getParameterInfo(self):
        storageTypes = ['Local', 'Amazon S3', 'Microsoft Azure', 'Google Cloud']    # 'local' must be the first element.
        optTemplates = arcpy.Parameter(
            displayName="Configuration File",
            name="optTemplates",
            datatype="GPString",
            parameterType="Required",
            direction="Input")
        optTemplates.filter.type = "ValueList"
        optTemplates.filter.list = returntemplatefiles()

        inType = arcpy.Parameter(
            displayName="Input Source",
            name="inType",
            datatype="GPString",
            parameterType="Required",
            direction="Input")
        inType.filter.type = "ValueList"
        inType.filter.list = storageTypes
        inType.value = storageTypes[0]

        inprofiles = arcpy.Parameter(
            displayName="Input Profile",
            name="inprofiles",
            datatype="GPString",
            parameterType="Required",
            direction="Input")
        inprofiles.filter.type = "ValueList"

        inBucket = arcpy.Parameter(
            displayName="Input Bucket/Container",
            name="inBucket",
            datatype="GPString",
            parameterType="Required",
            direction="Input")
        inBucket.filter.type = "ValueList"

        inPath = arcpy.Parameter(
            displayName="Input Path",
            name="inPath",
            datatype=['DEFolder', 'GPString'],
            parameterType="Required",
            direction="Input")

        intempFolder = arcpy.Parameter(
            displayName="Input Temporary Folder",
            name="intempFolder",
            datatype="DEFolder",
            parameterType="Optional",
            direction="Input")

        outType = arcpy.Parameter(
            displayName=" Output Destination",
            name="outType",
            datatype="GPString",
            parameterType="Required",
            direction="Input")
        outType.filter.type = "ValueList"
        outType.filter.list = storageTypes
        outType.value = storageTypes[0]

        outprofiles = arcpy.Parameter(
            displayName="Output Profile",
            name="outprofiles",
            datatype="GPString",
            parameterType="Required",
            direction="Input")
        outprofiles.filter.type = "ValueList"

        outBucket = arcpy.Parameter(
            displayName="Output Bucket/Container",
            name="outBucket",
            datatype="GPString",
            parameterType="Required",
            direction="Input")

        outPath = arcpy.Parameter(
            displayName="Output Path",
            name="outPath",
            datatype=['DEFolder', 'GPString'],
            parameterType="Required",
            direction="Input")

        outtempFolder = arcpy.Parameter(
            displayName="Output Temporary Folder",
            name="outtempFolder",
            datatype="DEFolder",
            parameterType="Optional",
            direction="Input")

        cloneMRFFolder = arcpy.Parameter(
            displayName="Raster Proxy Output Folder",
            name="rasterProxyFolder",
            datatype="DEFolder",
            parameterType="Optional",
            direction="Input")

        cacheMRFFolder = arcpy.Parameter(
            displayName="Cache Folder",
            name="cacheMRFFolder",
            datatype="DEFolder",
            parameterType="Optional",
            direction="Input")

        editValue = arcpy.Parameter(
            displayName="Edit Configuration Values",
            name="editValue",
            datatype="GPBoolean",
            parameterType="Optional",
            direction="Input")
        editValue.category = 'Advanced'

        configVals = arcpy.Parameter(
            displayName='Configuration Values:',
            name='configVals',
            datatype='GPValueTable',
            parameterType='Optional',
            direction='Input')
        configVals.columns = [['GPString', 'Parameter'], ['GPString', 'Value']]
        configVals.enabled = False
        configVals.category = 'Advanced'

        issecured = arcpy.Parameter(
            displayName='Secured Bucket',
            name='issecured',
            datatype='GPBoolean',
            parameterType='Optional',
            direction='Input')

        parameters = [optTemplates, inType, inprofiles, issecured, inBucket, inPath, intempFolder, outType, outprofiles, outBucket, outPath, outtempFolder, cloneMRFFolder, cacheMRFFolder, editValue, configVals]
        return parameters

    def updateParameters(self, parameters):
        if (parameters[4].value):
            parameters[4].value = parameters[4].value.strip()
        if (parameters[9].value):
            parameters[9].value = parameters[9].value.strip()
        configParams = parameters[0]
        configParams.filter.list = returntemplatefiles()
        if parameters[14].value == True:
            parameters[15].enabled = True
        else:
            parameters[15].enabled = False
        if parameters[0].altered == True:
            if parameters[15].altered == False:
                optTemplates = parameters[0].valueAsText
                global templateinUse
                templateinUse = optTemplates
                template_path = os.path.realpath(__file__)
                _CTEMPLATE_FOLDER = 'Templates'
                configFN = os.path.join(os.path.join(os.path.dirname(template_path), _CTEMPLATE_FOLDER), optTemplates + '.xml')
                if not os.path.exists(configFN):
                    _CTEMPLATE_FOLDER = 'UserTemplates'
                    configFN = os.path.join(os.path.join(os.path.dirname(template_path), _CTEMPLATE_FOLDER), optTemplates + '.xml')
                allValues = returnPaths(configFN)
                if (allValues):
                    attchValues(parameters[15], allValues)
            else:
                optTemplates = parameters[0].valueAsText
                if templateinUse != optTemplates:
                    template_path = os.path.realpath(__file__)
                    _CTEMPLATE_FOLDER = 'Templates'
                    configFN = os.path.join(os.path.join(os.path.dirname(template_path), _CTEMPLATE_FOLDER), optTemplates + '.xml')
                    if not os.path.exists(configFN):
                        _CTEMPLATE_FOLDER = 'UserTemplates'
                        configFN = os.path.join(os.path.join(os.path.dirname(template_path), _CTEMPLATE_FOLDER), optTemplates + '.xml')
                    allValues = returnPaths(configFN)
                    if (allValues):
                        attchValues(parameters[15], allValues)
                    templateinUse = optTemplates
        if parameters[1].altered == True:
            if parameters[1].valueAsText == 'Local':
                parameters[2].filter.list = []
                parameters[4].filter.list = []
                parameters[2].value = 'Profile'
                parameters[2].enabled = False
                parameters[4].enabled = False
                parameters[3].value = False
                parameters[3].enabled = False
            else:
                pFolder = pfileName = None
                parameters[3].enabled = True
                if parameters[1].valueAsText == 'Amazon S3':
                    pFolder = AwsRoot
                    pfileName = 'credentials'
                    parameters[2].enabled = True
                    parameters[4].enabled = True
                elif parameters[1].valueAsText == 'Microsoft Azure':
                    pFolder = AzureRoot
                    pfileName = 'azure_credentials'
                    parameters[2].enabled = True
                    parameters[4].enabled = True
                elif parameters[1].valueAsText == 'Google Cloud':
                    pFolder = GoogleRoot
                    pfileName = '*.json'
                    parameters[2].enabled = True
                    parameters[4].enabled = True
                if parameters[4].value == 'Local':
                    parameters[4].value = ''
                if parameters[2].value == 'Profile':
                    parameters[2].value = ''
                if (pFolder):
                    p2Config = config_Init(pFolder, pfileName)
                    if (p2Config):
                        p2List = p2Config.sections()
                        parameters[2].filter.list = p2List
        if parameters[2].altered == True:
            # fetch the list of bucket names available for the selected input profile
            response = getAvailableBuckets(parameters[1], parameters[2])
            results = response['response']['results']
            if (results and
                    response['response']['buckets']):
                parameters[4].filter.list = response['response']['buckets']        # 3 == bucket names
            else:
                if (parameters[1].value == 'Local'):
                    parameters[4].filter.list = [' ']
                    parameters[4].enabled = False
                    parameters[4].value = ' '
                else:
                    parameters[4].filter.list = []
                    if (parameters[2].value is not None and
                        not parameters[2].value.lower().startswith(EC2WIAMRole.lower()) and
                            not parameters[2].value.lower().startswith(PublicBucket.lower())):
                        if (not parameters[2].value.lower().endswith('public-buckets.json')):
                            if (not results):
                                parameters[4].value = ''
            # ends
        if parameters[7].altered == True:
            if parameters[7].valueAsText == 'Local':
                parameters[8].filter.list = []
                parameters[9].filter.list = []
                parameters[8].value = 'Profile'
                parameters[9].value = 'Local'
                parameters[8].enabled = False
                parameters[9].enabled = False
            else:
                pFolder = pfileName = None
                parameters[8].enabled = True
                parameters[9].enabled = True
                if parameters[7].valueAsText == 'Amazon S3':
                    pFolder = AwsRoot
                    pfileName = 'credentials'
                    parameters[8].enabled = True
                    parameters[9].enabled = True
                elif parameters[7].valueAsText == 'Microsoft Azure':
                    pFolder = AzureRoot
                    pfileName = 'azure_credentials'
                    parameters[8].enabled = True
                    parameters[9].enabled = True
                elif parameters[7].valueAsText == 'Google Cloud':
                    pFolder = GoogleRoot
                    pfileName = '*.json'
                    parameters[8].enabled = True
                    parameters[9].enabled = True
                if parameters[9].value == 'Local':
                    parameters[9].value = ''
                if parameters[8].value == 'Profile':
                    parameters[8].value = ''
                if (pFolder):
                    p6Config = config_Init(pFolder, pfileName)
                    if (p6Config):
                        p6List = p6Config.sections()
                        parameters[8].filter.list = p6List

        if parameters[9].altered == True:
            # fetch the list of bucket names available for the selected output profile
            response = getAvailableBuckets(parameters[7], parameters[8])
            results = response['response']['results']
            if (results and
                    response['response']['buckets']):
                parameters[9].filter.list = response['response']['buckets']        # 8 == bucket names
            else:
                if (parameters[7].value == 'Local'):
                    parameters[9].filter.list = [' ']
                    parameters[9].value = ' '
                    parameters[9].enabled = False
                else:
                    parameters[9].filter.list = []
                    if (parameters[8].value is not None and
                            not parameters[8].value.lower().startswith(EC2WIAMRole.lower())):
                        if (not parameters[8].value.lower().endswith('public-buckets.json')):
                            if (not results):
                                parameters[9].value = ''
            # ends
        if parameters[15].altered == True:
            configValList = parameters[15].value
            aVal = configValList[0][1].strip().lower()
            op = configValList[len(configValList) - 1][1].strip().lower()
            if (aVal == 'clonemrf' or aVal == 'cachingmrf' or aVal == 'rasterproxy'):
                parameters[11].enabled = False
                parameters[12].enabled = False
                parameters[13].enabled = True
            else:
                parameters[11].enabled = True
                parameters[12].enabled = True
                parameters[13].enabled = True
                if (op == 'copyonly'):
                    parameters[12].enabled = False
                    parameters[13].enabled = False

    def updateMessages(self, parameters):
        storageTypes = ('Local', 'Amazon S3', 'Microsoft Azure', 'Google Cloud')    # 'local' must be the first element.
        errMessageListOnly = 'Invalid Value. Pick from List only.'
        if parameters[1].altered == True:
            pType = parameters[1].valueAsText
            if (pType not in storageTypes):
                parameters[1].setErrorMessage(errMessageListOnly)
            else:
                if (not checkPrerequisites(parameters, pType, 1)):
                    return False
                parameters[1].clearMessage()
        if parameters[7].altered == True:
            pType = parameters[7].valueAsText
            if (pType not in storageTypes):
                parameters[7].setErrorMessage(errMessageListOnly)
            else:
                if (not checkPrerequisites(parameters, pType, 7)):
                    return False
                parameters[7].clearMessage()
                if (pType in storageTypes[1:]):  # skip the first element/local.
                    if parameters[11].altered == False:
                        if parameters[11].enabled == True:
                            parameters[11].SetWarningMessage('For cloud storage output, a temporary output location is required.')
                    else:
                        if parameters[11].valueAsText != '':
                            parameters[11].clearMessage()

    def isLicensed(parameters):
        """Set whether tool is licensed to execute."""
        return True

    def execute(self, parameters, messages):
        args = {}
        optTemplates = parameters[0].valueAsText
        template_path = os.path.realpath(__file__)
        _CTEMPLATE_FOLDER = 'Templates'
        configFN = os.path.join(os.path.join(os.path.dirname(template_path), _CTEMPLATE_FOLDER), optTemplates + '.xml')
        if os.path.exists(configFN) == False:
            _CTEMPLATE_FOLDER = 'UserTemplates'
            configFN = os.path.join(os.path.join(os.path.dirname(template_path), _CTEMPLATE_FOLDER), optTemplates + '.xml')

        inType = parameters[1].valueAsText
        inprofiles = parameters[2].valueAsText
        inBucket = parameters[4].valueAsText
        inPath = parameters[5].valueAsText
        intempFolder = parameters[6].valueAsText
        outType = parameters[7].valueAsText
        outprofiles = parameters[8].valueAsText
        outBucket = parameters[9].valueAsText
        outPath = parameters[10].valueAsText
        outtempFolder = parameters[11].valueAsText
        cloneMRFFolder = parameters[12].valueAsText
        cacheOutputFolder = parameters[13].valueAsText

        if parameters[14].enabled == True:
            if parameters[14].value == True:
                editedValues = parameters[15].value
                configFN = setPaths(configFN, editedValues)
        args['config'] = configFN
        args['output'] = outPath
        args['tempinput'] = intempFolder
        if (outtempFolder):
            args['tempoutput'] = outtempFolder  # used only if -cloudupload=true
        args['input'] = inPath
        if inType == 'Local':
            pass
        else:
            args['clouddownload'] = 'true'
            args['usetoken'] = parameters[3].valueAsText
            args['inputbucket'] = inBucket      # case-sensitive
            if (not inprofiles.lower().startswith(PublicBucket.lower())):
                args['inputprofile'] = inprofiles
            if inType == 'Amazon S3':
                args['clouddownloadtype'] = 'amazon'
            elif inType == 'Microsoft Azure':
                args['clouddownloadtype'] = 'azure'
            elif inType == 'Google Cloud':
                args['clouddownloadtype'] = 'google'
        if outType == 'Local':
            pass
        else:
            args['cloudupload'] = 'true'
            args['outputprofile'] = outprofiles
            clouduploadtype_ = 'amazon'
            if (outType == 'Microsoft Azure'):
                clouduploadtype_ = 'azure'
            elif (outType == 'Google Cloud'):
                clouduploadtype_ = 'google'
            args['clouduploadtype'] = clouduploadtype_
            args['outputbucket'] = outBucket
        if cacheOutputFolder is not None:
            args['cache'] = cacheOutputFolder
        if cloneMRFFolder is not None:
            args['rasterproxypath'] = cloneMRFFolder

        # let's run (OptimizeRasters)
        import OptimizeRasters
        app = OptimizeRasters.Application(args)
        if (not app.init()):
            arcpy.AddError('Err. Unable to initialize (OptimizeRasters module)')
            return False
        app.postMessagesToArcGIS = True
        return app.run()
        # ends
