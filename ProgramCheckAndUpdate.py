# ------------------------------------------------------------------------------
# Copyright 2013 Esri
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
# Name: ProgramCheckandUpdate.py
# Description: Checks and Updates workflow from Github if required.
# Version: 20191117
# Requirements:
# Author: Esri Imagery Workflows team
# ------------------------------------------------------------------------------
#!/usr/bin/env pythonimport requests

from datetime import datetime
import datetime as dt
import json
import os
import io
import requests
import zipfile
from dateutil.relativedelta import *


class ProgramCheckAndUpdate(object):

    def readCheckForUpdate(self, filepath):
        dict_check = {}
        try:
            with open(filepath) as f:
                content = f.read()
                dict_check = json.loads(content)
                return dict_check
        except BaseException:
            return None

    def readVersionJSON(self, checkFileURL):
        try:
            f = requests.get(checkFileURL)
            x = f.content
            versionJSON = json.loads(x)
            return versionJSON
        except BaseException:
            return None

    def checkUpdate(self, dict_check, versionJSON):
        try:
            current_date = datetime.today().strftime('%Y-%m-%d')
            latest_version = versionJSON['Version']
            dict_check['LastChecked'] = current_date
            currentVersion = dict_check['CurrentVersion']
            if(latest_version > currentVersion):
                dict_check['NewVersion'] = versionJSON['Version']
                dict_check['VersionMessage'] = versionJSON['Message']
                dict_check['UpdateLocation'] = versionJSON['Install']
                return[True, dict_check]
            else:
                return[False, dict_check]
        except BaseException:
            return [False, None]

    def UpdateLocalRepo(self, install_url, path):
        if(install_url.endswith('/')):
            download_url = install_url + 'archive/master.zip'
        else:
            download_url = install_url + '/archive/master.zip'
        repo_download = requests.get(download_url)
        zip_repo = zipfile.ZipFile(io.BytesIO(repo_download.content))
        zip_repo.extractall(path)

    def WriteNewCheckForUpdate(self, dict_check, filepath):
        try:
            with open(filepath, 'w') as f:
                json.dump(dict_check, f, indent=4)
            return True
        except BaseException:
            return False

    def IsCheckRequired(self, dict_check):
        try:
            currentVersion = dict_check['CurrentVersion']
            if("LastChecked" in dict_check.keys()):
                if(dict_check["LastChecked"] == ""):
                    lastChecked = "1970-01-01"
                else:
                    lastChecked = dict_check['LastChecked']
            else:
                lastChecked = "1970-01-01"
            lastChecked_dateobj = datetime.strptime(lastChecked, '%Y-%m-%d')
            checkForUpdate = dict_check['CheckForUpdate']
            current_date = datetime.today().strftime('%Y-%m-%d')
            if(checkForUpdate == "Never"):
                return False
            elif(checkForUpdate == "Daily"):
                if(current_date > lastChecked):
                    return True
                else:
                    return False
            elif(checkForUpdate == "Monthly"):
                update_date = (lastChecked_dateobj + dt.timedelta(days=+30)).strftime('%Y-%m-%d')
                if(current_date > update_date):
                    return True
                else:
                    return False
        except BaseException:
            return None

    def run(self, localrepo_path):
        try:
            checkUpdateFilePath = os.path.join(localrepo_path, "CheckForUpdate.json")
            chkupdate = self.readCheckForUpdate(checkUpdateFilePath)
            if chkupdate is None:
                return "Unable to read CheckForUpdate JSON"
            if(self.IsCheckRequired(chkupdate)):
                versionJSON = self.readVersionJSON(checkFileURL=chkupdate['CheckFile'])
                if versionJSON is None:
                    return "Unable to read VersionJSON"
                [update_available, dict_check] = self.checkUpdate(chkupdate, versionJSON)
                self.WriteNewCheckForUpdate(dict_check, checkUpdateFilePath)
                if(update_available):
                    if(dict_check['OnNewVersion'] == "Warn"):
                        return("Update Available. Please read " + str(checkUpdateFilePath))
                    elif(dict_check['OnNewVersion'] == "Ignore"):
                        return("Ignore")
                    elif(dict_check['OnNewVersion'] == "Update"):
                        self.UpdateLocalRepo(versionJSON['Install'], path=os.path.join((os.path.dirname(localrepo_path)), "Updated"))
                    else:
                        return("Incorrect Parameter. Please check OnNewVersion Parameter in " + str(checkUpdateFilePath))
                else:
                    return("Installed version is the latest version.")
            else:
                try:
                    if(chkupdate['NewVersion'] is not (None or '')):
                        return("Update Available. Please read "+ str(checkUpdateFilePath))
                    current_date = datetime.today().strftime('%Y-%m-%d')
                    chkupdate['LastChecked'] = current_date
                    self.WriteNewCheckForUpdate(chkupdate, checkUpdateFilePath)
                except Exception as e:
                    return(str(e))

        except Exception as e:
            return str(e)
