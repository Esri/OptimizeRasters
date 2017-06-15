# ------------------------------------------------------------------------------
# Copyright 2017 Esri
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
# Name: access_from_client.py.py
# Description: Sample code snippet to show how to consume OR from custom code.
# Version: 20170416
# Requirements: Python
# Required Arguments: N/A
# Optional Arguments: N/A
# Author: Esri Imagery Workflows team
# ------------------------------------------------------------------------------
# !/usr/bin/env python

import sys
import os

OptimizeRastersRoot = 'c:\Image_Mgmt_Workflows\OptimizeRasters'
sys.path.append(OptimizeRastersRoot)    # Set the root folder where the OptimizeRasters module is found unless the client app is loaded from the same root.

try:
    import OptimizeRasters  # import OptimizeRasters/OR module.
except ImportError as e:
    print ('Err. {}'.format(str(e)))
    exit(1)


def messages(msg, status):
    print ('**{} - {}'.format(msg, status))
    return True


def main():
    args = {
        'input': r'!!path_to_input_data!!',    # input path. eg. c:/input/mydata
        'output': r'!!output_path!!',  # processed output path. eg. c:/output/mydata
        'subs': 'true',    # Do we include subfolders?
        'config': '!!enter_path_to_template_file!!'  # eg. r'c:/Image_Mgmt_Workflows/OptimizeRasters/Templates/Imagery_to_MRF_LERC.xml'
    }
    app = OptimizeRasters.Application(args)  # The args{} can contain any valid cmd-line argument name without the prefix '-'
    app.registerMessageCallback(messages)   # Optional. If messages need to be brought back onto the caller's side.
    if (not app.init()):
        return False
    app.run()  # Do processing..
    rpt = app.getReport()   # Get report/log status
    isSuccess = False
    if (rpt and
            not rpt.hasFailures()):  # If log has no failures, consider the processing as successful.
        isSuccess = True
    print ('Results> {}'.format(str(isSuccess)))
    return isSuccess

if __name__ == '__main__':
    print (main())
