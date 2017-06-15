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
# Name: UsingUIComponents.py
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

OptimizeRastersRoot = 'c:\Image_Mgmt_Workflows\OptimizeRasters'  # default intallation location for OptimizeRasters.
sys.path.append(OptimizeRastersRoot)    # Set the root folder where the OptimizeRasters module is found unless the client app is loaded from the same root.

try:
    from OptimizeRasters import OptimizeRastersUI
except ImportError as e:
    print ('Err. {}'.format(str(e)))
    exit(1)

def main():
    ui = OptimizeRastersUI('!!your_amazon_profile_name!!', OptimizeRastersUI.TypeAmazon)    # profile names are loaded from the credential file @ c:/Users\%username%/.aws/
    availableBuckets = ui.getAvailableBuckets()
    # list all the bucket names attached to the AWS profile.
    errors = [msg for msg in ui.errors]
    if (errors):
        # print internal errors
        for msg in errors:
            print (msg)
        # Optionally, print custom error messsages.
        print ('\nNo buckets found/the user does not have the necessary access rights to list buckets.')
        return False
    if (availableBuckets):
        for bucket in availableBuckets:
            print (bucket)
    # ends
    return True

if __name__ == '__main__':
    print (main())
