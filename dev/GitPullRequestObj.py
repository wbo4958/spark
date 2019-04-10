#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# File name: GitPullRequestObj.py
# Author: Vaibhav Jagannath Desai
# Date created: 3/20/2019
# Date last modified: 4/09/2019
# Python Version: 2.7
#
# This class holds all the properties associated with Pull Request/Merge Request.
# It aims to abstract out the different response schema for a PR/MR from GitLab and GitHub
# into a consistant set of properties.
# Example:
#   Target Branch in GitHub PR API response can be found under `base->label`
#   Target Branch in GitLab MR API response can be found under `target_branch`
#

import os


class GitPullRequestObj(object):

    def __init__(self, pr_id, git_vendor, git_endpoint, project_url, oauth_key):

        self.pr_id = pr_id
        self.git_vendor = git_vendor
        self.headers = {}
        self.headers["Content-Type"] = "application/json"
        self.git_endpoint = git_endpoint
        self.project_url = project_url

        if self.git_vendor == "GitHub":
            self.headers["Authorization"] = "token %s" % oauth_key
            self.pr_api_url = self.git_endpoint + "/pulls/" + self.pr_id
        elif self.git_vendor == "GitLab":
            self.headers["Private-Token"] = oauth_key
            self.pr_api_url = self.git_endpoint + "/merge_requests/" + self.pr_id

    @property
    def pr_api_url(self):
        return self.__pr_api_url

    @pr_api_url.setter
    def pr_api_url(self, pr_api_url):
        if not pr_api_url:
            raise Exception("PR API Url cannot be empty")
        self.__pr_api_url = pr_api_url

    @property
    def pr_title(self):
        return self.__pr_title

    @pr_title.setter
    def pr_title(self, pr_title):
        if not pr_title:
            raise Exception("PR Title cannot be empty")
        self.__pr_title = pr_title

    @property
    def labels(self):
        return self.__labels

    @labels.setter
    def labels(self, labels):
        self.__labels = labels

    @property
    def head_sha(self):
        return self.__head_sha

    @head_sha.setter
    def head_sha(self, head_sha):
        self.__head_sha = head_sha

    @property
    def pr_id(self):
        return self.__pr_id

    @pr_id.setter
    def pr_id(self, pr_id):
        if not pr_id:
            raise Exception("PR ID cannot be empty")
        self.__pr_id = pr_id

    @property
    def headers(self):
        return self.__headers

    @headers.setter
    def headers(self, headers):
        self.__headers = headers

    @property
    def git_vendor(self):
        return self.__git_vendor

    @git_vendor.setter
    def git_vendor(self, git_vendor):
        if not git_vendor:
            raise Exception("Git Vendor cannot be empty")
        self.__git_vendor = git_vendor

    @property
    def commit_url(self):
        return self.project_url+"/commit/"+self.head_sha

    @property
    def comment_url(self):
        if self.git_vendor == "GitHub":
            return self.git_endpoint + "/issues/" + self.pr_id + "/comments"
        elif self.git_vendor == "GitLab":
            return self.git_endpoint + "/merge_requests/" + self.pr_id + "/notes"

    @property
    def comment_message(self):
        return self.__comment_message

    @comment_message.setter
    def comment_message(self, comment_message):
        self.__comment_message = comment_message
