#!/usr/bin/env python2

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

#
# This script interacts with GitHub/GitLab to post the PR status.
# It sets the required env variables and internally calls run-tests.py script.
# The Spark testsuites are ran only for specific modules based on the files changed.
#

from __future__ import print_function
import os
import sys
import json
import requests
import functools
import subprocess

from sparktestsupport import SPARK_HOME, ERROR_CODES, TEST_TIMEOUT
from sparktestsupport.shellutils import run_cmd

import json
import importlib
from GitPullRequestObj import *


def make_rest_api_calls(request_type, url, headers, payload=None):
    """
    Makes a GET/POST request API calls to GitHub/GitLab.
    """

    response = None
    if request_type == "GET":
        response = requests.get(url, headers=headers)
        if response.status_code == requests.codes.ok:
            return json.loads(response.text)

    elif request_type == "POST":
        response = requests.post(url, headers=headers, data=payload)
        if response.status_code == requests.codes.created:
            print(" > Post successful.")
            return
    else:
        # If the request type is not GET or POST
        print(" > Request Type: ", request_type)
        raise ValueError("Invalid Request Type")

    response.raise_for_status()


def post_pr_message(git_pr_obj):
    """
    Creates GitHub/GitLab PR comments API end point using properties of GitPullRequestObj
    and makes POST request to the url.
    """
    posted_message = json.dumps({"body": git_pr_obj.comment_message})
    make_rest_api_calls("POST", git_pr_obj.comment_url, git_pr_obj.headers, posted_message)


def pr_message_builder(pr_id, head_sha, commit_url, msg,
                       post_msg=''):
    # To build PR comment message using string formatting.
    str_args = (os.environ["BUILD_DISPLAY_NAME"],
                msg,
                os.environ["BUILD_URL"],
                pr_id,
                head_sha,
                commit_url,
                str(' ' + post_msg + '.') if post_msg else '.')
    return '**[Test build %s %s](%stestReport)** for PR %s at commit [`%s`](%s)%s' % str_args


def run_pr_checks(pr_tests, head_sha, sha1):
    """
    Executes a set of pull request checks to ease development and report issues with various
    components such as style, linting, dependencies, compatibilities, etc.
    @return a list of messages to post back to Github/GitLab.
    """
    # Ensure we save off the current HEAD to revert to
    current_pr_head = run_cmd(['git', 'rev-parse', 'HEAD'], return_output=True).strip()
    pr_results = list()

    for pr_test in pr_tests:
        test_name = pr_test + '.sh'
        pr_results.append(run_cmd(['bash', os.path.join(SPARK_HOME, 'dev', 'tests', test_name),
                                   head_sha, sha1],
                                  return_output=True).rstrip())
        # Ensure, after each test, that we're back on the current PR
        run_cmd(['git', 'checkout', '-f', current_pr_head])
    return pr_results


def run_tests():
    """
    Runs the `dev/run-tests` script and responds with the correct error message
    under the various failure scenarios.
    @return a list containing the test result code and the result note to post to Github/GitLab.
    """
    test_result_code = subprocess.Popen(['timeout',
                                        TEST_TIMEOUT,
                                        os.path.join(SPARK_HOME,
                                                     'dev',
                                                     'run-tests')]).wait()

    failure_note_by_errcode = {
        # error to denote run-tests script failures:
        1: 'executing the `dev/run-tests` script',
        ERROR_CODES["BLOCK_GENERAL"]: 'some tests',
        ERROR_CODES["BLOCK_RAT"]: 'RAT tests',
        ERROR_CODES["BLOCK_SCALA_STYLE"]: 'Scala style tests',
        ERROR_CODES["BLOCK_JAVA_STYLE"]: 'Java style tests',
        ERROR_CODES["BLOCK_PYTHON_STYLE"]: 'Python style tests',
        ERROR_CODES["BLOCK_R_STYLE"]: 'R style tests',
        ERROR_CODES["BLOCK_DOCUMENTATION"]: 'to generate documentation',
        ERROR_CODES["BLOCK_BUILD"]: 'to build',
        ERROR_CODES["BLOCK_BUILD_TESTS"]: 'build dependency tests',
        ERROR_CODES["BLOCK_MIMA"]: 'MiMa tests',
        ERROR_CODES["BLOCK_SPARK_UNIT_TESTS"]: 'Spark unit tests',
        ERROR_CODES["BLOCK_PYSPARK_UNIT_TESTS"]: 'PySpark unit tests',
        ERROR_CODES["BLOCK_PYSPARK_PIP_TESTS"]: 'PySpark pip packaging tests',
        ERROR_CODES["BLOCK_SPARKR_UNIT_TESTS"]: 'SparkR unit tests',
        ERROR_CODES["BLOCK_TIMEOUT"]: 'from timeout after a configured wait of `%s`' % (
            TEST_TIMEOUT)
    }

    if test_result_code == 0:
        test_result_note = ' * This patch passes all tests.'
    else:
        note = failure_note_by_errcode.get(
            test_result_code, "due to an unknown error code, %s" % test_result_code)
        test_result_note = ' * This patch **fails %s**.' % note

    return [test_result_code, test_result_note]


def main():
    """
    This function runs the Spark testsuites by calling `run-tests.py` and communicates back
    to either GitHub or GitLab based on the environment defined.
    Following important env variables:
    AMP_JENKINS_PRB - To check for files changes with respect to target branch(ex. master)
                      and run only the test cases that correspond to the changed modules.
    AMPLAB_JENKINS_BUILD_TOOL - {maven/sbt} Build tool used to build and test Spark project.
    AMPLAB_JENKINS_BUILD_PROFILE - Hadoop profile used to build and test Spark project.
    GIT_VENDOR - Either GitLab or GitHub based on requirement.
    See ../jenkinsfile to see the list of environment variables to be set.
    """

    if os.environ["PULL_REQUEST_ID"] == "null":
        test_result_code, test_result_note = run_tests()
        sys.exit(test_result_code)

    os.environ["AMP_JENKINS_PRB"] = "True"
    if os.environ["GIT_VENDOR"] == "GitHub":
        git_pr_obj = GitPullRequestObj(os.environ["PULL_REQUEST_ID"],
                                       os.environ["GIT_VENDOR"],
                                       os.environ["GITHUB_API_ENDPOINT"],
                                       os.environ["GITHUB_PROJECT_URL"],
                                       os.environ["GITHUB_OAUTH_KEY"])
    elif os.environ["GIT_VENDOR"] == "GitLab":
        git_pr_obj = GitPullRequestObj(os.environ["PULL_REQUEST_ID"],
                                       os.environ["GIT_VENDOR"],
                                       os.environ["GITLAB_API_ENDPOINT"],
                                       os.environ["GITLAB_PROJECT_URL"],
                                       os.environ["GITLAB_OAUTH_KEY"])
    else:
        print(" > Git Vendor: ", git_pr_obj.git_vendor)
        raise ValueError("Git Vendor not found.")

    response = make_rest_api_calls("GET", git_pr_obj.pr_api_url, git_pr_obj.headers)
    git_pr_obj.pr_title = response["title"]
    git_pr_obj.labels = response["labels"]

    if git_pr_obj.git_vendor == "GitHub":
        git_pr_obj.head_sha = response["head"]["sha"]
        os.environ["ghprbTargetBranch"] = response["base"]["label"]
    elif git_pr_obj.git_vendor == "GitLab":
        git_pr_obj.head_sha = response["diff_refs"]["head_sha"]
        os.environ["ghprbTargetBranch"] = response["target_branch"]

    sha1 = "origin/pr/"+git_pr_obj.pr_id+"/merge"

    os.environ["AMPLAB_JENKINS_BUILD_PROFILE"] = "hadoop2.7"
    if "test-maven" in git_pr_obj.pr_title:
        os.environ["AMPLAB_JENKINS_BUILD_TOOL"] = "maven"
    if "test-hadoop2.6" in git_pr_obj.pr_title:
        os.environ["AMPLAB_JENKINS_BUILD_PROFILE"] = "hadoop2.6"
    if "test-hadoop2.7" in git_pr_obj.pr_title:
        os.environ["AMPLAB_JENKINS_BUILD_PROFILE"] = "hadoop2.7"

    pr_tests = [
        "pr_merge_ability",
        "pr_public_classes"
    ]

    pr_message = functools.partial(pr_message_builder,
                                   git_pr_obj.pr_id,
                                   git_pr_obj.head_sha[0:7],
                                   git_pr_obj.commit_url)
    # post start message
    git_pr_obj.comment_message = pr_message('has started')
    post_pr_message(git_pr_obj)
    pr_check_results = run_pr_checks(pr_tests, git_pr_obj.head_sha, sha1)
    test_result_code, test_result_note = run_tests()

    # post end message
    git_pr_obj.comment_message = pr_message('has finished')
    git_pr_obj.comment_message += '\n' + test_result_note + '\n'
    git_pr_obj.comment_message += '\n'.join(pr_check_results)
    post_pr_message(git_pr_obj)

    sys.exit(test_result_code)


if __name__ == "__main__":
    main()
