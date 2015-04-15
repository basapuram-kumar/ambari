#!/usr/bin/env python

'''
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''
from mock.mock import MagicMock, call, patch
from stacks.utils.RMFTestCase import *
from unittest import skip
import datetime
import resource_management.libraries.functions

@patch("platform.linux_distribution", new = MagicMock(return_value="Linux"))
@patch.object(resource_management.libraries.functions, "get_unique_id_and_date", new = MagicMock(return_value=''))
class TestServiceCheck(RMFTestCase):
  COMMON_SERVICES_PACKAGE_DIR = "HBASE/1.1.0.2.3/package"
  STACK_VERSION = "2.3"

  def test_service_check_default(self):
    self.executeScript(self.COMMON_SERVICES_PACKAGE_DIR + "/scripts/service_check.py",
                        classname="HbaseServiceCheck",
                        command="service_check",
                        config_file="hbase_default.json",
                        hdp_stack_version = self.STACK_VERSION,
                        target = RMFTestCase.TARGET_COMMON_SERVICES
    )
    self.assertResourceCalled('File', '/tmp/hbaseSmokeVerify.sh',
      content = StaticFile('hbaseSmokeVerify.sh'),
      mode = 0755,
    )
    self.assertResourceCalled('File', '/tmp/hbase-smoke.sh',
      content = Template('hbase-smoke.sh.j2'),
      mode = 0755,
    )
    self.assertResourceCalled('Execute', ' /usr/hdp/current/hbase-regionserver/bin/hbase --config /etc/hbase/conf shell /tmp/hbase-smoke.sh',
      logoutput = True,
      tries = 3,
      user = 'ambari-qa',
      try_sleep = 5,
    )
    self.assertResourceCalled('Execute', ' /tmp/hbaseSmokeVerify.sh /etc/hbase/conf  /usr/hdp/current/hbase-regionserver/bin/hbase',
      logoutput = True,
      tries = 3,
      user = 'ambari-qa',
      try_sleep = 5,
    )
    self.assertNoMoreResources()
    
    
  def test_service_check_secured(self):
    self.executeScript(self.COMMON_SERVICES_PACKAGE_DIR + "/scripts/service_check.py",
                        classname="HbaseServiceCheck",
                        command="service_check",
                        config_file="hbase_secure.json",
                        hdp_stack_version = self.STACK_VERSION,
                        target = RMFTestCase.TARGET_COMMON_SERVICES
    )
    self.assertResourceCalled('File', '/tmp/hbaseSmokeVerify.sh',
      content = StaticFile('hbaseSmokeVerify.sh'),
      mode = 0755,
    )
    self.assertResourceCalled('File', '/tmp/hbase-smoke.sh',
      content = Template('hbase-smoke.sh.j2'),
      mode = 0755,
    )
    self.assertResourceCalled('File', '/tmp/hbase_grant_permissions.sh',
      content = Template('hbase_grant_permissions.j2'),
      owner = 'hbase',
      group = 'hadoop',
      mode = 0644,
    )
    self.assertResourceCalled('Execute', '/usr/bin/kinit -kt /etc/security/keytabs/hbase.headless.keytab hbase@EXAMPLE.COM; /usr/hdp/current/hbase-regionserver/bin/hbase shell /tmp/hbase_grant_permissions.sh',
      user = 'hbase',
    )
    self.assertResourceCalled('Execute', '/usr/bin/kinit -kt /etc/security/keytabs/smokeuser.headless.keytab ambari-qa@EXAMPLE.COM; /usr/hdp/current/hbase-regionserver/bin/hbase --config /etc/hbase/conf shell /tmp/hbase-smoke.sh',
      logoutput = True,
      tries = 3,
      try_sleep = 5,
      user = 'ambari-qa'
    )
    self.assertResourceCalled('Execute', '/usr/bin/kinit -kt /etc/security/keytabs/smokeuser.headless.keytab ambari-qa@EXAMPLE.COM; /tmp/hbaseSmokeVerify.sh /etc/hbase/conf  /usr/hdp/current/hbase-regionserver/bin/hbase',
      logoutput = True,
      tries = 3,
      try_sleep = 5,
      user = 'ambari-qa'
    )
    self.assertNoMoreResources()

  @skip("there's nothing to upgrade to yet")    
  def test_service_check_24(self):
    self.executeScript(self.COMMON_SERVICES_PACKAGE_DIR + "/scripts/service_check.py",
                        classname="HbaseServiceCheck",
                        command="service_check",
                        config_file="hbase-check-2.4.json",
                        hdp_stack_version = self.STACK_VERSION,
                        target = RMFTestCase.TARGET_COMMON_SERVICES
    )
    self.assertResourceCalled('File', '/tmp/hbaseSmokeVerify.sh',
      content = StaticFile('hbaseSmokeVerify.sh'),
      mode = 0755,
    )
    self.assertResourceCalled('File', '/tmp/hbase-smoke.sh',
      content = Template('hbase-smoke.sh.j2'),
      mode = 0755,
    )
    self.assertResourceCalled('Execute', ' /usr/hdp/current/hbase-client/bin/hbase --config /etc/hbase/conf shell /tmp/hbase-smoke.sh',
      logoutput = True,
      tries = 3,
      user = 'ambari-qa',
      try_sleep = 5,
    )
    self.assertResourceCalled('Execute', ' /tmp/hbaseSmokeVerify.sh /etc/hbase/conf  /usr/hdp/current/hbase-client/bin/hbase',
      logoutput = True,
      tries = 3,
      user = 'ambari-qa',
      try_sleep = 5,
    )
    self.assertNoMoreResources()
