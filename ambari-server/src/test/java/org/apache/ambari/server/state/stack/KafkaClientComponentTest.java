/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ambari.server.state.stack;

import java.io.File;
import java.io.FileReader;
import java.net.URL;
import java.util.Map;

import org.apache.ambari.server.state.kerberos.KerberosComponentDescriptor;
import org.apache.ambari.server.state.kerberos.KerberosDescriptor;
import org.apache.ambari.server.state.kerberos.KerberosDescriptorFactory;
import org.apache.ambari.server.state.kerberos.KerberosIdentityDescriptor;
import org.apache.ambari.server.state.kerberos.KerberosServiceDescriptor;
import org.junit.Assert;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

/**
 * Validates BIGTOP Kafka KAFKA_CLIENT stack definitions introduced in AMBARI-26593.
 */
public class KafkaClientComponentTest {

  private static final String KAFKA_KERBEROS_RESOURCE =
      "stacks/BIGTOP/3.2.0/services/KAFKA/kerberos.json";

  private static final String KAFKA_METAINFO_RESOURCE =
      "stacks/BIGTOP/3.2.0/services/KAFKA/metainfo.xml";

  private static final String STACK_PACKAGES_RESOURCE =
      "stacks/BIGTOP/3.2.0/properties/stack_packages.json";

  @Test
  public void testKafkaClientKerberosDescriptor() throws Exception {
    URL kerberosUrl = getClass().getClassLoader().getResource(KAFKA_KERBEROS_RESOURCE);
    Assert.assertNotNull("Kafka kerberos.json must be on the test classpath", kerberosUrl);

    KerberosDescriptor descriptor =
        new KerberosDescriptorFactory().createInstance(new File(kerberosUrl.toURI()));

    KerberosServiceDescriptor kafkaService = descriptor.getService("KAFKA");
    Assert.assertNotNull(kafkaService);

    KerberosComponentDescriptor clientComponent = kafkaService.getComponent("KAFKA_CLIENT");
    Assert.assertNotNull(clientComponent);
    Assert.assertEquals(1, clientComponent.getIdentities().size());

    KerberosIdentityDescriptor identity = clientComponent.getIdentities().get(0);
    Assert.assertEquals("kafka_kafka_client_kafka_broker", identity.getName());
    Assert.assertEquals("/KAFKA/KAFKA_BROKER/kafka_broker", identity.getReference());
  }

  @Test
  public void testKafkaClientStackPackagesEntry() throws Exception {
    URL packagesUrl = getClass().getClassLoader().getResource(STACK_PACKAGES_RESOURCE);
    Assert.assertNotNull("stack_packages.json must be on the test classpath", packagesUrl);

    Map<String, Map<String, Map<String, Map<String, Object>>>> packages;
    try (FileReader reader = new FileReader(new File(packagesUrl.toURI()))) {
      packages = new Gson().fromJson(
          reader,
          new TypeToken<Map<String, Map<String, Map<String, Map<String, Object>>>>>() {}.getType());
    }

    @SuppressWarnings("unchecked")
    Map<String, Object> kafkaClientPackages =
        (Map<String, Object>) packages.get("BIGTOP").get("stack-select").get("KAFKA").get("KAFKA_CLIENT");
    Assert.assertNotNull(kafkaClientPackages);
    Assert.assertEquals("kafka-broker", kafkaClientPackages.get("STACK-SELECT-PACKAGE"));
  }

  @Test
  public void testKafkaClientMetainfoResourceExists() {
    URL metainfoUrl = getClass().getClassLoader().getResource(KAFKA_METAINFO_RESOURCE);
    Assert.assertNotNull("Kafka metainfo.xml must be on the test classpath", metainfoUrl);
  }
}
