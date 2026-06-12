#!/usr/bin/env python3
"""
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

"""

import glob
import os

from ambari_commons import OSConst
from ambari_commons.os_family_impl import OsFamilyImpl
from resource_management.core import sudo
from resource_management.core.exceptions import ClientComponentHasNoStatus
from resource_management.core.logger import Logger
from resource_management.core.resources.system import Execute, File
from resource_management.core.shell import as_sudo
from resource_management.core.source import InlineTemplate, Template
from resource_management.libraries.functions import conf_select
from resource_management.libraries.functions import format
from resource_management.libraries.functions import stack_select
from resource_management.libraries.functions.constants import StackFeature
from resource_management.libraries.functions.stack_features import check_stack_feature
from resource_management.libraries.resources.properties_file import PropertiesFile
from resource_management.libraries.resources.template_config import TemplateConfig
from resource_management.libraries.script.script import Script

from kafka import ensure_base_directories, replace_sasl_related_config


class KafkaClient(Script):
  """Ambari CLIENT component for Kafka CLI and client tooling."""

  def configure(self, env):
    import params

    env.set_params(params)
    kafka_client_configure()

  def save_configs(self, env):
    self.configure(env)

  def start(self, env, upgrade_type=None):
    self.configure(env)

  def stop(self, env, upgrade_type=None):
    import params

    env.set_params(params)

  def status(self, env):
    raise ClientComponentHasNoStatus()


@OsFamilyImpl(os_family=OSConst.WINSRV_FAMILY)
class KafkaClientWindows(KafkaClient):
  def install(self, env):
    import params

    env.set_params(params)
    if "KAFKA_HOME" not in os.environ:
      self.install_packages(env)
    self.configure(env)


@OsFamilyImpl(os_family=OsFamilyImpl.DEFAULT)
class KafkaClientDefault(KafkaClient):
  def install(self, env):
    import params

    env.set_params(params)
    self.install_packages(env)
    self.create_client_config_version(env)
    self.configure(env)

  def pre_upgrade_restart(self, env, upgrade_type=None):
    Logger.info("Executing Kafka client Stack Upgrade pre-restart")
    import params

    env.set_params(params)

    if params.version and check_stack_feature(
      StackFeature.ROLLING_UPGRADE, params.version
    ):
      stack_select.select_packages(params.version)

  def create_client_config_version(self, env):
    """
    Convert /etc/kafka/conf into a stack-select versioned symlink.

    Mirrors conf_select handling in install_packages.py and stack hooks: the
    kafka-broker package install leaves a physical conf directory, which must
    be linked to /usr/<stack>/current/kafka-broker/conf. conf.backup is
    restored after conversion so Ambari-managed files are preserved. Broken
    symlinks are repaired before re-running conversion.
    """
    package_name = "kafka-broker"
    stack_root = Script.get_stack_root()
    current_dir = format("{stack_root}/current/kafka-broker/conf")
    directories = [{"conf_dir": "/etc/kafka/conf", "current_dir": current_dir}]
    stack_version = stack_select.get_stack_version_before_install(package_name)
    conf_dir = "/etc/kafka/conf"

    if stack_version:
      try:
        os.stat(conf_dir)
        conf_select.convert_conf_directories_to_symlinks(
          package_name, stack_version, directories
        )
        cp_cmd = as_sudo(
          ["cp", "-a", "-f", "/etc/kafka/conf.backup/.", "/etc/kafka/conf"]
        )
        Execute(cp_cmd, logoutput=True)
      except OSError as e:
        Logger.warning(
          "Error while accessing Kafka conf directory (possible broken symlink): {0}. "
          "Attempting to repair.".format(str(e))
        )
        sudo.unlink(conf_dir)
        sudo.makedirs(conf_dir, 0o755)
        for files in glob.glob("/etc/kafka/conf.backup/*"):
          cp_cmd = as_sudo(["cp", "-r", files, conf_dir])
          Execute(cp_cmd, logoutput=True)
        conf_select.convert_conf_directories_to_symlinks(
          package_name, stack_version, directories
        )


def _get_client_server_properties():
  """
  Minimal server.properties for client-only hosts where KAFKA_BROKER has not
  written a full server.properties file.
  """
  import params

  properties = {
    "bootstrap.servers": ",".join(
      [
        format("{host}:{port}", host=host, port=params.kafka_broker_port)
        for host in params.kafka_hosts
      ]
    )
  }

  kafka_broker_config = params.config["configurations"]["kafka-broker"]

  if params.kerberos_security_enabled and params.kafka_kerberos_enabled:
    inter_broker_protocol = kafka_broker_config.get(
      "security.inter.broker.protocol", "SASL_PLAINTEXT"
    )
    properties["security.protocol"] = replace_sasl_related_config(
      inter_broker_protocol, only_protocol=True
    )
    properties["sasl.mechanism"] = kafka_broker_config.get(
      "sasl.mechanism.inter.broker.protocol", "GSSAPI"
    )
    if params.kafka_bare_jaas_principal:
      properties["sasl.kerberos.service.name"] = params.kafka_bare_jaas_principal
  elif params.kafka_other_sasl_enabled:
    listener = kafka_broker_config.get("listeners", "").split(",")[0].strip()
    if "://" in listener:
      listener_protocol = listener.split("://")[0]
      properties["security.protocol"] = replace_sasl_related_config(
        listener_protocol, only_protocol=True
      )
    sasl_mechanisms = kafka_broker_config.get("sasl.enabled.mechanisms", "GSSAPI")
    properties["sasl.mechanism"] = sasl_mechanisms.split(",")[0].strip()

  return properties


def kafka_client_configure():
  """Configure Kafka client files on hosts running KAFKA_CLIENT."""
  import params

  Logger.info("Configuring Kafka Client")

  ensure_base_directories()

  File(
    format("{conf_dir}/kafka-env.sh"),
    owner=params.kafka_user,
    group=params.user_group,
    mode=0o755,
    content=InlineTemplate(params.kafka_env_sh_template),
  )

  if params.log4j_props is not None:
    File(
      format("{conf_dir}/log4j.properties"),
      mode=0o644,
      group=params.user_group,
      owner=params.kafka_user,
      content=InlineTemplate(params.log4j_props),
    )

  File(
    os.path.join(params.conf_dir, "tools-log4j.properties"),
    owner="root",
    group="root",
    mode=0o644,
    content=Template("tools-log4j.properties.j2"),
  )

  server_properties_path = os.path.join(params.conf_dir, "server.properties")
  if not os.path.exists(server_properties_path):
    PropertiesFile(
      "server.properties",
      mode=0o640,
      dir=params.conf_dir,
      properties=_get_client_server_properties(),
      owner=params.kafka_user,
      group=params.user_group,
    )

  if (
    params.kerberos_security_enabled and params.kafka_kerberos_enabled
  ) or params.kafka_other_sasl_enabled:
    client_jaas_path = format("{conf_dir}/kafka_client_jaas.conf")
    if params.kafka_client_jaas_conf_template:
      client_jaas_content = InlineTemplate(params.kafka_client_jaas_conf_template)
      File(
        client_jaas_path,
        owner=params.kafka_user,
        group=params.user_group,
        mode=0o644,
        content=client_jaas_content,
      )
    else:
      TemplateConfig(client_jaas_path, owner=params.kafka_user)
      client_jaas_content = None

    # KAFKA_OPTS in kafka-env.sh points to kafka_jaas.conf. On client-only nodes
    # the broker never ran, so create it here. Skip if already present (broker host).
    kafka_jaas_path = format("{conf_dir}/kafka_jaas.conf")
    if not os.path.exists(kafka_jaas_path):
      if client_jaas_content:
        File(
          kafka_jaas_path,
          owner=params.kafka_user,
          group=params.user_group,
          mode=0o644,
          content=client_jaas_content,
        )
      else:
        cp_cmd = as_sudo(["cp", "-f", client_jaas_path, kafka_jaas_path])
        Execute(cp_cmd, logoutput=True)
    Logger.info("Generated Kafka client JAAS configuration for Kerberos")

  Logger.info("Kafka Client configuration completed successfully")


if __name__ == "__main__":
  KafkaClient().execute()
