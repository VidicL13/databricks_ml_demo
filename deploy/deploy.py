# Databricks notebook source
#import os
#os.environ['DATABRICKS_HOST'] = 'https://eastus2.azuredatabricks.net/'
#os.environ['DATABRICKS_TOKEN'] = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

# COMMAND ----------

new_cluster_config = """
{
    "num_workers": 1,
    "cluster_name": "Vavdi, Gregor's Personal Compute Cluster",
    "spark_version": "14.1.x-scala2.12",
    "spark_conf": {
        "spark.databricks.cluster.profile": "singleNode",
        "spark.master": "local[*, 4]"
    },
    "azure_attributes": {
        "first_on_demand": 1,
        "availability": "ON_DEMAND_AZURE",
        "spot_bid_max_price": -1
    },
    "node_type_id": "Standard_DS3_v2",
    "driver_node_type_id": "Standard_DS3_v2",
    "ssh_public_keys": [],
    "custom_tags": {
        "ResourceClass": "SingleNode"
    },
    "spark_env_vars": {},
    "autotermination_minutes": 4320,
    "enable_elastic_disk": true,
    "init_scripts": [],
    "single_user_name": "gv5698@student.uni-lj.si",
    "policy_id": "001D10C2B6F0F61B",
    "enable_local_disk_encryption": false,
    "data_security_mode": "LEGACY_SINGLE_USER_STANDARD",
    "runtime_engine": "STANDARD",
    "cluster_id": "1231-100841-kvdxk9zh"
}
"""
# Existing cluster ID where integration test will be executed
existing_cluster_id = '1231-100841-kvdxk9zh'
# Path to the notebook with the integration test
notebook_path = '/test/demo.R'
repo_path = '/Repos/luka94vidic@gmail.com/databricks_ml_demo'


repos_path_prefix='/Repos/luka94vidic@gmail.com/databricks_ml_demo'
git_url = 'https://github.com/VidicL13/databricks_ml_demo'
provider = 'gitHub'
branch = 'main'

# COMMAND ----------

from argparse import ArgumentParser
import sys
p = ArgumentParser()

p.add_argument("--branch_name", required=False, type=str)
p.add_argument("--pr_branch", required=False, type=str)

namespace = p.parse_known_args(sys.argv + [ '', ''])[0]
branch_name = namespace.branch_name
print('Branch Name: ', branch_name)
pr_branch = namespace.pr_branch
print('PR Branch: ', pr_branch)

# COMMAND ----------

import json
import time
from datetime import datetime

from databricks_cli.configure.config import _get_api_client
from databricks_cli.configure.provider import EnvironmentVariableConfigProvider
from databricks_cli.sdk import JobsService, ReposService

# Let's create Databricks CLI API client to be able to interact with Databricks REST API
config = EnvironmentVariableConfigProvider().get_config()
api_client = _get_api_client(config, command_name="cicdtemplates-")

#Let's checkout the needed branch
if branch_name == 'merge':
  branch = pr_branch
else:
  branch = branch_name
print('Using branch: ', branch)
  
#Let's create Repos Service
repos_service = ReposService(api_client)

# Let's store the path for our new Repo
_b = branch.replace('/','_')
repo_path = f'{repos_path_prefix}_{_b}_{str(datetime.now().microsecond)}'
print('Checking out the following repo: ', repo_path)

# Let's clone our GitHub Repo in Databricks using Repos API
repo = repos_service.create_repo(url=git_url, provider=provider, path=repo_path)

try:
  repos_service.update_repo(id=repo['id'], branch=branch)

  #Let's create a jobs service to be able to start/stop Databricks jobs
  jobs_service = JobsService(api_client)

  notebook_task = {'notebook_path': repo_path + notebook_path}
  #new_cluster = json.loads(new_cluster_config)

  # Submit integration test job to Databricks REST API
  res = jobs_service.submit_run(run_name="Shitting blood", existing_cluster_id=existing_cluster_id,  notebook_task=notebook_task, )
  run_id = res['run_id']
  print(run_id)

  #Wait for the job to complete
  while True:
      status = jobs_service.get_run(run_id)
      print(status)
      result_state = status["state"].get("result_state", None)
      if result_state:
          print(result_state)
          assert result_state == "SUCCESS"
          break
      else:
          time.sleep(5)
finally:
  repos_service.delete_repo(id=repo['id'])
