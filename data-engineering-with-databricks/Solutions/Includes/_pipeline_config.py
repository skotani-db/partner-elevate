# Databricks notebook source
# MAGIC %run ./_stream_factory

# COMMAND ----------

class PipelineConfig:
    def __init__(self, name, notebooks, target, storage, source, env=None, custom_config=None):

        # add filepath of current notebook to set notebook paths
        basepath = dbgems.get_notebook_dir()  
        self.notebooks = [f"{basepath}/{n}" for n in notebooks]

        # set env names with optional suffix
        self.pipeline_name = name
        self.target = target
        self.storage = storage
        if env:
            self.pipeline_name += f"_{env}"
            self.target += f"_{env}"
            self.storage += f"/{env}" 

        # set custom parameters
        self.source = source
        self.custom_config = custom_config
         
    def __repr__(self):
        content = f"Name:      {self.pipeline_name}\nSource:    {self.source}\n"""
        content += f"Notebooks: {self.notebooks.pop(0)}"
        for notebook in self.notebooks: content += f"\n           {notebook}"
        return content

@DBAcademyHelper.monkey_patch
def configure_pipeline(self, name, notebooks, env=None, source=None, custom_config=None):

    if not source:
        source = self.paths.stream_source

    self.pipeline_config = PipelineConfig(
        name=f"{self.unique_name(sep='-')}: {name}", 
        notebooks=notebooks, 
        target=self.schema_name, 
        storage=self.paths.storage_location, 
        source=source, 
        env=env,
        custom_config=custom_config
    )    
    return self.pipeline_config


@DBAcademyHelper.monkey_patch
def generate_pipeline(self, print_config=True, display_validation=False):
    if print_config: 
      self.print_pipeline_config()
    self.create_pipeline()
    self.validate_pipeline_config(display=display_validation)  
  

@DBAcademyHelper.monkey_patch
def print_pipeline_config(self, config=None):
    if not config: config = self.pipeline_config  

    from dbacademy.dbhelper import ClustersHelper
        
    config_values = [
        ("Pipeline Name", config.pipeline_name),
        ("Source", config.source),
        ("Target", config.target),
        ("Storage Location", config.storage),
        ("Policy", ClustersHelper.POLICY_DLT_ONLY)]
    for i, path in enumerate(config.notebooks):
        config_values.append((f"Notebook #{i+1} Path", path))

    self.print_config_values(config_values)


@DBAcademyHelper.monkey_patch
def print_config_values(self, config_values):
    """Prints each pair of values in config_values list as text and input textbox."""

    html = """<table style="width:100%">"""
    for name, value in config_values:
        html += f"""
        <tr>
            <td style="white-space:nowrap; width:1em">{name}:</td>
            <td><input type="text" value="{value}" style="width: 100%"></td></tr>"""
    html += "</table>"

    displayHTML(html)


@DBAcademyHelper.monkey_patch    
def create_pipeline(self, config=None):
    "Provided by DBAcademy, this function creates the prescribed pipeline"
    
    if not config: config = self.pipeline_config  
    
    self.client.pipelines().delete_by_name(config.pipeline_name)  # Delete pipeline if it exists

    policy = self.get_dlt_policy()
    cluster = [{"num_workers": 0, "policy_id": policy.get("policy_id")}] if policy else [{"num_workers": 0}]

    response = self.client.pipelines().create(
        name = config.pipeline_name, 
        development=True,
        storage = config.storage, 
        target = config.target,
        notebooks = config.notebooks,
        configuration = {"source": config.source, "spark.master": "local[*]",},
        clusters=cluster)

    self.pipeline_id = response.get("pipeline_id")
    self.pipeline_url = f"{dbgems.get_workspace_url()}#joblist/pipelines/{self.pipeline_id}"
    self.pipeline_name = config.pipeline_name
    
    displayHTML(f"""
    Created the pipeline "{self.pipeline_name}": <a href={self.pipeline_url}>{self.pipeline_id}</a>
    """)


@DBAcademyHelper.monkey_patch
def validate_pipeline_config(self, display=True):
    "Provided by DBAcademy, this function validates the configuration of the pipeline"

    from dbacademy.dbhelper import ClustersHelper

    config = self.pipeline_config
    suite = self.tests.new("Pipeline Config")

    # validate pipeline with name exists        
    pipeline = self.client.pipelines().get_by_name(config.pipeline_name)        
    suite.test_not_none(lambda: pipeline, 
                        description=f"Create the pipeline \"<b>{config.pipeline_name}</b>\".", 
                        hint="Double check the spelling.")

    if pipeline is None: pipeline = {}
    spec = pipeline.get("spec", {})

    # validate storage location and target
    suite.test_equals(lambda: spec.get("storage", None), config.storage, 
                      description=f"Set the storage location to \"<b>{config.storage}</b>\".", 
                      hint=f"Found \"<b>[[ACTUAL_VALUE]]</b>\".")
    suite.test_equals(lambda: spec.get("target", None), config.target, 
                      description=f"Set the target to \"<b>{config.target}</b>\".", 
                      hint=f"Found \"<b>[[ACTUAL_VALUE]]</b>\".")

    # validate notebooks
    libraries = [l.get("notebook", {}).get("path") for l in spec.get("libraries", [])]
    def test_notebooks():
        if libraries is None: return False
        if len(libraries) != len(config.notebooks): return False
        for library in libraries:
            if library not in config.notebooks: return False
        return True

    hint = f"""Found the following {len(libraries)} notebook(s):<ul style="margin-top:0">"""
    for library in libraries: hint += f"""<li>{library}</li>"""
    hint += "</ul>"

    suite.test(test_function=test_notebooks, actual_value=libraries, description="Configure the Notebook library.", hint=hint)

    # validate two configuration parameters: source, spark.master
    suite.test_length(lambda: spec.get("configuration", {}), 2, description=f"Set the two configuration parameters.", 
                      hint=f"Found [[LEN_ACTUAL_VALUE]] configuration parameter(s).")
    suite.test_equals(lambda: spec.get("configuration", {}).get("source"), config.source, 
                      description=f"Set the \"<b>source</b>\" configuration parameter to \"<b>{config.source}</b>\".", 
                      hint=f"Found \"<b>[[ACTUAL_VALUE]]</b>\".")
    suite.test_equals(lambda: spec.get("configuration", {}).get("spark.master"), "local[*]", 
                      description=f"Set the \"<b>spark.master</b>\" configuration parameter to \"<b>local[*]</b>\".", 
                      hint=f"Found \"<b>[[ACTUAL_VALUE]]</b>\".")

    # validate cluster settings: cluster count, autoscaling disabled, cluster policy, worker count
    suite.test_length(lambda: spec.get("clusters"), expected_length=1, 
                      description=f"Expected one and only one cluster definition.", 
                      hint="Edit the config via the JSON interface to remove the second+ cluster definitions")
    suite.test_is_none(lambda: spec.get("clusters")[0].get("autoscale"), description=f"Autoscaling should be disabled.")

    def test_cluster_policy():
        cluster = spec.get("clusters")[0]
        policy_id = cluster.get("policy_id")
        if policy_id is None: dbgems.print_warning("WARNING: Policy Not Set", 
                                                   f"Expected the policy to be set to \"{ClustersHelper.POLICY_DLT_ONLY}\".")
        else:
            policy_name = self.client.cluster_policies.get_by_id(policy_id).get("name")
            if policy_id != self.get_dlt_policy().get("policy_id"):
                dbgems.print_warning("WARNING: Incorrect Policy", 
                                     f"Expected the policy to be set to \"{ClustersHelper.POLICY_DLT_ONLY}\", found \"{policy_name}\".")
        return True

    suite.test(test_function=test_cluster_policy, actual_value=None, 
               description=f"The cluster policy should be <b>\"{ClustersHelper.POLICY_DLT_ONLY}\"</b>.")
    suite.test_equals(lambda: spec.get("clusters")[0].get("num_workers"), 0, 
                      description=f"The number of spark workers should be <b>0</b>.", hint=f"Found [[ACTUAL_VALUE]] workers.")

    # validate pipeline development mode, current channel, photon enabled, pipeline triggered mode
    suite.test_true(lambda: spec.get("development") != self.is_smoke_test(), 
                    description=f"The pipeline mode should be set to \"<b>Development</b>\".")
    suite.test(test_function=lambda: {spec.get("channel") is None or spec.get("channel").upper() == "CURRENT"}, 
               actual_value=spec.get("channel"), 
               description=f"The channel should be set to \"<b>Current</b>\".", hint=f"Found \"<b>[[ACTUAL_VALUE]]</b>\"")
    suite.test_true(lambda: spec.get("photon"), description=f"Photon should be enabled.")
    suite.test_false(lambda: spec.get("continuous"), 
                     description=f"Expected the Pipeline mode to be \"<b>Triggered</b>\".", 
                     hint=f"Found \"<b>Continuous</b>\".")

    if display: suite.display_results()
    assert suite.passed, "One or more tests failed; please double check your work."


@DBAcademyHelper.monkey_patch
def start_pipeline(self):
    "Starts the pipeline and then blocks until it has completed, failed or was canceled"

    import time
    from dbacademy.dbrest import DBAcademyRestClient
    
    client = DBAcademyRestClient() 
    start = client.pipelines().start_by_id(self.pipeline_id)  # start pipeline
    update_id = start.get("update_id")

    # get status and block until done
    update = client.pipelines().get_update_by_id(self.pipeline_id, update_id)
    state = update.get("update").get("state")
    while state not in ["COMPLETED", "FAILED", "CANCELED"]:
        duration = 15
        time.sleep(duration)
        print(f"Current state is {state}, sleeping {duration} seconds.")    
        update = client.pipelines().get_update_by_id(self.pipeline_id, update_id)
        state = update.get("update").get("state")

    print(f"The final state is {state}.")
    assert state == "COMPLETED", f"Expected the state to be COMPLETED, found {state}"    


@DBAcademyHelper.monkey_patch
def get_dlt_policy(self):
    from dbacademy.dbhelper import ClustersHelper
    dlt_policy = self.client.cluster_policies.get_by_name(ClustersHelper.POLICY_DLT_ONLY)
    if dlt_policy is None: 
        dbgems.print_warning("WARNING: Policy Not Found", 
        f"Could not find the cluster policy \"{ClustersHelper.POLICY_DLT_ONLY}\".\nPlease run the notebook Includes/Workspace-Setup to address this error.")
    return dlt_policy      

None   

