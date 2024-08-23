import json

from tunip.snapshot_utils import snapshot_now, SnapshotPathProvider, SnapshotCleaner
from tunip.file_utils import services as file_services

from inkling.dag_stat_utils import DAGStatusDigester
from inkling import LOGGER


class RegisterSchema(DAGStatusDigester):
    def __init__(self, service_config, task_config):
        super().__init__(service_config)

        self.service_config = service_config
        self.task_config = task_config

        self.is_create = self.task_config["ingress"]["create"]
        self.is_update = self.task_config["ingress"]["update"]

        self.schema_list = task_config["ingress"]["schema_list"]
        self.egress_path = f"/user/{self.service_config.username}/{self.task_config['egress']['repository_type']}/{self.task_config['egress']['domain_name']}"

        self.num_paths_to_left = self.task_config["egress"].get("num_paths_to_left")

    def __call__(self, context, run_snapshot_dt=None):
        snapshot_path_provider = SnapshotPathProvider(self.service_config)
        file_service = file_services(self.service_config)

        # create: True / update: True
        if self.is_create and self.is_update:
            raise ValueError("Register Schema's create and update cannot be both True.")
        # create: False / update: False
        elif not (self.is_create or self.is_update):
            LOGGER.info(f"===== No Schema to Update or Create. (Skip task: {__class__.__name__}) =====")
        # create or update
        elif self.is_create or self.is_update:
            save_snapshot_dt = run_snapshot_dt or snapshot_now()

            # Check schema path exists.
            schema_to_write = []
            for schema_info in self.schema_list:
                schema_root_path = f"{self.egress_path}/{schema_info['schema_type']}.schema"

                is_schema_exists = file_service.exists(schema_root_path)

                if self.is_create and is_schema_exists:
                    LOGGER.warn(f"{self.service_config.filesystem.upper()} Path Already Exists : {schema_root_path}")
                elif self.is_update and not is_schema_exists:
                    raise FileExistsError(f"{self.service_config.filesystem.upper()} Path Not Exists (update requires path) : {schema_root_path}")
                else:
                    schema_to_write.append(schema_info)

            for schema_info in schema_to_write:
                schema_root_path = f"{self.egress_path}/{schema_info['schema_type']}.schema"
                
                save_path = f"{schema_root_path}/{save_snapshot_dt}"

                schema_source = schema_info["schema_sourcing"]
                sourcing_action_type = schema_source["sourcing_action"]

                # Build schema from json
                if sourcing_action_type == "read_json":
                    schema_json = schema_source["body"]
                # Build schema from registered schema
                elif sourcing_action_type == "read_schema_list":
                    merged_schema = {}
                    source_schema_list = schema_source["source_schema_list"]
                    # keys_for_merge = schema_source["keys_for_merge"]

                    name2field = dict()

                    for source in source_schema_list:
                        schema_root_path = f"/user/{self.service_config.username}/lake/{self.task_config['egress']['domain_name']}/{source}.schema"

                        latest_schema_path = snapshot_path_provider.latest(schema_root_path, return_only_nauts_path=True)

                        schema_dict_json = file_service.load(f"{latest_schema_path}/data.json").decode("utf-8")
                        schema_dict = json.loads(schema_dict_json)

                        for field in schema_dict["fields"]:
                            name2field[field["name"]] = field
    
                    merged_schema = {
                        "type": "struct",
                        "fields": list(name2field.values())
                    }
                    schema_json = json.dumps(merged_schema, ensure_ascii=False)
                else:
                    raise ValueError(f"Source [{sourcing_action_type}] is not implemented.")

                # Write Schema
                file_service.mkdirs(save_path)
                file_service.write(save_path + "/" + "data.json", schema_json)

                if self.num_paths_to_left:
                    snapshot_cleaner = SnapshotCleaner(service_config=self.service_config, paths_left=self.num_paths_to_left)
                    snapshot_cleaner.clean(root_path=self.egress_path)
                    LOGGER.info(f"===== Cleaning old data... ({self.num_paths_to_left} snapshots will remain.) =====")
                else:
                    LOGGER.info(f"===== Skip cleaning data... (All snapshots will remain.) =====")

                LOGGER.info(f"Create or Update schema: {schema_info['schema_type']} in domain [{self.task_config['egress']['domain_name']}]")

            LOGGER.info(f"Create or Update {len(schema_to_write)} schema in domain [{self.task_config['egress']['domain_name']}]")
