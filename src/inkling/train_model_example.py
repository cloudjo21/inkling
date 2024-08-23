"""
DEPRECATED
"""

import yaml
import argparse
import logging
import torch
import urllib.parse

from transformers import set_seed

from tunip.config import Config
from tunip.logger import init_logging_handler
from tunip.service_config import get_service_config
from tunip.model_uploader import GcsModelLoader
from tunip.yaml_loader import YamlLoader

from tweak.utils.task_set_yaml_parser import TaskSetYamlParser
from tweak.dataset.multitask_dataset import MultitaskResourceBuilder
from tweak.model.multitask.modeling_multitask import MultitaskModel
from tweak.trainer.multitask_trainer import MultitaskTrainer
from tweak.data_collate import DataCollatorFactory, DummyDataCollator
from tweak.trainer.multitask_trainer import MultitaskTrainer
from tweak.model.multitask.dump import MultitaskPredictionDumper

from tweak.model.convert.torchscript.service import TorchScriptModelConvertService
from tweak.model.convert.requests import Torch2TorchScriptRequest
from tweak.model.convert.torchscript.runtime_path import RuntimePathProviderFactory
from tweak.model.convert.torchscript import TorchScriptHfModelConverter


set_seed(42)
device = torch.device("cuda") if torch.cuda.is_available() else torch.device("cpu")

parser = argparse.ArgumentParser()
parser.add_argument("--service_stage_config", type=str)
parser.add_argument("--task_config", type=str)
parser.add_argument("--upload_config", type=str, default='')
parser.add_argument("--extract_config", type=str, default='')
args = parser.parse_args()

config = get_service_config()
upload_config = YamlLoader(args.upload_config).load()

task_set_parser = TaskSetYamlParser(yaml_file=args.task_config, config=config)
task_set = task_set_parser.parse()
assert len(task_set.tasks) > 0

logger = init_logging_handler(name=f"{task_set.snapshot_dt}_train", level=logging.DEBUG)
logger.info(task_set.training_args)

mt_resource_builder = MultitaskResourceBuilder(task_set)

multitask_model = MultitaskModel.create(
    model_name=task_set.tasks[0].pretrained_model_name,
    model_types=mt_resource_builder.model_types,
    model_configs=mt_resource_builder.model_configs,
)

data_collator_dict = {}
for task_name in task_set.names:
    data_collator_dict[task_name] = DataCollatorFactory.create(
        # Ignoring task, use default data collator
        # TODO more test on DataCollatorForTokenClassification
        task_name=None,
        tokenizer=multitask_model.tokenizer,
    )

trainer = MultitaskTrainer(
    task_name_list=task_set.names,
    mtl_data_collator=data_collator_dict,
    model=multitask_model.to(device),
    args=task_set.training_args,
    data_collator=DummyDataCollator(),
    train_dataset=mt_resource_builder.train_dataset,
    eval_dataset=mt_resource_builder.validation_dataset,
    compute_metrics=mt_resource_builder.compute_metrics
)

trainer.train()
trainer.save_model()

""" Save TorchScript Model """
converter = TorchScriptModelConvertService(config)
conv_req = Torch2TorchScriptRequest(
    model_type="hf.token_classification_model",
    domain_name=task_set.domain_name,
    domain_snapshot=task_set.snapshot_dt,
    task_name=task_set.tasks[0].task_name,
    tokenizer_name=task_set.tasks[0].pretrained_model_name,
    pt_model_name=task_set.tasks[0].pretrained_model_name,
    max_length=task_set.tasks[0].max_length,
    checkpoint="",
    lang="ko"
)
converter(conv_req)

predict_results = trainer.predict(mt_resource_builder.test_dataset)

eval_result_dumper = MultitaskPredictionDumper(config, task_set)
eval_result_dumper.dump(
    mt_resource_builder.all_label_list,
    mt_resource_builder.test_dataset,
    predict_results
)

with open(args.upload_config, 'r') as up_conf:
    upload_config = yaml.load(up_conf, Loader=yaml.Loader)
    upload_config["snapshot_dt"] = task_set.snapshot_dt
    
    with open(args.upload_config, 'w') as up_conf:
        up_conf.write(yaml.dump(upload_config))

model_uploader = GcsModelLoader(upload_config)
model_uploader.upload()

with open(args.extract_config, 'r') as ext_conf:
    extract_config = yaml.load(ext_conf, Loader=yaml.Loader)
    extract_config["model_snapshot_dt"] = task_set.snapshot_dt
    extract_config["model_name"] = urllib.parse.quote(task_set.tasks[0].pretrained_model_name, safe='')
    extract_config["task_name"] = task_set.names[0]
    # extract_config["task_type"] = [task_type.name for task_type in task_set.types]
    extract_config["task_type"] = task_set.types[0].name

    with open(args.extract_config, 'w') as ext_conf:
        ext_conf.write(yaml.dump(extract_config))