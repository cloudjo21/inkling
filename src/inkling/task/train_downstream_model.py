import yaml
import argparse
import logging
import torch
import urllib.parse

from transformers import set_seed

from tunip.config import Config
from tunip.logger import init_logging_handler
from tunip.service_config import get_service_config
from tunip.yaml_loader import YamlLoader

from tweak.utils.task_set_yaml_parser import TaskSetYamlParser
from tweak.dataset.multitask_dataset import MultitaskResourceBuilder
from tweak.model.multitask.modeling_multitask import MultitaskModel
from tweak.trainer.multitask_trainer import MultitaskTrainer
from tweak.data_collate import DataCollatorFactory, DummyDataCollator
from tweak.trainer.multitask_trainer import MultitaskTrainer
from tweak.model.multitask.dump import MultitaskPredictionDumper
from tweak.model.multitask.trainer import Trainer


class TrainDownstreamModel:
    def __init__(self, service_config, task_config_path):
        self.service_config = service_config
        self.trainer = Trainer(service_config, task_config_path)
        
    def __call__(self):
        task_set = self.trainer.train()

        predict_config_paths = []
        for task in task_set:
            model_path = TaskPath(self.service_config.username, task_set.domain_name, task_set.snapshot_dt, task.task_name)
            predict_config_paths.append(f"{self.service_config.local_prefix}/{model_path}/predict.json")
        
        return predict_config_paths
