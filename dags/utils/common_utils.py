import os
import yaml
import json
import pandas as pd
import pandasql as ps
import time

THIS_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)))
env = os.environ['ENV']

def read_config_file():
    config_path = os.path.join(os.path.dirname(
        THIS_DIR), 'config', 'CONFIG_{0}.yml'.format(env))

    with open(config_path, 'r') as f:
        config = yaml.full_load(f)
    return config

def read_schedule_file():
    config_path = os.path.join(os.path.dirname(
        THIS_DIR), '../include', 'schedule.yml')

    with open(config_path, 'r') as f:
        config = yaml.full_load(f)
    return config

def date_obj_parser(ts):
    if ts is None:
        return None
    else:
        return '{0}-{1:0=2d}-{2:0=2d} {3:0=2d}:{4:0=2d}:{5:0=2d}'.format(
            ts.date.year,
            ts.date.month,
            ts.date.day,
            ts.hour,
            ts.minute,
            ts.second)