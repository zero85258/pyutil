import json
import os

from util.log import LogUtil
from util.pgdbc import PGDBC
from util.msdbc import MSDBC
from util.odbc import ODBC
from pyspark.sql import SparkSession
from util.rabbitmq import RabbitMQ
import datetime
import subprocess

"""
商務邏輯仰賴這個類別(框架) 而不是依賴零碎的功能
"""


class Framework:
    env = {}
    conf_file = "env.json"

    def __init__(self, conf_file: str):
        self.conf_file: str = conf_file if os.path.isfile(conf_file) else "env.json"

        self.load_config_from_env()
        self.env["conf_file_path"]: str = os.getcwd() + "/" + self.conf_file
        self.env["curr_date"] = datetime.datetime.today().strftime("%Y%m%d")
        self.log: LogUtil = LogUtil("./logs/default.log")

        self.connections = {
            "jcs_repo": PGDBC(
                self.log,
                self.env["jcs_repo_ip"],
                self.env["jcs_repo_db"],
                self.env["jcs_repo_user"],
                self.env["jcs_repo_password"],
            ),
            "omni": ODBC(
                self.log,
                self.env["omni_ip"],
                self.env["omni_db"],
                self.env["omni_user"],
                self.env["omni_password"],
            ),
            "spark":  SparkSession.builder.appName("PythonWordCount").getOrCreate(),
            "composeall": RabbitMQ("rabbitmq"),
        }

        # 所有商務邏輯應該依賴這個
        self.services = {
            "log": self.log,
            # "file": FileOperate(),
            # "pg": PgUtil(self.env, self.log),
            "connections": self.connections,
            "env": self.env
        }

        self.open_conn()

    def load_config_from_env(self) -> None:
        with open(self.conf_file) as f:
            config = json.load(f)
            f.close()

        for key, value in config.items():
            if type(value) == str:
                config[key] = value.replace("<<RootFolderName>>", os.getcwd())
            else:
                config[key] = value

        self.env: dict = config

    def open_conn(self):
        self.log.info("open_conn")
        self.connections["jcs_repo"].open_conn()
        return self

    def close_conn(self):
        self.log.info("close_conn")
        self.connections["jcs_repo"].close_conn()
        self.services["log"].close_file()
        self.connections["spark"].stop()
        return self
