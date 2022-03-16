from abc import ABC, abstractmethod
from functools import lru_cache
import importlib
from json import loads

import pandas as pd

from src.data_model import DraftInfo, AppInfo, DriverInfo
from src.utilities import log


class AbstractDriver(ABC):
    @abstractmethod
    @log
    def get_source_name(self, draft_info: DraftInfo, app_info: AppInfo) -> str:
        """
        Construct name of the dataset in a specific source; meant to be overriden for every single source.
        :param draft_info: info about the draft
        :param app_info: info about the app
        :return:
        """
        pass


class ReadDriver(AbstractDriver):
    @abstractmethod
    @log
    def read(self, draft_info: DraftInfo, app_info: AppInfo, **discovery_params) -> pd.DataFrame:
        """
        Read data from a dataset implementation.
        :param draft_info: info about the draft
        :param app_info: info about the app
        :param discovery_params:
        :return:
        """
        pass


class WriteDriver(AbstractDriver):
    @abstractmethod
    @log
    def write(self, dataset: pd.DataFrame, draft_info: DraftInfo, app_info: AppInfo, **discovery_params) -> None:
        """
        Write data to a dataset implementation.
        :param dataset: dataset to write down
        :param draft_info: info about the draft
        :param app_info: info about the app
        :param discovery_params:
        :return:
        """
        pass


class DriverLoader:
    @classmethod
    @lru_cache(128)
    @log
    def load(cls, driver_info: DriverInfo) -> AbstractDriver:
        """
        Load driver using info about it.
        :param driver_info: info about the driver
        :return: driver used to either read or write down data
        """
        location_info = loads(driver_info.location_info)
        package_name = location_info["package_name"]
        class_name = location_info["class_name"]
        module = importlib.import_module(package_name)
        driver = getattr(module, class_name)(driver_info)

        return driver

