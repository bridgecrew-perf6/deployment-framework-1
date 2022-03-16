from abc import ABC
from typing import List, Tuple, Iterator

import pandas as pd

from src.dataset_implementation_discovery import DatasetImplementationDiscoveryFactory
from src.driver_construction import DriverLoader
from src.driver_discovery import ReadDriverDiscovery, WriteDriverDiscovery
from src.retrievers import AppInfoRetriever, ProcessorBackendInfoRetriever, DraftInfoRetriever, \
    DatasetImplementationInfoRetriever
from src.utilities import logger, log


class AbstractIO(ABC):
    pass


class Reader(AbstractIO):
    def __init__(self, app_name: str, processor_backend_name: str):
        """
        Provides functionality to read data from different sources without knowing what these sources actually are.
        :param app_name: name of the app; app roughly corresponds to a set of dataset implementations
        :param processor_backend_name: name of the backend this script is written in
        """
        logger.debug("Creating Reader()")
        self.app_info = AppInfoRetriever.get_info_by_name(app_name)
        self.processor_backend_info = ProcessorBackendInfoRetriever.get_info_by_name(processor_backend_name)

    @log
    def read_by_draft_id(self, draft_id: str) -> pd.DataFrame:
        """
        Read data that implement the draft specified by its id. It's important to note that the developer knows nothings about the
        data implementation under the hood; the api stays the same.
        :param draft_id: id of the draft
        :return: dataset
        """
        draft_info = DraftInfoRetriever.get_info_by_id(draft_id)
        _priority = DatasetImplementationDiscoveryFactory().get("on_read").discover_highest_priority(draft_info, self.app_info)

        dataset_implementation_info = DatasetImplementationDiscoveryFactory().get("on_read").discover(draft_info=draft_info,
                                                                                                      app_info=self.app_info,
                                                                                                      priority=_priority)
        driver_info = ReadDriverDiscovery().discover(dataset_implementation_info, self.processor_backend_info)
        driver = DriverLoader.load(driver_info)
        dataset = driver.read(draft_info=dataset_implementation_info.draft_info,
                              app_info=self.app_info)

        return dataset

    @log
    def read_by_draft_ids(self, draft_ids: List[str]) -> Iterator[Tuple[str, pd.DataFrame]]:
        """
        Read multiple datasets by their draft_ids; the developer knows nothing about specific implementations of
        these drafts.
        :param draft_ids: list of draft_ids
        :return: iterator of datasets
        """
        for _draft_id, in draft_ids:
            dataset = self.read_by_draft_id(_draft_id)

            yield (_draft_id, dataset)

    @log
    def read_by_draft_name(self, draft_name: str) -> pd.DataFrame:
        """
        Read data that implement the draft specified by its name. It's important to note that the developer knows nothing about the
        data implementation under the hood; the api stays the same.
        :param draft_name: name of the draft
        :return:
        """
        draft_info = DraftInfoRetriever.get_info_by_name(draft_name)
        _priority = DatasetImplementationDiscoveryFactory().get("on_read").discover_highest_priority(draft_info, self.app_info)

        dataset_implementation_info = DatasetImplementationDiscoveryFactory().get("on_read").discover(draft_info=draft_info,
                                                                                  app_info=self.app_info,
                                                                                  priority=_priority)

        driver_info = ReadDriverDiscovery().discover(dataset_implementation_info, self.processor_backend_info)
        driver = DriverLoader.load(driver_info)
        dataset = driver.read(draft_info=dataset_implementation_info.draft_info,
                              app_info=self.app_info)

        return dataset

    @log
    def read_by_draft_names(self, draft_names: List[str]) -> Iterator[Tuple[str, pd.DataFrame]]:
        """
        Read multiple datasets by their draft_names; the developer knows nothing about specific implementations of
        these drafts.
        :param draft_names: list of draft_names
        :return: iterator of datasets
        """
        for _draft_name in draft_names:
            dataset = self.read_by_draft_name(_draft_name)

            yield (_draft_name, dataset)

    @log
    def read_by_dataset_implementation_id(self, dataset_implementation_id: str) -> pd.DataFrame:
        """
        Read data from a specific dataset implementation. Direct usage of this method is discouraged; use
        read_by_draft_id method instead.
        :param dataset_implementation_id:  id of dataset implementation
        :return: dataset
        """
        dataset_implementation_info = DatasetImplementationInfoRetriever.get_info(dataset_implementation_id)

        driver_info = ReadDriverDiscovery().discover(dataset_implementation_info, self.processor_backend_info)
        driver = DriverLoader.load(driver_info)
        dataset = driver.read(draft_info=dataset_implementation_info.draft_info,
                              app_info=self.app_info)

        return dataset

    @log
    def read_by_dataset_implementation_ids(self, dataset_implementation_ids: List[str]) -> Iterator[Tuple[str, pd.DataFrame]]:
        """
        Read multiple datasets by their dataset implementation ids. Direct usage of this method is discouraged; use
        read_by_draft_ids method instead.
        :param dataset_implementation_ids: list of ids of dataset implementations
        :return: iterator of datasets
        """
        for dataset_implementation_id in dataset_implementation_ids:
            yield self.read_by_dataset_implementation_id(dataset_implementation_id)

    @log
    def read_by_dataset_implementation_name(self, dataset_implementation_name: str) -> pd.DataFrame:
        """
        Read data by the id of dataset. Direct usage of this method is discouraged; use read_by_draft_name method
        instead.
        :param dataset_implementation_name: name of dataset implementation
        :return: dataset
        """
        dataset_implementation_info = DatasetImplementationInfoRetriever.get_info_by_name(dataset_implementation_name)

        driver_info = ReadDriverDiscovery().discover(dataset_implementation_info, self.processor_backend_info)
        driver = DriverLoader.load(driver_info)
        dataset = driver.read(draft_info=dataset_implementation_info.draft_info,
                              app_info=self.app_info)

        return dataset

    @log
    def read_by_dataset_implementation_names(self, dataset_implementation_names: List[str]) -> Iterator[Tuple[str, pd.DataFrame]]:
        """
        Read multiple datasets by their dataset implementation names. Direct usage of this method is discouraged; use
        read_by_draft_names method instead.
        :param dataset_implementation_names: list of names of dataset implementations.
        :return: iterator of datasets
        """
        for dataset_implementation_name in dataset_implementation_names:
            yield self.read_by_dataset_implementation_name(dataset_implementation_name)


class Writer(AbstractIO):
    def __init__(self, app_name: str, processor_backend_name: str):
        """
        Provides functionality to write data to different sinks without knowing what these sinks actually are.
        :param app_name: name of the app; the app roughly corresponds to a set of dataset implementations
        :param processor_backend_name: name of the backend the processor is written in
        """
        logger.debug("Creating Writer()")
        self.app_info = AppInfoRetriever.get_info_by_name(app_name)
        self.processor_backend_info = ProcessorBackendInfoRetriever.get_info_by_name(processor_backend_name)

    @log
    def write_by_draft_id_single_sink(self, dataset: pd.DataFrame, draft_id: str, priority: int = None) -> None:
        """
        Write data that implement the draft specified by its id to a single sink.
        It's important to note that the developer knows nothings about the
        data implementation under the hood; the api stays the same.
        :param dataset: dataset to write to the sink
        :param draft_id: id of the draft
        :param priority: priority of the implementation; used by methods like read_by_draft_id etc.
        :return: None
        """
        draft_info = DraftInfoRetriever.get_info_by_id(draft_id)
        if priority is None:
            _priority = DatasetImplementationDiscoveryFactory().get("on_write").discover_highest_priority(draft_info, self.app_info)
        else:
            _priority = priority

        dataset_implementation_info = DatasetImplementationDiscoveryFactory().get("on_write").discover(draft_info, self.app_info, priority=_priority)

        driver_info = WriteDriverDiscovery().discover(dataset_implementation_info, self.processor_backend_info)
        driver = DriverLoader.load(driver_info)

        driver.write(dataset, draft_info, self.app_info)

    @log
    def write_by_draft_id(self, dataset: pd.DataFrame, draft_id: str, priorities: List[int] = None) -> None:
        """
        Write data that implement the draft specified by its id to multiple sinks.
        It's important to note that the developer knows nothings about the
        data implementations under the hood; the api stays the same.
        :param dataset: dataset to write to the sink
        :param draft_id: id of the draft
        :param priorities: priorities of the implementations; used by methods like read_by_draft_id etc.
        :return: None
        """
        draft_info = DraftInfoRetriever.get_info_by_id(draft_id)
        if priorities is None:
            _priorities = [DatasetImplementationDiscoveryFactory().get("on_write").discover_highest_priority(draft_info, self.app_info)]
        else:
            _priorities = priorities

        for _priority in _priorities:
            self.write_by_draft_id_single_sink(dataset, draft_id, _priority)

    @log
    def write_by_draft_name_single_sink(self, dataset: pd.DataFrame, draft_name: str, priority: int = None) -> None:
        """
        Write data that implement the draft specified by its id to a single sink.
        It's important to note that the developer knows nothings about the
        data implementation under the hood; the api stays the same.
        :param dataset: dataset to write to the sink
        :param draft_name: name of the draft
        :param priority: priority of the implementation; used by methods like read_by_draft_id etc.
        :return: None
        """
        draft_info = DraftInfoRetriever.get_info_by_name(draft_name)
        if priority is None:
            _priority = DatasetImplementationDiscoveryFactory().get("on_write").discover_highest_priority(draft_info, self.app_info)
        else:
            _priority = priority

        dataset_implementation_info = DatasetImplementationDiscoveryFactory().get("on_write").discover(draft_info, self.app_info, priority=_priority)

        driver_info = WriteDriverDiscovery().discover(dataset_implementation_info, self.processor_backend_info)
        driver = DriverLoader.load(driver_info)

        driver.write(dataset, draft_info, self.app_info)

    @log
    def write_by_draft_name(self, dataset: pd.DataFrame, draft_name: str, priorities: List[int] = None) -> None:
        """
        Write data that implement the draft specified by its id to multiple sinks.
        It's important to note that the developer knows nothings about the
        data implementations under the hood; the api stays the same.
        :param dataset: dataset to write to the sink
        :param draft_name: name of the draft
        :param priorities: priorities of the implementations; used by methods like read_by_draft_id etc.
        :return: None
        """
        draft_info = DraftInfoRetriever.get_info_by_name(draft_name)
        if priorities is None:
            _priorities = [DatasetImplementationDiscoveryFactory().get("on_write").discover_highest_priority(draft_info, self.app_info)]
        else:
            _priorities = priorities

        for _priority in _priorities:
            self.write_by_draft_name_single_sink(dataset, draft_name, _priority)

    @log
    def write_by_dataset_implementation_id(self, dataset: pd.DataFrame, dataset_implementation_id: str) -> None:
        """
        Write data to a sink specified by dataset implementation id.
        The direct usage of this method is discouraged; use write_by_draft_id method instead.
        :param dataset: dataset to write to the sink
        :param dataset_implementation_id: id of the dataset implementation
        :return: None
        """
        dataset_implementation_info = DatasetImplementationInfoRetriever.get_info(dataset_implementation_id)

        driver_info = WriteDriverDiscovery().discover(dataset_implementation_info, self.processor_backend_info)
        driver = DriverLoader.load(driver_info)

        driver.write(dataset, dataset_implementation_info.draft_info, self.app_info)

    @log
    def write_by_dataset_implementation_ids(self, dataset: pd.DataFrame, dataset_implementation_ids: List[str]) -> None:
        """
        Write data to multiple sinks specified by dataset implementation ids.
        The direct usage of this method is discouraged; use write_by_draft_ids method instead.
        :param dataset: dataset to write to the sink
        :param dataset_implementation_ids: id of the dataset implementations
        :return: None
        """
        for dataset_implementation_id in dataset_implementation_ids:
            self.write_by_dataset_implementation_id(dataset, dataset_implementation_id)

    @log
    def write_by_dataset_implementation_name(self, dataset: pd.DataFrame, dataset_implementation_name: str) -> None:
        """
        Write data to a sink specified by dataset implementation name.
        The direct usage of this method is discouraged; use write_by_draft_name method instead.
        :param dataset: dataset to write to the sink
        :param dataset_implementation_name: name of the dataset implementation
        :return: None
        """
        dataset_implementation_info = DatasetImplementationInfoRetriever.get_info_by_name(dataset_implementation_name)

        driver_info = WriteDriverDiscovery().discover(dataset_implementation_info, self.processor_backend_info)
        driver = DriverLoader.load(driver_info)

        driver.write(dataset, dataset_implementation_info.draft_info, self.app_info)

    @log
    def write_by_dataset_implementation_names(self, dataset: pd.DataFrame, dataset_implementation_names: List[str]) -> None:
        """
        Write data to multiple sinks specified by dataset implementation names.
        The direct usage of this method is discouraged; use write_by_draft_ids method instead.
        :param dataset: dataset to write to the sink
        :param dataset_implementation_names: names of the dataset implementations
        :return: None
        """
        for dataset_implementation_name in dataset_implementation_names:
            self.write_by_dataset_implementation_name(dataset, dataset_implementation_name)

