from abc import abstractmethod, ABC
from typing import Tuple, Iterator

import pandas as pd

from src.config import metadata_engine
from src.data_model import AppInfo, DraftInfo, SourceInfo, DatasetImplementationInfo
from src.retrievers import DatasetImplementationInfoRetriever
from src.utilities import log


class DatasetImplementationDiscoveryFactory:
    @log
    def get(self,
            function: str):
        """ Get either DatasetImplementationDiscoveryRead or DatasetImplementationDiscoveryWrite (based on passed function).
        :param function: either 'on_read' or 'on_write'
        :return: either DatasetImplementationDiscoveryRead or DatasetImplementationDiscoveryWrite
        """
        if function == "on_read":
            dataset_implementation_discovery = self.get_read()
        else:
            dataset_implementation_discovery = self.get_write()

        return dataset_implementation_discovery

    @log
    def get_read(self):
        """ Get DatasetImplementationDiscoveryRead.
        :return: DatasetImplementationDiscoveryRead
        """
        return DatasetImplementationDiscoveryRead()

    @log
    def get_write(self):
        """ Get DatasetImplementationDiscoveryWrite.
        :return: DatasetImplementationDiscoveryWrite
        """
        return DatasetImplementationDiscoveryWrite()


class DatasetImplementationDiscovery(ABC):
    @abstractmethod
    @log
    def discover(self,
                 draft_info: DraftInfo,
                 app_info: AppInfo,
                 **discovery_params) -> DatasetImplementationInfo:
        """ Get info about the most appropriate dataset implementation using info about draft and app.
        :param draft_info: info about draft
        :param app_info: info about app
        :param discovery_params: params used to discover the most appropriate dataset implementation
        :return: info about dataset implementation
        """
        pass

    @abstractmethod
    @log
    def discover_in_source(self, draft_info: DraftInfo,
                           app_info: AppInfo,
                           source_info: SourceInfo,
                           **discovery_params) -> DatasetImplementationInfo:
        """ Get info about dataset implementation in a specific source using info about draft and app.
        :param draft_info: info about draft
        :param app_info: info about app
        :param source_info: info about source
        :param discovery_params: params used to discover the most appropriate dataset implementation
        :return: info about dataset implementation
        """
        pass

    @abstractmethod
    @log
    def discover_all_dataset_implementations(self,
                                             draft_info: DraftInfo,
                                             app_info: AppInfo,
                                             **discovery_params) -> Iterator[Tuple[int, DatasetImplementationInfo]]:
        """ Discover all dataset implementations using info about draft and app.
        :param draft_info: info about draft
        :param app_info: info about app
        :param discovery_params: params used to discover the most appropriate dataset implementation
        :return: info about dataset implementation
        """
        pass

    @abstractmethod
    @log
    def discover_highest_priority(self,
                                  draft_info: DraftInfo,
                                  app_info: AppInfo) -> int:
        """ Discover the highest priority among all dataset implementations.
        :param draft_info: info about draft
        :param app_info: info about app
        :return: the highest priority
        """
        pass

    @abstractmethod
    @log
    def discover_lowest_priority(self,
                                 draft_info: DraftInfo,
                                 app_info: AppInfo) -> int:
        """ Discover the lowest priority among all dataset implementations.
        :param draft_info: info about draft
        :param app_info: info about app
        :return: the lowest priority
        """
        pass


class DatasetImplementationDiscoveryRead(DatasetImplementationDiscovery):
    @log
    def discover(self,
                 draft_info: DraftInfo,
                 app_info: AppInfo, **discovery_params) -> DatasetImplementationInfo:
        """ Discover the most appropriate dataset implementation (read).
        :param draft_info: info about draft
        :param app_info: info about app
        :param discovery_params: params used to discover dataset implementation.
        :return: info about dataset implementation
        """
        priority = discovery_params["priority"]
        query = f"""
        select di.dataset_implementation_id
        from dataset_implementation_info di
        inner join dataset_implementation_priorities priorities
            on di.dataset_implementation_id = priorities.dataset_implementation_id
        where di.app_id = '{app_info.app_id}'
            and di.draft_id = '{draft_info.draft_id}'
            and function = 'on_read'
            and priority = '{priority}'
        """
        data = pd.read_sql_query(query, metadata_engine)
        top_row = data.iloc[0, :]
        dataset_implementation_info = DatasetImplementationInfoRetriever.get_info(top_row.at["dataset_implementation_id"])

        return dataset_implementation_info

    @log
    def discover_in_source(self,
                           draft_info: DraftInfo,
                           app_info: AppInfo,
                           source_info: SourceInfo,
                           **discovery_params) -> DatasetImplementationInfo:
        """ Discover the most appropriate dataset implementation (write).
        :param draft_info: info about draft
        :param app_info: info about app
        :param source_info: info about source
        :param discovery_params: params used to discover the most appropriate dataset implementation
        :return:
        """
        priority = discovery_params["priority"]
        query = f"""
        select di.dataset_implementation_id
        from dataset_implementation_info di
        inner join dataset_implementation_priorities priorities
            on di.dataset_implementation_id = priorities.dataset_implementation_id
        where di.app_id = '{app_info.app_id}'
            and di.draft_id = '{draft_info.draft_id}'
            and di.source_id = '{source_info.source_id}'
            and function = 'on_read'
            and priority = '{priority}'
        """
        data = pd.read_sql_query(query, metadata_engine)
        top_row = data.iloc[0, :]
        dataset_implementation_info = DatasetImplementationInfoRetriever.get_info(top_row.at["dataset_implementation_id"])

        return dataset_implementation_info

    @log
    def discover_all_dataset_implementations(self,
                                             draft_info: DraftInfo,
                                             app_info: AppInfo,
                                             **discovery_params) -> Iterator[Tuple[int, DatasetImplementationInfo]]:
        """ Discover all appropriate dataset implementations.
        :param draft_info: info about draft
        :param app_info: info about app
        :param discovery_params: params used to discover dataset implementations
        :return: iterator of dataset implementation infos
        """
        query = f"""
        select di.dataset_implementation_id, priorities.priority
        from dataset_implementation_info di
        inner join dataset_implementation_priorities priorities
            on di.dataset_implementation_id = priorities.dataset_implementation_id
        where di.app_id = '{app_info.app_id}' 
            and di.draft_id = '{draft_info.draft_id}'
            and function = 'on_read'
        """
        data = pd.read_sql_query(query, metadata_engine)
        for index, row in data.iterrows():
            dataset_implementation_info = DatasetImplementationInfoRetriever.get_info(row["dataset_implementation_id"])
            priority = row["priority"]
            yield (priority, dataset_implementation_info)

    @log
    def discover_highest_priority(self,
                                  draft_info: DraftInfo,
                                  app_info: AppInfo) -> int:
        """ Discover the highest priority among all implementations of a specific draft.
        :param draft_info: info about draft
        :param app_info: info about app
        :return: the highest priority
        """
        all_implementations = list(self.discover_all_dataset_implementations(draft_info, app_info))
        if len(all_implementations) == 0:
            priority = None
        else:
            priority, _ = all_implementations[0]

        return priority

    @log
    def discover_lowest_priority(self,
                                 draft_info: DraftInfo,
                                 app_info: AppInfo) -> int:
        """ Discover the lowest priority among all implementations of a specific draft.
        :param draft_info: info about draft
        :param app_info: info about app
        :return: the lowest priority
        """
        all_implementations = list(self.discover_all_dataset_implementations(draft_info, app_info))
        if len(all_implementations) == 0:
            priority = None
        else:
            priority, _ = all_implementations[-1]

        return priority


class DatasetImplementationDiscoveryWrite(DatasetImplementationDiscovery):
    @log
    def discover(self,
                 draft_info: DraftInfo,
                 app_info: AppInfo,
                 **discovery_params) -> DatasetImplementationInfo:
        """ Discover the most appropriate dataset implementation (write).
        :param draft_info: info about draft
        :param app_info: info about app
        :param discovery_params: params used to discover the most appropriate dataset implementation
        :return: info about dataset implementaion
        """
        priority = discovery_params["priority"]
        query = f"""
        select di.dataset_implementation_id
        from dataset_implementation_info di
        inner join dataset_implementation_priorities priorities
            on di.dataset_implementation_id = priorities.dataset_implementation_id
        where di.app_id = '{app_info.app_id}'
            and di.draft_id = '{draft_info.draft_id}'
            and function = 'on_write'
            and priority = '{priority}'
        """
        data = pd.read_sql_query(query, metadata_engine)
        top_row = data.iloc[0, :]
        dataset_implementation_info = DatasetImplementationInfoRetriever.get_info(top_row.at["dataset_implementation_id"])

        return dataset_implementation_info

    @log
    def discover_in_source(self,
                           draft_info: DraftInfo,
                           app_info: AppInfo,
                           source_info: SourceInfo,
                           **discovery_params) -> DatasetImplementationInfo:
        """ Discover the most appropriate dataset implementation in a specific source.
        :param draft_info: info about draft
        :param app_info: info about app
        :param source_info: info about source
        :param discovery_params: params used to discover the most appropriate dataset implementation
        :return: info about dataset implementation
        """
        priority = discovery_params["priority"]
        query = f"""
        select di.dataset_implementation_id
        from dataset_implementation_info di
        inner join dataset_implementation_priorities priorities
            on di.dataset_implementation_id = priorities.dataset_implementation_id
        where di.app_id = '{app_info.app_id}'
            and di.draft_id = '{draft_info.draft_id}'
            and di.source_id = '{source_info.source_id}' 
            and function = 'on_write'
            and priority = '{priority}'
        """
        data = pd.read_sql_query(query, metadata_engine)
        top_row = data.iloc[0, :]
        dataset_implementation_info = DatasetImplementationInfoRetriever.get_info(top_row.at["dataset_implementation_id"])

        return dataset_implementation_info

    @log
    def discover_all_dataset_implementations(self,
                                             draft_info: DraftInfo,
                                             app_info: AppInfo,
                                             **discovery_params) -> Iterator[Tuple[int, DatasetImplementationInfo]]:
        """ Discover all appropriate dataset implementations.
        :param draft_info: info about draft
        :param app_info: info about app
        :param discovery_params: params used to discover appropriate dataset implementations.
        :return: iterator of dataset implementations
        """
        query = f"""
        select di.dataset_implementation_id, priorities.priority
        from dataset_implementation_info di
        inner join dataset_implementation_priorities priorities
            on di.dataset_implementation_id = priorities.dataset_implementation_id
        where di.app_id = '{app_info.app_id}' 
            and di.draft_id = '{draft_info.draft_id}'
            and function = 'on_write'
        """
        data = pd.read_sql_query(query, metadata_engine)
        for index, row in data.iterrows():
            dataset_implementation_info = DatasetImplementationInfoRetriever.get_info(row["dataset_implementation_id"])
            priority = row["priority"]
            yield (priority, dataset_implementation_info)

    @log
    def discover_highest_priority(self,
                                  draft_info: DraftInfo,
                                  app_info: AppInfo) -> int:
        """ Discover the highest priority among all dataset implementations.
        :param draft_info: info about draft
        :param app_info: info about app
        :return: the highest priority
        """
        all_implementations = list(self.discover_all_dataset_implementations(draft_info, app_info))
        if len(all_implementations) == 0:
            priority = None
        else:
            priority, _ = all_implementations[0]

        return priority

    @log
    def discover_lowest_priority(self,
                                 draft_info: DraftInfo,
                                 app_info: AppInfo) -> int:
        """ Discover the highest priority among all dataset implementations.
        :param draft_info: info about draft
        :param app_info: info about app
        :return: the highest priority
        """
        all_implementations = list(self.discover_all_dataset_implementations(draft_info, app_info))
        if len(all_implementations) == 0:
            priority = None
        else:
            priority, _ = all_implementations[-1]

        return priority

