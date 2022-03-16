from abc import ABC, abstractmethod

import pandas as pd

from src.data_model import DatasetImplementationInfo, ProcessorBackendInfo, DriverInfo
from src.retrievers import DriverInfoRetriever
from src.config import metadata_engine
from src.utilities import log


class AbstractDriverDiscovery(ABC):
    @abstractmethod
    @log
    def discover(self,
                 dataset_implementation_info: DatasetImplementationInfo,
                 processor_backend_info: ProcessorBackendInfo,
                 **discovery_params) -> DriverInfo:
        """
        Discover appropriate driver using info about dataset implementation info and processor backend info.
        :param dataset_implementation_info: info about the dataset implementation
        :param processor_backend_info: info about the processor backend
        :param discovery_params: arbitrary params used to discover a driver
        :return:
        """
        pass


class ReadDriverDiscovery(AbstractDriverDiscovery):
    @log
    def discover(self,
                 dataset_implementation_info: DatasetImplementationInfo,
                 processor_backend_info: ProcessorBackendInfo,
                 **discovery_params) -> DriverInfo:
        """
        Discover appropriate read driver using info about dataset implementation info and processor backend info.
        :param dataset_implementation_info: info about the dataset implementation
        :param processor_backend_info: info about the processor backend
        :param discovery_params: arbitrary params used to discover a driver
        :return:
        """
        query = f"""
        select driver_info.driver_id
        from driver_info
        where driver_info.source_id = '{dataset_implementation_info.source_info.source_id}'
            and processor_backend_id = '{processor_backend_info.processor_backend_id}' 
            and driver_function = 'on_read'
        """
        data = pd.read_sql_query(query, metadata_engine)
        top_row = data.iloc[0, :]

        driver_info = DriverInfoRetriever.get_info(top_row.at["driver_id"])

        return driver_info


class WriteDriverDiscovery(AbstractDriverDiscovery):
    @log
    def discover(self,
                 dataset_implementation_info: DatasetImplementationInfo,
                 processor_backend_info: ProcessorBackendInfo,
                 **discovery_params) -> DriverInfo:
        """
        Discover appropriate write driver using info about dataset implementation ifno and processor backend info.
        :param dataset_implementation_info: info about the dataset implementation
        :param processor_backend_info: info about the processor backend
        :param discovery_params: arbitrary params used to discover a driver
        :return:
        """
        query = f"""
        select driver_info.driver_id
        from driver_info
        where driver_info.source_id = '{dataset_implementation_info.source_info.source_id}'
            and processor_backend_id = '{processor_backend_info.processor_backend_id}'
            and driver_function = 'on_write'
        """
        data = pd.read_sql_query(query, metadata_engine)
        top_row = data.iloc[0, :]

        driver_info = DriverInfoRetriever.get_info(top_row.at["driver_id"])

        return driver_info

