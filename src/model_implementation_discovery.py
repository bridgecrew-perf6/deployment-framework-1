from abc import abstractmethod, ABC

import pandas as pd

from src.config import metadata_engine
from src.data_model import ProcessorBackendInfo, AbstractModelInfo, ModelImplementationInfo
from src.retrievers import ModelImplementationInfoRetriever
from src.utilities import log


class AbstractModelImplementationDiscovery(ABC):
    @abstractmethod
    @log
    def discover(self,
                 abstract_model_info,
                 processor_backend_info,
                 **discovery_params):
        """ Discover the most appropriate model implementation.
        :param abstract_model_info: info about abstract model
        :param processor_backend_info: info about processor backend
        :param discovery_params: params to use when discover params
        :return:
        """
        pass


class ModelImplementationDiscovery(AbstractModelImplementationDiscovery):
    @log
    def discover(self,
                 abstract_model_info: AbstractModelInfo,
                 processor_backend_info: ProcessorBackendInfo,
                 **discovery_params) -> ModelImplementationInfo:
        """ Discover the most appropriate model implementation.
        :param abstract_model_info: info about abstract model
        :param processor_backend_info: info about processor backend
        :param discovery_params: params to use when discover params
        :return:
        """
        query = f"""
        select model_implementation_info.model_implementation_id, value
        from model_implementation_info 
        inner join model_backends_available
            on model_implementation_info.model_backend_id = model_backends_available.model_backend_id
        inner join model_performance_info
            on model_implementation_info.model_implementation_id = model_performance_info.model_implementation_id
        where model_backends_available.processor_backend_id = '{processor_backend_info.processor_backend_id}' 
            and metric = 'f1-score'
        """
        data = pd.read_sql_query(query, metadata_engine)
        top_row = data.sort_values(by="value", ascending=False).iloc[0, :]

        model_implementation_info = ModelImplementationInfoRetriever.get_info_by_id(top_row.at["model_implementation_id"])

        return model_implementation_info

