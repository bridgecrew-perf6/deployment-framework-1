import importlib
import os
from abc import ABC, abstractmethod
from typing import Any, Dict

import pandas as pd

from src.config import metadata_engine, path_to_models
from src.data_model import ModelDriverInfo, ModelImplementationInfo, ProcessorBackendInfo, \
    ModelImplementationInfoFactory
from src.model_implementation_discovery import ModelImplementationDiscovery
from src.registration import ModelPerformanceHandler
from src.retrievers import ModelDriverInfoRetriever, ProcessorBackendInfoRetriever, AbstractModelInfoRetriever, \
    ModelBackendInfoRetriever
from src.utilities import log, logger


class ModelImplementation:
    def __init__(self,
                 model_implementation_info: ModelImplementationInfo,
                 processor_backend_info: ProcessorBackendInfo,
                 model: Any):
        """ Model implementation (comprises info about model and model itself).
        :param model_implementation_info: info about model implementation
        :param processor_backend_info: info about processor backend
        :param model: model
        """
        logger.debug("Creating ModelImplementation()")
        self.model_implementation_info = model_implementation_info
        self.processor_backend_info = processor_backend_info
        self.model = model

    @abstractmethod
    @log
    def predict_by_app_name(self, app_name: str) -> None:
        """ ???
        :param app_name: name of app
        :return:
        """
        pass

    @abstractmethod
    @log
    def predict_by_app_id(self, app_id: str) -> None:
        """ ???
        :param app_id: id of app
        :return:
        """
        pass


class AbstractModelDriver(ABC):
    @log
    def __init__(self,
                 model_driver_info: ModelDriverInfo):
        """ Abstract model driver; subclass of all model driver implementations.
        :param model_driver_info: info about model driver
        """
        self.model_driver_info = model_driver_info

    @classmethod
    @log
    def path_to_model(cls,
                      model_implementation_info: ModelImplementationInfo) -> str:
        """ Construct path to model.
        :param model_implementation_info: info about model implementation
        :return: path to model
        """
        directory = os.path.join(path_to_models,
                                 model_implementation_info.model_backend_info.model_backend_name,
                                 model_implementation_info.implements_abstract_model.abstract_model_name)
        try:
            os.makedirs(directory)
        except:

            pass

        filename = model_implementation_info.model_implementation_name
        full_path = os.path.join(directory, filename)

        return full_path


class ModelReadDriver(AbstractModelDriver):
    @abstractmethod
    @log
    def read(self,
             model_implementation_info: ModelImplementationInfo) -> ModelImplementation:
        """ Construct model driver (read).
        :param model_implementation_info: info about model implementation
        :return: model implementation
        """
        pass


class ModelWriteDriver(AbstractModelDriver):
    @abstractmethod
    @log
    def write(self,
              model: Any,
              model_implementation_info: ModelImplementationInfo) -> None:
        """ Write down model using info about it.
        :param model: model
        :param model_implementation_info: info about model implementation
        :return: None
        """
        pass


class ModelDriverDiscovery:
    @log
    def __init__(self,
                 function: str):
        """ Used to discover the most appropriate model drivers.
        :param function: either 'on_read' or 'on_write'
        """
        self.function = function

    @log
    def discover(self,
                 model_implementation_info: ModelImplementationInfo,
                 processor_backend_info: ProcessorBackendInfo) -> ModelDriverInfo:
        """ Discover the most appropriate model driver using info about model implementation and processor backend.
        :param model_implementation_info: info about model implementation
        :param processor_backend_info: info about processor backend
        :return: info about model driver
        """
        query = f"""
        select model_driver_id
        from model_driver_info 
        where function = '{self.function}'
            and model_backend_id = '{model_implementation_info.model_backend_info.model_backend_id}'
            and processor_backend_id = '{processor_backend_info.processor_backend_id}' 
        """
        data = pd.read_sql_query(query, metadata_engine)
        top_row = data.iloc[0, :]

        model_driver_info = ModelDriverInfoRetriever.get_info_by_id(top_row.at["model_driver_id"])

        return model_driver_info


class ModelDriverLoader:
    @classmethod
    @log
    def load(cls,
             model_driver_info: ModelDriverInfo) -> AbstractModelDriver:
        """
        Used to load driver using info about model driver.
        :param model_driver_info: info about model driver
        :return: model driver used to either read models or write them down
        """
        package_name = model_driver_info.model_backend_info.model_backend_name + "." + model_driver_info.model_driver_name
        module = importlib.import_module(package_name)
        driver_to_read = "ModelWriteDriverImplementation" if model_driver_info.function == "on_write" else "ModelReadDriverImplementation"
        model_driver = getattr(module, driver_to_read)(model_driver_info)

        return model_driver


class AbstractModelIO(ABC):
    @log
    def __init__(self,
                 processor_backend_name: str):
        """ Used to either read or write down model.
        :param processor_backend_name: name of processor backend.
        """
        # структура путей в хранилище моделей
        # model_backend
        #              \_ abstract_model_name
        #                                    \_ model_implementation_name
        self.processor_backend_info = ProcessorBackendInfoRetriever.get_info_by_name(processor_backend_name)

    @log
    def get_path_to_model(self,
                          abstract_model_name: str,
                          model_backend_name: str) -> str:
        """ Get path to model.
        :param abstract_model_name: name of abstract model
        :param model_backend_name: name of model backend
        :return: path to model
        """
        path_to_model = os.path.join(model_backend_name, abstract_model_name)

        return path_to_model


class ModelReader(AbstractModelIO):
    @log
    def read_by_model_name(self,
                           abstract_model_name: str,
                           **discovery_params) -> ModelImplementation:
        """ Read model using its name.
        :param abstract_model_name: name of abstract model
        :param discovery_params: params used to discover model implementation
        :return: implementation of the model
        """
        abstract_model_info = AbstractModelInfoRetriever.get_info_by_name(abstract_model_name)
        model_implementation_info = ModelImplementationDiscovery().discover(abstract_model_info,
                                                                            self.processor_backend_info,
                                                                            **discovery_params)
        # получает информацию о подходящем драйвере
        model_driver_info = ModelDriverDiscovery("on_read").discover(model_implementation_info,
                                                                     self.processor_backend_info)
        # загружает драйвер
        model_driver = ModelDriverLoader.load(model_driver_info)
        model_implementation = model_driver.read(model_implementation_info)

        return model_implementation


class ModelWriter(AbstractModelIO):
    @log
    def write_by_model_name(self,
                            model: Any,
                            metrics: Dict[str, float],
                            model_backend_name: str,
                            abstract_model_name: str,
                            model_implementation_name: str) -> None:
        """ Used to write down model using its name.
        :param model: model to write down
        :param metrics: performance metrics of the model
        :param model_backend_name: name of model's backend
        :param abstract_model_name: name of abstract model
        :param model_implementation_name: name of model implementation
        :return: None
        """
        model_backend_info = ModelBackendInfoRetriever.get_info_by_name(model_backend_name)
        abstract_model_info = AbstractModelInfoRetriever.get_info_by_name(abstract_model_name)
        model_implementation_info = ModelImplementationInfoFactory.construct_by_name(model_implementation_name,
                                                                                     abstract_model_info,
                                                                                     model_backend_info)
        # получить информацию о подходящем драйвере
        model_driver_info = ModelDriverDiscovery("on_write").discover(model_implementation_info,
                                                                      self.processor_backend_info)
        # загрузить драйвер
        model_driver = ModelDriverLoader.load(model_driver_info)
        # записать модель
        model_driver.write(model, model_implementation_info)
        # записать информацию о модели
        ModelPerformanceHandler().register(model_implementation_info,
                                           metrics)

