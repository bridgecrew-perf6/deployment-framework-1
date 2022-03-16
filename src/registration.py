from hashlib import md5
from abc import ABC, abstractmethod
import json
from typing import Any, Dict

from neo4j import GraphDatabase
from networkx import DiGraph
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from src.config import metadata_engine, graph_database_uri, graph_database_auth, logger
from src.data_model import ProcessorProducesSinkInfo, ProcessorBackendInfo, ProcessorUponPrimarySourceDependencyInfo, \
    ProcessorUponProcessorDependencyInfo, ProcessorImplementationInfo, AbstractProcessorInfo, DatasetImplementationInfo, \
    DriverInfo, SourceInfo, DraftInfo, AppInfo, DagBuildInfo, DagFlowLevelInfo, \
    AbstractProcessorToImplementationMappingInfo, DagImplementationInfo, DagBuildInfoFactory, \
    DagImplementationInfoFactory, DagFlowLevelInfoFactory, ProcessorBackendInfoFactory, AbstractProcessorInfoFactory, \
    DatasetImplementationInfoFactory, DriverInfoFactory, SourceInfoFactory, DraftInfoFactory, AppInfoFactory, \
    AbstractModelInfo, AbstractModelInfoFactory, ModelBackendInfo, ModelUponInletDraftDependencyInfo, \
    ModelProducesOutletDraftInfo, ModelImplementationInfo, ModelDriverInfo, ModelBackendsAvailableInfo
from src.dataset_implementation_discovery import DatasetImplementationDiscoveryFactory
from src.retrievers import (AppInfoRetriever, DraftInfoRetriever, SourceInfoRetriever,
                            DriverInfoRetriever, DatasetImplementationInfoRetriever, AbstractProcessorInfoRetriever,
                            ProcessorImplementationInfoRetriever,
                            ProcessorUponProcessorDependencyRetriever, ProcessorUponPrimarySourceDependencyRetriever,
                            ProcessorProducesSinkRetriever, ProcessorBackendInfoRetriever, DagBuildInfoRetriever,
                            DagFlowLevelInfoRetriever, DagImplementationInfoRetriever, AbstractModelInfoRetriever,
                            ModelBackendInfoRetriever, ModelImplementationInfoRetriever, ModelDriverInfoRetriever)
from src.utilities import increment_priority, decrement_priority, logger, log


class ResourceHandler(ABC):
    @abstractmethod
    def register(self, _info):
        pass

    @abstractmethod
    def drop_by_id(self, _id):
        pass

    @abstractmethod
    def drop_by_name(self, _name):
        pass


class AppHandler(ResourceHandler):
    def __init__(self):
        """
        Used to register and delete apps.
        """
        logger.debug("Creating AppHandler()")
        pass

    @log
    def register(self, app_info: AppInfo):
        """
        Register new app.
        :param app_info: app info
        :return:
        """
        query = f"""
        insert into app_info (
            app_id,
            app_name
        ) values (
            '{app_info.app_id}',
            '{app_info.app_name}'
        )
        """
        try:
            result = metadata_engine.execute(text(query).execution_options(autocommit=True))
        except:
            logger.info("failed to register new app in register method of " + str(type(self)) + "; probably already registered")
            result = None

        return result

    @log
    def register_by_name(self, app_name: str):
        """
        Register app by name.
        :param app_name: name of the app
        :return:
        """
        app_info = AppInfoFactory.construct_by_name(
            app_name=app_name
        )
        self.register(app_info=app_info)

    @log
    def drop_by_id(self, app_id: str):
        """
        Drop app by id.
        :param app_id: app id
        :return:
        """
        query = f"""
        delete from app_info
        where app_id = '{app_id}' 
        """
        result = metadata_engine.execute(text(query).execution_options(autocommit=True))

        return result

    @log
    def drop_by_name(self, app_name: str):
        """
        Drop app by its name.
        :param app_name: app name
        :return:
        """
        app_id = AppInfoRetriever.get_info_by_name(app_name).app_id
        result = self.drop_by_id(app_id)

        return result


class DraftHandler(ResourceHandler):
    def __init__(self):
        """
        Class used to register and drop apps.
        """
        logger.debug("Creating DraftHandler()")
        pass

    @log
    def register(self, draft_info: DraftInfo):
        """
        Register draft using draft_info.
        :param draft_info: draft info
        :return:
        """
        query = f"""
        insert into draft_info (
            draft_id,
            draft_name
        ) values (
            '{draft_info.draft_id}',
            '{draft_info.draft_name}'
        )
        """
        try:
            result = metadata_engine.execute(text(query).execution_options(autocommit=True))
        except:
            logger.info("failed to register draft info in register method of " + str(type(self)) + "; probably already registered")
            result = None

        return result

    @log
    def register_by_name(self, draft_name: str):
        """
        Register draft by name.
        :param draft_name: draft name
        :return:
        """
        draft_info = DraftInfoFactory.construct_by_name(
            draft_name=draft_name
        )

        self.register(draft_info)

    @log
    def drop_by_id(self, draft_id: str):
        """
        Register draft by id.
        :param draft_id: draft id
        :return:
        """
        query = f"""
        delete from draft_info
        where draft_id = '{draft_id}'
        """
        result = metadata_engine.execute(text(query).execution_options(autocommit=True))

        return result

    @log
    def drop_by_name(self, draft_name: str):
        """
        Register draft by name.
        :param draft_name: draft name
        :return:
        """
        draft_id = DraftInfoRetriever.get_info_by_name(draft_name).draft_id
        result = self.drop_by_id(draft_id)

        return result


class SourceHandler(ResourceHandler):
    def __init__(self):
        """
        Used to register and drop sources.
        """
        logger.debug("Creating SourceHandler()")
        pass

    @log
    def register(self, source_info: SourceInfo):
        """
        Register source by its source_info.
        :param source_info: source info
        :return:
        """
        query = f"""
        insert into source_info (
            source_id,
            source_name,
            connection_info
        ) values (
            '{source_info.source_id}',
            '{source_info.source_name}',
            '{source_info.connection_info}'
        )
        """
        try:
            result = metadata_engine.execute(text(query).execution_options(autocommit=True))
        except:
            logger.info("failed to register source info in register method of " + str(type(self)) + "; probably already registered")
            result = None

        return result

    @log
    def register_by_name(self, source_name: str, connection_info: dict):
        """
        Register source by its name and connection info.
        :param source_name: source name
        :param connection_info: connection info
        :return:
        """
        source_info = SourceInfoFactory.construct_by_name(
            source_name,
            connection_info
        )

        self.register(source_info)

    @log
    def drop_by_id(self, source_id: str):
        """
        Drop source by its id.
        :param source_id: source id
        :return:
        """
        query = f"""
        delete from source_info
        where source_id = '{source_id}'
        """
        result = metadata_engine.execute(text(query).execution_options(autocommit=True))

        return result

    @log
    def drop_by_name(self, source_name: str):
        """
        Drop source by its name.
        :param source_name: source name
        :return:
        """
        source_id = SourceInfoRetriever.get_info_by_name(source_name).source_id
        result = self.drop_by_id(source_id)

        return result


class DriverHandler(ResourceHandler):
    def __init__(self):
        """
        Used to register and drop drivers.
        """
        logger.debug("Creating DriverHandler()")
        pass

    @log
    def register(self, driver_info: DriverInfo):
        """
        Register driver by its driver_info.
        :param driver_info: driver info
        :return:
        """
        SourceHandler().register(driver_info.source_info)
        ProcessorBackendHandler().register(driver_info.processor_backend_info)
        query = f"""
        insert into driver_info (
            driver_id,
            driver_name,
            driver_function,
            location_info,
            source_id,
            processor_backend_id 
        ) values (
            '{driver_info.driver_id}',
            '{driver_info.driver_name}',
            '{driver_info.driver_function}',
            '{json.dumps(driver_info.location_info)}',
            '{driver_info.source_info.source_id}',
            '{driver_info.processor_backend_info.processor_backend_id}'
        )
        """
        try:
            result = metadata_engine.execute(text(query).execution_options(autocommit=True))
        except:
            logger.info("failed to register driver info in method of " + str(type(self)) + "; probably already registered")
            result = None

        return result

    @log
    def register_by_name(self,
                         driver_name: str,
                         driver_function: str,
                         location_info: dict,
                         source_info: SourceInfo,
                         processor_backend_info: ProcessorBackendInfo):
        """
        Register driver using its name only.
        :param driver_name: name of the driver
        :param driver_function: function of the driver (either on_write or on_read)
        :param location_info: location of the driver
        :param source_info: info about the source
        :param processor_backend_info: info about the backend
        :return:
        """
        driver_info = DriverInfoFactory.construct_by_name(
            driver_name=driver_name,
            driver_function=driver_function,
            location_info=location_info,
            source_info=source_info,
            processor_backend_info=processor_backend_info
        )

        self.register(driver_info)

    @log
    def drop_by_id(self, driver_id: str):
        """
        Drop driver by its id.
        :param driver_id: driver id
        :return:
        """
        query = f"""
        delete from driver_info
        where driver_id = '{driver_id}'
        """
        result = metadata_engine.execute(text(query).execution_options(autocommit=True))

        return result

    @log
    def drop_by_name(self, driver_name: str):
        """
        Drop driver using its name only.
        :param driver_name: name
        :return:
        """
        driver_id = DriverInfoRetriever.get_info_by_name(driver_name).driver_id
        result = self.drop_by_id(driver_id)

        return result


class DatasetImplementationHandler:
    def __init__(self, function: str):
        """
        Used to register and drop dataset implementations with regard to their function.
        :param function: either on_write or on_read
        """
        logger.debug("Creating DatasetImplementationHandler()")
        self.app_handler = AppHandler()
        self.draft_handler = DraftHandler()
        self.source_handler = SourceHandler()
        self.function = function

    @log
    def register_with_priority(self,
                               dataset_implementation_info: DatasetImplementationInfo,
                               priority: int) -> None:
        """
        Register dataset implementation with some priority.
        :param dataset_implementation_info: info about dataset implementation
        :param priority: priority
        :return:
        """
        AppHandler().register(dataset_implementation_info.app_info)
        DraftHandler().register(dataset_implementation_info.draft_info)
        SourceHandler().register(dataset_implementation_info.source_info)
        main_query = f"""
        insert into dataset_implementation_info (
            dataset_implementation_id,
            dataset_implementation_name,
            app_id,
            draft_id,
            source_id
        ) values (
            '{dataset_implementation_info.dataset_implementation_id}',
            '{dataset_implementation_info.dataset_implementation_name}',
            '{dataset_implementation_info.app_info.app_id}',
            '{dataset_implementation_info.draft_info.draft_id}',
            '{dataset_implementation_info.source_info.source_id}'
        )
        """
        priority_query = f"""
        insert into dataset_implementation_priorities (
            dataset_implementation_id,
            function,
            priority 
        ) values (
            '{dataset_implementation_info.dataset_implementation_id}',
            '{self.function}',
            '{priority}' 
        )  
        """
        Session = sessionmaker(bind=metadata_engine)
        session = Session()
        try:
            session.execute(main_query)
            session.execute(priority_query)
            session.commit()
        except:
            logger.info("failed to register with priority method of " + str(type(self)) + "; probably already registered")
            session.execute(priority_query)
            session.commit()

    @log
    def register_with_priority_by_name(self,
                                       dataset_implementation_name: str,
                                       app_info: AppInfo,
                                       draft_info: DraftInfo,
                                       source_info: SourceInfo,
                                       priority: int):
        """
        Registers dataset implementation with some priority using its name only, not id.
        :param dataset_implementation_name: name of the dataset implementation
        :param app_info: info about the app
        :param draft_info: info about the draft
        :param source_info: info about the source
        :param priority: priority
        :return:
        """
        dataset_implementation_info = DatasetImplementationInfoFactory.construct_by_name(
            dataset_implementation_name=dataset_implementation_name,
            app_info=app_info,
            draft_info=draft_info,
            source_info=source_info
        )

        self.register_with_priority(dataset_implementation_info, priority)

    @log
    def register_with_highest_priority(self, dataset_implementation_info: DatasetImplementationInfo):
        """
        Register dataset implementation with the highest priority possible.
        :param dataset_implementation_info: info about the dataset
        :return:
        """
        highest_priority = DatasetImplementationDiscoveryFactory().get(self.function).discover_highest_priority(dataset_implementation_info.draft_info,
                                                                                                              dataset_implementation_info.app_info)

        new_highest_priority = increment_priority(highest_priority)
        self.register_with_priority(dataset_implementation_info, new_highest_priority)

    @log
    def register_with_lowest_priority(self, dataset_implementation_info: DatasetImplementationInfo):
        """
        Register dataset implementation with the lowest priority possible.
        :param dataset_implementation_info: info about the dataset
        :return:
        """
        lowest_priority = DatasetImplementationDiscoveryFactory().get(self.function).discover_lowest_priority(dataset_implementation_info.draft_info,
                                                                                                            dataset_implementation_info.app_info)
        new_lowest_priority = decrement_priority(lowest_priority)
        self.register_with_priority(dataset_implementation_info, new_lowest_priority)

    @log
    def drop_by_id(self, dataset_implementation_id: str):
        """
        Drop dataset implementation by its id.
        :param dataset_implementation_id: id of the dataset implementation
        :return:
        """
        query = f"""
        delete from dataset_implementation_info
        where dataset_implementation_id = '{dataset_implementation_id}'
        """
        result = metadata_engine.execute(text(query).execution_options(autocommit=True))

        return result

    @log
    def drop_by_name(self, dataset_implementation_name: str):
        """
        Drop dataset implementation by its name
        :param dataset_implementation_name: name of the implementation
        :return:
        """
        dataset_implementation_id = DatasetImplementationInfoRetriever.get_info_by_name(dataset_implementation_name).dataset_implementation_id
        result = self.drop_by_id(dataset_implementation_id)

        return result


class DatasetImplementationHandlerFactory:
    """
    Abstract factory to produce dataset implementation handlers
    """
    @classmethod
    @log
    def get_read_handler(cls) -> DatasetImplementationHandler:
        """
        Produces read handler.
        :return:
        """
        return DatasetImplementationHandler("on_read")

    @classmethod
    @log
    def get_write_handler(cls) -> DatasetImplementationHandler:
        """
        Produces write handler.
        :return:
        """
        return DatasetImplementationHandler("on_write")


class AbstractProcessorHandler(ResourceHandler):
    def __init__(self):
        """
        Registers and drops abstract processors.
        """
        logger.debug("Creating AbstractProcessorHandler()")
        pass

    @log
    def register(self, abstract_processor_info: AbstractProcessorInfo):
        """
        Registers abstract processor by its info.
        :param abstract_processor_info: info about abstract processor
        :return:
        """
        query = f"""
        insert into abstract_processor_info (
            abstract_processor_id,
            abstract_processor_name
        ) values (
            '{abstract_processor_info.abstract_processor_id}',
            '{abstract_processor_info.abstract_processor_name}'
        )
        """
        try:
            result = metadata_engine.execute(text(query).execution_options(autocommit=True))
        except:
            logger.info("failed to register abstract processor in register method of " + str(type(self)) + "; probably already registered")
            result = None

        return result

    @log
    def register_by_name(self, abstract_processor_name: str):
        """
        Registers abstract processor by its name.
        :param abstract_processor_name: name of the abstract processor
        :return:
        """
        abstract_processor_info = AbstractProcessorInfoFactory.construct_by_name(
            abstract_processor_name
        )

        self.register(abstract_processor_info)

    @log
    def drop_by_id(self, abstract_processor_id: str):
        """
        Drop abstract processor by its id.
        :param abstract_processor_id: id of the abstract processor
        :return:
        """
        query = f"""
        delete from abstract_processor_info
        where abstract_processor_id = '{abstract_processor_id}' 
        """
        result = metadata_engine.execute(text(query).execution_options(autocommit=True))

        return result

    @log
    def drop_by_name(self, abstract_processor_name: str):
        """
        Drop abstract processor by its name.
        :param abstract_processor_name: name of the processor
        :return:
        """
        abstract_processor_id = AbstractProcessorInfoRetriever.get_info_by_name(abstract_processor_name).abstract_processor_id
        result = self.drop_by_id(abstract_processor_id)

        return result


class ProcessorImplementationHandler(ResourceHandler):
    def __init__(self):
        """
        Used to register processor implementations and drop them.
        """
        logger.debug("Creating ProcessorImplementationHandler()")
        pass

    @log
    def register(self, processor_implementation_info: ProcessorImplementationInfo):
        """
        Register processor implementation using info about it.
        :param processor_implementation_info: info about processor implementation
        :return:
        """
        AbstractProcessorHandler().register(processor_implementation_info.implements_abstract_processor_info)
        ProcessorBackendHandler().register(processor_implementation_info.backend_info)
        query = f"""
        insert into processor_implementation_info (
            processor_implementation_id,
            processor_implementation_name,
            implements_abstract_processor_id,
            backend_id
        ) values (
            '{processor_implementation_info.processor_implementation_id}',
            '{processor_implementation_info.processor_implementation_name}',
            '{processor_implementation_info.implements_abstract_processor_info.abstract_processor_id}',
            '{processor_implementation_info.backend_info.processor_backend_id}'
        )
        """
        try:
            result = metadata_engine.execute(text(query).execution_options(autocommit=True))
        except:
            logger.info("failed to register processor implementation with priority in register method of " + str(type(self)) + "; probably already registered")
            result = None

        return result

    @log
    def register_by_name(self,
                         processor_implementation_name: str,
                         implements_abstract_processor_info: AbstractProcessorInfo,
                         backend_info: ProcessorBackendInfo):
        """
        Register processor implementation using its name only, not id.
        :param processor_implementation_name: name of processor implementation
        :param implements_abstract_processor_info: name of the abstract processor it implements
        :param backend_info: info about the backend this processor is implemented in
        :return:
        """
        processor_implementation_info = ProcessorImplementationInfo(
            processor_implementation_id=md5(processor_implementation_name.encode("utf-8")).hexdigest(),
            processor_implementation_name=processor_implementation_name,
            implements_abstract_processor_info=implements_abstract_processor_info,
            backend_info=backend_info
        )

        self.register(processor_implementation_info)

    @log
    def drop_by_id(self, processor_implementation_id: str):
        """
        Drop processor implementation by its id.
        :param processor_implementation_id: id of processor implementation
        :return:
        """
        query = f"""
        delete from processor_implementation_info
        where processor_implementation_id = '{processor_implementation_id}'
        """
        result = metadata_engine.execute(text(query).execution_options(autocommit=True))

        return result

    @log
    def drop_by_name(self, processor_implementation_name: str):
        """
        Drop processor implementation by its name.
        :param processor_implementation_name: name of the processor implementation
        :return:
        """
        processor_implementation_id = ProcessorImplementationInfoRetriever.get_info_by_name(processor_implementation_name).processor_implementation_id
        result = self.drop_by_id(processor_implementation_id)

        return result


class ProcessorUponProcessorDependencyHandler(ResourceHandler):
    def __init__(self):
        """
        Used to register and drop processor upon processor dependencies.
        """
        logger.debug("Creating ProcessorUponProcessorDependencyHandler()")
        pass

    @log
    def register(self, processor_upon_processor_dependency_info: ProcessorUponProcessorDependencyInfo):
        """
        Register processor upon processor dependency using info about it.
        :param processor_upon_processor_dependency_info: info about processor upon processor dependency
        :return:
        """
        AbstractProcessorHandler().register(processor_upon_processor_dependency_info.processor_info)
        AbstractProcessorHandler().register(processor_upon_processor_dependency_info.depends_on_processor_info)
        query = f"""
        insert into processor_upon_processor_dependency_info (
            processor_upon_processor_dependency_id,
            processor_id,
            depends_on_processor_id
        ) values (
            '{processor_upon_processor_dependency_info.processor_upon_processor_dependency_id}',
            '{processor_upon_processor_dependency_info.processor_info.abstract_processor_id}',
            '{processor_upon_processor_dependency_info.depends_on_processor_info.abstract_processor_id}'
        )
        """
        try:
            result = metadata_engine.execute(text(query).execution_options(autocommit=True))
        except:
            logger.info("failed to register in register method of " + str(type(self)) + "; probably already registered")
            result = None

        return result

    @log
    def drop_by_id(self, processor_upon_processor_dependency_id: str):
        """
        Drop processor upon processor dependency using its id.
        :param processor_upon_processor_dependency_id: id of the dependency
        :return:
        """
        query = f"""
        delete from processor_upon_processor_dependency_info
        where processor_upon_processor_dependency_id = '{processor_upon_processor_dependency_id}' 
        """
        result = metadata_engine.execute(text(query).execution_options(autocommit=True))

        return result

    @log
    def drop_by_name(self, processor_upon_processor_dependency_name: str):
        """
        Drop processor upon processor dependency using its name (not implemented).
        :param processor_upon_processor_dependency_name: name of the dependency
        :return:
        """
        raise NotImplementedError("Processor upon processor dependencies have no name.")

    @log
    def drop_all_dependencies(self, abstract_processor_id: str):
        """
        Drop all dependencies of a specific abstract processor using its id.
        :param abstract_processor_id: id of the abstract processor.
        :return:
        """
        info_dependencies = ProcessorUponProcessorDependencyRetriever.get_all_dependencies(abstract_processor_id)
        dependency_ids = [dependency_info.processor_upon_processor_dependency_id
                          for dependency_info in info_dependencies]

        for dependency_id in dependency_ids:
            result = self.drop_by_id(dependency_id)
            yield result

    @log
    def drop_all_dependencies_by_name(self, abstract_processor_name: str):
        """
        Drop all dependencies of a specific abstract processor using its name.
        :param abstract_processor_name:
        :return:
        """
        abstract_processor_id = AbstractProcessorInfoRetriever.get_info_by_name(abstract_processor_name).abstract_processor_id
        results = self.drop_all_dependencies(abstract_processor_id)

        yield from results


class ProcessorUponPrimarySourceDependencyHandler(ResourceHandler):
    def __init__(self):
        """
        Used to register and drop processor upon primary source dependencies.
        """
        logger.debug("Creating ProcessorUponPrimarySourceDependencyHandler()")
        pass

    @log
    def register(self, processor_upon_primary_source_dependency_info: ProcessorUponPrimarySourceDependencyInfo):
        """
        Register processor upon primary source dependency using info about it.
        :param processor_upon_primary_source_dependency_info: info about the dependency
        :return:
        """
        AbstractProcessorHandler().register(processor_upon_primary_source_dependency_info.processor_info)
        DraftHandler().register(processor_upon_primary_source_dependency_info.depends_on_draft_info)
        query = f"""
        insert into processor_upon_primary_source_dependency_info (
            processor_upon_primary_source_dependency_id,
            processor_id,
            depends_on_draft_id 
        ) values (
            '{processor_upon_primary_source_dependency_info.processor_upon_primary_source_dependency_id}',
            '{processor_upon_primary_source_dependency_info.processor_info.abstract_processor_id}',
            '{processor_upon_primary_source_dependency_info.depends_on_draft_info.draft_id}' 
        ) 
        """
        try:
            result = metadata_engine.execute(text(query).execution_options(autocommit=True))
        except:
            logger.info("failed to register in register method of " + str(type(self)) + "; probably already registered")
            result = None

        return result

    @log
    def drop_by_id(self, processor_upon_primary_source_dependency_id: str):
        """
        Drop processor upon primary source dependency using its id.
        :param processor_upon_primary_source_dependency_id: id of the dependency
        :return:
        """
        query = f"""
        delete from processor_upon_primary_source_dependency_info
        where processor_upon_primary_source_dependency_id = '{processor_upon_primary_source_dependency_id}' 
        """
        result = metadata_engine.execute(text(query).execution_options(autocommit=True))

        return result

    @log
    def drop_by_name(self, processor_upon_primary_source_dependency_name: str):
        """
        Drop processor upon primary source dependency using its name (not implemented).
        :param processor_upon_primary_source_dependency_name:
        :return:
        """
        raise NotImplementedError("Processor upon primary source dependencies have no name.")

    @log
    def drop_all_dependencies(self, abstract_processor_id: str) -> None:
        """
        Drop all dependencies of an abstract processor using its id.
        :param abstract_processor_id: id of the processor
        :return:
        """
        info_dependencies = ProcessorUponPrimarySourceDependencyRetriever.get_all_dependencies(abstract_processor_id)
        dependency_ids = [dependency_info.processor_upon_primary_source_dependency_id
                          for dependency_info in info_dependencies]

        for dependency_id in dependency_ids:
            self.drop_by_id(dependency_id)

    @log
    def drop_all_dependencies_by_name(self, abstract_processor_name: str) -> None:
        """
        Drop all dependencies of ab abstract processor using its name.
        :param abstract_processor_name: name of the processor
        :return:
        """
        abstract_processor_id = AbstractProcessorInfoRetriever.get_info_by_name(abstract_processor_name).abstract_processor_id

        self.drop_all_dependencies(abstract_processor_id)


class ProcessorProducesSinkHandler(ResourceHandler):
    def __init__(self):
        """
        Registers and drops info about what sinks does this abstract processor produce.
        """
        logger.debug("Creating ProcessorProducesSinkHandler()")
        pass

    @log
    def register(self, processor_produces_sink_info: ProcessorProducesSinkInfo):
        """
        Register info about what sinks does this abstract processor produce.
        :param processor_produces_sink_info: info about the sinks produced
        :return:
        """
        AbstractProcessorHandler().register(processor_produces_sink_info.processor_info)
        DraftHandler().register(processor_produces_sink_info.produces_draft_info)
        query = f"""
        insert into processor_produces_sink_info (
            processor_produces_sink_id,
            processor_id,
            produces_draft_id
        ) values (
            '{processor_produces_sink_info.processor_produces_sink_id}',
            '{processor_produces_sink_info.processor_info.abstract_processor_id}',
            '{processor_produces_sink_info.produces_draft_info.draft_id}'
        )
        """
        try:
            result = metadata_engine.execute(text(query).execution_options(autocommit=True))
        except:
            logger.info("failed to register in register method of " + str(type(self)) + "; probably already registered")
            result = None

        return result

    @log
    def drop_by_id(self, processor_produces_sink_id: str):
        """
        Drop info about produced sinks using id of the entry.
        :param processor_produces_sink_id: id of the entry
        :return:
        """
        query = f"""
        delete from processor_produces_sink_info
        where processor_produces_sink_id = '{processor_produces_sink_id}'
        """
        result = metadata_engine.execute(text(query).execution_options(autocommit=True))

        return result

    @log
    def drop_by_name(self, processor_produces_sink_name: str):
        """
        Drop info about produced sinks using name of the entry (not implemented).
        :param processor_produces_sink_name: name of the entry
        :return:
        """
        raise NotImplementedError("Processor produces sink entries have no name.")

    @log
    def drop_all_dependencies(self, abstract_processor_id: str):
        """
        Drop all entries about what sinks does this abstract processor produce.
        :param abstract_processor_id: id of the processor
        :return:
        """
        info_dependencies = ProcessorProducesSinkRetriever.get_all_dependencies(abstract_processor_id)
        dependency_ids = [dependency_info.processor_produces_sink_id
                          for dependency_info in info_dependencies]

        for dependency_id in dependency_ids:
            self.drop_by_id(dependency_id)

    @log
    def drop_all_dependencies_by_name(self, abstract_processor_name: str):
        """
        Drop all entries about what sinks does this abstract processor produce.
        :param abstract_processor_name: name of the processor
        :return:
        """
        abstract_processor_id = AbstractProcessorInfoRetriever.get_info_by_name(abstract_processor_name).abstract_processor_id

        self.drop_all_dependencies(abstract_processor_id)


class ProcessorBackendHandler(ResourceHandler):
    def __init__(self):
        """
        Registers and drops info about processor backends.
        """
        logger.debug("Creating ProcessorBackendsHandler()")
        pass

    @log
    def register(self, processor_backend_info: ProcessorBackendInfo):
        """
        Register processor backend using info about it.
        :param processor_backend_info: info about processor backend.
        :return:
        """
        query = f"""
        insert into processor_backend_info (
            processor_backend_id,
            processor_backend_name 
        ) values (
            '{processor_backend_info.processor_backend_id}',
            '{processor_backend_info.processor_backend_name}' 
        )     
        """
        try:
            result = metadata_engine.execute(text(query).execution_options(autocommit=True))
        except:
            logger.info("failed to register in register method of " + str(type(self)) + "; probably already registered")
            result = None

        return result

    @log
    def register_by_name(self, processor_backend_name: str):
        """
        Register processor backend using its name only, not id.
        :param processor_backend_name: name of the processor backend.
        :return:
        """
        processor_backend_info = ProcessorBackendInfoFactory.construct_by_name(
            processor_backend_name
        )

        self.register(processor_backend_info)

    @log
    def drop_by_id(self, processor_backend_id: str):
        """
        Drop processor backend using its id.
        :param processor_backend_id: id of the processor backend
        :return:
        """
        query = f"""
        delete from processor_backend_info
        where processor_backend_id = '{processor_backend_id}'
        """
        result = metadata_engine.execute(text(query).execution_options(autocommit=True))

        return result

    @log
    def drop_by_name(self, processor_backend_name: str):
        """
        Drop processor backend using its name.
        :param processor_backend_name: name of the processor backend.
        :return:
        """
        processor_backend_id = ProcessorBackendInfoRetriever.get_info_by_name(processor_backend_name).processor_backend_id
        result = self.drop_by_id(processor_backend_id)

        return result


class FilledDraftsInfoHandler(ResourceHandler):
    def __init__(self):
        """
        Used to both register and drop info about FilledDrafts.
        """
        logger.debug("Creating FilledDraftsInfoHandler()")
        pass

    @log
    def register(self, dataset_implementation_info: DatasetImplementationInfo):
        """ Register filled dataset implementations.
        :param dataset_implementation_info: info about dataset implementation
        :return:
        """
        query = f"""
        insert into filled_drafts_info (
            dataset_implementation_id
        ) values (
            '{dataset_implementation_info.dataset_implementation_id}'
        )
        """
        try:
            result = metadata_engine.execute(text(query).execution_options(autocommit=True))
        except:
            logger.info("failed to register in register method of " + str(type(self)) + "; probably already registered")
            result = None

        return result

    @log
    def drop_by_id(self, dataset_implementation_id: str):
        """ Drop filled drafts by dataset implementation id
        :param dataset_implementation_id: id of the dataset implementation to drop
        :return:
        """
        query = f"""
        delete from filled_drafts_info
        where dataset_implementation_id = '{dataset_implementation_id}'
        """
        result = metadata_engine.execute(text(query).execution_options(autocommit=True))

        return result

    @log
    def drop_by_name(self, name: str):
        """ Drop filled drafts by name.
        :param name:
        :return:
        """
        raise NotImplementedError("You cannot drop filled drafts by name")


class DagFlowLevelHandler(ResourceHandler):
    def __init__(self):
        """
        Used to register and drop info about flow level dags.
        """
        logger.debug("Creating DagFlowLevelHandler()")
        self.driver = GraphDatabase.driver(graph_database_uri, auth=graph_database_auth)

    @log
    def write_graph(self, dag_flow_level_info: DagFlowLevelInfo):
        """
        Write down graph to graph engine.
        :param dag_flow_level_info: info about the graph
        :return:
        """
        with self.driver.session() as session:
            for node in dag_flow_level_info.dag_flow_level.nodes:
                session.run(f"""
                create (n: AbstractProcessor {{ abstract_processor_id: "{node.abstract_processor_id}",
                                                abstract_processor_name: "{node.abstract_processor_name}",
                                                dag_flow_level_id: "{dag_flow_level_info.dag_flow_level_id}",
                                                dag_flow_level_name: "{dag_flow_level_info.dag_flow_level_name}" }} ) 
                return n
                """)

            for edge in dag_flow_level_info.dag_flow_level.edges:
                start, end = edge
                session.run(f"""
                match (abstractProcessor:AbstractProcessor {{ abstract_processor_id: "{start.abstract_processor_id}",
                                                              dag_flow_level_id: "{dag_flow_level_info.dag_flow_level_id}" }} ), 
                      (dependsOnAbstractProcessor:AbstractProcessor {{ abstract_processor_id: "{end.abstract_processor_id}",
                                                                       dag_flow_level_id: "{dag_flow_level_info.dag_flow_level_id}" }} ) 
                create (abstractProcessor) - 
                       [d:Dependency {{ processor_id: "{start.abstract_processor_id}",
                                        depends_on_processor_id: "{end.abstract_processor_id}",
                                        processor_upon_processor_dependency_id: "{DiGraph.get_edge_data(dag_flow_level_info.dag_flow_level, 
                                                                                                        start, 
                                                                                                        end)['dependency'].processor_upon_processor_dependency_id}",
                                        dag_flow_level_id: "{dag_flow_level_info.dag_flow_level_id}",
                                        dag_flow_level_name: "{dag_flow_level_info.dag_flow_level_name}" }} ] -> 
                       (dependsOnAbstractProcessor)
                return d
                """)

    @log
    def drop_graph_by_flow_id(self, dag_flow_level_id: str):
        """
        Drop graph from graph engine using its id.
        :param dag_flow_level_id:
        :return:
        """
        with self.driver.session() as session:
            session.run(f"""
            match (s:AbstractProcessor {{ dag_flow_level_id: "{dag_flow_level_id}" }} ) - 
                  [d:Dependency {{ dag_flow_level_id: "{dag_flow_level_id}" }} ] -> 
                  (e:AbstractProcessor {{ dag_flow_level_id: "{dag_flow_level_id}" }} )
            detach delete s, d, e
            """)

    @log
    def drop_graph_by_flow_name(self, dag_flow_level_name: str):
        """
        Drop graph from graph engine using its name.
        :param dag_flow_level_name:
        :return:
        """
        with self.driver.session() as session:
            session.run(f"""
            match (s:AbstractProcessor {{ dag_flow_level_name: "{dag_flow_level_name}" }} ) -
                  [d:Dependency {{ dag_flow_level_name: "{dag_flow_level_name}" }} ] ->
                  (e:AbstractProcessor {{ dag_flow_level_name: "{dag_flow_level_name}" }} )
            detach delete s, d, e
            """)

    @log
    def register(self, dag_flow_level_info: DagFlowLevelInfo):
        """
        Register dag in metastore and graph engine using info about it.
        :param dag_flow_level_info: info about dag
        :return:
        """
        self.write_graph(dag_flow_level_info)

        query = f"""
        insert into dag_flow_level_info (
            dag_flow_level_id,
            dag_flow_level_name
        ) values (
            '{dag_flow_level_info.dag_flow_level_id}',
            '{dag_flow_level_info.dag_flow_level_name}'
        )
        """
        try:
            result = metadata_engine.execute(text(query).execution_options(autocommit=True))
        except:
            logger.info("failed to register in register method of " + str(type(self)) + "; probably already registered")
            result = None

        return result

    @log
    def register_by_name(self, dag_flow_level_name: str, dag_flow_level: DiGraph):
        """
        Register dag in metastore and graph engine using its name only, not id.
        :param dag_flow_level_name: name of the dag
        :param dag_flow_level: dag
        :return:
        """
        dag_flow_level_info = DagFlowLevelInfoFactory.construct_by_name(
            dag_flow_level_name=dag_flow_level_name,
            dag_flow_level=dag_flow_level
        )

        self.register(dag_flow_level_info)

    @log
    def drop_by_id(self, dag_flow_level_id: str):
        """
        Drop dag from metastore using its id.
        :param dag_flow_level_id: id of the dag
        :return:
        """
        self.drop_graph_by_flow_id(dag_flow_level_id)

        query = f"""
        delete from dag_flow_level_info
        where dag_flow_level_id = '{dag_flow_level_id}'
        """
        result = metadata_engine.execute(text(query).execution_options(autocommit=True))

        return result

    @log
    def drop_by_name(self, dag_flow_level_name: str):
        """
        Drop dag from metastore using its name.
        :param dag_flow_level_name: name of the dag
        :return:
        """
        dag_flow_level_id = DagFlowLevelInfoRetriever.get_info_by_name(dag_flow_level_name).dag_flow_level_id
        result = self.drop_by_id(dag_flow_level_id)

        return result


class AbstractProcessorToImplementationMappingHandler:
    def __init__(self):
        """
        Used to register and drop mappings from abstract processor to processor implementation
        """
        logger.debug("Creating AbstractProcessorToImplementationMappingHandler()")
        pass

    @log
    def register(self, abstract_processor_to_implementation_mapping_info: AbstractProcessorToImplementationMappingInfo):
        """
        Register mapping using info about it.
        :param abstract_processor_to_implementation_mapping_info: info about the mapping
        :return:
        """
        for abstract_processor_info, processor_implementation_info in abstract_processor_to_implementation_mapping_info.mapping.items():
            AbstractProcessorHandler().register(abstract_processor_info)
            ProcessorImplementationHandler().register(processor_implementation_info)
        query = """
        insert into abstract_processor_to_implementation_mapping_info (
            abstract_processor_to_implementation_mapping_id,
            abstract_processor_id,
            processor_implementation_id
        ) values (
            :abstract_processor_to_implementation_mapping_id,
            :abstract_processor_id,
            :processor_implementation_id
        )
        """
        values = [
            {
                "abstract_processor_to_implementation_mapping_id": abstract_processor_to_implementation_mapping_info.abstract_processor_to_implementation_mapping_id,
                "abstract_processor_id": abstract_processor_info.abstract_processor_id,
                "processor_implementation_id": processor_implementation_info.processor_implementation_id
            }
            for abstract_processor_info, processor_implementation_info
            in abstract_processor_to_implementation_mapping_info.mapping.items()
        ]
        try:
            result = metadata_engine.execute(text(query).execution_options(autocommit=True), values)
        except:
            logger.info("failed to register in register method of " + str(type(self)) + "; probably already registered")
            result = None

        return result

    @log
    def drop_by_mapping_id(self, abstract_processor_to_implementation_mapping_id: str):
        """
        Drop mapping using its id.
        :param abstract_processor_to_implementation_mapping_id: id of the mapping
        :return:
        """
        query = f"""
        delete from abstract_processor_to_implementation_mapping_info
        where abstract_processor_to_implementation_mapping_id = {abstract_processor_to_implementation_mapping_id}
        """
        result = metadata_engine.execute(text(query).execution_option_option(autocommit=True))

        return result


class DagImplementationHandler(ResourceHandler):
    def __init__(self):
        """
        Used to register and drop dag implementations.
        """
        logger.debug("Creating DagImplementationHandler()")
        pass

    @log
    def register(self, dag_implementation_info: DagImplementationInfo):
        """
        Register dag implementation using info about it.
        :param dag_implementation_info: info about dag implementation
        :return:
        """
        query = f"""
        insert into dag_implementation_info (
            dag_implementation_id,
            dag_implementation_name,
            implements_dag_flow_level_id,
            abstract_processor_to_implementation_mapping_id 
        ) values (
            '{dag_implementation_info.dag_implementation_id}',
            '{dag_implementation_info.dag_implementation_name}',
            '{dag_implementation_info.dag_flow_level_info.dag_flow_level_id}',
            '{dag_implementation_info.abstract_processor_to_implementation_mapping_info.abstract_processor_to_implementation_mapping_id}' 
        ) 
        """
        try:
            result = metadata_engine.execute(text(query).execution_options(autocommit=True))
        except:
            logger.info("failed to register in register method of " + str(type(self)) + "; probably already registered")
            result = None

        return result

    @log
    def register_by_name(self,
                         dag_implementation_name: str,
                         dag_flow_level_info: DagFlowLevelInfo,
                         abstract_processor_to_implementation_mapping_info: AbstractProcessorToImplementationMappingInfo):
        """
        Register dag implementation using its name only, not id.
        :param dag_implementation_name: name of the dag implementation
        :param dag_flow_level_info: info about the flow level dag this dag implements
        :param abstract_processor_to_implementation_mapping_info: info about mapping
        :return:
        """
        dag_implementation_info = DagImplementationInfoFactory.construct_by_name(
            dag_implementation_name=dag_implementation_name,
            dag_flow_level_info=dag_flow_level_info,
            abstract_processor_to_implementation_mapping_info=abstract_processor_to_implementation_mapping_info
        )

        self.register(dag_implementation_info)

    @log
    def drop_by_id(self, dag_implementation_id: str):
        """
        Drop dag implementation using its id.
        :param dag_implementation_id: id of dag implementation
        :return:
        """
        query = f"""
        delete from dag_implementation_info
        where dag_implementation_id = '{dag_implementation_id}'
        """
        result = metadata_engine.execute(text(query).execute_options(autocommit=True))

        return result

    @log
    def drop_by_name(self, dag_implementation_name: str):
        """
        Drop dag implementation using its name.
        :param dag_implementation_name: name of dag implementation
        :return:
        """
        dag_implementation_id = DagImplementationInfoRetriever.get_info_by_name(dag_implementation_name).dag_implementation_id
        result = self.drop_by_id(dag_implementation_id)

        return result


class DagBuildHandler(ResourceHandler):
    def __init__(self):
        """
        Used to register and drop dag builds.
        """
        logger.debug("Creating DagBuildHandler()")
        pass

    @log
    def register(self, dag_build_info: DagBuildInfo):
        """
        Register dag build using info about it.
        :param dag_build_info: info about the dag build
        :return:
        """
        AppHandler().register(dag_build_info.app_info)
        query = f"""
        insert into dag_build_info (
            dag_build_id,
            dag_build_name,
            app_id,
            dag_implementation_id,
            build_parameters
        ) values (
            '{dag_build_info.dag_build_id}',
            '{dag_build_info.dag_build_name}',
            '{dag_build_info.app_info.app_id}',
            '{dag_build_info.dag_implementation_info.dag_implementation_id}',
            '{json.dumps(dag_build_info.build_parameters)}'
        )
        """
        try:
            result = metadata_engine.execute(text(query).execution_options(autocommit=True))
        except:
            logger.info("failed to register in register method of " + str(type(self)) + "; probably already registered")
            result = None

        return result

    @log
    def register_by_name(self,
                         dag_build_name: str,
                         app_info: AppInfo,
                         dag_implementation_info: DagImplementationInfo,
                         build_parameters: Dict[str, Any]):
        """
        Register dag using its name only, not id.
        :param dag_build_name: name of the dag build
        :param app_info: info about the app
        :param dag_implementation_info: info about the dag implementation
        :param build_parameters: parameters of the build
        :return:
        """
        dag_build_info = DagBuildInfoFactory.construct_by_name(
            dag_build_name=dag_build_name,
            app_info=app_info,
            dag_implementation_info=dag_implementation_info,
            build_parameters=build_parameters
        )

        self.register(dag_build_info)

    @log
    def drop_by_id(self, dag_build_id: str):
        """
        Drop dag build using its id.
        :param dag_build_id: id of the dag build
        :return:
        """
        query = f"""
        delete from dag_build_info
        where dag_build_id = '{dag_build_id}'
        """
        result = metadata_engine.execute(text(query).execution_options(autocommit=True))

        return result

    @log
    def drop_by_name(self, dag_build_name: str):
        """
        Drop dag build using its name.
        :param dag_build_name: name of the dag build
        :return:
        """
        dag_build_id = DagBuildInfoRetriever.get_info_by_name(dag_build_name).dag_build_id
        result = self.drop_by_id(dag_build_id)

        return result


class AbstractModelHandler(ResourceHandler):
    def __init__(self):
        logger.debug("Creating AbstractModelHandler()")
        pass

    @log
    def register(self,
                 abstract_model_info: AbstractModelInfo):
        """ Register info about abstract model.
        :param abstract_model_info: info about model
        :return:
        """
        query = f"""
        insert into abstract_model_info (
            abstract_model_id,
            abstract_model_name
        ) values (
            '{abstract_model_info.abstract_model_id}',
            '{abstract_model_info.abstract_model_name}'
        )
        """
        try:
            result = metadata_engine.execute(text(query).execution_options(autocommit=True))
        except:
            logger.info("failed to register in register method of " + str(type(self)) + "; probably already registered")
            result = None

        return result

    @log
    def register_by_name(self,
                         abstract_model_name: str):
        """ Register abstract model by its name.
        :param abstract_model_name: Name of abstract model.
        :return:
        """
        abstract_model_info = AbstractModelInfoFactory.construct_by_name(abstract_model_name)

        self.register(abstract_model_info)

    @log
    def drop_by_id(self,
                   abstract_model_id: str):
        """ Drop abstract model by id.
        :param abstract_model_id: id of abstract model
        :return:
        """
        query =f"""
        delete from abstract_model_info
        where abstract_model_id = '{abstract_model_id}'
        """
        result = metadata_engine.execute(text(query).execution_options(autocommit=True))

        return result

    @log
    def drop_by_name(self,
                     abstract_model_name: str):
        """ Drop abstract model by name.
        :param abstract_model_name: name of abstract model
        :return:
        """
        abstract_model_id = AbstractModelInfoRetriever.get_info_by_name(abstract_model_name).abstract_model_id
        result = self.drop_by_id(abstract_model_id)

        return result


class ModelUponInletDraftDependencyHandler(ResourceHandler):
    def __init__(self):
        logger.debug("Creating ModelUponInletDraftDependencyHandler()")
        pass

    @log
    def register(self,
                 model_upon_inlet_draft_dependency_info: ModelUponInletDraftDependencyInfo):
        """ Register model_upon_intlet_draft by info about it.
        :param model_upon_inlet_draft_dependency_info: info about a draft the model depends on
        :return:
        """
        query = f"""
        insert into model_upon_inlet_draft_dependency_info (
            model_upon_inlet_draft_dependency_id,
            abstract_model_id,
            depends_on_inlet_draft_id
        ) values (
            '{model_upon_inlet_draft_dependency_info.model_upon_inlet_draft_dependency_id}',
            '{model_upon_inlet_draft_dependency_info.abstract_model.abstract_model_id}',
            '{model_upon_inlet_draft_dependency_info.depends_on_inlet_draft.draft_id}'
        )
        """
        try:
            result = metadata_engine.execute(text(query).execution_options(autocommit=True))
        except:
            logger.info("failed to register in register method of " + str(type(self)) + "; probably already registered")
            result = None

        return result

    @log
    def drop_by_id(self,
                   model_upon_inlet_draft_dependency_id: str):
        """ Drop model_upon_inlet_draft_dependency using its id.
        :param model_upon_inlet_draft_dependency_id: id of the dependency
        :return:
        """
        query = f"""
        delete from model_upon_inlet_draft_dependency_info
        where model_upon_inlet_draft_dependency_id = '{model_upon_inlet_draft_dependency_id}'
        """
        result = metadata_engine.execute(text(query).execution_options(autocommit=True))

        return result

    @log
    def drop_by_name(self,
                     model_upon_inlet_draft_dependency_name: str):
        """ Drop model_upon_inlet_draft_dependency using its name.
        :param model_upon_inlet_draft_dependency_name: name of the dependency
        :return:
        """
        raise NotImplementedError("Dependencies have no name.")

    @log
    def drop_all_dependencies(self,
                              abstract_model_id: str):
        """ Drop all inlet dependencies of a model.
        :param abstract_model_id: id of the model
        :return:
        """
        query = f"""
        delete from model_upon_inlet_draft_dependency_info
        where abstract_model_id = '{abstract_model_id}'
        """
        result = metadata_engine.execute(text(query).execution_options(autocommit=True))

        return result


class ModelProducesOutletDraftHandler(ResourceHandler):
    def __init__(self):
        logger.debug("Creating ModelProducesOutletDraftHandler()")
        pass

    @log
    def register(self,
                 model_produces_outlet_draft_info: ModelProducesOutletDraftInfo):
        """ Register model_produces_outlet_draft by info about it.
        :param model_produces_outlet_draft_info: info about model_produces_outlet_draft
        :return:
        """
        query = f"""
        insert into model_produces_outlet_draft_info (
            model_produces_outlet_draft_id,
            abstract_model_id,
            produces_outlet_draft_id
        ) values (
            '{model_produces_outlet_draft_info.model_produces_outlet_draft_id}',
            '{model_produces_outlet_draft_info.abstract_model.abstract_model_id}',
            '{model_produces_outlet_draft_info.produces_outlet_draft.draft_id}'
        )
        """
        try:
            result = metadata_engine.execute(text(query).execution_options(autocommit=True))
        except:
            logger.info("failed to register in register method of " + str(type(self)) + "; probably already registered")
            result = None

        return result

    @log
    def drop_by_id(self,
                   model_produces_outlet_draft_id: str):
        """ Drop model_produces_outlet_draft by its id.
        :param model_produces_outlet_draft_id: id of model_produces_outlet_draft
        :return:
        """
        query = f"""
        delete from model_produces_outlet_draft_info
        where model_produces_outlet_draft_id = '{model_produces_outlet_draft_id}'
        """
        result = metadata_engine.execute(text(query).execution_options(autocommit=True))

        return result

    @log
    def drop_by_name(self,
                     model_produces_outlet_draft_name: str):
        """ Drop model_produces_outlet_draft by its name.
        :param model_produces_outlet_draft_name: name of model_produces_outlet_draft
        :return:
        """
        raise NotImplementedError("Dependencies have no name.")

    @log
    def drop_all_dependencies(self,
                              abstract_model_id: str):
        """ Drop all outlet dependencies of a specific model.
        :param abstract_model_id: id of abstract model
        :return:
        """
        query = f"""
        delete from model_produces_outlet_draft_info
        where abstract_model_id = '{abstract_model_id}'
        """
        result = metadata_engine.execute(text(query).execution_options(autocommit=True))

        return result


class ModelBackendHandler(ResourceHandler):
    def __init__(self):
        logger.debug("Creating ModelBackendHandler()")
        pass

    @log
    def register(self,
                 model_backend_info: ModelBackendInfo):
        """ Register model backend using info about it.
        :param model_backend_info: info about model backend
        :return:
        """
        query = f"""
        insert into model_backend_info (
            model_backend_id,
            model_backend_name
        ) values (
            '{model_backend_info.model_backend_id}',
            '{model_backend_info.model_backend_name}' 
        )
        """
        try:
            result = metadata_engine.execute(text(query).execution_options(autocommit=True))
        except:
            logger.info("failed to register in register method of " + str(type(self)) + "; probably already registered")
            result = None

        return result

    @log
    def drop_by_id(self,
                   model_backend_id: str):
        """ Drop model backend using its id.
        :param model_backend_id: id of the model backend
        :return:
        """
        query = f"""
        delete from model_backend_info
        where model_backend_id = '{model_backend_id}'
        """
        result = metadata_engine.execute(text(query).execution_options(autocommit=True))

        return result

    @log
    def drop_by_name(self,
                     model_backend_name: str):
        """ Drop model backend using its name.
        :param model_backend_name: name of the model backend
        :return:
        """
        model_backend_id = ModelBackendInfoRetriever.get_info_by_name(model_backend_name).model_backend_id
        result = self.drop_by_id(model_backend_id)

        return result


class ModelBackendsAvailableHandler:
    def __init__(self):
        logger.debug("Creating ModelBackendsAvailableHandler()")
        pass

    @log
    def register(self,
                 model_backends_available_info: ModelBackendsAvailableInfo):
        """ Register info about available model backends.
        :param model_backends_available_info: info about available backends
        :return:
        """
        query = f"""
        insert into model_backends_available (
            processor_backend_id,
            model_backend_id
        ) values (
            '{model_backends_available_info.processor_backend_info.processor_backend_id}',
            '{model_backends_available_info.model_backend_info.model_backend_id}'
        )
        """
        try:
            result = metadata_engine.execute(text(query).execution_options(autocommit=True))
        except:
            logger.info("failed to register in register method of " + str(type(self)) + "; probably already registered")
            result = None

        return result

    @log
    def drop_all_by_model_backend_id(self,
                                   model_backend_id: str):
        """ Drop all available backends by id of model backend.
        :param model_backend_id: id of model backend
        :return:
        """
        query = f"""
        delete from model_backends_available
        where model_backend_id = '{model_backend_id}'
        """
        result = metadata_engine.execute(text(query).execution_options(autocommit=True))

        return result

    @log
    def drop_all_by_processor_backend_id(self,
                                         processor_backend_id: str):
        """ Drop all available backends by id of processor backend.
        :param processor_backend_id: id of processor backend
        :return:
        """
        query = f"""
        delete from model_backends_available
        where processor_backend_id = '{processor_backend_id}'
        """
        result = metadata_engine.execute(text(query).execution_options(autocommit=True))

        return result


class ModelImplementationHandler(ResourceHandler):
    def __init__(self):
        logger.debug("Creating ModelImplementationHandler()")
        pass

    @log
    def register(self,
                 model_implementation_info: ModelImplementationInfo):
        """ Register model implementation by info about it.
        :param model_implementation_info: info about model
        :return:
        """
        query = f"""
        insert into model_implementation_info (
            model_implementation_id,
            model_implementation_name,
            implements_abstract_model_id,
            model_backend_id
        ) values (
            '{model_implementation_info.model_implementation_id}',
            '{model_implementation_info.model_implementation_name}',
            '{model_implementation_info.implements_abstract_model.abstract_model_id}',
            '{model_implementation_info.model_backend_info.model_backend_id}'
        )
        """
        try:
            result = metadata_engine.execute(text(query).execution_options(autocommit=True))
        except:
            logger.info("failed to register in register method of " + str(type(self)) + "; probably already registered")
            result = None

        return result

    @log
    def drop_by_id(self,
                   model_implementation_id: str):
        """ Drop model implementation using its id.
        :param model_implementation_id: id of the model implementation
        :return:
        """
        query = f"""
        delete from model_implementation_info
        where model_implementation_id = '{model_implementation_id}'
        """
        result = metadata_engine.execute(text(query).execution_options(autocommit=True))

        return result

    @log
    def drop_by_name(self,
                     model_implementation_name: str):
        """ Drop model implementation using its name.
        :param model_implementation_name: name of the model implementation
        :return:
        """
        model_implementation_id = ModelImplementationInfoRetriever.get_info_by_name(model_implementation_name).model_implementation_id
        result = self.drop_by_id(model_implementation_id)

        return result


class ModelDriverHandler:
    def __init__(self):
        logger.debug("Creating ModelDriverHandler()")
        pass

    @log
    def register(self,
                 model_driver_info: ModelDriverInfo):
        """ Register driver by driver info.
        :param model_driver_info: info about driver
        :return:
        """
        query = f"""
        insert into model_driver_info (
            model_driver_id,
            model_driver_name,
            processor_backend_id,
            model_backend_id,
            function
        ) values (
            '{model_driver_info.model_driver_id}',
            '{model_driver_info.model_driver_name}',
            '{model_driver_info.processor_backend_info.processor_backend_id}',
            '{model_driver_info.model_backend_info.model_backend_id}',
            '{model_driver_info.function}'
        )
        """
        try:
            result = metadata_engine.execute(text(query).execution_options(autocommit=True))
        except:
            logger.info("failed to register in register method of " + str(type(self)) + "; probably already registered")
            result = None

        return result

    @log
    def drop_by_id(self,
                   model_driver_id: str):
        """ Drop model driver by model driver id.
        :param model_driver_id: id of the driver
        :return:
        """
        query = f"""
        delete from model_driver_info
        where model_driver_id = '{model_driver_id}'
        """
        result = metadata_engine.execute(text(query).execution_options(autocommit=True))

        return result

    @log
    def drop_by_name(self,
                     model_driver_name: str):
        """ Drop model driver by model driver name.
        :param model_driver_name: name of the driver
        :return:
        """
        model_driver_id = ModelDriverInfoRetriever.get_info_by_name(model_driver_name).model_driver_id
        result = self.drop_by_id(model_driver_id)

        return result


class ModelPerformanceHandler:
    def __init__(self):
        logger.debug("Creating ModelPerformanceHandler()")
        pass

    @log
    def register(self,
                 model_implementation_info: ModelImplementationInfo,
                 metrics: Dict[str, float]):
        """ Register model performance using info about model and corresponding metrics.
        :param model_implementation_info: info about model
        :param metrics: model performance metrics as dict
        :return:
        """
        query = f"""
        insert into model_performance_info (
            model_implementation_id,
            metric,
            value
        ) values (
            '{model_implementation_info.model_implementation_id}',
            :metric,
            :value
        )
        """
        try:
            result = metadata_engine.execute(text(query).execution_options(autocommit=True), *[
                {"metric": metric, "value": metrics[metric]}
                for metric in metrics
            ])
        except:
            logger.info("failed to register in register method of " + str(type(self)) + "; probably already registered")
            result = None

        return result

    @log
    def drop_by_model_implementation_id(self,
                                        model_implementation_id: str):
        """ Drop performance info by model implementation id.
        :param model_implementation_id: model implementation id
        :return:
        """
        query = f"""
        delete from model_performance_info
        where model_implementation_id = '{model_implementation_id}'
        """
        result = metadata_engine.execute(text(query).execution_options(autocommit=True))

        return result

