from _md5 import md5
from dataclasses import dataclass
from typing import Dict, Any, List

from networkx import DiGraph


@dataclass(frozen=True)
class AppInfo:
    """ Info about app. App roughly corresponds to a set of inlet dataset implementations;
    given a flow level dag, it also produces consistent set of outlet dataset implementations.
    """
    app_id: str
    app_name: str


@dataclass(frozen=True)
class DraftInfo:
    """ Info about draft. Draft is a platform-independent sketch of dataset; when implemented using a
    specific platform, it becomes a dataset implementation.
    For the time being, it's fully untyped, that is its schema is not specified.
    """
    draft_id: str
    draft_name: str


@dataclass(frozen=True)
class SourceInfo:
    """ Info about source. Source is something that can be used to store data and uniquely identify a namespace:
    schema of RDBMS, folder with csv files, Kafka topic etc.
    """
    source_id: str
    source_name: str
    connection_info: dict


@dataclass(frozen=True)
class ProcessorBackendInfo:
    """ Info about processor backend. Processor backend roughly corresponds to something scripts can be executed on:
    python environment of specific version with specific packages installed, java of specific version etc.
    """
    processor_backend_id: str
    processor_backend_name: str


@dataclass(frozen=True)
class DriverInfo:
    """ Info about driver. Driver is used to either read data from sources or write to them.
    Driver is always tightly connected to a processor backend and a source.
    """
    driver_id: str
    driver_name: str
    driver_function: str
    location_info: dict
    source_info: SourceInfo
    processor_backend_info: ProcessorBackendInfo


@dataclass(frozen=True)
class DatasetImplementationInfo:
    """ Info about dataset implementation. Dataset implmentation is an implementation of a specific dataset in a
    specific source.
    """
    dataset_implementation_id: str
    dataset_implementation_name: str
    app_info: AppInfo
    draft_info: DraftInfo
    source_info: SourceInfo


@dataclass(frozen=True)
class AbstractProcessorInfo:
    """ Info about abstract processor. Abstract processor is fully determined by inlet drafts it depends on
    and outlet drafts it produces.
    """
    abstract_processor_id: str
    abstract_processor_name: str


@dataclass(frozen=True)
class ProcessorImplementationInfo:
    """ Info about processor implementation.
    Processor implementation corresponds to a script that implements abstract processor on a specific backend.
    """
    processor_implementation_id: str
    processor_implementation_name: str
    implements_abstract_processor_info: AbstractProcessorInfo
    backend_info: ProcessorBackendInfo


@dataclass(frozen=True)
class ProcessorUponProcessorDependencyInfo:
    """ Info about abstract processor this one depends upon.
    In the future, this will be used to automatically build dependency graphs.
    """
    processor_upon_processor_dependency_id: str
    processor_info: AbstractProcessorInfo
    depends_on_processor_info: AbstractProcessorInfo


@dataclass(frozen=True)
class ProcessorUponPrimarySourceDependencyInfo:
    """ Info about primary sources (that is, inlet drafts) this abstract processor depends upon.
    In the future, this will be used to automatically create primary sources before graph launch.
    """
    processor_upon_primary_source_dependency_id: str
    processor_info: AbstractProcessorInfo
    depends_on_draft_info: DraftInfo


@dataclass(frozen=True)
class ProcessorProducesSinkInfo:
    """ Info about outlet drafts this abstract processor produces.
    In the future, this will be used to automatically create primary sources before graph launch.
    """
    processor_produces_sink_id: str
    processor_info: AbstractProcessorInfo
    produces_draft_info: DraftInfo


@dataclass(frozen=True)
class FilledDrafts:
    """ Info about what drafts are filled.
    Supposed to be used to block dag implementation construction if not all primary sources are filled.
    """
    dataset_implementations_info: List[DatasetImplementationInfo]


@dataclass(frozen=True)
class DatasetImplementationPriorities:
    """ Priorities of dataset implementations.
    Usually readers and writers read/write from/to implementations with the highest priorities.
    """
    dataset_implementation_info: DatasetImplementationInfo
    function: str
    priority: int


@dataclass(frozen=True)
class DagFlowLevelInfo:
    """ Flow level dag, i.e. a dag that consists of abstract processors and dependencies between them.
    Concrete implementations consist of flow level dags and mappings between abstract processors and their
    implementations.
    """
    dag_flow_level_id: str
    dag_flow_level_name: str
    dag_flow_level: DiGraph  # of AbstractProcessorInfo


@dataclass(frozen=True)
class AbstractProcessorToImplementationMappingInfo:
    """ Mapping between abstract processors and their implementations.
    Used when constructing dag implementations of flow level dags.
    """
    abstract_processor_to_implementation_mapping_id: str
    mapping: Dict[AbstractProcessorInfo, ProcessorImplementationInfo]


@dataclass(frozen=True)
class DagImplementationInfo:
    """ Dag implementation.
    Used to make dag build that can be executed on a specific pipeline engine (e.g. Airflow).
    Implements a flow level dag using a mapping between abstract processors and processor implementations.
    """
    dag_implementation_id: str
    dag_implementation_name: str
    dag_flow_level_info: DagFlowLevelInfo
    abstract_processor_to_implementation_mapping_info: AbstractProcessorToImplementationMappingInfo


@dataclass(frozen=True)
class DagBuildInfo:
    """ Dag build.
    Used to build a script that can be executed by pipeline engine (e.g. Airflow etc.).
    Comprises info about dag implementation and build parameters (e.g. once or daily etc.).
    """
    dag_build_id: str
    dag_build_name: str
    app_info: AppInfo
    dag_implementation_info: DagImplementationInfo
    build_parameters: Dict[str, Any]


@dataclass(frozen=True)
class AbstractModelInfo:
    """ Abstract model.
    Corresponds to something that can digest features and produce targets, and also be stored as artifact.
    """
    abstract_model_id: str
    abstract_model_name: str


@dataclass(frozen=True)
class ModelUponInletDraftDependencyInfo:
    """ Info about what inlet drafts an abstract model depends on.
    """
    model_upon_inlet_draft_dependency_id: str
    abstract_model: AbstractModelInfo
    depends_on_inlet_draft: DraftInfo


@dataclass(frozen=True)
class ModelProducesOutletDraftInfo:
    """ Info about what outlet drafts an abstract model writes to.
    """
    model_produces_outlet_draft_id: str
    abstract_model: AbstractModelInfo
    produces_outlet_draft: DraftInfo


@dataclass(frozen=True)
class ModelBackendInfo:
    """ Info about model backend.
    Model backend corresponds to a format the model is stored in.
    It's important to note that it's usually independent of processor backend.
    """
    model_backend_id: str
    model_backend_name: str


@dataclass(frozen=True)
class ModelBackendsAvailableInfo:
    """ Info about what model backends are available.
    It's important to note that usually model backend is independent of processor backend but there can be some exceptions.
    """
    processor_backend_info: ProcessorBackendInfo
    model_backend_info: ModelBackendInfo


@dataclass(frozen=True)
class ModelImplementationInfo:
    """ Info about model implementation.
    Model implementation implements an abstract model on a specific model backend.
    It's important to note that there can be a lot of different models with different performance values.
    """
    model_implementation_id: str
    model_implementation_name: str
    implements_abstract_model: AbstractModelInfo
    model_backend_info: ModelBackendInfo


@dataclass(frozen=True)
class ModelDriverInfo:
    """ Info about model driver.
    Model driver is used to load models into memory; it uses info about processor backend and model backend.
    Function can be either 'on_read' or 'on_write'.
    """
    model_driver_id: str
    model_driver_name: str
    processor_backend_info: ProcessorBackendInfo
    model_backend_info: ModelBackendInfo
    function: str


@dataclass(frozen=True)
class ModelPerformanceInfo:
    """ Info about model performance.
    It comprises info about how performant is a specific model.
    """
    model_implementation_info: ModelImplementationInfo
    metrics: Dict[str, float]


class AppInfoFactory:
    @classmethod
    def construct(cls, app_id: str, app_name: str) -> AppInfo:
        """
        Construct AppInfo using both app_id and app_name.
        :param app_id: id of the app
        :param app_name: name of the app
        :return: info about the app
        """
        return AppInfo(app_id=app_id, app_name=app_name)

    @classmethod
    def construct_by_name(cls, app_name: str) -> AppInfo:
        """
        Construct AppInfo without explicitly specifying app_id.
        app_id is calculated using md5 hash function.
        :param app_name: name of the app
        :return: info about the app
        """
        return AppInfo(
            app_id=md5(app_name.encode("utf-8")).hexdigest(),
            app_name=app_name
        )


class DraftInfoFactory:
    @classmethod
    def construct(cls, draft_id: str, draft_name: str) -> DraftInfo:
        """
        Construct DraftInfo using both draft_id and draft_name.
        :param draft_id: id of the draft
        :param draft_name: name of the draft
        :return: info about the draft
        """
        return DraftInfo(draft_id=draft_id, draft_name=draft_name)

    @classmethod
    def construct_by_name(cls, draft_name: str) -> DraftInfo:
        """
        Construct DraftInfo without explicitly specifying draft_id.
        draft_id is calculated using md5 hash function.
        :param draft_name: name of the draft
        :return: info about the draft
        """
        return DraftInfo(
            draft_id=md5(draft_name.encode("utf-8")).hexdigest(),
            draft_name=draft_name
        )


class SourceInfoFactory:
    @classmethod
    def construct(cls, source_id: str, source_name: str, connection_info: dict) -> SourceInfo:
        """
        Construct SourceInfo using both source_id and source_name.
        :param source_id: id of the source
        :param source_name: name of the source
        :param connection_info: information about how to connect to the source
        :return: info about the source
        """
        return SourceInfo(
            source_id=source_id,
            source_name=source_name,
            connection_info=connection_info
        )

    @classmethod
    def construct_by_name(cls, source_name: str, connection_info: dict) -> SourceInfo:
        """
        Construct SourceInfo without explicitly specifying source_id.
        source_id is calculated using md5 hash function.
        :param source_name: name of the source
        :param connection_info: information about how to connect to the source
        :return: info about the source
        """
        return SourceInfo(
            source_id=md5(source_name.encode("utf-8")).hexdigest(),
            source_name=source_name,
            connection_info=connection_info
        )


class ProcessorBackendInfoFactory:
    @classmethod
    def construct(cls, processor_backend_id: str, processor_backend_name: str) -> ProcessorBackendInfo:
        """
        Construct ProcessorBackendInfo using both processor_backend_id and processor_backend_name.
        :param processor_backend_id: id of the backend
        :param processor_backend_name: name of the backend
        :return: info about the processor backend
        """
        return ProcessorBackendInfo(processor_backend_id=processor_backend_id,
                                    processor_backend_name=processor_backend_name)

    @classmethod
    def construct_by_name(cls, processor_backend_name: str):
        """
        Construct ProcessorBackendInfo without explicitly specifying processor_backend_id.
        processor_backend_id is calculated using md5 hash function.
        :param processor_backend_name: name of the processor backend
        :return: info about the processor backend
        """
        return ProcessorBackendInfo(
            processor_backend_id=md5(processor_backend_name.encode("utf-8")).hexdigest(),
            processor_backend_name=processor_backend_name
        )


class DriverInfoFactory:
    @classmethod
    def construct(cls,
                  driver_id: str,
                  driver_name: str,
                  driver_function: str,
                  location_info: dict,
                  source_info: SourceInfo,
                  processor_backend_info: ProcessorBackendInfo) -> DriverInfo:
        """
        Construct DriverInfo using both driver_id and driver_name.
        :param driver_id: id of the driver
        :param driver_name: name of the driver
        :param driver_function: either on_write or on_read
        :param location_info: info about how to find the driver in the filesystem
        :param source_info: info about the source the driver works with
        :param processor_backend_info: info about the processor backend the driver works with
        :return: info about the driver
        """
        return DriverInfo(driver_id=driver_id,
                          driver_name=driver_name,
                          driver_function=driver_function,
                          location_info=location_info,
                          source_info=source_info,
                          processor_backend_info=processor_backend_info)

    @classmethod
    def construct_by_name(cls,
                  driver_name: str,
                  driver_function: str,
                  location_info: dict,
                  source_info: SourceInfo,
                  processor_backend_info: ProcessorBackendInfo) -> DriverInfo:
        """
        Construct DriverInfo without explicitly specifying id of the driver.
        driver_id is calculated using md5 hash function.
        :param driver_name: name of the driver
        :param driver_function: either on_write or on_read
        :param location_info: info about how to find the driver in the filysystem
        :param source_info: info about the source the driver works with
        :param processor_backend_info: info about the processor backend the driver works with
        :return: info about the driver
        """
        return DriverInfo(driver_id=md5(driver_name.encode("utf-8")).hexdigest(),
                          driver_name=driver_name,
                          driver_function=driver_function,
                          location_info=location_info,
                          source_info=source_info,
                          processor_backend_info=processor_backend_info)


class DatasetImplementationInfoFactory:
    @classmethod
    def construct(cls,
                 dataset_implementation_id: str,
                 dataset_implementation_name: str,
                 app_info: AppInfo,
                 draft_info: DraftInfo,
                 source_info: SourceInfo) -> DatasetImplementationInfo:
        """
        Construct DatasetImplementationInfo using both dataset_implementation_id and dataset_implementation_name.
        :param dataset_implementation_id: id of the dataset implementation
        :param dataset_implementation_name: name of the dataset implementation
        :param app_info: info about the app this dataset implementation is connected to
        :param draft_info: info about the draft this dataset implementation implements
        :param source_info: info about the source this dataset implementation is connected to
        :return: info about the dataset implementation
        """
        return DatasetImplementationInfo(dataset_implementation_id=dataset_implementation_id,
                                        dataset_implementation_name=dataset_implementation_name,
                                        app_info=app_info,
                                        draft_info=draft_info,
                                        source_info=source_info)

    @classmethod
    def construct_by_name(cls,
                         dataset_implementation_name: str,
                         app_info: AppInfo,
                         draft_info: DraftInfo,
                         source_info: SourceInfo) -> DatasetImplementationInfo:
        """
        Construct DatasetImplementationInfo without explicitly specifying dataset_implementation_id.
        dataset_implementation_id is calculated using md5 hash function.
        :param dataset_implementation_name: name of the dataset implementation
        :param app_info: info about the app this dataset implementation is connected to
        :param draft_info: info about the draft this dataset implementation implements
        :param source_info: info about the source this dataset implementation is connected to
        :return: info about the dataset implementation
        """
        return DatasetImplementationInfo(dataset_implementation_id=md5(dataset_implementation_name.encode("utf-8")).hexdigest(),
                                        dataset_implementation_name=dataset_implementation_name,
                                        app_info=app_info,
                                        draft_info=draft_info,
                                        source_info=source_info)


class AbstractProcessorInfoFactory:
    @classmethod
    def construct(cls,
                  abstract_processor_id: str,
                  abstract_processor_name: str) -> AbstractProcessorInfo:
        """
        Construct AbstractProcessorInfo using both abstract_processor_id and abstract_processor_name.
        :param abstract_processor_id: id of the abstract processor
        :param abstract_processor_name: name of the abstract processor
        :return: info about the abstract processor
        """
        return AbstractProcessorInfo(abstract_processor_id=abstract_processor_id,
                                     abstract_processor_name=abstract_processor_name)

    @classmethod
    def construct_by_name(cls,
                          abstract_processor_name: str) -> AbstractProcessorInfo:
        """
        Construct AbstractProcessorInfo without explicitly specifying abstract_processor_id.
        abstract_processor_id is calculated using md5 hash function.
        :param abstract_processor_name: name of the abstract processor
        :return: info about the abstract processor
        """
        return AbstractProcessorInfo(abstract_processor_id=md5(abstract_processor_name.encode("utf-8")).hexdigest(),
                                     abstract_processor_name=abstract_processor_name)


class ProcessorImplementationInfoFactory:
    @classmethod
    def construct(cls,
                  processor_implementation_id: str,
                  processor_implementation_name: str,
                  implements_abstract_processor_info: AbstractProcessorInfo,
                  backend_info: ProcessorBackendInfo) -> ProcessorImplementationInfo:
        """
        Construct ProcessorImplementationInfo using both processor_implementation_id and processor_implementation_name.
        :param processor_implementation_id: id of the processor implementation
        :param processor_implementation_name: name of the processor implementation
        :param implements_abstract_processor_info: info about the abstract processor this implementation deals with
        :param backend_info: info about the backend this implementation deals with
        :return: info about the processor implementation
        """
        return ProcessorImplementationInfo(processor_implementation_id=processor_implementation_id,
                                           processor_implementation_name=processor_implementation_name,
                                           implements_abstract_processor_info=implements_abstract_processor_info,
                                           backend_info=backend_info)

    @classmethod
    def construct_by_name(cls,
                  processor_implementation_name: str,
                  implements_abstract_processor_info: AbstractProcessorInfo,
                  backend_info: ProcessorBackendInfo) -> ProcessorImplementationInfo:
        """
        Construct ProcessorImplementationInfo without explicitly specifying processor_implementation_id.
        processor_implementation_id is calculated using md5 hash function.
        :param processor_implementation_name: name of the processor implementation
        :param implements_abstract_processor_info: info about the abstract processor this implementation deals with
        :param backend_info: info about the backend this implementation deals with
        :return: info about the processor implementation
        """
        return ProcessorImplementationInfo(processor_implementation_id=md5(processor_implementation_name.encode("utf-8")).hexdigest(),
                                           processor_implementation_name=processor_implementation_name,
                                           implements_abstract_processor_info=implements_abstract_processor_info,
                                           backend_info=backend_info)


class ProcessorUponProcessorDependencyInfoFactory:
    @classmethod
    def construct(cls,
                  processor_upon_processor_dependency_id: str,
                  processor_info: AbstractProcessorInfo,
                  depends_on_processor_info: AbstractProcessorInfo) -> ProcessorUponProcessorDependencyInfo:
        """
        Construct ProcessorUponProcessorDependencyInfo using processor_upon_processor_dependency_id.
        :param processor_upon_processor_dependency_id: id of the processor upon processor dependency
        :param processor_info: info about the abstract processor
        :param depends_on_processor_info: info about the abstract processor this processor depends upon
        :return: info about the processor upon processor dependency
        """
        return ProcessorUponProcessorDependencyInfo(
            processor_upon_processor_dependency_id=processor_upon_processor_dependency_id,
            processor_info=processor_info,
            depends_on_processor_info=depends_on_processor_info)


class ProcessorUponPrimarySourceDependencyInfoFactory:
    @classmethod
    def construct(cls,
                  processor_upon_primary_source_dependency_id: str,
                  processor_info: AbstractProcessorInfo,
                  depends_on_draft_info: DraftInfo) -> ProcessorUponPrimarySourceDependencyInfo:
        """
        Construct ProcessorUponPrimarySourceDependencyInfo using processor_upon_primary_source_dependency_id.
        :param processor_upon_primary_source_dependency_id: id of the processor upon primary source dependency
        :param processor_info: info about the abstract processor
        :param depends_on_draft_info: info about the draft this processor depends upon
        :return: info about the processor upon primary source dependency
        """
        return ProcessorUponPrimarySourceDependencyInfo(
            processor_upon_primary_source_dependency_id=processor_upon_primary_source_dependency_id,
            processor_info=processor_info,
            depends_on_draft_info=depends_on_draft_info
        )


class ProcessorProducesSinkInfoFactory:
    @classmethod
    def construct(cls,
                  processor_produces_sink_id: str,
                  processor_info: AbstractProcessorInfo,
                  produces_draft_info: DraftInfo) -> ProcessorProducesSinkInfo:
        """
        Construct ProcessorProducesSinkInfo using processor_produces_sink_id.
        :param processor_produces_sink_id: id of the processor produces primary sink
        :param processor_info: info about the abstract processor
        :param produces_draft_info: info about the draft this processor produces
        :return: info about the processor produces sink info
        """
        return ProcessorProducesSinkInfo(
            processor_produces_sink_id=processor_produces_sink_id,
            processor_info=processor_info,
            produces_draft_info=produces_draft_info
        )


class FilledDraftsFactory:
    @classmethod
    def construct(cls,
                  dataset_implementations_info: List[DatasetImplementationInfo]) -> FilledDrafts:
        """
        Consruct FilledDraftsFactory using dataset_implementation_info.
        :param dataset_implementation_info: info about the dataset implementation
        :return: FilledDrafts instance
        """
        return FilledDrafts(dataset_implementations_info)


class DatasetImplementationPrioritiesFactory:
    @classmethod
    def construct(cls,
                  dataset_implementation_info: DatasetImplementationInfo,
                  function: str,
                  priority: int) -> DatasetImplementationPriorities:
        """
        Construct DatasetImplementationPriorities.
        :param dataset_implementation_info: info about the dataset implementation
        :param function: is it priority on_read or on_write
        :param priority: priority
        :return: info about dataset implementation priorities.
        """
        return DatasetImplementationPriorities(
            dataset_implementation_info=dataset_implementation_info,
            function=function,
            priority=priority
        )


class DagFlowLevelInfoFactory:
    @classmethod
    def construct(cls,
                  dag_flow_level_id: str,
                  dag_flow_level_name: str,
                  dag_flow_level: DiGraph) -> DagFlowLevelInfo:
        """
        Construct DagFlowLevelInfo using both dag_flow_level_id and dag_flow_level_name.
        :param dag_flow_level_id: id of the dag flow level
        :param dag_flow_level_name: name of the dag flow level
        :param dag_flow_level: the dag itself
        :return: info about the dag flow level
        """
        return DagFlowLevelInfo(dag_flow_level_id=dag_flow_level_id,
                                dag_flow_level_name=dag_flow_level_name,
                                dag_flow_level=dag_flow_level)

    @classmethod
    def construct_by_name(cls,
                  dag_flow_level_name: str,
                  dag_flow_level: DiGraph) -> DagFlowLevelInfo:
        """
        Construct DagFlowLevelInfo without explicitly specifying dag_flow_level_id.
        dag_flow_level_id is calculated using md5 hash function.
        :param dag_flow_level_name: name of the dag flow level
        :param dag_flow_level: the dag itself
        :return: info about the dag flow level
        """
        return DagFlowLevelInfo(dag_flow_level_id=md5(dag_flow_level_name.encode("utf-8")).hexdigest(),
                                dag_flow_level_name=dag_flow_level_name,
                                dag_flow_level=dag_flow_level)


class AbstractProcessorToImplementationMappingInfoFactory:
    @classmethod
    def construct(cls,
                  abstract_processor_to_implementation_mapping_id: str,
                  mapping: Dict[AbstractProcessorInfo, ProcessorImplementationInfo]) -> AbstractProcessorToImplementationMappingInfo:
        """
        Construct AbstractProcessorToImplementationMappingInfo.
        :param abstract_processor_to_implementation_mapping_id: id of the mapping
        :param mapping: dictionary of [AbstractProcessorInfo, ProcessorImplementationInfo]; the mapping itself
        :return: info about the mapping
        """
        return AbstractProcessorToImplementationMappingInfo(
            abstract_processor_to_implementation_mapping_id=abstract_processor_to_implementation_mapping_id,
            mapping=mapping
        )


class DagImplementationInfoFactory:
    @classmethod
    def construct(cls,
                  dag_implementation_id: str,
                  dag_implementation_name: str,
                  dag_flow_level_info: DagFlowLevelInfo,
                  abstract_processor_to_implementation_mapping_info: AbstractProcessorToImplementationMappingInfo) -> DagImplementationInfo:
        """
        Construct DagImplementationInfo using both dag_implementation_id and dag_implementation_name.
        :param dag_implementation_id: id of the dag implementation
        :param dag_implementation_name: name of the dag implementation
        :param dag_flow_level_info: info about the dag
        :param abstract_processor_to_implementation_mapping_info: info about the mapping
        :return: info about the dag implementation
        """
        return DagImplementationInfo(
            dag_implementation_id=dag_implementation_id,
            dag_implementation_name=dag_implementation_name,
            dag_flow_level_info=dag_flow_level_info,
            abstract_processor_to_implementation_mapping_info=abstract_processor_to_implementation_mapping_info
        )

    @classmethod
    def construct_by_name(cls,
                  dag_implementation_name: str,
                  dag_flow_level_info: DagFlowLevelInfo,
                  abstract_processor_to_implementation_mapping_info: AbstractProcessorToImplementationMappingInfo) -> DagImplementationInfo:
        """
        Construct DagImplementationInfo without explicitly specifying dag_implementation_id.
        dag_implementation_id is caclulated using md5 hash function
        :param dag_implementation_name: name of the dag implementation
        :param dag_flow_level_info: info about the dag
        :param abstract_processor_to_implementation_mapping_info: info about the mapping
        :return: info about the dag implementation
        """
        return DagImplementationInfo(
            dag_implementation_id=md5(dag_implementation_name.encode("utf-8")).hexdigest(),
            dag_implementation_name=dag_implementation_name,
            dag_flow_level_info=dag_flow_level_info,
            abstract_processor_to_implementation_mapping_info=abstract_processor_to_implementation_mapping_info
        )


class DagBuildInfoFactory:
    @classmethod
    def construct(cls,
                  dag_build_id: str,
                  dag_build_name: str,
                  app_info: AppInfo,
                  dag_implementation_info: DagImplementationInfo,
                  build_parameters: Dict[str, Any]) -> DagBuildInfo:
        """
        Construct DagBuildInfo using both dag_build_id and dag_build_name.
        :param dag_build_id: id of the dag build
        :param dag_build_name: name of the dag build
        :param app_info: info about the app
        :param dag_implementation_info: info about the dag implementation
        :param build_parameters: build parameters (currently only airflow)
        :return: info about the dag build
        """
        return DagBuildInfo(
            dag_build_id=dag_build_id,
            dag_build_name=dag_build_name,
            app_info=app_info,
            dag_implementation_info=dag_implementation_info,
            build_parameters=build_parameters
        )

    @classmethod
    def construct_by_name(cls,
                  dag_build_name: str,
                  app_info: AppInfo,
                  dag_implementation_info: DagImplementationInfo,
                  build_parameters: Dict[str, Any]) -> DagBuildInfo:
        """
        Construct DagBuildInfo without explicitly specifying dag_build_id.
        dag_build_id is calculated using md5 hash function.
        :param dag_build_name: name of the dag build
        :param app_info: info about the app
        :param dag_implementation_info: info about the dag implementation
        :param build_parameters: build parameters (currently only airflow)
        :return: info about the dag build
        """
        return DagBuildInfo(
            dag_build_id=md5(dag_build_name.encode("utf-8")).hexdigest(),
            dag_build_name=dag_build_name,
            app_info=app_info,
            dag_implementation_info=dag_implementation_info,
            build_parameters=build_parameters
        )


class AbstractModelInfoFactory:
    @classmethod
    def construct(cls,
                  abstract_model_id: str,
                  abstract_model_name: str) -> AbstractModelInfo:
        """ Used to construct AbstractModelInfo using both name and id.
        :param abstract_model_id: id of abstract model
        :param abstract_model_name: name of abstract model
        :return: AbstractModelInfo
        """
        return AbstractModelInfo(abstract_model_id,
                                 abstract_model_name)

    @classmethod
    def construct_by_name(cls,
                          abstract_model_name: str) -> AbstractModelInfo:
        """ Used to construct AbstractModelInfo using only name; its id is calculated automatically
        using md5 hash function.
        :param abstract_model_name: name of abstract model
        :return: AbstractModelInfo
        """
        return cls.construct(md5(abstract_model_name.encode("utf-8")).hexdigest(),
                             abstract_model_name)


class ModelUponInletDraftDependencyInfoFactory:
    @classmethod
    def construct(cls,
                  model_upon_inlet_draft_dependency_id: str,
                  abstract_model: AbstractModelInfo,
                  depends_on_inlet_draft: DraftInfo) -> ModelUponInletDraftDependencyInfo:
        """ Construct ModelUponInletDraftDependencyInfo using its id and info about an abstract model and
        a draft it depends on.
        :param model_upon_inlet_draft_dependency_id: id of dependency
        :param abstract_model: info about abstract model
        :param depends_on_inlet_draft: info about draft the model depends upon
        :return: ModelUponInletDraftDependencyInfo
        """
        return ModelUponInletDraftDependencyInfo(model_upon_inlet_draft_dependency_id,
                                                 abstract_model,
                                                 depends_on_inlet_draft)

    @classmethod
    def construct_with_auto_id(cls,
                               abstract_model: AbstractModelInfo,
                               depends_on_inlet_draft: DraftInfo) -> ModelUponInletDraftDependencyInfo:
        """ Construct ModelUponInletDraftDependencyInfo using only info about an abstract model and
        a draft it depends on.
        :param abstract_model: info about abstract model
        :param depends_on_inlet_draft: info about draft the model depends upon
        :return: ModelUponInletDraftDependencyInfo
        """
        return ModelUponInletDraftDependencyInfo(md5(f"{abstract_model.abstract_model_id}{depends_on_inlet_draft}".
                                                     encode("utf-8")).hexdigest(),
                                                 abstract_model,
                                                 depends_on_inlet_draft)


class ModelProducesOutletDraftInfoFactory:
    @classmethod
    def construct(cls,
                  model_produces_outlet_draft_id: str,
                  abstract_model: AbstractModelInfo,
                  produces_outlet_draft: DraftInfo) -> ModelProducesOutletDraftInfo:
        """ Construct ModelProducesOutletDraftInfo using abstract_model_info and info about a draft it produces.
        This method needs an id to be explicitly specified.
        :param model_produces_outlet_draft_id: id of the dependency
        :param abstract_model: info about abstract model
        :param produces_outlet_draft: info about outlet draft
        :return: ModelProducesOutletDraftInfo
        """
        return ModelProducesOutletDraftInfo(model_produces_outlet_draft_id,
                                            abstract_model,
                                            produces_outlet_draft)

    @classmethod
    def construct_with_auto_id(cls,
                               abstract_model: AbstractModelInfo,
                               produces_outlet_draft: DraftInfo) -> ModelProducesOutletDraftInfo:
        """ Construct ModelProducesOutletDraftInfo using abstract_model_info and info about a draft it produces only.
        Id here is generated automatically.
        :param abstract_model: info about abstract model
        :param produces_outlet_draft: info about outlet draft it produces
        :return: ModelProducesOutletDraftInfo
        """
        return ModelProducesOutletDraftInfo(md5(f"{abstract_model.abstract_model_id}{produces_outlet_draft.draft_id}".
                                                encode("utf-8")).hexdigest(),
                                            abstract_model,
                                            produces_outlet_draft)


class ModelBackendInfoFactory:
    @classmethod
    def construct(cls,
                  model_backend_id: str,
                  model_backend_name: str) -> ModelBackendInfo:
        """ Construct ModelBackendInfo using both id of model backend and its name
        :param model_backend_id: id of the model backend
        :param model_backend_name: name of the model backend
        :return: info about model backend
        """
        return ModelBackendInfo(model_backend_id,
                                model_backend_name)

    @classmethod
    def construct_by_name(cls,
                          model_backend_name: str) -> ModelBackendInfo:
        """ Construct ModelBackendInfo using its name only; id is calculated automatically using md5 hash function.
        :param model_backend_name: name of the model backend
        :return: info about model backend
        """
        return cls.construct(md5(model_backend_name.encode("utf-8")).hexdigest(),
                             model_backend_name)


class ModelImplementationInfoFactory:
    @classmethod
    def construct(cls,
                  model_implementation_id: str,
                  model_implementation_name: str,
                  implements_abstract_model: AbstractModelInfo,
                  model_backend: ModelBackendInfo) -> ModelImplementationInfo:
        """ Construct ModelImplementationInfo using its id, name, info about what abstract model it implements
        and finally info about what model backend is used to implement it.
        :param model_implementation_id: id of model implementation
        :param model_implementation_name: name of model implementation
        :param implements_abstract_model: info about what abstract model this model implementation implements
        :param model_backend: info about what model backend is used to implement the model
        :return: info about model implementation
        """
        return ModelImplementationInfo(model_implementation_id,
                                       model_implementation_name,
                                       implements_abstract_model,
                                       model_backend)

    @classmethod
    def construct_by_name(cls,
                          model_implementation_name: str,
                          implements_abstract_model: AbstractModelInfo,
                          model_backend: ModelBackendInfo) -> ModelImplementationInfo:
        """ Construct ModelImplementationInfo using only its name, info about what abstract model it implements
        and finally info about what model backend is used to implement it; its id is calculated automatically
        using md5 hash function.
        :param model_implementation_name: name of model implementation
        :param implements_abstract_model: info about what abstract model does this model implementation implements
        :param model_backend: info about what model backend is used to implement the model
        :return: info about model implementation
        """
        return cls.construct(md5("model_implementation_id".encode("utf-8")).hexdigest(),
                             model_implementation_name,
                             implements_abstract_model,
                             model_backend)


class ModelDriverInfoFactory:
    @classmethod
    def construct(cls,
                  model_driver_id: str,
                  model_driver_name: str,
                  processor_backend_info: ProcessorBackendInfo,
                  model_backend_info: ModelBackendInfo,
                  function: str):
        """ Construct model driver using its id, name and some other information.
        :param model_driver_id: id of the model driver
        :param model_driver_name: name of the model driver
        :param processor_backend_info: processor backend which is used to load the model
        :param model_backend_info: info about model backend
        :param function: can be either "on_read" or "on_write"
        :return: info about the model driver
        """
        return ModelDriverInfo(model_driver_id,
                               model_driver_name,
                               processor_backend_info,
                               model_backend_info,
                               function)

    @classmethod
    def construct_by_name(cls,
                          model_driver_name: str,
                          processor_backend_info: ProcessorBackendInfo,
                          model_backend_info: ModelBackendInfo,
                          function: str):
        """ Construct model driver using its id (not name) only as well as some other information.
        :param model_driver_name: name of the model driver
        :param processor_backend_info: info of the processor backend
        :param model_backend_info: info about the model backend
        :param function: can be either "on_read" or "on_write" depending on what do you want to use the driver for
        :return:
        """
        return ModelDriverInfo(md5(model_driver_name.encode("utf-8")).hexdigest(),
                               model_driver_name,
                               processor_backend_info,
                               model_backend_info,
                               function)


class ModelPerformanceInfoFactory:
    @classmethod
    def construct(cls,
                  model_implementation_info: ModelImplementationInfo,
                  metrics: Dict[str, float]):
        """ Construct ModelPerformanceInfo using info about model implementation and info about its performance.
        :param model_implementation_info: info about model implementation
        :param metrics: info about model performance; dict of "metric: value"
        :return: info about model performance
        """
        return ModelPerformanceInfo(model_implementation_info,
                                    metrics)
