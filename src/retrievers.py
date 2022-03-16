import datetime as dt
import json
from typing import Any, List

import pandas as pd

from neo4j import GraphDatabase
from neo4j.types.graph import Graph
import networkx as nx

from src.data_model import (AppInfo, DraftInfo, SourceInfo, DatasetImplementationInfo, DriverInfo,
                            AbstractProcessorInfo,
                            ProcessorImplementationInfo, ProcessorUponProcessorDependencyInfo,
                            ProcessorUponPrimarySourceDependencyInfo, ProcessorProducesSinkInfo, ProcessorBackendInfo,
                            DagFlowLevelInfo, DagImplementationInfo, AbstractProcessorToImplementationMappingInfo,
                            DagBuildInfo, AbstractModelInfo, ModelUponInletDraftDependencyInfo,
                            ModelUponInletDraftDependencyInfoFactory, ModelProducesOutletDraftInfo,
                            ModelProducesOutletDraftInfoFactory, ModelBackendInfo, ModelBackendInfoFactory,
                            ModelImplementationInfo, ModelImplementationInfoFactory, ModelDriverInfo,
                            ModelDriverInfoFactory, ModelBackendsAvailableInfo, ModelPerformanceInfo,
                            FilledDraftsFactory, FilledDrafts)
from src.config import metadata_engine, graph_database_auth, graph_database_uri
from src.utilities import logger, log


@log
def neo4j_graph_flow_level_to_networkx(store_native_graph: Graph) -> nx.DiGraph:
    """
    :param store_native_graph: graph native to the storage system (neo4j)
    :return: networkx graph
    """
    graph = nx.DiGraph()

    for edge in store_native_graph.relationships:
        start_abstract_processor_id = edge.start_node["abstract_processor_id"]
        end_abstract_processor_id = edge.end_node["abstract_processor_id"]

        processor_upon_processor_dependency_id = edge["processor_upon_processor_dependency_id"]
        start_node = AbstractProcessorInfoRetriever.get_info(start_abstract_processor_id)
        end_node = AbstractProcessorInfoRetriever.get_info(end_abstract_processor_id)

        dependency_info = ProcessorUponProcessorDependencyInfo(
            processor_upon_processor_dependency_id,
            start_node,
            end_node
        )
        graph.add_node(start_node)
        graph.add_node(end_node)
        graph.add_edge(start_node, end_node, dependency=dependency_info)

    random_edge = list(store_native_graph.relationships)[0]
    graph_name = random_edge["dag_flow_level_name"]
    graph.graph["dag_flow_level_name"] = graph_name

    return graph


class AppInfoRetriever:
    @classmethod
    @log
    def get_info(cls, app_id: str) -> AppInfo:
        """ Get info about app by its id.
        :param app_id: id of an app
        :return:
        """
        query = f"""
        select app_id, app_name
        from app_info
        where app_id = '{app_id}'
        """
        data = pd.read_sql_query(query, metadata_engine)
        if data.shape[0] != 0:
            top_row = data.iloc[0, :]

            app_info = AppInfo(
                top_row.at["app_id"],
                top_row.at["app_name"]
            )
        else:
            app_info = None

        return app_info

    @classmethod
    @log
    def get_info_by_name(cls, app_name: str) -> AppInfo:
        """ Get info about app by its name.
        :param app_name: name of an app
        :return:
        """
        query = f"""
        select app_id, app_name
        from app_info
        where app_name = '{app_name}'
        """
        data = pd.read_sql_query(query, metadata_engine)
        top_row = data.iloc[0, :]

        app_info = AppInfo(
            top_row.at["app_id"],
            top_row.at["app_name"]
        )

        return app_info


class DraftInfoRetriever:
    @classmethod
    @log
    def get_info_by_id(cls, draft_id: str) -> DraftInfo:
        """ Get info about draft.
        :param draft_id: id of a draft
        :return:
        """
        query = f"""
        select draft_id, draft_name
        from draft_info
        where draft_id = '{draft_id}'
        """
        data = pd.read_sql_query(query, metadata_engine)
        top_row = data.iloc[0, :]

        draft_info = DraftInfo(
            top_row.at["draft_id"],
            top_row.at["draft_name"]
        )

        return draft_info

    @classmethod
    @log
    def get_info_by_name(cls, draft_name: str) -> DraftInfo:
        """ Get info about draft by its name.
        :param draft_name: name of a draft
        :return:
        """
        query = f"""
        select draft_id, draft_name
        from draft_info
        where draft_name = '{draft_name}'
        """
        data = pd.read_sql_query(query, metadata_engine)
        top_row = data.iloc[0, :]

        draft_info = DraftInfo(
            top_row.at["draft_id"],
            top_row.at["draft_name"]
        )

        return draft_info


class SourceInfoRetriever:
    @classmethod
    @log
    def get_info(cls, source_id: str) -> SourceInfo:
        """ Get info about source by its id.
        :param source_id: id of a source
        :return:
        """
        query = f"""
        select source_id, source_name, connection_info
        from source_info
        where source_id = '{source_id}'
        """
        data = pd.read_sql_query(query, metadata_engine)
        top_row = data.iloc[0, :]

        source_info = SourceInfo(
            top_row.at["source_id"],
            top_row.at["source_name"],
            top_row.at["connection_info"]
        )

        return source_info

    @classmethod
    @log
    def get_info_by_name(cls, source_name: str) -> SourceInfo:
        """ Get info about source by its name.
        :param source_name: name of a source
        :return:
        """
        query = f"""
        select source_id, source_name, connection_info
        from source_info
        where source_name = '{source_name}'
        """
        data = pd.read_sql_query(query, metadata_engine)
        top_row = data.iloc[0, :]

        source_info = SourceInfo(
            top_row.at["source_id"],
            top_row.at["source_name"],
            top_row.at["connection_info"]
        )

        return source_info


class DatasetImplementationInfoRetriever:
    @classmethod
    @log
    def get_info(cls, dataset_implementation_id: str) -> DatasetImplementationInfo:
        """ Get info about dataset implementation using its id.
        :param dataset_implementation_id: id of a dataset implementation
        :return:
        """
        query = f"""
        select dataset_implementation_id,
               dataset_implementation_name,
               app_id,
               draft_id,
               source_id
        from dataset_implementation_info
        where dataset_implementation_id = '{dataset_implementation_id}' 
        """
        data = pd.read_sql_query(query, metadata_engine)
        top_row = data.iloc[0, :]

        dataset_implementation_info = DatasetImplementationInfo(
            top_row.at["dataset_implementation_id"],
            top_row.at["dataset_implementation_name"],
            AppInfoRetriever.get_info(top_row.at["app_id"]),
            DraftInfoRetriever.get_info_by_id(top_row.at["draft_id"]),
            SourceInfoRetriever.get_info(top_row.at["source_id"])
        )

        return dataset_implementation_info

    @classmethod
    @log
    def get_info_by_name(cls, dataset_implementation_name: str) -> DatasetImplementationInfo:
        """ Get info about dataset implementation using its name.
        :param dataset_implementation_name: name of a dataset implementation
        :return:
        """
        query = f"""
        select dataset_implementation_id,
               dataset_implementation_name,
               app_id,
               draft_id,
               source_id
        from dataset_implementation_info
        where dataset_implementation_name = '{dataset_implementation_name}'
        """
        data = pd.read_sql_query(query, metadata_engine)
        top_row = data.iloc[0, :]

        dataset_implementation_info = DatasetImplementationInfo(
            top_row.at["dataset_implementation_id"],
            top_row.at["dataset_implementation_name"],
            AppInfoRetriever.get_info(top_row.at["app_id"]),
            DraftInfoRetriever.get_info_by_id(top_row.at["draft_id"]),
            SourceInfoRetriever.get_info(top_row.at["source_id"])
        )

        return dataset_implementation_info


class DriverInfoRetriever:
    @classmethod
    @log
    def get_info(cls, driver_id: str) -> DriverInfo:
        """ Get info about driver by its id.
        :param driver_id: id of a driver
        :return:
        """
        query = f"""
        select driver_id,
               driver_name,
               driver_function,
               location_info,
               source_id,
               processor_backend_id
        from driver_info
        where driver_id = '{driver_id}'
        """
        data = pd.read_sql_query(query, metadata_engine)
        top_row = data.iloc[0, :]

        driver_info = DriverInfo(
            top_row.at["driver_id"],
            top_row.at["driver_name"],
            top_row.at["driver_function"],
            top_row.at["location_info"],
            SourceInfoRetriever.get_info(top_row.at["source_id"]),
            ProcessorBackendInfoRetriever.get_info_by_id(top_row.at["processor_backend_id"])
        )

        return driver_info

    @classmethod
    @log
    def get_info_by_name(cls, driver_name: str) -> DriverInfo:
        """ Get info about driver using its name.
        :param driver_name: name of a driver
        :return:
        """
        query = f"""
        select driver_id,
               driver_name,
               driver_function,
               location_info,
               source_id,
               processor_backend_id
        from driver_info
        where driver_name = '{driver_name}'
        """
        data = pd.read_sql_query(query, metadata_engine)
        top_row = data.iloc[0, :]

        driver_info = DriverInfo(
            top_row.at["driver_id"],
            top_row.at["driver_name"],
            top_row.at["driver_function"],
            top_row.at["location_info"],
            SourceInfoRetriever.get_info(top_row.at["source_id"]),
            ProcessorBackendInfoRetriever.get_info_by_id(top_row.at["processor_backend_id"])
        )

        return driver_info


class AbstractProcessorInfoRetriever:
    @classmethod
    @log
    def get_info(cls, abstract_processor_id: str) -> AbstractProcessorInfo:
        """ Get info about an abstract processor using its id.
        :param abstract_processor_id: id of the abstract processor
        :return:
        """
        query = f"""
        select abstract_processor_id,
               abstract_processor_name
        from abstract_processor_info
        where abstract_processor_id = '{abstract_processor_id}'
        """
        data = pd.read_sql_query(query, metadata_engine)
        top_row = data.iloc[0, :]

        abstract_processor_info = AbstractProcessorInfo(
            top_row.at["abstract_processor_id"],
            top_row.at["abstract_processor_name"]
        )

        return abstract_processor_info

    @classmethod
    @log
    def get_info_by_name(cls, abstract_processor_name: str) -> AbstractProcessorInfo:
        """ Get info about an abstract processor using its name.
        :param abstract_processor_name: name of the abstract processor
        :return:
        """
        query = f"""
        select abstract_processor_id,
               abstract_processor_name
        from abstract_processor_info
        where abstract_processor_name = '{abstract_processor_name}' 
        """
        data = pd.read_sql_query(query, metadata_engine)
        top_row = data.iloc[0, :]

        abstract_processor_info = AbstractProcessorInfo(
            top_row.at["abstract_processor_id"],
            top_row.at["abstract_processor_name"]
        )

        return abstract_processor_info


class ProcessorImplementationInfoRetriever:
    @classmethod
    @log
    def get_info(cls, processor_implementation_id: str) -> ProcessorImplementationInfo:
        """ Get info about processor implementation using its id.
        :param processor_implementation_id: id of a processor implementation
        :return:
        """
        query = f"""
        select
            processor_implementation_id,
            processor_implementation_name,
            implements_abstract_processor_id,
            backend_id
        from processor_implementation_info
        where processor_implementation_id = '{processor_implementation_id}'
        """
        data = pd.read_sql_query(query, metadata_engine)
        top_row = data.iloc[0, :]

        processor_implementation_info = ProcessorImplementationInfo(
            top_row.at["processor_implementation_id"],
            top_row.at["processor_implementation_name"],
            AbstractProcessorInfoRetriever.get_info(top_row.at["implements_abstract_processor_id"]),
            ProcessorBackendInfoRetriever.get_info_by_id(top_row.at["backend_id"])
        )

        return processor_implementation_info

    @classmethod
    @log
    def get_info_by_name(cls, processor_implementation_name: str) -> ProcessorImplementationInfo:
        """ Get info about processor implementation using its name.
        :param processor_implementation_name: name of a processor implementation
        :return:
        """
        query = f"""
        select
            processor_implementation_id,
            processor_implementation_name,
            implements_abstract_processor_id,
            backend_id
        from processor_implementation_info
        where processor_implementation_name = '{processor_implementation_name}'
        """
        data = pd.read_sql_query(query, metadata_engine)
        top_row = data.iloc[0, :]

        processor_implementation_info = ProcessorImplementationInfo(
            top_row.at["processor_implementation_id"],
            top_row.at["processor_implementation_name"],
            AbstractProcessorInfoRetriever.get_info(top_row.at["implements_abstract_processor_id"]),
            ProcessorBackendInfoRetriever.get_info_by_id(top_row.at["backend_id"])
        )

        return processor_implementation_info


class ProcessorUponProcessorDependencyRetriever:
    @classmethod
    @log
    def get_info(cls, processor_upon_processor_dependency_id: str) -> ProcessorUponProcessorDependencyInfo:
        """ Get info about processor upon processor dependency using its id.
        :param processor_upon_processor_dependency_id: id of dependency
        :return:
        """
        query = f"""
        select processor_upon_processor_dependency_id,
               processor_id,
               depends_on_processor_id
        from processor_upon_processor_dependency_info
        where processor_upon_processor_dependency_id = '{processor_upon_processor_dependency_id}'
        """
        data = pd.read_sql_query(query, metadata_engine)
        top_row = data.iloc[0, :]

        processor_upon_processor_dependency_info = ProcessorUponProcessorDependencyInfo(
            top_row.at["processor_upon_processor_dependency_id"],
            AbstractProcessorInfoRetriever.get_info(top_row.at["processor_id"]),
            AbstractProcessorInfoRetriever.get_info(top_row.at["depends_on_processor_id"])
        )

        return processor_upon_processor_dependency_info

    @classmethod
    @log
    def get_info_by_name(cls, processor_upon_processor_dependency_name: str) -> ProcessorUponProcessorDependencyInfo:
        """ Get info about processor upon processor dependency using its name.
        :param processor_upon_processor_dependency_name: name of dependency
        :return:
        """
        raise NotImplementedError("Processor upon processor dependencies have no name.")

    @classmethod
    @log
    def get_all_dependencies(cls, abstract_processor_id: str) -> List[ProcessorUponProcessorDependencyInfo]:
        """ Get all processor upon processor dependencies of abstract processor using its id.
        :param abstract_processor_id: id of abstract processor
        :return:
        """
        query = f"""
        select processor_upon_processor_dependency_id,
               processor_id,
               depends_on_processor_id
        from processor_upon_processor_dependency_info
        where processor_id = '{abstract_processor_id}'
        """
        data = pd.read_sql_query(query, metadata_engine)

        info_entries = [
            ProcessorUponProcessorDependencyInfo(
                row["processor_upon_processor_dependency_id"],
                AbstractProcessorInfoRetriever.get_info(row["processor_id"]),
                AbstractProcessorInfoRetriever.get_info(row["depends_on_processor_id"])
            )
            for (index, row) in data.iterrows()
        ]

        return info_entries


class ProcessorUponPrimarySourceDependencyRetriever:
    @classmethod
    @log
    def get_info(cls, processor_upon_primary_source_dependency_id: str) -> ProcessorUponPrimarySourceDependencyInfo:
        """ Get processor upon primary source dependency info by its id.
        :param processor_upon_primary_source_dependency_id: id of the dependency
        :return:
        """
        query = f"""
        select 
            processor_upon_primary_source_dependency_id,
            processor_id,
            depends_on_draft_id
        from processor_upon_primary_source_dependency_info
        where processor_upon_primary_source_dependency_id = '{processor_upon_primary_source_dependency_id}'
        """
        data = pd.read_sql_query(query, metadata_engine)
        top_row = data.iloc[0, :]

        processor_upon_primary_source_dependency_info = ProcessorUponPrimarySourceDependencyInfo(
            top_row.at["processor_upon_primary_source_dependency_id"],
            AbstractProcessorInfoRetriever.get_info(top_row.at["processor_id"]),
            DraftInfoRetriever.get_info_by_id(top_row.at["depends_on_draft_id"])
        )

        return processor_upon_primary_source_dependency_info

    @classmethod
    @log
    def get_info_by_name(cls, processor_upon_primary_source_dependency_name: str) -> ProcessorUponPrimarySourceDependencyInfo:
        """ Get processor upon primary source dependency info by its name.
        :param processor_upon_primary_source_dependency_name: name of the dependency
        :return:
        """
        raise NotImplementedError("Processor upon primary source dependencies have no name.")

    @classmethod
    @log
    def get_all_dependencies(cls, abstract_processor_id: str) -> List[ProcessorUponPrimarySourceDependencyInfo]:
        """ Get all processor upon primary source dependencies using abstract processor id.
        :param abstract_processor_id: id of the abstract processor
        :return:
        """
        query = f"""
        select 
            processor_upon_primary_source_dependency_id,
            processor_id,
            depends_on_draft_id 
        from processor_upon_primary_source_dependency_info
        where processor_id = '{abstract_processor_id}'
        """
        data = pd.read_sql_query(query, metadata_engine)

        info_entries = [
            ProcessorUponPrimarySourceDependencyInfo(
                row["processor_upon_primary_source_dependency_id"],
                AbstractProcessorInfoRetriever.get_info(row["processor_id"]),
                DraftInfoRetriever.get_info_by_id(row["depends_on_draft_id"])
            )
            for (index, row) in data.iterrows()
        ]

        return info_entries


class ProcessorProducesSinkRetriever:
    @classmethod
    @log
    def get_info(cls, processor_produces_sink_id: str) -> ProcessorProducesSinkInfo:
        """ Get info about what sink does a processor produce using its id.
        :param processor_produces_sink_id: id of the dependency
        :return:
        """
        query = f"""
        select
            processor_produces_sink_id,
            processor_id,
            produces_draft_id
        from processor_produces_sink_info
        where processor_produces_sink_id = '{processor_produces_sink_id}'
        """
        data = pd.read_sql_query(query, metadata_engine)
        top_row = data.iloc[0, :]

        processor_produces_sink_info = ProcessorProducesSinkInfo(
            top_row.at["processor_produces_sink_id"],
            AbstractProcessorInfoRetriever.get_info(top_row.at["processor_id"]),
            DraftInfoRetriever.get_info_by_id(top_row.at["produces_draft_id"])
        )

        return processor_produces_sink_info

    @classmethod
    @log
    def get_info_by_name(cls, processor_produces_sink_name: str) -> ProcessorProducesSinkInfo:
        """ Get info about what sink does a producer produce using its id.
        :param processor_produces_sink_name: name of the dependency
        :return:
        """
        raise NotImplementedError("Processor produces sink have no name.")

    @classmethod
    def get_all_dependencies(cls, abstract_processor_id: str) -> List[ProcessorProducesSinkInfo]:
        """ Get info about all sinks an abstract processor produce using its id.
        :param abstract_processor_id:
        :return:
        """
        query = f"""
        select
            processor_produces_sink_id,
            processor_id,
            produces_draft_id
        from processor_produces_sink_info
        where processor_id = '{abstract_processor_id}'
        """
        data = pd.read_sql_query(query, metadata_engine)
        info_entries = [
            ProcessorProducesSinkInfo(
                row["processor_produces_sink_id"],
                AbstractProcessorInfoRetriever.get_info(row["processor_id"]),
                DraftInfoRetriever.get_info_by_id(row["produces_draft_id"])
            )
            for (index, row) in data.iterrows()
        ]

        return info_entries


class FilledDraftsInfoRetriever:
    @classmethod
    @log
    def get_all_implementations(cls) -> FilledDrafts:
        query = f"""
        select dataset_implementation_id
        from filled_drafts_info
        """
        data = pd.read_sql_query(query, metadata_engine)
        dataset_implementations_info = [
            DatasetImplementationInfoRetriever.get_info(dataset_implementation_id)
            for dataset_implementation_id in data["dataset_implementation_id"].tolist()
        ]
        filled_drafts_info = FilledDraftsFactory.construct(dataset_implementations_info)

        return filled_drafts_info

    @classmethod
    @log
    def get_all_implementations_by_app(cls, app_info: AppInfo) -> FilledDrafts:
        query = f"""
        select dii.dataset_implementation_id
        from filled_drafts_info fdi
        inner join dataset_implementation_info dii
            on fdi.dataset_implementation_id = dii.dataset_implementation_id 
        where dii.app_id = '{app_info.app_id}' 
        """
        data = pd.read_sql_query(query, metadata_engine)
        dataset_implementations_info = [
            DatasetImplementationInfoRetriever.get_info(dataset_implementation_id)
            for dataset_implementation_id in data["dataset_implementation_id"]
        ]
        filled_drafts_info = FilledDraftsFactory.construct(dataset_implementations_info)

        return filled_drafts_info


class ProcessorBackendInfoRetriever:
    @classmethod
    @log
    def get_info_by_id(cls, processor_backend_id: str) -> ProcessorBackendInfo:
        """ Get info about processor backend using its id.
        :param processor_backend_id: id of the backend
        :return: processor backend info
        """
        query = f"""
        select
            processor_backend_id,
            processor_backend_name
        from processor_backend_info
        where processor_backend_id = '{processor_backend_id}'
        """
        data = pd.read_sql_query(query, metadata_engine)
        top_row = data.iloc[0, :]

        processor_backend_info = ProcessorBackendInfo(
            top_row.at["processor_backend_id"],
            top_row.at["processor_backend_name"]
        )

        return processor_backend_info

    @classmethod
    @log
    def get_info_by_name(cls, processor_backend_name: str) -> ProcessorBackendInfo:
        """ Get info about processor backend using its name.
        :param processor_backend_name: name of the backend
        :return: processor backend info
        """
        query = f"""
        select
            processor_backend_id,
            processor_backend_name
        from processor_backend_info
        where processor_backend_name = '{processor_backend_name}'
        """
        data = pd.read_sql_query(query, metadata_engine)
        top_row = data.iloc[0, :]

        processor_backend_info = ProcessorBackendInfo(
            top_row.at["processor_backend_id"],
            top_row.at["processor_backend_name"]
        )

        return processor_backend_info


class DagFlowLevelRetriever:
    def __init__(self):
        """
        Этот класс нужен для того, чтобы доставать граф из базы данных по id.
        """
        logger.debug("Creating DagFlowLevelRetriever()")
        self.driver = GraphDatabase.driver(graph_database_uri, auth=graph_database_auth)

    @log
    def get_graph_by_id(self, dag_flow_level_id: str) -> nx.DiGraph:
        """ Get flow level graph (not info) by its id.
        :param dag_flow_level_id: id of the flow level graph
        :return: networkx graph
        """
        with self.driver.session() as session:
            result = session.run(f"""
            match (abstractProcessor: AbstractProcessor {{ dag_flow_level_id: "{dag_flow_level_id}" }} ) - 
                  [processorUponProcessorDependency: Dependency {{ dag_flow_level_id: "{dag_flow_level_id}" }} ] ->
                  (dependsOnAbstractProcessor: AbstractProcessor {{ dag_flow_level_id: "{dag_flow_level_id}" }} )
            return *
            """)

            store_native_graph = result.graph()

            graph = neo4j_graph_flow_level_to_networkx(store_native_graph)

            return graph

    @log
    def get_graph_by_name(self, dag_flow_level_name: str) -> nx.DiGraph:
        """ Get flow level graph by its name.
        :param dag_flow_level_name: name of the flow level graph
        :return: networkx graph
        """
        with self.driver.session() as session:
            result = session.run(f"""
            match (abstractProcessor: AbstractProcessor {{ dag_flow_level_name: "{dag_flow_level_name}" }} ) - 
                  [processorUponProcessorDependency: Dependency {{ dag_flow_level_name: "{dag_flow_level_name}" }} ] ->
                  (dependsOnAbstractProcessor: AbstractProcessor {{ dag_flow_level_name: "{dag_flow_level_name}" }})
            return *
            """)

            store_native_graph = result.graph()

            graph = neo4j_graph_flow_level_to_networkx(store_native_graph)

            return graph


class DagFlowLevelInfoRetriever:
    @classmethod
    @log
    def get_info_by_id(cls, dag_flow_level_id: str) -> DagFlowLevelInfo:
        """ Get dag flow level info by its id
        :param dag_flow_level_id: id of the flow level dag
        :return: flow level dag info
        """
        query = f"""
        select
            dag_flow_level_id,
            dag_flow_level_name
        from dag_flow_level_info
        where dag_flow_level_id = '{dag_flow_level_id}'
        """
        data = pd.read_sql_query(query, metadata_engine)
        top_row = data.iloc[0, :]

        graph = DagFlowLevelRetriever().get_graph_by_id(dag_flow_level_id)

        dag_flow_level_info = DagFlowLevelInfo(
            top_row.at["dag_flow_level_id"],
            top_row.at["dag_flow_level_name"],
            graph
        )

        return dag_flow_level_info

    @classmethod
    @log
    def get_info_by_name(cls, dag_flow_level_name: str) -> DagFlowLevelInfo:
        """ Get dag flow level info by its name
        :param dag_flow_level_name: namme of the flow level dag
        :return: flow level dag info
        """
        query = f"""
        select
            dag_flow_level_id,
            dag_flow_level_name
        from dag_flow_level_info
        where dag_flow_level_name = '{dag_flow_level_name}'
        """
        data = pd.read_sql_query(query, metadata_engine)
        top_row = data.iloc[0, :]

        graph = DagFlowLevelRetriever().get_graph_by_name(dag_flow_level_name)

        dag_flow_level_info = DagFlowLevelInfo(
            top_row.at["dag_flow_level_id"],
            top_row.at["dag_flow_level_name"],
            graph
        )

        return dag_flow_level_info


class AbstractProcessorToImplementationMappingInfoRetriever:
    @classmethod
    @log
    def get_info_by_id(cls, abstract_processor_to_implementation_mapping_id: str) -> AbstractProcessorToImplementationMappingInfo:
        """ Get info about processor to implementation mapping by its id.
        :param abstract_processor_to_implementation_mapping_id: id of abstract processor to implementation mapping
        :return: info about abstract processor to implementation mapping
        """
        query = f"""
        select
            abstract_processor_id,
            processor_implementation_id
        from abstract_processor_to_implementation_mapping_info
        where abstract_processor_to_implementation_mapping_id = '{abstract_processor_to_implementation_mapping_id}'  
        """
        data = pd.read_sql_query(query, metadata_engine)
        mapping = {
            AbstractProcessorInfoRetriever.get_info(row["abstract_processor_id"]):
                ProcessorImplementationInfoRetriever.get_info(row["processor_implementation_id"])
            for _, row in data.iterrows()
        }

        abstract_processor_to_implementation_mapping_info = AbstractProcessorToImplementationMappingInfo(
            abstract_processor_to_implementation_mapping_id,
            mapping
        )

        return abstract_processor_to_implementation_mapping_info


class DagImplementationInfoRetriever:
    @classmethod
    @log
    def get_info_by_id(cls, dag_implementation_id: str) -> DagImplementationInfo:
        """ Get info about dag implementation by its id.
        :param dag_implementation_id: id of dag implementation
        :return: info about dag implementation
        """
        query = f"""
        select
            dag_implementation_id,
            dag_implementation_name,
            implements_dag_flow_level_id,
            abstract_processor_to_implementation_mapping_id
        from dag_implementation_info
        where dag_implementation_id = '{dag_implementation_id}'
        """
        data = pd.read_sql_query(query, metadata_engine)
        top_row = data.iloc[0, :]

        dag_flow_level_info = DagFlowLevelInfoRetriever.get_info_by_id(top_row.at["implements_dag_flow_level_id"])

        abstract_processor_to_implementation_mapping_info = AbstractProcessorToImplementationMappingInfoRetriever.get_info_by_id(
            top_row.at["abstract_processor_to_implementation_mapping_id"]
        )

        dag_implementation_info = DagImplementationInfo(
            top_row.at["dag_implementation_id"],
            top_row.at["dag_implementation_name"],
            dag_flow_level_info,
            abstract_processor_to_implementation_mapping_info
        )

        return dag_implementation_info

    @classmethod
    @log
    def get_info_by_name(cls, dag_implementation_name: str) -> DagImplementationInfo:
        """ Get info about dag implementation by its name.
        :param dag_implementation_name: name of dag implementation
        :return: info about dag implementation
        """
        query = f"""
        select
            dag_implementation_id,
            dag_implementation_name,
            implements_dag_flow_level_id,
            abstract_processor_to_implementation_mapping_id
        from dag_implementation_info
        where dag_implementation_name = '{dag_implementation_name}'
        """
        data = pd.read_sql_query(query, metadata_engine)
        top_row = data.iloc[0, :]

        dag_flow_level_info = DagFlowLevelInfoRetriever.get_info_by_id(top_row.at["implements_dag_flow_level_id"])

        abstract_processor_to_implementation_mapping_info = AbstractProcessorToImplementationMappingInfoRetriever.get_info_by_id(
            top_row.at["abstract_processor_to_implementation_mapping_id"]
        )

        dag_implementation_info = DagImplementationInfo(
            top_row.at["dag_implementation_id"],
            top_row.at["dag_implementation_name"],
            dag_flow_level_info,
            abstract_processor_to_implementation_mapping_info
        )

        return dag_implementation_info


class DagBuildInfoRetriever:
    @classmethod
    @log
    def deserialize_build_parameters(cls, build_parameters: dict) -> dict:
        """ Deserialize build parameters used to build graph.
        :param build_parameters: parameters used to build graph
        :return:
        """
        def deserialize_param(param_name: str, param: Any) -> Any:
            """ Deserialize single parameter used to build graph.
            :param param_name: name of the parameter
            :param param: parameter
            :return: deserialized parameter
            """
            if param_name in ("start_date", "end_date"):
                result = dt.datetime.strptime(param, "%d.%m.%Y")
            else:
                result = param

            return result

        return {
            param: deserialize_param(param, build_parameters[param])
            for param in build_parameters
        }

    @classmethod
    @log
    def get_info_by_id(cls, dag_build_id: str) -> DagBuildInfo:
        """ Get info by dag build using its id.
        :param dag_build_id: id of dag build
        :return: info about dag build
        """
        query = f"""
        select
            dag_build_id,
            dag_build_name,
            app_id,
            dag_implementation_id,
            build_parameters
        from dag_build_info
        where dag_build_id = '{dag_build_id}'
        """
        data = pd.read_sql_query(query, metadata_engine)
        top_row = data.iloc[0, :]

        app_info = AppInfoRetriever.get_info(top_row.at["app_id"])

        dag_implementation_info = DagImplementationInfoRetriever.get_info_by_id(top_row.at["dag_implementation_id"])

        build_parameters = json.loads(top_row.at["build_parameters"])

        dag_build_info = DagBuildInfo(
            top_row.at["dag_build_id"],
            top_row.at["dag_build_name"],
            app_info,
            dag_implementation_info,
            cls.deserialize_build_parameters(build_parameters)
        )

        return dag_build_info

    @classmethod
    @log
    def get_info_by_name(cls, dag_build_name: str) -> DagBuildInfo:
        """ Get info by dag build using its name.
        :param dag_build_name: name of dag build
        :return: info about dag build
        """
        query = f"""
        select
            dag_build_id,
            dag_build_name,
            app_id,
            dag_implementation_id,
            build_parameters
        from dag_build_info
        where dag_build_name = '{dag_build_name}'
        """
        data = pd.read_sql_query(query, metadata_engine)
        top_row = data.iloc[0, :]

        app_info = AppInfoRetriever.get_info(top_row.at["app_id"])

        dag_implementation_info = DagImplementationInfoRetriever.get_info_by_id(top_row.at["dag_implementation_id"])

        build_parameters = json.loads(top_row.at["build_parameters"])

        dag_build_info = DagBuildInfo(
            top_row.at["dag_build_id"],
            top_row.at["dag_build_name"],
            app_info,
            dag_implementation_info,
            cls.deserialize_build_parameters(build_parameters)
        )

        return dag_build_info


class AbstractModelInfoRetriever:
    @classmethod
    @log
    def get_info_by_id(cls,
                       abstract_model_id: str) -> AbstractModelInfo:
        """ Get info about abstract model using its id.
        :param abstract_model_id: id of abstract model
        :return: info about abstract model
        """
        query = f"""
        select
            abstract_model_id,
            abstract_model_name 
        from abstract_model_info
        where abstract_model_id = '{abstract_model_id}'
        """
        data = pd.read_sql_query(query, metadata_engine)
        top_row = data.iloc[0, :]

        abstract_model_info = AbstractModelInfo(top_row.at["abstract_model_id"],
                                                top_row.at["abstract_model_name"])

        return abstract_model_info

    @classmethod
    @log
    def get_info_by_name(cls,
                         abstract_model_name: str) -> AbstractModelInfo:
        """ Get info about abstract model using its name.
        :param abstract_model_name: name of abstract model
        :return: info about abstract model
        """
        query = f"""
        select
            abstract_model_id,
            abstract_model_name
        from abstract_model_info
        where abstract_model_name = '{abstract_model_name}'
        """
        data = pd.read_sql_query(query, metadata_engine)
        top_row = data.iloc[0, :]

        abstract_model_info = AbstractModelInfo(top_row.at["abstract_model_id"],
                                                top_row.at["abstract_model_name"])

        return abstract_model_info


class ModelUponInletDraftDependencyInfoRetriever:
    @classmethod
    @log
    def get_info_by_id(cls,
                       model_upon_inlet_draft_dependency_id: str) -> ModelUponInletDraftDependencyInfo:
        """ Get info about model upon inlet draft dependency using its id.
        :param model_upon_inlet_draft_dependency_id: id of model upon inlet draft dependency
        :return: info about model upon inlet draft dependency
        """
        query = f"""
        select
            model_upon_inlet_draft_dependency_id,
            abstract_model_id,
            depends_on_inlet_draft_id
        from model_upon_inlet_draft_dependency_info
        where model_upon_inlet_draft_dependency_id = '{model_upon_inlet_draft_dependency_id}'
        """
        data = pd.read_sql_query(query, metadata_engine)
        top_row = data.iloc[0, :]

        abstract_model_info = AbstractModelInfoRetriever.get_info_by_id(top_row.at["abstract_model_id"])
        inlet_draft_info = DraftInfoRetriever.get_info_by_id(top_row.at["depends_on_inlet_draft_id"])

        model_upon_inlet_draft_dependency_info = ModelUponInletDraftDependencyInfoFactory.construct(model_upon_inlet_draft_dependency_id,
                                                                                                    abstract_model_info,
                                                                                                    inlet_draft_info)

        return model_upon_inlet_draft_dependency_info

    @classmethod
    @log
    def get_info_by_name(cls,
                         model_upon_inlet_draft_dependency_name: str) -> ModelUponInletDraftDependencyInfo:
        """ Get info about model upon inlet draft dependency using its name.
        :param model_upon_inlet_draft_dependency_name: name of model upon inlet draft dependency
        :return: info about model upon inlet draft dependency
        """
        raise NotImplementedError("Dependencies have no name.")

    @classmethod
    @log
    def get_all_dependencies(cls,
                             abstract_model_id: str) -> List[ModelUponInletDraftDependencyInfo]:
        """ Get all model upon inlet draft dependencies using id of abstract model.
        :param abstract_model_id: id of abstract model
        :return: list of model upon inlet draft dependencies
        """
        query = f"""
        select
            model_upon_inlet_draft_dependency_id,
            abstract_model_id,
            depends_on_inlet_draft_id 
        from model_upon_inlet_draft_dependency_info
        where abstract_model_id = '{abstract_model_id}'
        """
        data = pd.read_sql_query(query, metadata_engine)

        abstract_model_info = AbstractModelInfoRetriever.get_info_by_id(abstract_model_id)

        info_entries = [
            ModelUponInletDraftDependencyInfoFactory.construct(row["model_upon_inlet_draft_dependency_id"],
                                                               abstract_model_info,
                                                               DraftInfoRetriever.get_info_by_id(row["depends_on_inlet_draft_id"]))
            for index, row in data.iterrows()
        ]

        return info_entries


class ModelProducesOutletDraftInfoRetriever:
    @classmethod
    @log
    def get_info_by_id(cls,
                       model_produces_outlet_draft_id: str) -> ModelProducesOutletDraftInfo:
        """ Get info about model produces outlet draft dependency using its id.
        :param model_produces_outlet_draft_id: id of model produces outlet draft
        :return: info about model produces outlet draft
        """
        query = f"""
        select
            model_produces_outlet_draft_id,
            abstract_model_id,
            produces_outlet_draft_id
        from model_produces_outlet_draft_info
        where model_produces_outlet_draft_id = '{model_produces_outlet_draft_id}'
        """
        data = pd.read_sql_query(query, metadata_engine)
        top_row = data.iloc[0, :]

        abstract_model_info = AbstractModelInfoRetriever.get_info_by_id(top_row.at["abstract_model_id"])
        outlet_draft_info = DraftInfoRetriever.get_info_by_id(top_row.at["produces_outlet_draft_id"])

        model_produces_outlet_draft_info = ModelProducesOutletDraftInfoFactory.construct(model_produces_outlet_draft_id,
                                                                                         abstract_model_info,
                                                                                         outlet_draft_info)

        return model_produces_outlet_draft_info

    @classmethod
    @log
    def get_info_by_name(cls,
                         model_produces_outlet_draft_name: str) -> ModelProducesOutletDraftInfo:
        """ Get info about model produces outlet draft dependency using its name.
        :param model_produces_outlet_draft_name: name of model produces outlet draft
        :return: info about model produces outlet draft
        """
        raise NotImplementedError("Dependencies have no name.")

    @classmethod
    @log
    def get_all_dependencies(cls,
                             abstract_model_id: str) -> List[ModelProducesOutletDraftInfo]:
        """ Get info about all model produces outlet draft dependencies.
        :param abstract_model_id: id of abstract model
        :return: list of all model produces outlet draft dependencies
        """
        query = f"""
        select
            model_produces_outlet_draft_id,
            abstract_model_id,
            produces_outlet_draft_id
        from model_produces_outlet_draft_info
        where abstract_model_id = '{abstract_model_id}'
        """
        data = pd.read_sql_query(query, metadata_engine)

        abstract_model_info = AbstractModelInfoRetriever.get_info_by_id(abstract_model_id)

        info_entries = [
            ModelProducesOutletDraftInfoFactory.construct(row["model_produces_outlet_draft_id"],
                                                          abstract_model_info,
                                                          DraftInfoRetriever.get_info_by_id(row["produces_outlet_draft_id"]))
            for index, row in data.iterrows()
        ]

        return info_entries


class ModelBackendInfoRetriever:
    @classmethod
    @log
    def get_info_by_id(cls,
                       model_backend_id: str) -> ModelBackendInfo:
        """ Get info about model backend using its id.
        :param model_backend_id: id of model backend
        :return: info about model backend
        """
        query = f"""
        select
            model_backend_id,
            model_backend_name
        from model_backend_info
        where model_backend_id = '{model_backend_id}'
        """
        data = pd.read_sql_query(query, metadata_engine)
        top_row = data.iloc[0, :]

        model_backend_info = ModelBackendInfoFactory.construct(top_row.at["model_backend_id"],
                                                               top_row.at["model_backend_name"])

        return model_backend_info

    @classmethod
    @log
    def get_info_by_name(cls,
                         model_backend_name: str) -> ModelBackendInfo:
        """ Get info about model backend using its name.
        :param model_backend_name: name of model backend
        :return: info about model backend
        """
        query = f"""
        select
            model_backend_id,
            model_backend_name
        from model_backend_info
        where model_backend_name = '{model_backend_name}'
        """
        data = pd.read_sql_query(query, metadata_engine)
        top_row = data.iloc[0, :]

        model_backend_info = ModelBackendInfoFactory.construct(top_row.at["model_backend_id"],
                                                              top_row.at["model_backend_name"])

        return model_backend_info


class ModelBackendsAvailableInfoRetriever:
    @classmethod
    @log
    def get_info_by_model_backend_id(cls,
                                     model_backend_id: str) -> ModelBackendsAvailableInfo:
        """ Get info about available model backends by model backend id.
        :param model_backend_id: id of model backend
        :return: info about available model backends
        """
        query = f"""
        select
            processor_backend_id,
            model_backend_id
        from model_backends_available
        where model_backend_id = '{model_backend_id}'
        """
        data = pd.read_sql_query(query, metadata_engine)

        for index, row in data.iterrows():
            yield ModelBackendsAvailableInfo(ProcessorBackendInfoRetriever.get_info_by_id(row["processor_backend_id"]),
                                             ModelBackendInfoRetriever.get_info_by_id(row["model_backend_id"]))

    @classmethod
    @log
    def get_info_by_processor_backend_id(cls,
                                         processor_backend_id: str) -> ModelBackendsAvailableInfo:
        """ Get info about available model backends by model backend name.
        :param processor_backend_id: name of model backend
        :return: info about available model backends
        """
        query = f"""
        select
            processor_backend_id,
            model_backend_id
        from_model_backends_available
        where processor_backend_id = '{processor_backend_id}'
        """
        data = pd.read_sql_query(query, metadata_engine)

        for index, row in data.iterrows():
            yield ModelBackendsAvailableInfo(ProcessorBackendInfoRetriever.get_info_by_id(row["processor_backend_id"]),
                                             ModelBackendInfoRetriever.get_info_by_id(row["model_backend_id"]))


class ModelImplementationInfoRetriever:
    @classmethod
    @log
    def get_info_by_id(cls,
                       model_implementation_id: str) -> ModelImplementationInfo:
        """ Get info about model implementation using its id.
        :param model_implementation_id: id of model implementation
        :return: info about model implementation
        """
        query = f"""
        select
            model_implementation_id,
            model_implementation_name,
            implements_abstract_model_id,
            model_backend_id
        from model_implementation_info
        where model_implementation_id = '{model_implementation_id}'
        """
        data = pd.read_sql_query(query, metadata_engine)
        top_row = data.iloc[0, :]

        implements_abstract_model_info = AbstractModelInfoRetriever.get_info_by_id(top_row.at["implements_abstract_model_id"])
        model_backend_info = ModelBackendInfoRetriever.get_info_by_id(top_row.at["model_backend_id"])

        model_implementation_info = ModelImplementationInfoFactory.construct(top_row.at["model_implementation_id"],
                                                                             top_row.at["model_implementation_name"],
                                                                             implements_abstract_model_info,
                                                                             model_backend_info)

        return model_implementation_info

    @classmethod
    @log
    def get_info_by_name(cls,
                         model_implementation_name: str) -> ModelImplementationInfo:
        """ Get info about model implementation using its name.
        :param model_implementation_name: name of model implementation
        :return: info about model implementation
        """
        query = f"""
        select
            model_implementation_id,
            model_implementation_name,
            implements_abstract_model_id,
            model_backend_id
        from model_implementation_info
        where model_implementation_name = '{model_implementation_name}'
        """
        data = pd.read_sql_query(query, metadata_engine)
        top_row = data.iloc[0, :]

        implements_abstract_model_info = AbstractModelInfoRetriever.get_info_by_id(top_row.at["implements_abstract_model_id"])
        model_backend_info = ModelBackendInfoRetriever.get_info_by_id(top_row.at["model_backend_id"])

        model_implementation_info = ModelImplementationInfoFactory.construct(top_row.at["model_implementation_id"],
                                                                             top_row.at["model_implementation_name"],
                                                                             implements_abstract_model_info,
                                                                             model_backend_info)

        return model_implementation_info


class ModelDriverInfoRetriever:
    @classmethod
    @log
    def get_info_by_id(cls,
                       model_driver_id: str) -> ModelDriverInfo:
        """ Get info about model driver by its id.
        :param model_driver_id: id of model driver
        :return: info about model driver
        """
        query = f"""
        select
            model_driver_id,
            model_driver_name,
            processor_backend_id,
            model_backend_id,
            function
        from model_driver_info
        where model_driver_id = '{model_driver_id}'
        """
        data = pd.read_sql_query(query, metadata_engine)
        top_row = data.iloc[0, :]

        processor_backend_info = ProcessorBackendInfoRetriever.get_info_by_id(top_row.at["processor_backend_id"])
        model_backend_info = ModelBackendInfoRetriever.get_info_by_id(top_row.at["model_backend_id"])

        model_driver_info = ModelDriverInfoFactory.construct(top_row.at["model_driver_id"],
                                                             top_row.at["model_driver_name"],
                                                             processor_backend_info,
                                                             model_backend_info,
                                                             top_row.at["function"])

        return model_driver_info

    @classmethod
    @log
    def get_info_by_name(cls,
                         model_driver_name: str) -> ModelDriverInfo:
        """ Get info about model driver by its name.
        :param model_driver_name: name of model driver
        :return: info about model driver
        """
        query = f"""
        select
            model_driver_id,
            model_driver_name,
            processor_backend_id,
            model_backend_id,
            function
        from model_driver_info
        where model_driver_name = '{model_driver_name}'
        """
        data = pd.read_sql_query(query, metadata_engine)
        top_row = data.iloc[0, :]

        processor_backend_info = ProcessorBackendInfoRetriever.get_info_by_id(top_row.at["processor_backend_id"])
        model_backend_info = ModelBackendInfoRetriever.get_info_by_id(top_row.at["model_backend_id"])

        model_driver_info = ModelDriverInfoFactory.construct(top_row.at["model_driver_id"],
                                                             top_row.at["model_driver_name"],
                                                             processor_backend_info,
                                                             model_backend_info,
                                                             top_row.at["function"])

        return model_driver_info


class ModelPerformanceInfoRetriever:
    @classmethod
    @log
    def get_info_by_id(cls,
                       model_implementation_id: str) -> ModelPerformanceInfo:
        """ Get info about model performance using model implementation id.
        :param model_implementation_id: id of model implementation
        :return: info about model performance
        """
        query = f"""
        select
            metric,
            value
        from model_performance_info
        where model_implementation_id = '{model_implementation_id}'
        """
        data = pd.read_sql_query(query, metadata_engine)

        metrics = {
            row["metric"]: row["value"]
            for index, row in data.iterrows()
        }

        return ModelPerformanceInfo(ModelImplementationInfoRetriever.get_info_by_id(model_implementation_id),
                                    metrics)

