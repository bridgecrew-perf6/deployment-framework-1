from airflow import DAG

from src.operators import OperatorFactory
from src.retrievers import DagBuildInfoRetriever

build_info = DagBuildInfoRetriever.get_info_by_id("{dag_build_id}")
build_params = build_info.build_parameters

with DAG(dag_id="{dag_build_id}",
         **build_params) as dag:
    nodes = dict()

    # add tasks
    for abstract_processor_info in build_info.dag_implementation_info.dag_flow_level_info.dag_flow_level.nodes:
        processor_implementation_info = build_info. \
                                        dag_implementation_info. \
                                        abstract_processor_to_implementation_mapping_info. \
                                        mapping[abstract_processor_info]
        node = OperatorFactory().get(processor_implementation_info, dag)

        nodes[abstract_processor_info] = node

    # add dependencies
    for abstract_processor, depends_on_abstract_processor in build_info.dag_implementation_info.dag_flow_level_info.dag_flow_level.edges:
        nodes[depends_on_abstract_processor] >> nodes[abstract_processor]
