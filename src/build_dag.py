import os

from jinja2 import Template

from src.config import dag_folder, path_to_dag_template
from src.data_model import DagBuildInfo
from src.utilities import log


class DagBuilder:
    @log
    def __init__(self, dag_build_info: DagBuildInfo):
        self.dag_build_info = dag_build_info

    @log
    def build_dag(self) -> str:
        """
        Construct dag to be written to the folder of airflow dags.
        :return: piece of python code describing an airflow dag
        """
        with open(path_to_dag_template) as file:
            content = file.read().format(dag_build_id=self.dag_build_info.dag_build_id)
            dag_template = Template(content)

        template_args = self.dag_build_info.build_parameters

        dag_description = dag_template.render(**template_args)

        return dag_description

    @log
    def write_dag(self) -> None:
        """
        Write down a dag to the folder of airflow dags.
        :return:
        """
        dag_description = self.build_dag()

        with open(os.path.join(dag_folder, self.dag_build_info.dag_build_name + ".py"), "w") as file:
           file.write(dag_description)
