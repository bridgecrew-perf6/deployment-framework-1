import os
from importlib.util import spec_from_file_location, module_from_spec

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from src.config import processor_store
from src.data_model import ProcessorImplementationInfo
from src.utilities import log


class OperatorFactory:
    @log
    def get(self, processor_implementation_info: ProcessorImplementationInfo, dag: DAG):
        """
        Constructs airflow operators using info about processor implementation.
        :param processor_implementation_info: info about processor implementation that you want to build airflow task of
        :param dag: airflow DAG
        :return:
        """
        # по уму, конечно, эти классы подсасываются динамически (как и драйверы, например)
        if processor_implementation_info.backend_info.processor_backend_name == "py37ds":
            module_name = processor_implementation_info.processor_implementation_name
            spec = spec_from_file_location(module_name, os.path.join(processor_store, module_name + ".py"))
            module = module_from_spec(spec)
            spec.loader.exec_module(module)
            python_callable = getattr(module, processor_implementation_info.processor_implementation_name)

            return PythonOperator(task_id=processor_implementation_info.processor_implementation_name,
                                  python_callable=python_callable,
                                  dag=dag)
        else:
            raise ValueError("there is no appropriate operator for such a backend")