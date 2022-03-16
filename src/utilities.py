from hashlib import md5
from json import dumps

from sqlalchemy.engine import Engine

from src.config import postgres_pdm_localhost_connection_string, logger
from src.data_model import AppInfo, AppInfoFactory, DraftInfo, DraftInfoFactory, SourceInfo, SourceInfoFactory, \
    ProcessorBackendInfo, ProcessorBackendInfoFactory, DatasetImplementationInfo, DatasetImplementationInfoFactory, \
    AbstractProcessorInfo, AbstractProcessorInfoFactory, ProcessorImplementationInfo, \
    ProcessorImplementationInfoFactory, ProcessorUponProcessorDependencyInfo, \
    ProcessorUponProcessorDependencyInfoFactory, ProcessorUponPrimarySourceDependencyInfo, \
    ProcessorUponPrimarySourceDependencyInfoFactory, ProcessorProducesSinkInfo, ProcessorProducesSinkInfoFactory, \
    FilledDrafts, FilledDraftsFactory


def log(func):
    """ Decorator that logs every call of the function.
    :param func: function whose call to log
    :return:
    """
    def wrapper(*args, **kwargs):
        logger.debug(func.__name__ + " function called")

        return func(*args, **kwargs)

    return wrapper


@log
def increment_priority(priority: int) -> int:
    """ Increment priority; set to 0 if undefined.
    :param priority: priority (integer number)
    :return: return incremented priority
    """
    if priority is None:
        _priority = 0
    else:
        _priority = priority + 1

    return _priority


@log
def decrement_priority(priority: int) -> int:
    """ Decrement priority; set to 0 if undefined.
    :param priority: priority (integer number)
    :return: return incremented priority
    """
    if priority is None:
        _priority = 0
    else:
        _priority = priority - 1

    return _priority


@log
def define_metadata(metadata_engine: Engine) -> None:
    connection = metadata_engine.connect()
    transaction = connection.begin()
    try:
        connection.execute("""
            drop table if exists app_info;
            """)
        connection.execute("""
            create table if not exists app_info (
            app_id varchar(60) primary key,
            app_name varchar(60) not null unique
            );
            """)

        connection.execute("""
            drop table if exists draft_info;
            """)
        connection.execute("""
            create table if not exists draft_info (
            draft_id varchar(60) primary key,
            draft_name varchar(60) not null unique
            );
            """)

        connection.execute("""
            drop table if exists source_info;
            """)
        connection.execute("""
            create table if not exists source_info (
            source_id varchar(60) primary key,
            source_name varchar(60) not null unique,
            connection_info json not null
            );
            """)

        connection.execute("""
            drop table if exists processor_backend_info;
            """)
        connection.execute("""
            create table if not exists processor_backend_info (
            processor_backend_id varchar(60) primary key,
            processor_backend_name varchar(60) not null unique
            );
            """)

        connection.execute("""
            drop table if exists driver_info;
            """)
        connection.execute("""
            create table if not exists driver_info (
            driver_id varchar(60) primary key,
            driver_name varchar(60) not null unique,
            driver_function varchar(60) not null,
            location_info json not null,
            source_id varchar(60) not null references source_info(source_id) on delete restrict,
            processor_backend_id varchar(60) not null references processor_backend_info(processor_backend_id) on delete restrict,
            constraint driver_function_check check (driver_function in ('on_read', 'on_write'))
            );
            """)

        connection.execute("""
            drop table if exists dataset_implementation_info;
            """)
        connection.execute("""
            create table if not exists dataset_implementation_info (
            dataset_implementation_id varchar(60) primary key,
            dataset_implementation_name varchar(60) not null unique,
            app_id varchar(60) not null references app_info(app_id) on delete restrict,
            draft_id varchar(60) not null references draft_info(draft_id) on delete restrict,
            source_id varchar(60) not null references source_info(source_id) on delete restrict
            );
            """)

        connection.execute("""
            drop table if exists abstract_processor_info;
            """)
        connection.execute("""
            create table if not exists abstract_processor_info (
            abstract_processor_id varchar(60) primary key,
            abstract_processor_name varchar(60) not null unique
            );
            """)

        connection.execute("""
            drop table if exists processor_implementation_info;
            """)
        connection.execute("""
            create table if not exists processor_implementation_info (
            processor_implementation_id varchar(60) primary key,
            processor_implementation_name varchar(60) not null unique,
            implements_abstract_processor_id varchar(60) not null references abstract_processor_info(abstract_processor_id) on delete restrict,
            backend_id varchar(60) not null references processor_backend_info(processor_backend_id)
            );
            """)

        connection.execute("""
            drop table if exists processor_upon_processor_dependency_info;
            """)
        connection.execute("""
            create table if not exists processor_upon_processor_dependency_info (
            processor_upon_processor_dependency_id varchar(60) primary key,
            processor_id varchar(60) not null references abstract_processor_info(abstract_processor_id) on delete restrict,
            depends_on_processor_id varchar(60) not null references abstract_processor_info(abstract_processor_id) on delete restrict
            );
            """)

        connection.execute("""
            drop table if exists processor_upon_primary_source_dependency_info;
            """)
        connection.execute("""
            create table if not exists processor_upon_primary_source_dependency_info (
            processor_upon_primary_source_dependency_id varchar(60) primary key,
            processor_id varchar(60) not null references abstract_processor_info(abstract_processor_id) on delete restrict,
            depends_on_draft_id varchar(60) not null references draft_info(draft_id) on delete restrict
            );
            """)

        connection.execute("""
            drop table if exists processor_produces_sink_info;
            """)
        connection.execute("""
            create table if not exists processor_produces_sink_info (
            processor_produces_sink_id varchar(60) primary key,
            processor_id varchar(60) not null references abstract_processor_info(abstract_processor_id) on delete restrict,
            produces_draft_id varchar(60) not null references draft_info(draft_id) on delete restrict
            );
            """)

        connection.execute("""
            drop table if exists filled_drafts_info;
            """)
        connection.execute("""
            create table if not exists filled_drafts_info (
            dataset_implementation_id varchar(60) primary key references dataset_implementation_info(dataset_implementation_id) on delete restrict
            );
            """)

        connection.execute("""
            drop table if exists dataset_implementation_priorities;
            """)
        connection.execute("""
            create table if not exists dataset_implementation_priorities (
            dataset_implementation_id varchar(60) references dataset_implementation_info(dataset_implementation_id) on delete restrict,
            function varchar(60) not null,
            priority int not null,
            constraint priorities_function_check check (function in ('on_read', 'on_write'))
            );
            """)

        connection.execute("""
            drop table if exists dag_flow_level_info;
            """)
        connection.execute("""
            create table if not exists dag_flow_level_info (
            dag_flow_level_id varchar(60) primary key,
            dag_flow_level_name varchar(60) not null unique
            );
            """)

        connection.execute("""
            drop table if exists abstract_processor_to_implementation_mapping_info;
            """)
        connection.execute("""
            create table if not exists abstract_processor_to_implementation_mapping_info (
            abstract_processor_to_implementation_mapping_id varchar(60) not null,
            abstract_processor_id                           varchar(60) not null references abstract_processor_info(abstract_processor_id) on delete restrict,
            processor_implementation_id                     varchar(60) not null references processor_implementation_info(processor_implementation_id) on delete restrict,
            primary key (abstract_processor_to_implementation_mapping_id, abstract_processor_id, processor_implementation_id)
            );
            """)

        connection.execute("""
            drop table if exists dag_implementation_info;
            """)
        connection.execute("""
            create table if not exists dag_implementation_info (
            dag_implementation_id varchar(60) primary key,
            dag_implementation_name varchar(60) not null unique,
            implements_dag_flow_level_id varchar(60) not null references dag_flow_level_info(dag_flow_level_id) on delete restrict,
            abstract_processor_to_implementation_mapping_id varchar(60) not null
            references abstract_processor_to_implementation_mapping_info(abstract_processor_to_implementation_mapping_id) on delete restrict
            );
            """)

        connection.execute("""
            drop table if exists dag_build_info;
            """)
        connection.execute("""
            create table if not exists dag_build_info (
            dag_build_id varchar(60) primary key,
            dag_build_name varchar(60) not null unique,
            app_id varchar(60) not null references app_info(app_id) on delete restrict,
            dag_implementation_id varchar(60) not null references dag_implementation_info(dag_implementation_id) on delete restrict,
            build_parameters json not null
            );
            """)

        connection.execute("""
            drop table if exists abstract_model_info;
            """)
        connection.execute("""
            create table if not exists abstract_model_info (
            abstract_model_id varchar(60) primary key,
            abstract_model_name varchar(60) not null unique
            );
            """)

        connection.execute("""
            drop table if exists model_upon_inlet_draft_dependency_info;
            """)
        connection.execute("""
            create table if not exists model_upon_inlet_draft_dependency_info (
            model_upon_inlet_draft_dependency_id varchar(60) primary key,
            abstract_model_id varchar(60) not null references abstract_model_info(abstract_model_id) on delete restrict,
            depends_on_inlet_draft_id varchar(60) not null references draft_info(draft_id) on delete restrict
            );
            """)

        connection.execute("""
            drop table if exists model_produces_outlet_draft_info;
            """)
        connection.execute("""
            create table if not exists model_produces_outlet_draft_info (
            model_produces_outlet_draft_id varchar(60) primary key,
            abstract_model_id varchar(60) not null references abstract_model_info(abstract_model_id) on delete restrict,
            produces_outlet_draft_id varchar(60) not null references draft_info(draft_id) on delete restrict
            );
            """)

        connection.execute("""
            drop table if exists model_backend_info;
            """)
        connection.execute("""
            create table if not exists model_backend_info (
            model_backend_id varchar(60) primary key,
            model_backend_name varchar(60) not null unique
            );
            """)

        connection.execute("""
            drop table if exists model_implementation_info;
            """)
        connection.execute("""
            create table if not exists model_implementation_info (
            model_implementation_id varchar(60) primary key,
            model_implementation_name varchar(60) not null unique,
            implements_abstract_model_id varchar(60) not null references abstract_model_info(abstract_model_id) on delete restrict,
            model_backend_id varchar(60) not null references model_backend_info(model_backend_id) on delete restrict
            );
            """)

        connection.execute("""
            drop table if exists model_backends_available;
            """)
        connection.execute("""
            create table if not exists model_backends_available (
            processor_backend_id varchar(60) not null,
            model_backend_id varchar(60) not null,
            primary key (processor_backend_id, model_backend_id)
            );
            """)

        connection.execute("""
            drop table if exists model_driver_info;
            """)
        connection.execute("""
            create table if not exists model_driver_info (
            model_driver_id varchar(60) primary key,
            model_driver_name varchar(60) not null unique,
            processor_backend_id varchar(60) not null references processor_backend_info(processor_backend_id) on delete restrict,
            model_backend_id varchar(60) not null references model_backend_info(model_backend_id) on delete restrict,
            function varchar(60) not null,
            constraint driver_function_check check (function in ('on_read', 'on_write'))
            );
            """)

        connection.execute("""
            drop table if exists model_performance_info;
            """)
        connection.execute("""
            create table if not exists model_performance_info (
            model_implementation_id varchar(60) not null references model_implementation_info(model_implementation_id) on delete restrict,
            metric varchar(60) not null,
            value float not null,
            primary key (model_implementation_id, metric)
            );
            """)

        transaction.commit()
    except:
        transaction.rollback()
        logger.info("Error: failed to define metadata; something went wrong.")
        raise

    connection.close()


@log
def drop_metadata(metadata_engine: Engine) -> None:
    connection = metadata_engine.connect()
    transaction = connection.begin()
    try:
        connection.execute("""
            drop table if exists app_info;
        """)
        connection.execute("""
            drop table if exists draft_info;
        """)
        connection.execute("""
            drop table if exists source_info;
        """)
        connection.execute("""
            drop table if exists processor_backend_info;
        """)
        connection.execute("""
            drop table if exists driver_info;
        """)
        connection.execute("""
            drop table if exists dataset_implementation_info;
        """)
        connection.execute("""
            drop table if exists abstract_processor_info;
        """)
        connection.execute("""
            drop table if exists processor_implementation_info;
        """)
        connection.execute("""
            drop table if exists processor_upon_processor_dependency_info;
        """)
        connection.execute("""
            drop table if exists processor_upon_primary_source_dependency_info;
        """)
        connection.execute("""
            drop table if exists processor_produces_sink_info;
        """)
        connection.execute("""
            drop table if exists filled_drafts_info;
        """)
        connection.execute("""
            drop table if exists dataset_implementation_priorities;
        """)
        connection.execute("""
            drop table if exists dag_flow_level_info;
        """)
        connection.execute("""
            drop table if exists abstract_processor_to_implementation_mapping_info;
        """)
        connection.execute("""
            drop table if exists dag_implementation_info;
        """)
        connection.execute("""
            drop table if exists dag_build_info;
        """)
        connection.execute("""
            drop table if exists abstract_model_info;
        """)
        connection.execute("""
            drop table if exists model_upon_inlet_draft_dependency_info;
        """)
        connection.execute("""
            drop table if exists model_produces_outlet_draft_info;
        """)
        connection.execute("""
            drop table if exists model_backend_info;
        """)
        connection.execute("""
            drop table if exists model_implementation_info;
        """)
        connection.execute("""
            drop table if exists model_backends_available;
        """)
        connection.execute("""
            drop table if exists model_driver_info;
        """)
        connection.execute("""
            drop table if exists model_performance_info;
        """)

        transaction.commit()
    except:
        transaction.rollback()
        logger.info("Error: failed to drop metadata; something went wrong.")
        raise

    connection.close()


@log
def clear_metadata(metadata_engine: Engine) -> None:
    connection = metadata_engine.connect()
    transaction = connection.begin()
    try:
        connection.execute("""
            delete from app_info;
        """)
        connection.execute("""
            delete from draft_info;
        """)
        connection.execute("""
            delete from source_info;
        """)
        connection.execute("""
            delete from processor_backend_info;
        """)
        connection.execute("""
            delete from driver_info;
        """)
        connection.execute("""
            delete from dataset_implementation_info;
        """)
        connection.execute("""
            delete from abstract_processor_info;
        """)
        connection.execute("""
            delete from processor_implementation_info;
        """)
        connection.execute("""
            delete from processor_upon_processor_dependency_info;
        """)
        connection.execute("""
            delete from processor_upon_primary_source_dependency_info;
        """)
        connection.execute("""
            delete from processor_produces_sink_info;
        """)
        connection.execute("""
            delete from filled_drafts_info;
        """)
        connection.execute("""
            delete from dataset_implementation_priorities;
        """)
        connection.execute("""
            delete from dag_flow_level_info;
        """)
        connection.execute("""
            delete from abstract_processor_to_implementation_mapping_info;
        """)
        connection.execute("""
            delete from dag_implementation_info;
        """)
        connection.execute("""
            delete from dag_build_info;
        """)
        connection.execute("""
            delete from abstract_model_info;
        """)
        connection.execute("""
            delete from model_upon_inlet_draft_dependency_info;
        """)
        connection.execute("""
            delete from model_produces_outlet_draft_info;
        """)
        connection.execute("""
            delete from model_backend_info;
        """)
        connection.execute("""
            delete from model_implementation_info;
        """)
        connection.execute("""
            delete from model_backends_available;
        """)
        connection.execute("""
            delete from model_driver_info;
        """)
        connection.execute("""
            delete from model_performance_info;
        """)

        transaction.commit()
    except:
        transaction.rollback()
        logger.info("Error: failed to clear metadata; something went wrong.")
        raise

    connection.close()


class DataModelSampleConstructor:
    @classmethod
    @log
    def get_app_info(cls) -> AppInfo:
        app_info = AppInfoFactory.construct_by_name("test_app")

        return app_info

    @classmethod
    @log
    def get_draft_info(cls) -> DraftInfo:
        draft_info = DraftInfoFactory.construct_by_name("test_draft")

        return draft_info

    @classmethod
    @log
    def get_source_info(cls) -> SourceInfo:
        postgres_pdm_localhost_connection_info = dumps(
            dict(connection_string=postgres_pdm_localhost_connection_string))
        postgres_pdm_localhost_source_info = SourceInfoFactory.construct_by_name("postgres-pdm-localhost",
                                                                                 postgres_pdm_localhost_connection_info)

        return postgres_pdm_localhost_source_info

    @classmethod
    @log
    def get_processor_backend_info(cls) -> ProcessorBackendInfo:
        py37ds_processor_backend_info = ProcessorBackendInfoFactory.construct_by_name("py37ds")

        return py37ds_processor_backend_info

    @classmethod
    @log
    def get_dataset_implementation_info(cls) -> DatasetImplementationInfo:
        normalized_visits_postgres_pdm_localhost = DatasetImplementationInfoFactory.construct_by_name(
            "normalized_visits_postgres_pdm_localhost",
            cls.get_app_info(),
            cls.get_draft_info(),
            cls.get_source_info()
        )

        return normalized_visits_postgres_pdm_localhost

    @classmethod
    @log
    def get_abstract_processor_info(cls) -> AbstractProcessorInfo:
        normalized_visits_abstract_processor_info = AbstractProcessorInfoFactory.construct_by_name("normalize_visits")

        return normalized_visits_abstract_processor_info

    @classmethod
    @log
    def get_processor_implementation_info(cls) -> ProcessorImplementationInfo:
        normalized_visits_processor_implementation_info = ProcessorImplementationInfoFactory.construct_by_name(
            "normalize_visits_py37",
            cls.get_abstract_processor_info(),
            cls.get_processor_backend_info()
        )

        return normalized_visits_processor_implementation_info

    @classmethod
    @log
    def get_processor_upon_processor_dependency_info(cls) -> ProcessorUponProcessorDependencyInfo:
        processor_upon_processor_dependency_info = ProcessorUponProcessorDependencyInfoFactory.construct(
            md5("normalized_visits-->binding_transactions_ambient_visits".encode("utf-8")).hexdigest(),
            cls.get_abstract_processor_info(),
            cls.get_abstract_processor_info())

        return processor_upon_processor_dependency_info

    @classmethod
    @log
    def get_processor_upon_primary_source_dependency_info(cls) -> ProcessorUponPrimarySourceDependencyInfo:
        processor_upon_primary_source_dependency_info = ProcessorUponPrimarySourceDependencyInfoFactory.construct(
            md5("processor_upon_primary_source_dependency_info".encode("utf-8")).hexdigest(),
            cls.get_abstract_processor_info(),
            cls.get_draft_info()
        )

        return processor_upon_primary_source_dependency_info

    @classmethod
    @log
    def get_processor_produces_sink_info(cls) -> ProcessorProducesSinkInfo:
        processor_produces_sink_info = ProcessorProducesSinkInfoFactory.construct(
            md5("processor_produces_sink_info".encode("utf-8")).hexdigest(),
            cls.get_abstract_processor_info(),
            cls.get_draft_info()
        )

        return processor_produces_sink_info

    @classmethod
    @log
    def get_filled_drafts(cls) -> FilledDrafts:
        filled_drafts = FilledDraftsFactory.construct([cls.get_dataset_implementation_info()])

        return filled_drafts


