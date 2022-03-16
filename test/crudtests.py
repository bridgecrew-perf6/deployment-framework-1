from hashlib import md5
from json import dumps
from unittest import TestCase, skip

import pandas as pd

from src.config import metadata_engine, sql_source_connection_string
from src.data_model import AppInfo, AppInfoFactory, DraftInfo, DraftInfoFactory, SourceInfo, SourceInfoFactory, \
    ProcessorBackendInfo, ProcessorBackendInfoFactory, DriverInfo, DriverInfoFactory, DatasetImplementationInfo, \
    AbstractProcessorInfo
from src.registration import AppHandler, DraftHandler, SourceHandler, ProcessorBackendHandler, DriverHandler, \
    DatasetImplementationHandler, AbstractProcessorHandler, ProcessorImplementationHandler, \
    ProcessorUponProcessorDependencyHandler, ProcessorUponPrimarySourceDependencyHandler, ProcessorProducesSinkHandler, \
    FilledDraftsInfoHandler
from src.retrievers import AppInfoRetriever, DraftInfoRetriever, SourceInfoRetriever, ProcessorBackendInfoRetriever, \
    DriverInfoRetriever, DatasetImplementationInfoRetriever, AbstractProcessorInfoRetriever, \
    ProcessorImplementationInfoRetriever, ProcessorUponProcessorDependencyRetriever, \
    ProcessorUponPrimarySourceDependencyRetriever, ProcessorProducesSinkRetriever, FilledDraftsInfoRetriever
from src.utilities import define_metadata, drop_metadata, clear_metadata, DataModelSampleConstructor


class AppInfoTest(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        define_metadata(metadata_engine)

    def setUp(self) -> None:
        self.app_name = "test_app"
        self.app_info = AppInfo(md5(self.app_name.encode("utf-8")).hexdigest(),
                                self.app_name)
        self.app_handler = AppHandler()

    def test_construct_by_name(self) -> None:
        app_info = AppInfoFactory.construct_by_name(self.app_name)

        self.assertEqual(self.app_info,
                         app_info,
                         "construct_by_name method doesn't work properly")

    def test_register_by_id(self) -> None:
        self.app_handler.register(self.app_info)
        data = pd.read_sql_table("app_info", metadata_engine)
        app_id = data.iloc[0, :]["app_id"]
        app_name = data.iloc[0, :]["app_name"]
        app_info = AppInfoFactory.construct(app_id, app_name)

        self.assertEqual(data.shape[0], 1, "Wrong number of entries written")
        self.assertEqual(self.app_info, app_info, "Failed to write right info")

    def test_retrieve_by_name(self) -> None:
        self.app_handler.register(self.app_info)
        app_info = AppInfoRetriever.get_info_by_name(self.app_name)

        self.assertEqual(self.app_info, app_info, "Failed to retrieve app_info")

    def test_drop_by_name(self) -> None:
        self.app_handler.register(self.app_info)
        self.app_handler.drop_by_name(self.app_name)
        data = pd.read_sql_table("app_info", metadata_engine)

        self.assertEqual(data.shape[0], 0, "Failed to remove app info")

    def tearDown(self):
        clear_metadata(metadata_engine)

    @classmethod
    def tearDownClass(cls) -> None:
        drop_metadata(metadata_engine)


class DraftInfoTest(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        define_metadata(metadata_engine)

    def setUp(self) -> None:
        self.draft_name = "test_draft"
        self.draft_info = DraftInfo(md5(self.draft_name.encode("utf-8")).hexdigest(),
                                    self.draft_name)
        self.draft_handler = DraftHandler()

    def test_construct_by_name(self) -> None:
        draft_info = DraftInfoFactory.construct_by_name(self.draft_name)

        self.assertEqual(self.draft_info,
                         draft_info,
                         "construct_by_name method doesn't work properly")

    def test_register_by_id(self) -> None:
        self.draft_handler.register(self.draft_info)
        data = pd.read_sql_table("draft_info", metadata_engine)
        draft_id = data.iloc[0, :]["draft_id"]
        draft_name = data.iloc[0, :]["draft_name"]
        draft_info = DraftInfoFactory.construct(draft_id, draft_name)

        self.assertEqual(data.shape[0], 1, "Wrong number of entries written")
        self.assertEqual(self.draft_info, draft_info, "Failed to write right info")

    def test_retrieve_by_name(self) -> None:
        self.draft_handler.register(self.draft_info)
        draft_info = DraftInfoRetriever.get_info_by_name(self.draft_name)

        self.assertEqual(self.draft_info, draft_info, "Failed to retrieve draft_info")

    def test_drop_by_name(self) -> None:
        self.draft_handler.register(self.draft_info)
        self.draft_handler.drop_by_name(self.draft_name)
        data = pd.read_sql_table("draft_info", metadata_engine)

        self.assertEqual(data.shape[0], 0, "Failed to remove draft info")

    def tearDown(self):
        clear_metadata(metadata_engine)


class SourceInfoTest(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        define_metadata(metadata_engine)

    def setUp(self) -> None:
        self.source_name = "test_source"
        self.connection_info = dumps(dict(connection_string="something"))
        self.source_info = SourceInfo(md5(self.source_name.encode("utf-8")).hexdigest(),
                                      self.source_name,
                                      self.connection_info)
        self.source_handler = SourceHandler()

    def test_construct_by_name(self) -> None:
        source_info = SourceInfoFactory.construct_by_name(self.source_name,
                                                          self.connection_info)

        self.assertEqual(self.source_info,
                         source_info,
                         "construct_by_name method doesn't work properly")

    def test_register_by_id(self) -> None:
        self.source_handler.register(self.source_info)
        data = pd.read_sql_table("source_info", metadata_engine)
        source_id = data.iloc[0, :]["source_id"]
        source_name = data.iloc[0, :]["source_name"]
        source_info = SourceInfoFactory.construct(source_id, source_name, self.connection_info)

        self.assertEqual(data.shape[0], 1, "Wrong number of entries written")
        self.assertEqual(self.source_info, source_info, "Failed to write right info")

    def test_retrieve_by_name(self) -> None:
        self.source_handler.register(self.source_info)
        source_info = SourceInfoRetriever.get_info_by_name(self.source_name)

        self.assertEqual(self.source_info, source_info, "Failed to retrieve source_info")

    def test_drop_by_name(self) -> None:
        self.source_handler.register(self.source_info)
        self.source_handler.drop_by_name(self.source_name)
        data = pd.read_sql_table("source_info", metadata_engine)

        self.assertEqual(data.shape[0], 0, "Failed to remove source info")

    def tearDown(self):
        clear_metadata(metadata_engine)


class ProcessorBackendInfoTest(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        define_metadata(metadata_engine)

    def construct_processor_backend(self):
        processor_backend_name = "test_processor_backend"
        processor_backend_info = ProcessorBackendInfo(md5(processor_backend_name.encode("utf-8")).hexdigest(),
                                                      processor_backend_name)

        return processor_backend_info

    def setUp(self) -> None:
        self.processor_backend_info = self.construct_processor_backend()
        self.processor_backend_handler = ProcessorBackendHandler()

    def test_construct_by_name(self) -> None:
        processor_backend_info = ProcessorBackendInfoFactory.construct_by_name(self.processor_backend_info.processor_backend_name)

        self.assertEqual(self.processor_backend_info,
                         processor_backend_info,
                         "construct_by_name method doesn't work properly")

    def test_register_by_id(self) -> None:
        self.processor_backend_handler.register(self.processor_backend_info)
        data = pd.read_sql_table("processor_backend_info", metadata_engine)
        processor_backend_id = data.iloc[0, :]["processor_backend_id"]
        processor_backend_name = data.iloc[0, :]["processor_backend_name"]
        processor_backend_info = ProcessorBackendInfoFactory.construct(processor_backend_id,
                                                                       processor_backend_name)

        self.assertEqual(data.shape[0], 1, "Wrong number of entries written")
        self.assertEqual(self.processor_backend_info,
                         processor_backend_info,
                         "Failed to write right info")

    def test_retrieve_by_name(self) -> None:
        self.processor_backend_handler.register(self.processor_backend_info)
        processor_backend_info = ProcessorBackendInfoRetriever.get_info_by_name(self.processor_backend_info.processor_backend_name)

        self.assertEqual(self.processor_backend_info,
                         processor_backend_info,
                         "Failed to retrieve processor_backend_info")

    def test_drop_by_name(self) -> None:
        self.processor_backend_handler.register(self.processor_backend_info)
        self.processor_backend_handler.drop_by_name(self.processor_backend_info.processor_backend_name)
        data = pd.read_sql_table("source_info", metadata_engine)

        self.assertEqual(data.shape[0], 0, "Failed to remove processor backend info")

    def tearDown(self):
        clear_metadata(metadata_engine)


class DriverInfoTest(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        define_metadata(metadata_engine)

    def construct_info(self) -> DriverInfo:
        processor_backend_name = "py37ds"

        driver_name = "test_driver"
        driver_function = "on_read"
        location_info = dict(package_name="python_drivers.sqlite_read_driver",
                             class_name="SqliteReadDriver")
        sqlite_connection_info = dumps(
            dict(connection_string=sql_source_connection_string))
        source_info = SourceInfoFactory.construct_by_name("sqlite3-public", sqlite_connection_info)
        processor_backend_info = ProcessorBackendInfoFactory.construct_by_name(processor_backend_name)

        driver_info = DriverInfoFactory.construct(md5(driver_name.encode("utf-8")).hexdigest(),
                                                  driver_name,
                                                  driver_function,
                                                  location_info,
                                                  source_info,
                                                  processor_backend_info)

        return driver_info

    def setUp(self) -> None:
        self.driver_info = self.construct_info()
        self.driver_handler = DriverHandler()

    def test_construct_by_name(self) -> None:
        driver_info = DriverInfoFactory.construct_by_name(self.driver_info.driver_name,
                                                          self.driver_info.driver_function,
                                                          self.driver_info.location_info,
                                                          self.driver_info.source_info,
                                                          self.driver_info.processor_backend_info)

        self.assertEqual(self.driver_info,
                         driver_info,
                         "construct_by_name method doesn't work properly")

    def test_register_by_id(self) -> None:
        self.driver_handler.register(self.driver_info)
        data = pd.read_sql_table("driver_info", metadata_engine)

        driver_id = data.iloc[0, :]["driver_id"]
        driver_name = data.iloc[0, :]["driver_name"]
        driver_function = data.iloc[0, :]["driver_function"]
        location_info = data.iloc[0, :]["location_info"]
        source_id = data.iloc[0, :]["source_id"]
        processor_backend_id = data.iloc[0, :]["processor_backend_id"]

        driver_info = DriverInfoFactory.construct(driver_id,
                                                  driver_name,
                                                  driver_function,
                                                  location_info,
                                                  SourceInfoRetriever.get_info(source_id),
                                                  ProcessorBackendInfoRetriever.get_info_by_id(processor_backend_id))

        self.assertEqual(data.shape[0], 1, "Wrong number of entries written")
        self.assertEqual(self.driver_info, driver_info, "Failed to write right info")

    def test_retrieve_by_id(self) -> None:
        self.driver_handler.register(self.driver_info)
        driver_info = DriverInfoRetriever.get_info(self.driver_info.driver_id)

        self.assertEqual(self.driver_info.driver_id,
                         driver_info.driver_id,
                         "driver ids don't match ")
        self.assertEqual(self.driver_info.driver_name,
                         driver_info.driver_name,
                         "driver names don't match ")
        self.assertEqual(self.driver_info.driver_function,
                         driver_info.driver_function,
                         "driver function don't match ")
        self.assertEqual(str(self.driver_info.location_info).replace("'", "\""),
                         str(driver_info.location_info).replace("'", "\""),
                         "location info doesn't match ")
        self.assertEqual(self.driver_info.source_info,
                         driver_info.source_info,
                         "source info don't match ")
        self.assertEqual(self.driver_info.processor_backend_info,
                         driver_info.processor_backend_info,
                         "processor backend info don't match ")

    def test_retrieve_by_name(self) -> None:
        self.driver_handler.register(self.driver_info)
        driver_info = DriverInfoRetriever.get_info_by_name(self.driver_info.driver_name)

        self.assertEqual(self.driver_info.driver_id,
                         driver_info.driver_id,
                         "driver ids don't match ")
        self.assertEqual(self.driver_info.driver_name,
                         driver_info.driver_name,
                         "driver names don't match ")
        self.assertEqual(self.driver_info.driver_function,
                         driver_info.driver_function,
                         "driver function don't match ")
        self.assertEqual(str(self.driver_info.location_info).replace("'", "\""),
                         str(driver_info.location_info).replace("'", "\""),
                         "location info doesn't match ")
        self.assertEqual(self.driver_info.source_info,
                         driver_info.source_info,
                         "source info don't match ")
        self.assertEqual(self.driver_info.processor_backend_info,
                         driver_info.processor_backend_info,
                         "processor backend info don't match ")

    def test_drop_by_id(self) -> None:
        self.driver_handler.register(self.driver_info)
        self.driver_handler.drop_by_id(self.driver_info.driver_id)
        data = pd.read_sql_table("driver_info", metadata_engine)

        self.assertEqual(data.shape[0], 0, "Failed to remove driver info by id")

    def test_drop_by_name(self) -> None:
        self.driver_handler.register(self.driver_info)
        self.driver_handler.drop_by_name(self.driver_info.driver_name)
        data = pd.read_sql_table("driver_info", metadata_engine)

        self.assertEqual(data.shape[0], 0, "Failed to remove driver info by name")

    def tearDown(self):
        clear_metadata(metadata_engine)


class DatasetImplementationInfoTest(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        define_metadata(metadata_engine)

    def setUp(self) -> None:
        self.dataset_implementation_info = DataModelSampleConstructor.get_dataset_implementation_info()

    def test_register_by_id(self) -> None:
        DatasetImplementationHandler("on_read").register_with_highest_priority(self.dataset_implementation_info)
        data = pd.read_sql_table("dataset_implementation_info", metadata_engine)
        self.assertEqual(data.shape[0], 1, "Too many dataset implementations")

        self.assertEqual(data.iloc[0, :]["dataset_implementation_id"],
                         self.dataset_implementation_info.dataset_implementation_id,
                         "dataset_implementation_ids don't match ")
        self.assertEqual(data.iloc[0, :]["dataset_implementation_name"],
                         self.dataset_implementation_info.dataset_implementation_name,
                         "dataset_implementation_names don't match ")
        self.assertEqual(data.iloc[0, :]["app_id"],
                         self.dataset_implementation_info.app_info.app_id,
                         "app_ids don't match ")
        self.assertEqual(data.iloc[0, :]["draft_id"],
                         self.dataset_implementation_info.draft_info.draft_id,
                         "draft_ids don't match ")
        self.assertEqual(data.iloc[0, :]["source_id"],
                         self.dataset_implementation_info.source_info.source_id)

    def test_retrieve_by_id(self) -> None:
        DatasetImplementationHandler("on_read").register_with_highest_priority(self.dataset_implementation_info)
        dataset_implementation_info = DatasetImplementationInfoRetriever.get_info(self.dataset_implementation_info.dataset_implementation_id)
        self.assertEqual(self.dataset_implementation_info,
                         dataset_implementation_info,
                         "Dataset implementation infos don't match ")

    def test_retrieve_by_name(self) -> None:
        DatasetImplementationHandler("on_read").register_with_highest_priority(self.dataset_implementation_info)
        dataset_implementation_info = DatasetImplementationInfoRetriever.get_info_by_name(self.dataset_implementation_info.dataset_implementation_name)
        self.assertEqual(self.dataset_implementation_info,
                         dataset_implementation_info,
                         "Dataset implementation_infos don't match ")

    def test_drop_by_id(self) -> None:
        DatasetImplementationHandler("on_read").register_with_highest_priority(self.dataset_implementation_info)
        DatasetImplementationHandler("on_read").drop_by_id(self.dataset_implementation_info.dataset_implementation_id)
        data = pd.read_sql_table("dataset_implementation_info", metadata_engine)
        self.assertEqual(data.shape[0], 0, "Failed to drop dataset implementation info by its id")

    def test_drop_by_name(self) -> None:
        DatasetImplementationHandler("on_read").register_with_highest_priority(self.dataset_implementation_info)
        DatasetImplementationHandler("on_read").drop_by_name(self.dataset_implementation_info.dataset_implementation_name)
        data = pd.read_sql_table("dataset_implementation_info", metadata_engine)
        self.assertEqual(data.shape[0], 0, "Failed to drop dataset implementation info by its name")

    def tearDown(self):
        clear_metadata(metadata_engine)


class AbstractProcessorInfoTest(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        define_metadata(metadata_engine)

    def setUp(self) -> None:
        self.abstract_processor_info = DataModelSampleConstructor.get_abstract_processor_info()

    def test_register_by_id(self) -> None:
        AbstractProcessorHandler().register(self.abstract_processor_info)
        data = pd.read_sql_table("abstract_processor_info", metadata_engine)
        abstract_processor_info = AbstractProcessorInfo(data.iloc[0, :]["abstract_processor_id"],
                                                        data.iloc[0, :]["abstract_processor_name"])
        self.assertEqual(self.abstract_processor_info,
                         abstract_processor_info,
                         "Failed to register abstract processor by its id")

    def test_register_by_name(self) -> None:
        AbstractProcessorHandler().register_by_name(self.abstract_processor_info.abstract_processor_name)
        data = pd.read_sql_table("abstract_processor_info", metadata_engine)
        abstract_processor_info = AbstractProcessorInfo(data.iloc[0, :]["abstract_processor_id"],
                                                        data.iloc[0, :]["abstract_processor_name"])
        self.assertEqual(self.abstract_processor_info,
                         abstract_processor_info,
                         "Failed to register abstract processor by its name")

    def test_retrieve_by_id(self) -> None:
        AbstractProcessorHandler().register(self.abstract_processor_info)
        abstract_processor_info = AbstractProcessorInfoRetriever.get_info(self.abstract_processor_info.abstract_processor_id)

        self.assertEqual(self.abstract_processor_info,
                         abstract_processor_info,
                         "Failed to retrieve abstract processor info by its id")

    def test_retrieve_by_name(self) -> None:
        AbstractProcessorHandler().register(self.abstract_processor_info)
        abstract_processor_info = AbstractProcessorInfoRetriever.get_info_by_name(self.abstract_processor_info.abstract_processor_name)

        self.assertEqual(self.abstract_processor_info,
                         abstract_processor_info,
                         "Failed to retrieve abstract processor info by its name")

    def test_drop_by_id(self) -> None:
        AbstractProcessorHandler().register(self.abstract_processor_info)
        AbstractProcessorHandler().drop_by_id(self.abstract_processor_info.abstract_processor_id)
        data = pd.read_sql_table("abstract_processor_info", metadata_engine)

        self.assertEqual(data.shape[0], 0, "Failed to drop abstract processor info by its id")

    def test_drop_by_name(self) -> None:
        AbstractProcessorHandler().register(self.abstract_processor_info)
        AbstractProcessorHandler().drop_by_name(self.abstract_processor_info.abstract_processor_name)
        data = pd.read_sql_table("abstract_processor_info", metadata_engine)

        self.assertEqual(data.shape[0], 0, "Failed to drop abstract processor info by its name")

    def tearDown(self) -> None:
        clear_metadata(metadata_engine)


class ProcessorImplementationInfoTest(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        define_metadata(metadata_engine)

    def setUp(self) -> None:
        self.processor_implementation_info = DataModelSampleConstructor.get_processor_implementation_info()

    def test_register_by_info(self) -> None:
        ProcessorImplementationHandler().register(self.processor_implementation_info)
        data = pd.read_sql_table("processor_implementation_info", metadata_engine)

        self.assertEqual(self.processor_implementation_info.processor_implementation_id,
                         data.iloc[0, :]["processor_implementation_id"],
                         "Processor implementation ids don't match ")
        self.assertEqual(self.processor_implementation_info.processor_implementation_name,
                         data.iloc[0, :]["processor_implementation_name"],
                         "Processor implementation names don't match ")
        self.assertEqual(self.processor_implementation_info.implements_abstract_processor_info.abstract_processor_id,
                         data.iloc[0, :]["implements_abstract_processor_id"],
                         "Abstract processor ids don't match ")
        self.assertEqual(self.processor_implementation_info.backend_info.processor_backend_id,
                         data.iloc[0, :]["backend_id"],
                         "Processor backend ids don't match ")

    def test_register_by_name(self) -> None:
        ProcessorImplementationHandler().register_by_name(self.processor_implementation_info.processor_implementation_name,
                                                          self.processor_implementation_info.implements_abstract_processor_info,
                                                          self.processor_implementation_info.backend_info)
        data = pd.read_sql_table("processor_implementation_info", metadata_engine)

        self.assertEqual(self.processor_implementation_info.processor_implementation_id,
                         data.iloc[0, :]["processor_implementation_id"],
                         "Processor implementation ids don't match ")
        self.assertEqual(self.processor_implementation_info.processor_implementation_name,
                         data.iloc[0, :]["processor_implementation_name"],
                         "Processor implementation names don't match ")
        self.assertEqual(self.processor_implementation_info.implements_abstract_processor_info.abstract_processor_id,
                         data.iloc[0, :]["implements_abstract_processor_id"],
                         "Abstract processor ids don't match ")
        self.assertEqual(self.processor_implementation_info.backend_info.processor_backend_id,
                         data.iloc[0, :]["backend_id"],
                         "Processor backend ids don't match ")

    def test_retrieve_by_id(self) -> None:
        ProcessorImplementationHandler().register(self.processor_implementation_info)
        processor_implementation_info = ProcessorImplementationInfoRetriever.get_info(self.processor_implementation_info.processor_implementation_id)

        self.assertEqual(self.processor_implementation_info,
                         processor_implementation_info,
                         "Failed to retrieve the right processor implementation info by its id")

    def test_retrieve_by_name(self) -> None:
        ProcessorImplementationHandler().register(self.processor_implementation_info)
        processor_implementation_info = ProcessorImplementationInfoRetriever.get_info_by_name(self.processor_implementation_info.processor_implementation_name)

        self.assertEqual(self.processor_implementation_info,
                         processor_implementation_info,
                         "Failed to retrieve the right processor implementation info by its name")

    def test_drop_by_id(self) -> None:
        ProcessorImplementationHandler().register(self.processor_implementation_info)
        ProcessorImplementationHandler().drop_by_id(self.processor_implementation_info.processor_implementation_id)
        data = pd.read_sql_table("processor_implementation_info", metadata_engine)

        self.assertEqual(data.shape[0], 0, "Failed to remove processor implementation info by its id")

    def test_drop_by_name(self) -> None:
        ProcessorImplementationHandler().register(self.processor_implementation_info)
        ProcessorImplementationHandler().drop_by_name(self.processor_implementation_info.processor_implementation_name)
        data = pd.read_sql_table("processor_implementation_info", metadata_engine)

        self.assertEqual(data.shape[0], 0, "Failed to remove processor implementation info by its name")

    def tearDown(self) -> None:
        clear_metadata(metadata_engine)


class ProcessorUponProcessorDependencyInfoTest(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        define_metadata(metadata_engine)

    def setUp(self) -> None:
        self.processor_upon_processor_dependency_info = DataModelSampleConstructor.get_processor_upon_processor_dependency_info()

    def test_register(self) -> None:
        ProcessorUponProcessorDependencyHandler().register(self.processor_upon_processor_dependency_info)
        data = pd.read_sql_table("processor_upon_processor_dependency_info", metadata_engine)

        self.assertEqual(data.iloc[0, :]["processor_upon_processor_dependency_id"],
                         self.processor_upon_processor_dependency_info.processor_upon_processor_dependency_id,
                         "Dependency ids don't match ")
        self.assertEqual(data.iloc[0, :]["processor_id"],
                         self.processor_upon_processor_dependency_info.processor_info.abstract_processor_id,
                         "Abstract processor ids don't match ")
        self.assertEqual(data.iloc[0, :]["depends_on_processor_id"],
                         self.processor_upon_processor_dependency_info.depends_on_processor_info.abstract_processor_id,
                         "Processor ids that processors depend upon don't match ")

    def test_retrieve(self) -> None:
        ProcessorUponProcessorDependencyHandler().register(self.processor_upon_processor_dependency_info)
        processor_upon_processor_dependency_info = ProcessorUponProcessorDependencyRetriever.get_info(
            self.processor_upon_processor_dependency_info.processor_upon_processor_dependency_id)

        self.assertEqual(self.processor_upon_processor_dependency_info,
                         processor_upon_processor_dependency_info,
                         "Retriever dependency info doesn't match ground truth ")

    def test_drop(self) -> None:
        ProcessorUponProcessorDependencyHandler().register(self.processor_upon_processor_dependency_info)
        ProcessorUponProcessorDependencyHandler().drop_by_id(
            self.processor_upon_processor_dependency_info.processor_upon_processor_dependency_id)

        data = pd.read_sql_table("processor_upon_processor_dependency_info", metadata_engine)

        self.assertEqual(data.shape[0], 0, "Failed to drop dependency ")

    @skip("Skipping since there is no auxiliary method to register multiple dependencies ")
    def test_drop_all_dependencies(self) -> None:
        pass

    def tearDown(self) -> None:
        clear_metadata(metadata_engine)


class ProcessorUponPrimarySourceDependencyInfoTest(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        define_metadata(metadata_engine)

    def setUp(self) -> None:
        self.processor_upon_primary_source_dependency_info = DataModelSampleConstructor.get_processor_upon_primary_source_dependency_info()

    def test_register(self) -> None:
        ProcessorUponPrimarySourceDependencyHandler().register(self.processor_upon_primary_source_dependency_info)
        data = pd.read_sql_table("processor_upon_primary_source_dependency_info", metadata_engine)

        self.assertGreater(data.shape[0], 0, "Failed to register any entries")
        self.assertLess(data.shape[0], 2, "Register too many entries")

        self.assertEqual(self.processor_upon_primary_source_dependency_info.processor_upon_primary_source_dependency_id,
                         data.iloc[0, :]["processor_upon_primary_source_dependency_id"],
                         "Ids don't match ")
        self.assertEqual(self.processor_upon_primary_source_dependency_info.processor_info.abstract_processor_id,
                         data.iloc[0, :]["processor_id"],
                         "Processor ids don't match ")
        self.assertEqual(self.processor_upon_primary_source_dependency_info.depends_on_draft_info.draft_id,
                         data.iloc[0, :]["depends_on_draft_id"],
                         "Draft ids don't match ")

    def test_retrieve(self) -> None:
        ProcessorUponPrimarySourceDependencyHandler().register(self.processor_upon_primary_source_dependency_info)
        processor_upon_primary_source_dependency_info = ProcessorUponPrimarySourceDependencyRetriever.get_info(
            self.processor_upon_primary_source_dependency_info.processor_upon_primary_source_dependency_id)

        self.assertEqual(self.processor_upon_primary_source_dependency_info,
                         processor_upon_primary_source_dependency_info,
                         "Failed to retrieve info about processor upon primary source dependencies ")

    def test_retrieve_all_dependencies(self) -> None:
        ProcessorUponPrimarySourceDependencyHandler().register(self.processor_upon_primary_source_dependency_info)
        processor_upon_primary_source_dependencies = list(ProcessorUponPrimarySourceDependencyRetriever.get_all_dependencies(
            self.processor_upon_primary_source_dependency_info.processor_info.abstract_processor_id
        ))

        self.assertEqual(len(processor_upon_primary_source_dependencies), 1, "Expected the only entry ")
        self.assertEqual(self.processor_upon_primary_source_dependency_info,
                         processor_upon_primary_source_dependencies[0],
                         "Retrieved dependency info doesn't match the ground truth ")

    def test_drop(self) -> None:
        ProcessorUponPrimarySourceDependencyHandler().register(self.processor_upon_primary_source_dependency_info)
        ProcessorUponPrimarySourceDependencyHandler().drop_by_id(
            self.processor_upon_primary_source_dependency_info.processor_upon_primary_source_dependency_id)
        data = pd.read_sql_table("processor_upon_primary_source_dependency_info", metadata_engine)

        self.assertEqual(data.shape[0], 0, "Failed to drop info about processor upon primary source dependency")

    def test_drop_all_dependencies(self) -> None:
        ProcessorUponPrimarySourceDependencyHandler().register(self.processor_upon_primary_source_dependency_info)
        ProcessorUponPrimarySourceDependencyHandler().drop_all_dependencies(
            self.processor_upon_primary_source_dependency_info.processor_info.abstract_processor_id)
        data = pd.read_sql_table("processor_upon_primary_source_dependency_info", metadata_engine)

        self.assertEqual(data.shape[0],
                         0,
                         "Failed to drop info about all processor upon primary source dependencies ")

    def test_drop_all_dependencies_by_name(self) -> None:
        ProcessorUponPrimarySourceDependencyHandler().register(self.processor_upon_primary_source_dependency_info)
        ProcessorUponPrimarySourceDependencyHandler().drop_all_dependencies_by_name(
            self.processor_upon_primary_source_dependency_info.processor_info.abstract_processor_name)
        data = pd.read_sql_table("processor_upon_primary_source_dependency_info", metadata_engine)

        self.assertEqual(data.shape[0],
                         0,
                         "Failed to drop info about all processor upon primary source dependencies by processor's name ")

    def tearDown(self) -> None:
        clear_metadata(metadata_engine)


class ProcessorProducesSinkInfoTest(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        define_metadata(metadata_engine)

    def setUp(self) -> None:
        self.processor_produces_sink_info = DataModelSampleConstructor.get_processor_produces_sink_info()

    def test_register(self) -> None:
        ProcessorProducesSinkHandler().register(self.processor_produces_sink_info)
        data = pd.read_sql_table("processor_produces_sink_info", metadata_engine)

        self.assertGreater(data.shape[0], 0, "Failed to register any processor produces sink entries ")
        self.assertLess(data.shape[0], 2, "Probably registered some excessive entries ")

        self.assertEqual(self.processor_produces_sink_info.processor_produces_sink_id,
                         data.iloc[0, :]["processor_produces_sink_id"],
                         "Ids of processor produces sink don't match ")
        self.assertEqual(self.processor_produces_sink_info.processor_info.abstract_processor_id,
                         data.iloc[0, :]["processor_id"],
                         "Abstract processor ids don't match ")
        self.assertEqual(self.processor_produces_sink_info.produces_draft_info.draft_id,
                         data.iloc[0, :]["produces_draft_id"],
                         "Draft ids don't match ")

    def test_retrieve(self) -> None:
        ProcessorProducesSinkHandler().register(self.processor_produces_sink_info)
        processor_produces_sink_info = ProcessorProducesSinkRetriever.get_info(
            self.processor_produces_sink_info.processor_produces_sink_id)

        self.assertEqual(self.processor_produces_sink_info,
                         processor_produces_sink_info,
                         "Failed to retrieve processor_produces_sink info ")

    def test_retrieve_all_dependencies(self) -> None:
        ProcessorProducesSinkHandler().register(self.processor_produces_sink_info)
        processor_produces_sink_infos = ProcessorProducesSinkRetriever.get_all_dependencies(
            self.processor_produces_sink_info.processor_info.abstract_processor_id)

        self.assertEqual(len(processor_produces_sink_infos), 1, "Failed to retrieve right number of info entries")
        self.assertEqual(processor_produces_sink_infos[0],
                         self.processor_produces_sink_info,
                         "Failed to retrieve the right entry ")

    def test_drop(self) -> None:
        ProcessorProducesSinkHandler().register(self.processor_produces_sink_info)
        ProcessorProducesSinkHandler().drop_by_id(self.processor_produces_sink_info.processor_produces_sink_id)
        data = pd.read_sql_table("processor_produces_sink_info", metadata_engine)

        self.assertEqual(data.shape[0],
                         0,
                         "Failed to remove entry ")

    def test_drop_all_dependencies(self) -> None:
        ProcessorProducesSinkHandler().register(self.processor_produces_sink_info)
        ProcessorProducesSinkHandler().drop_all_dependencies(
            self.processor_produces_sink_info.processor_info.abstract_processor_id)
        data = pd.read_sql_table("processor_produces_sink_info", metadata_engine)

        self.assertEqual(data.shape[0],
                         0,
                         "Failed to remove all entries by abstract processor's id ")

    def test_drop_all_dependencies_by_name(self) -> None:
        ProcessorProducesSinkHandler().register(self.processor_produces_sink_info)
        ProcessorProducesSinkHandler().drop_all_dependencies_by_name(
            self.processor_produces_sink_info.processor_info.abstract_processor_name)
        data = pd.read_sql_table("processor_produces_sink_info", metadata_engine)

        self.assertEqual(data.shape[0],
                         0,
                         "Failed to remove all entries by abstract processor's name ")

    def tearDown(self) -> None:
        clear_metadata(metadata_engine)


class FilledDraftsTest(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        define_metadata(metadata_engine)

    def setUp(self) -> None:
        self.filled_drafts_info = DataModelSampleConstructor.get_filled_drafts()

    def test_register(self) -> None:
        FilledDraftsInfoHandler().register(self.filled_drafts_info.dataset_implementations_info[0])
        data = pd.read_sql_table("filled_drafts_info", metadata_engine)

        self.assertGreater(data.shape[0], 0, "Failed to register any entries ")
        self.assertLess(data.shape[0], 2, "Registered excessive entries ")

        self.assertEqual(data.iloc[0, :]["dataset_implementation_id"],
                         self.filled_drafts_info.dataset_implementations_info[0].dataset_implementation_id,
                         "Dataset implementation ids don't match ")

    def test_get_all_implementations(self) -> None:
        DatasetImplementationHandler("on_read").register_with_highest_priority(self.filled_drafts_info.dataset_implementations_info[0])
        FilledDraftsInfoHandler().register(self.filled_drafts_info.dataset_implementations_info[0])
        filled_drafts_info = FilledDraftsInfoRetriever.get_all_implementations()

        self.assertEqual(self.filled_drafts_info,
                         filled_drafts_info,
                         "Failed to get all filled drafts ")

    def test_get_all_implementations_by_app(self) -> None:
        DatasetImplementationHandler("on_read").register_with_highest_priority(self.filled_drafts_info.dataset_implementations_info[0])
        FilledDraftsInfoHandler().register(self.filled_drafts_info.dataset_implementations_info[0])
        filled_drafts_info = FilledDraftsInfoRetriever.get_all_implementations_by_app(
            DataModelSampleConstructor.get_app_info())

        self.assertEqual(self.filled_drafts_info,
                         filled_drafts_info,
                         "Failed to get all filled drafts ")

    def test_drop_by_id(self) -> None:
        FilledDraftsInfoHandler().register(DataModelSampleConstructor.get_dataset_implementation_info())
        FilledDraftsInfoHandler().drop_by_id(self.filled_drafts_info.dataset_implementations_info[0].dataset_implementation_id)
        data = pd.read_sql_table("filled_drafts_info", metadata_engine)

        self.assertEqual(data.shape[0], 0, "Failed to drop info about filled draft by id ")

    def tearDown(self) -> None:
        clear_metadata(metadata_engine)




