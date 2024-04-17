import logging
import typing

from goldenverba.verba_manager import VerbaManager
from goldenverba.server.ConfigManager import ConfigManager
from goldenverba.server.util import setup_managers

logger = logging.getLogger(__name__)

class VerbaNativeAdapter():
    def __init__(self):
        self._verba_manager = self._init_verba_manager()

    def upload_documents(
        self,
        doctype: str,
        doc_contents: typing.List[str],
        doc_names: typing.List[str],
        units: int = 100,
        overlap: int = 50,
    ):
        assert len(doc_contents) == len(doc_names)
        logger.info(f"Inserting {len(doc_contents)} documents to Weaviate...")
        documents = self._verba_manager.import_data(
            [],
            doc_contents,
            [''],
            doc_names,
            doctype,
            units,
            overlap,
        )
        if not documents:
            document_count = chunks_count = 0
        else:
            document_count = len(documents)
            chunks_count = sum([len(document.chunks) for document in documents])
        logger.info(f"{document_count} docs and {chunks_count} chunks have been inserted to Weaviate")
        return document_count, chunks_count

    def delete_documents_by(self, doc_type: str, doc_name: str):
        if doc_name == '':
            documents = self._verba_manager.retrieve_all_documents(doc_type)
        else:
            documents = self._verba_manager.search_documents(doc_name, doc_type)
        logger.info(f"Starting to delete {len(documents)} documents of type '{doc_type}' and name '{doc_name}'")
        for doc in documents:
            self._verba_manager.delete_document_by_id(doc['_additional']['id'])
        logger.info(f"Deletion documents of type '{doc_type}' and name '{doc_name}' finished")

    def retrieve_all_documents(self, doc_type: str = ''):
        logger.info(f"Retrieving all the documents from Weaviate...")
        result = self._verba_manager.retrieve_all_documents_without_limitation()
        if doc_type:
            return [i for i in result if i['doc_type'] == doc_type]
        else:
            return result

    def update_document(self, old_doc_uuid, doc_type, new_doc_name, new_doc_content):
        self._verba_manager.delete_document_by_id(old_doc_uuid)
        self.upload_documents(doc_type, [new_doc_content], [new_doc_name])

    @staticmethod
    def _init_verba_manager():
        manager = VerbaManager()
        config_manager = ConfigManager()
        readers = manager.reader_get_readers()
        chunker = manager.chunker_get_chunker()
        embedders = manager.embedder_get_embedder()
        retrievers = manager.retriever_get_retriever()
        generators = manager.generator_get_generator()
        setup_managers(
            manager, config_manager, readers, chunker, embedders, retrievers, generators
        )
        # config_manager.save_config()
        return manager
