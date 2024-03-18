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
            return 0, 0

        document_count = len(documents)
        chunks_count = sum([len(document.chunks) for document in documents])
        return document_count, chunks_count

    def delete_all_by_doctype(self, doc_type: str):
        documents = self._verba_manager.retrieve_all_documents(doc_type)
        logger.info(f"Starting to delete {len(documents)} documents of type '{doc_type}'")
        for doc in documents:
            self._verba_manager.delete_document_by_id(doc['_additional']['id'])
        logger.info(f"Deletion documents of type '{doc_type}' finished")


    def _init_verba_manager(self):
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
