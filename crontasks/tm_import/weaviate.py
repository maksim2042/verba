import weaviate
import weaviate.classes as wvc
import os


def get_weaviate_client():
    if not hasattr(get_weaviate_client, 'client'):
        WCS_CLUSTER_URL = os.environ['WCS_CLUSTER_URL']
        WCS_API_KEY = os.environ['WCS_API_KEY']
        OPENAI_APIKEY = os.environ['OPENAI_API_KEY']

        get_weaviate_client.client = weaviate.connect_to_wcs(
            cluster_url=WCS_CLUSTER_URL,
            auth_credentials=weaviate.auth.AuthApiKey(WCS_API_KEY),
            headers={
                "X-OpenAI-Api-Key": OPENAI_APIKEY
            }
        )

    return get_weaviate_client.client



def get_or_create_collection(db_name):
    client = get_weaviate_client()
    if not client.collections.exists(db_name):
        return client.collections.create(
            name=db_name,
            vectorizer_config=wvc.config.Configure.Vectorizer.text2vec_openai(),  # If set to "none" you must always provide vectors yourself. Could be any other "text2vec-*" also.
            generative_config=wvc.config.Configure.Generative.openai()  # Ensure the `generative-openai` module is used for generative queries
        )

    return client.collections.get(db_name)
