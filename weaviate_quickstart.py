import weaviate
import json
import requests
import json



client = weaviate.Client(
    url = "https://ohn-sandbox-gtpeorx0.weaviate.network",  # Replace with your endpoint
    auth_client_secret=weaviate.auth.AuthApiKey(api_key="xv0JD7TPpk0YPKMGVL7i2BIo74Uk5jg0Nbfi"),  # Replace w/ your Weaviate instance API key
    additional_headers = {
        "X-OpenAI-Api-Key": "QLPLIAVVgdigDU3cBPc5T3BlbkFJ4Ogeq1HdItnzdDLRwKBg"  # Replace with your inference API key
    }
)

class_obj = {
    "class": "Question",
    "vectorizer": "text2vec-openai",  # If set to "none" you must always provide vectors yourself. Could be any other "text2vec-*" also.
    "moduleConfig": {
        "text2vec-openai": {},
        "generative-openai": {}  # Ensure the `generative-openai` module is used for generative queries
    }
}
client.schema.create_class(class_obj)

class_obj2 = {
    'class': 'CT',
    'vectorizer':"text2vec-openai",
     "moduleConfig": {
        "text2vec-openai": {},
        "generative-openai": {}  # Ensure the `generative-openai` module is used for generative queries
    }   
}


client.schema.create_class(class_obj2)



resp = requests.get('https://raw.githubusercontent.com/weaviate-tutorials/quickstart/main/data/jeopardy_tiny.json')
data = json.loads(resp.text)  # Load data

client.batch.configure(batch_size=100)  # Configure batch
with client.batch as batch:  # Initialize a batch process
    for i, d in enumerate(data):  # Batch import data
        print(f"importing question: {i+1}")
        properties = {
            "answer": d["Answer"],
            "question": d["Question"],
            "category": d["Category"],
        }
        batch.add_data_object(
            data_object=properties,
            class_name="Question"
        )


response = (
    client.query
    .get("Question", ["question", "answer", "category"])
    .with_near_text({"concepts": ["biology"]})
    .with_limit(2)
    .do()
)

print(json.dumps(response, indent=4))


response = (
    client.query
    .get("Question", ["question", "answer", "category"])
    .with_near_text({"concepts": ["biology"]})
    .with_where({
        "path": ["category"],
        "operator": "Equal",
        "valueText": "ANIMALS"
    })
    .with_limit(2)
    .do()
)

print(json.dumps(response, indent=4))