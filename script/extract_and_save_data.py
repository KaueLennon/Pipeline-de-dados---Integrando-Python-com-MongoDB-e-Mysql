from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import requests

def connect_mongo(uri):
    # Create a new client and connect to the server
    client = MongoClient(uri)

    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)

    return client

def create_connect_db(client, db_name):
    db = client[db_name] #Criando um banco de dados
    return db

def create_connect_collection(db, col_name):
    collection = db[col_name] #Criando uma coleção (que fica dentro do banco de dados, nesse caso é uma coleção PRODUTOS)
    return collection

def extract_api_data(url):
    return requests.get(url).json()

def insert_data(col, data):
    docs = col.insert_many(data)
    n_docs_inseridos = len(docs.inserted_ids)
    return n_docs_inseridos

if __name__ == "__main__":
    client = connect_mongo("mongodb+srv://kauelennon10:12345@cluster-pipeline.tfuu73h.mongodb.net/?retryWrites=true&w=majority&appName=Cluster-pipeline")
    db = create_connect_db(client, "db_produtos_desafio")
    col = create_connect_collection(db, "produtos")
    data = extract_api_data("https://labdados.com/produtos")
    print(f"\nQuantidade de dados extraidos: {len(data)}")
    n_docs = insert_data(col, data)
    print(f"\nQuantidade de documentos extraidos: {n_docs}")
    client.close()