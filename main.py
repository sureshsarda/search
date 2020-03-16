import elasticsearch
import pandas as pd
import json

client = elasticsearch.Elasticsearch()


def read_data():
    df = pd.read_csv('data/tmdb-movie-metadata/tmdb_5000_movies.csv')
    mini_df = df[['title', 'tagline']]
    mini_df.info()
    return json.loads(mini_df.to_json(orient='records'))



def drop_index_and_create_new():
    client.indices.delete(index='rnd', ignore_unavailable=True)

    with open('index_mapping.json') as f:
        content = f.read()
        print(content)
    client.indices.create(index='rnd', body=content)

    for r in read_data():
        print(r)
        client.index(index='rnd', body=r)


if __name__ == '__main__':
    # drop_index_and_create_new()
    drop_index_and_create_new()