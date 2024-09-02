"""Libraries for google.cloud"""

import json
import os
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig
from libraries.utils import get_secrets_sellers


dict_credentials = get_secrets_sellers().get("bigquery")


def save_dict_to_json_file(dictionary: dict, file_name: str) -> str:
    """Save dictionary content to json file"""
    with open(file_name, 'w', encoding='utf-8') as f:
        json.dump(dictionary, f, ensure_ascii=False)

    return file_name


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = save_dict_to_json_file(
        dict_credentials,
        "file.json"
    )


def get_bq_client(project):
    """Get client by project"""

    return bigquery.Client(project=project)


def get_bg_config(project, data_types_schema, write_disposition, autodetect):
    """Configure project and write disposition"""

    __client_bg = get_bq_client(project)
    __job_config = LoadJobConfig(schema=data_types_schema,
                                 write_disposition=write_disposition,
                                 autodetect=autodetect
                            )

    return __client_bg, __job_config


def save_table(df,
               project,
               dataset,
               table_name,
               data_types_schema=[],
               write_disposition='WRITE_TRUNCATE',
               autodetect=True
            ):
    """Save df to biguquery table"""
    __client_bg, __job_config = get_bg_config(project, data_types_schema, write_disposition, autodetect)

    __client_bg.load_table_from_dataframe(df,
                                          f'{project}.{dataset}.{table_name.lower()}',
                                          job_config=__job_config).result()


def execute_query_bigquery(project, query):
    """Execute bigquery query"""

    __client_bg = get_bq_client(project)
    query_job = __client_bg.query(query)

    return query_job.result().to_dataframe()