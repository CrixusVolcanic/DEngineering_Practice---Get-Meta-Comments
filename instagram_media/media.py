import requests
from libraries.utils import default_logger
from libraries.bq_utils import save_table
import pandas as pd
import json
import traceback
import os
import base64

class InstragramMedia:
    """
    A class to handle Instagram Media data extraction, processing, and storage.

    Attributes:
        access_token (str): The access token for Instagram API.
        ig_business_account_id (str): The Instagram Business Account ID.
        page_id (str): The Facebook page ID.
        country (str): The country code.
        df_media_items (pd.DataFrame): DataFrame to store media items.
        df_comments (pd.DataFrame): DataFrame to store comments on media items.
        df_replies (pd.DataFrame): DataFrame to store replies to comments.

    Example:
        export country=US
        export token=xxxx
        export ig_business_account_id=xxxx
    """

    def __init__(self, country = None) -> None:
        """
        Initializes the InstragramMedia class with configuration based on the specified country.

        Args:
            country (str): The country code to fetch configuration for.

        Raises:
            ValueError: If META_COUNTRY_CONFIG environment variable is missing or if 
                        the specified country is not in the configuration.
        """

        # Obtén la configuración del país desde la variable de entorno codificada en Base64
        encoded_config = os.getenv("META_COUNTRY_CONFIG")
        
        if not encoded_config:
            raise ValueError("META_COUNTRY_CONFIG environment variable is missing.")
        
        # Decodifica la configuración
        decoded_config = base64.b64decode(encoded_config).decode()
        
        # Convierte el JSON decodificado en un diccionario
        country_config = json.loads(decoded_config)

        # Verifica si el país está en la configuración
        if country not in country_config:
            raise ValueError(f"Configuration for country '{country}' is not defined in COUNTRY_CONFIG.")

        # Selecciona la configuración basada en el país
        config = country_config[country]

        self.access_token = config.get("access_token")
        self.ig_business_account_id = config.get("ig_business_account_id")
        self.page_id = config.get("page_id")
        self.country = country

        default_logger.info(f"\tCountry set with {self.country}")

        self.df_media_items = pd.DataFrame()
        self.df_comments = pd.DataFrame()
        self.df_replies = pd.DataFrame()

    def get_media_items(self, paging_url = None):
        """
        Fetches media items data from Instagram API.

        Args:
            paging_url (str, optional): URL to fetch the next page of results.

        Returns:
            dict: The JSON response from the Instagram API.

        Raises:
            requests.exceptions.HTTPError: If the HTTP request fails.
        """

        if paging_url:
            response = requests.get(paging_url)

        else:

            url = f'https://graph.facebook.com/v20.0/{self.ig_business_account_id}/media'
            params = {"access_token" : self.access_token
                      ,"fields": ("fields=id,comments_count,permalink,media_type"
                                  ",like_count,ig_id,timestamp,caption,is_shared_to_feed,media_product_type")
                      ,"limit": 100
            }
            
            response = requests.get(url, params=params)

        response.raise_for_status()
        return response.json()
    
    def get_all_media_items(self):
        """
        Retrieves all media items data, including paginated results, and stores it in a DataFrame.
        """

        media_items = []
        page = 1

        default_logger.info("\tTriying to get all media items")

        try:

            first_response = self.get_media_items()
            media_items.extend(f for f in first_response['data'])
            url = first_response.get('paging', {}).get('next')

            while url:

                response = self.get_media_items(paging_url=url)
                media_items.extend(f for f in response['data'])
                url = response.get('paging', {}).get('next')
                page += 1

                """ if page == 10:
                    break """

            df = pd.json_normalize(media_items)
            df = df.sort_values(by='timestamp', ascending=False)
            df["caption"] = df["caption"].str.replace("\n", ' ')

            default_logger.info(f"\tMedia items Dataframe's shape {df.shape}")

            # df.to_csv("results/media_items_raw.csv", index=False)

            self.df_media_items = df
    
        except requests.exceptions.HTTPError as http_error:
            
            if http_error.response is not None:
                error_info = http_error.response.json()
                default_logger.error(json.dumps(error_info, indent=2))

        except Exception as err:
            
            default_logger.error(f"Other error occured {err}")

    def get_comments(self, media_id = None, paging_url = None):
        """
        Fetches comments data from Instagram API for a given media item.

        Args:
            media_id (str, optional): The media item ID to fetch comments for.
            paging_url (str, optional): URL to fetch the next page of results.

        Returns:
            dict: The JSON response from the Instagram API.

        Raises:
            requests.exceptions.HTTPError: If the HTTP request fails.
        """

        if paging_url:
            url = paging_url
        else:
            url = f"https://graph.facebook.com/v20.0/{media_id}/comments"

        params = {
            "access_token": self.access_token,
            "fields": (
                "hidden,id,like_count,text,timestamp,username"
                ",replies.limit(100){hidden,id,like_count,text,timestamp,username,parent_id}"
            ),
            "limit": 100,
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    
    def get_all_comments(self):
        """
        Retrieves all comments for media items and stores them in a DataFrame.
        """

        default_logger.info(f"\tTrying to get all media comments")

        try:

            comments = []

            for media in self.df_media_items.itertuples():
                media_id = media.id

                if media.comments_count != 0:

                    page = 1
                    # default_logger.info(f"\tIterating in all media ... media_id: {media_id}, page: {page}")
                    first_response = self.get_comments(media_id=media_id)

                    for c in first_response["data"]:
                        c['media_id'] = media_id
                        comments.append(c) 
                    url = first_response.get("paging", {}).get("next")

                    while url:
                        page += 1
                        response = self.get_comments(paging_url=url)
                        for c in response["data"]:
                            c['media_id'] = media_id
                            comments.append(c) 
                        url = response.get("paging", {}).get("next")
                        # default_logger.info(f"\tIterating in all media ... media_id: {media_id}, page: {page}")


            df = pd.json_normalize(comments)

            df.rename(columns={'timestamp': 'created_time'
                               ,'text': 'message'
                               ,'replies.data': 'replies'}, inplace=True)
            
            df['message'] = df['message'].str.replace('\n', ' ')
            df = df.sort_values("created_time", ascending=False)

            # df.to_csv("results/ig_principal_comments.csv", index=False, encoding='utf-8')

            self.df_comments = df

            default_logger.info(f"\tComments Dataframe's shape {self.df_comments.shape}")

        except requests.exceptions.HTTPError as http_error:
            if http_error.response is not None:
                error_info = http_error.response.json()
                default_logger.error(json.dumps(error_info, indent=2))

        except Exception as err:
            default_logger.error(f"Other error occurred: {err}")  # Otros errores
            default_logger.error(traceback.format_exc())

    def fn_extract_replies(self):
        """
        Extracts replies from the comments DataFrame and stores them in a separate DataFrame.
        """

        default_logger.info(f"\tExtracting replies")

        df_filtered = self.df_comments[self.df_comments['replies'].notnull()]

        replies = [
            {
                'comment_parent_id': sc.get('parent_id'),
                'id': sc.get('id'),
                'created_time': sc.get('timestamp'),
                'hidden': sc.get('hidden'),
                'like_count': sc.get('like_count'),
                'message': sc.get('text'),
                'username': sc.get('username'),
            }
            for row in df_filtered.itertuples()
            for sc in row.replies
        ]

        self.df_replies = pd.DataFrame(replies)

        self.df_replies['message'] = self.df_replies['message'].str.replace('\n',' ')
        self.df_replies["created_time"] = pd.to_datetime(self.df_replies["created_time"])

        default_logger.info(f"\tReplies Dataframe's shape {self.df_replies.shape}")

        self.df_comments.drop(columns=['replies'], inplace=True)

        # self.df_replies.to_csv("results/ig_replies.csv", index = False)

    def fn_clean_data(self):
        """
        Cleans the media items, comments, and replies data.

        This method performs cleaning operations such as converting data types, 
        filling missing values, and ensuring consistency in the data format.
        """

        default_logger.info("\tCleaning data")

        # df_media_items section
        self.df_media_items.rename(columns={'timestamp': 'created_time'
                                    ,"permalink": "url"}, inplace=True)
        self.df_media_items["created_time"] = pd.to_datetime(self.df_media_items["created_time"])
        
        columns = ['caption', 'ig_id', 'url', 'media_type', 'media_product_type', 'id']
        self.df_media_items[columns] = self.df_media_items[columns].astype("string")

        columns = ['comments_count', 'like_count']
        self.df_media_items[columns] = self.df_media_items[columns].astype("int")

        self.df_media_items['is_shared_to_feed'] = self.df_media_items['is_shared_to_feed'].astype("bool")
        self.df_media_items.drop_duplicates(inplace=True)
        
        # df_comments section

        self.df_comments["created_time"] = pd.to_datetime(self.df_comments["created_time"])

        columns = ['id', 'message', 'username', 'media_id']
        self.df_comments[columns] = self.df_comments[columns].astype("string")

        self.df_comments['like_count'] = self.df_comments['like_count'].astype("int")
        self.df_comments['hidden'] = self.df_comments['hidden'].astype("bool")
        self.df_comments.drop_duplicates(inplace=True)

        # df_replies section
        self.df_replies["created_time"] = pd.to_datetime(self.df_replies["created_time"])

        columns = ['comment_parent_id', 'message', 'username', 'id']
        self.df_replies[columns] = self.df_replies[columns].astype("string")

        self.df_replies['like_count'] = self.df_replies['like_count'].astype("int")
        self.df_replies['hidden'] = self.df_replies['hidden'].astype("bool")
        self.df_replies.drop_duplicates(inplace=True)

    def fn_save_data(self):
        """
        Saves the cleaned media items, comments, and replies data to BigQuery tables.

        Uses environment variables to determine the project and dataset to save the data.
        
        Raises:
            Exception: If an error occurs during the saving process.
        """

        default_logger.info("\tSaving data on BigQuery")

        PROJECT = os.getenv("BQ_PROJECT")
        DATASET = 'meta_comments'
        TABLE_NAME_MEDIA = f'{self.country}_instragram_media'
        TABLE_NAME_MEDIA_COMMENTS = f'{self.country}_instragram_media_comments'
        TABLE_NAME_MEDIA_REPLIES = f'{self.country}_instragram_media_replies'

        try:
            save_table(df=self.df_media_items, project=PROJECT, dataset=DATASET, table_name=TABLE_NAME_MEDIA)
            save_table(df=self.df_comments, project=PROJECT, dataset=DATASET, table_name=TABLE_NAME_MEDIA_COMMENTS)
            save_table(df=self.df_replies, project=PROJECT, dataset=DATASET, table_name=TABLE_NAME_MEDIA_REPLIES)

        except Exception as err:
                default_logger.error(f"\tError saving data: {err}")  # Otros errores