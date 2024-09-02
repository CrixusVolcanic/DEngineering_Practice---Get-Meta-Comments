import requests
import json
import pandas as pd
from libraries.utils import default_logger
from libraries.bq_utils import save_table
import os
import base64

class FeedPost:
    """
    A class to handle Facebook Feed Posts data extraction, processing, and storage.

    Attributes:
        access_token (str): The access token for Facebook API.
        account_id (str): The Facebook account ID.
        page_id (str): The Facebook page ID.
        country (str): The country code.
        df_feed (pd.DataFrame): DataFrame to store feed post data.
        df_principal_comments (pd.DataFrame): DataFrame to store principal comments.
        df_sub_comments (pd.DataFrame): DataFrame to store sub-comments.

    Example:
        export account_id=4815449846
        export token=xxxx
    """

    def __init__(self, country = None) -> None:
        """
        Initializes the FeedPost class with configuration based on the specified country.

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
        self.account_id = config.get("account_id")
        self.page_id = config.get("page_id")
        self.country = country

        default_logger.info(f"\tCountry set with {self.country}")

        self.df_feed = pd.DataFrame()
        self.df_principal_comments = pd.DataFrame()
        self.df_sub_comments = pd.DataFrame()


    def get_feed_post(self, paging_url = None):
        """
        Fetches feed post data from Facebook API.

        Args:
            paging_url (str, optional): URL to fetch the next page of results.

        Returns:
            dict: The JSON response from the Facebook API.

        Raises:
            requests.exceptions.HTTPError: If the HTTP request fails.
        """

        if paging_url:
            response = requests.get(paging_url)

        else:

            url = f'https://graph.facebook.com/v20.0/{self.page_id}/feed'
            params = {"access_token" : self.access_token
                      ,"fields": ("id,created_time,shares,is_published,is_hidden,message,permalink_url")
                      ,"limit": 100
            }
            
            response = requests.get(url, params=params)

        response.raise_for_status()
        return response.json()
    
    def get_all_feed_post(self):
        """
        Retrieves all feed posts data, including paginated results, and stores it in a DataFrame.
        """

        feeds = []
        page = 1

        default_logger.info("\tTriying to get all feed post")

        try:

            first_response = self.get_feed_post()
            feeds.extend(f for f in first_response['data'])
            url = first_response.get('paging', {}).get('next')
            page = 1

            while url:

                response = self.get_feed_post(paging_url=url)
                feeds.extend(f for f in response['data'])
                url = response.get('paging', {}).get('next')
                page += 1

                """ if page == 2:
                    break """

            df = pd.json_normalize(feeds)
            df = df.sort_values(by='created_time', ascending=False)
            df["message"] = df["message"].str.replace("\n", ' ')

            df.rename(columns={'shares.count': 'shares'
                               ,"permalink_url": "url"}, inplace=True)
            df["shares"] = df["shares"].astype('int', errors="ignore")

            default_logger.info(f"\tFeed Dataframe's shape {df.shape}")

            # df.to_csv("results/feed_post_raw.csv", index=False)

            self.df_feed = df
    
        except requests.exceptions.HTTPError as http_error:
            
            if http_error.response is not None:
                error_info = http_error.response.json()
                default_logger.error(json.dumps(error_info, indent=2))

        except Exception as err:
            
            default_logger.error(f"Other error occured {err}")

    def get_comments(self, post_id = None, paging_url = None):
        """
        Fetches comments data from Facebook API for a given post.

        Args:
            post_id (str, optional): The post ID to fetch comments for.
            paging_url (str, optional): URL to fetch the next page of results.

        Returns:
            dict: The JSON response from the Facebook API.

        Raises:
            requests.exceptions.HTTPError: If the HTTP request fails.
        """

        if paging_url:
            url = paging_url
        else:
            url = f"https://graph.facebook.com/v20.0/{post_id}/comments"

        params = {
            "access_token": self.access_token,
            "fields": (
                "effective_object_story_id,comment_count,created_time,from,id,is_hidden,like_count,message"
                ",comments{created_time,from,id,is_hidden,is_private,like_count,message,user_likes}"
                ",permalink_url"
            ),
            "limit": 100,
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    
    def get_all_comments(self):
        """
        Retrieves all comments for feed posts and stores them in a DataFrame.
        """

        default_logger.info(f"\tTrying to get all feed post comments")

        try:

            comments = []

            for post in self.df_feed.itertuples():
                post_id = post.id

                if True: # post_id in ('120470019348648_1054737376278681'):
                    page = 1
                    # default_logger.info(f"\tIterating in all feed post ... post_id: {post_id}, page: {page}")
                    first_response = self.get_comments(post_id=post_id)

                    for c in first_response["data"]:
                        c['post_id'] = post_id
                        comments.append(c) 
                    url = first_response.get("paging", {}).get("next")

                    while url:
                        page += 1
                        response = self.get_comments(paging_url=url)
                        for c in response["data"]:
                            c['post_id'] = post_id
                            comments.append(c) 
                        url = response.get("paging", {}).get("next")
                        # default_logger.info(f"\tIterating in all feed post ... post_id: {post_id}, page: {page}")


            df = pd.json_normalize(comments)

            df = df.sort_values("created_time", ascending=False)

            # Lista de columnas a eliminar
            columns_to_drop = ['comments.paging.cursors.before'
                                ,'comments.paging.cursors.after'
                                ,'from.name'
                                ,'from.id'
                                ,'comments.paging.next']

            columns_existing = [col for col in columns_to_drop if col in df.columns]
            df.drop(columns=columns_existing, inplace=True)
            df.drop(columns=[], inplace=True)

             
            df.rename(columns={'comments.data': 'sub_comments'
                               ,'permalink_url': 'url'}, inplace=True)
            df['message'] = df['message'].str.replace('\n', ' ')
            # df.to_csv("results/principal_comments.csv", index=False, encoding='utf-8')

            self.df_principal_comments = df

            default_logger.info(f"\tPrincipal comments Dataframe's shape {self.df_principal_comments.shape}")

        except requests.exceptions.HTTPError as http_error:
            if http_error.response is not None:
                error_info = http_error.response.json()
                default_logger.error(json.dumps(error_info, indent=2))

        except Exception as err:
            default_logger.error(f"Other error occurred: {err}")  # Otros errores

    def fn_extract_sub_comments(self):
        """
        Extracts sub-comments from the principal comments DataFrame and stores them in a separate DataFrame.
        """

        default_logger.info(f"\tExtracting all feed post sub comments")

        df_filtered = self.df_principal_comments[self.df_principal_comments['sub_comments'].notnull()]

        sub_comments = [
            {
                'comment_parent_id': row.id,
                'id': sc.get('id'),
                'created_time': sc.get('created_time'),
                'is_hidden': sc.get('is_hidden'),
                'is_private': sc.get('is_private'),
                'like_count': sc.get('like_count'),
                'message': sc.get('message'),
                'user_likes': sc.get('user_likes'),
            }
            for row in df_filtered.itertuples()
            for sc in row.sub_comments
        ]

        self.df_sub_comments = pd.DataFrame(sub_comments)

        self.df_sub_comments['message'] = self.df_sub_comments['message'].str.replace('\n',' ')

        default_logger.info(f"\tSub comments Dataframe's shape {self.df_sub_comments.shape}")

        self.df_principal_comments.drop(columns=['sub_comments'], inplace=True)

        # self.df_sub_comments.to_csv("results/sub_comments.csv", index = False)

    def fn_clean_data(self):
        """
        Cleans the feed post, principal comments, and sub-comments data.

        This method performs cleaning operations such as converting data types, 
        filling missing values, and ensuring consistency in the data format.
        """

        default_logger.info("\tCleaning data")
        
        # df_feed section
        self.df_feed['created_time'] = pd.to_datetime(self.df_feed['created_time'])
        self.df_feed['shares'] = self.df_feed['shares'].fillna(0).astype("int")

        columns = ['message', 'url', 'id']
        self.df_feed[columns] = self.df_feed[columns].astype("string")

        columns = ['is_published', 'is_hidden']
        self.df_feed[columns] = self.df_feed[columns].astype("bool")

        self.df_feed.drop_duplicates(inplace=True)

        # df_principal_comments section
        self.df_principal_comments['created_time'] = pd.to_datetime(self.df_principal_comments['created_time'])

        columns = ['comment_count', 'like_count']
        self.df_principal_comments[columns] = self.df_principal_comments[columns].fillna(0).astype("int")

        columns = ['post_id', 'url', 'message', 'id']
        self.df_principal_comments[columns] = self.df_principal_comments[columns].astype("string")

        self.df_principal_comments['is_hidden'] = self.df_principal_comments['is_hidden'].astype("bool")

        self.df_principal_comments.drop_duplicates(inplace=True)

        # df_sub_comments section
        self.df_sub_comments['created_time'] = pd.to_datetime(self.df_sub_comments['created_time'])

        columns = ['comment_parent_id', 'id', 'message']
        self.df_sub_comments[columns] = self.df_sub_comments[columns].astype("string")

        columns = ['is_hidden', 'is_private', 'user_likes']
        self.df_sub_comments[columns] = self.df_sub_comments[columns].astype("bool")
        
        self.df_sub_comments['like_count'] = self.df_sub_comments['like_count'].astype("int")

        self.df_sub_comments.drop_duplicates(inplace=True)

    def fn_save_data(self):
        """
        Saves the cleaned data to BigQuery tables.
        """

        default_logger.info("\tSaving data on BigQuery")

        PROJECT = os.getenv("BQ_PROJECT")
        DATASET = 'meta_comments'
        TABLE_NAME_POST = f'{self.country}_facebook_posts'
        TABLE_NAME_POST_COMMENTS = f'{self.country}_facebook_posts_comments'
        TABLE_NAME_POST_SUBCOMMENTS = f'{self.country}_facebook_posts_sub_comments'

        try:
        
            save_table(df=self.df_feed, project=PROJECT, dataset=DATASET, table_name=TABLE_NAME_POST)
            save_table(df=self.df_principal_comments, project=PROJECT, dataset=DATASET, table_name=TABLE_NAME_POST_COMMENTS)
            save_table(df=self.df_sub_comments, project=PROJECT, dataset=DATASET, table_name=TABLE_NAME_POST_SUBCOMMENTS)

        except Exception as err:
                default_logger.error(f"\tError saving data: {err}")