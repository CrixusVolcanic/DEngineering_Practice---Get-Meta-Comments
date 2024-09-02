import requests 
import json
import pandas as pd
from libraries.utils import default_logger, retry_on_rate_limit
import os
import traceback
import base64
from libraries.bq_utils import save_table

class Ads:
    """
    A class to handle Facebook Ads data extraction and processing.

    Attributes:
        access_token (str): The access token for Facebook API.
        account_id (str): The Facebook account ID.
        page_id (str): The Facebook page ID.
        country (str): The country code.
        df_ads (pd.DataFrame): DataFrame to store ad data.
        df_ads_creative (pd.DataFrame): DataFrame to store ad creative data.
        df_principal_comments (pd.DataFrame): DataFrame to store principal comments.
        df_sub_comments (pd.DataFrame): DataFrame to store sub-comments.
    """

    def __init__(self, country = None) -> None:
        """
        Initializes the Ads class with configuration based on the specified country.

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

        self.df_ads = pd.DataFrame()
        self.df_ads_creative = pd.DataFrame()
        self.df_principal_comments = pd.DataFrame()
        self.df_sub_comments = pd.DataFrame()

    @retry_on_rate_limit(max_retries=5, initial_backoff=60)
    def get_ads(self, paging_url = None):
        """
        Fetches ads data from Facebook API.

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

            url = f"https://graph.facebook.com/v20.0/act_{self.account_id}/ads"
            params = {
                "access_token": self.access_token,
                "fields": (
                    "name,id,campaign_id,adset_id,created_time,source_ad_id,"
                    "adcreatives{object_url,name,link_url,id,body,"
                    "effective_object_story_id,link_destination_display_url,likes_count,created_time}"
                ),
                "limit": 100,
            }

            response = requests.get(url, params=params)

        response.raise_for_status()
        return response.json()
    
    def get_all_ads(self):
        """
        Retrieves all ads data, including paginated results, and stores it in a DataFrame.
        """

        ads = []
        page = 1

        default_logger.info(f"\tTrying to get all ads")

        try:

            # default_logger.info(f"\tIterating in get all ads ... page {page}")
            first_response = self.get_ads()
            ads.extend(a for a in first_response['data'])
            url = first_response.get("paging", {}).get("next")

            while url:
                page += 1 
                response_data = self.get_ads(paging_url=url)
                ads.extend(a for a in response_data['data'])
                url = response_data.get("paging", {}).get("next")

                """ if page == 2:
                    break """
                # default_logger.info(f"\tIterating in get all ads ... page {page}")

            df = pd.json_normalize(ads)
            df = df.sort_values(by='created_time', ascending=False)

            default_logger.info(f"\tAds Dataframe's shape {df.shape}")

            df['adcreatives_list'] = df['adcreatives.data']
            # df.to_csv("results/ads_raw.csv", index=False)
            self.df_ads = df

        except requests.exceptions.HTTPError as http_err:
            if http_err.response is not None:
                error_info = http_err.response.json()
                default_logger.error(json.dumps(error_info, indent=2))

        except Exception as err:
            default_logger.error(f"Other error occurred: {err}")  # Otros errores


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

        try:
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
        except requests.exceptions.HTTPError as http_error:
            if http_error.response is not None:
                error_info = http_error.response.json()['error']
                default_logger.error(f"Error in post_id: {post_id}, the error was {error_info['message']}")

            return {"data":""}
    
    def get_all_comments(self):
        """
        Retrieves all comments for ads and stores them in a DataFrame.
        """

        default_logger.info(f"\tTrying to get all comments")

        try:

            comments = []

            for post in self.df_ads_creative.itertuples():
            #for post in posts:
                post_id = post.post_id
                #post_id = post

                if True: #post_id in ('239813012550995_122145487328232502'):
                    page = 1
                    # default_logger.info(f"\tIterating in all ads creative ... post_id: {post_id}, page: {page}")
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
                        # default_logger.info(f"\tIterating in all ads creative ... post_id: {post_id}, page: {page}")

            df = pd.json_normalize(comments)
            df = df.sort_values("created_time", ascending=False)
            # Lista de columnas que quieres eliminar
            columns_to_drop = ['comments.paging.cursors.before'
                                ,'comments.paging.cursors.after'
                                ,'from.name'
                                ,'from.id'
                                ,'comments.paging.next']

            columns_existing = [col for col in columns_to_drop if col in df.columns]
            df.drop(columns=columns_existing, inplace=True)
            df.drop(columns=[], inplace=True)
            df.rename(columns={'comments.data': 'sub_comments'}, inplace=True)
            df['message'] = df['message'].str.replace('\n', ' ')
            # df.to_csv("results/principal_comments.csv", index=False, encoding='utf-8')

            self.df_principal_comments = df

            default_logger.info(f"\tPrincipal comments Dataframe's shape {self.df_principal_comments.shape}")

        except Exception as err:
            default_logger.error(f"Other error occurred: {err}")  # Otros errores
            default_logger.error(traceback.format_exc())

    def fn_extract_adcreatives(self):
        """
        Extracts ad creative data from the ads DataFrame and stores it in a separate DataFrame.
        """

        default_logger.info(f"\tExtracting all adcreatives")

        ads_creative = [
            {
                'ads_id': row.id,
                'post_id': ad.get('effective_object_story_id'),
                'name': ad.get('name'),
                'body': ad.get('body')
            }
            for row in self.df_ads.itertuples()
            for ad in row.adcreatives_list
        ]

        self.df_ads_creative = pd.DataFrame(ads_creative)

        default_logger.info(f"\tAds creative Dataframe's shape {self.df_ads_creative.shape}")

        # self.df_ads_creative.to_csv("results/ads_creative.csv", index = False)

    def fn_extract_sub_comments(self):
        """
        Extracts sub-comments from the principal comments DataFrame and stores them in a separate DataFrame.
        """

        default_logger.info(f"\tExtracting all sub comments")

        if 'sub_comments' in self.df_principal_comments.columns:

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

            # self.df_sub_comments.to_csv("results/sub_comments.csv", index = False)


    def fn_clean_data(self):
        """
        Cleans the ad creative and comments data.

        This method performs cleaning operations on the ad creative and comments data,
        such as removing special characters and ensuring consistency in the data format.
        """

        default_logger.info("\tCleaning data")

        # df_ads section
        self.df_ads["name"] = self.df_ads["name"].astype("string")
        self.df_ads["created_time"] = pd.to_datetime(self.df_ads["created_time"])
        columns = ['id', 'campaign_id', 'adset_id', 'source_ad_id']
        self.df_ads[columns] = self.df_ads[columns].astype("int")
        self.df_ads.drop(columns=['adcreatives_list'], inplace=True)
        self.df_ads.drop(columns=['adcreatives.data'], inplace=True)
        self.df_ads.drop_duplicates(inplace=True)

        # df_ads_creative section
        self.df_ads_creative['ads_id'] = self.df_ads_creative['ads_id'].astype("int")
        columns = ['name', 'body', 'post_id']
        self.df_ads_creative[columns] = self.df_ads_creative[columns].astype("string")
        self.df_ads_creative.drop_duplicates(inplace=True)

        # df_principal_comments section
        columns = ['comment_count', 'like_count']
        self.df_principal_comments[columns] = self.df_principal_comments[columns].astype("int")

        columns = ['message', 'permalink_url', 'post_id', "id"]
        self.df_principal_comments[columns] = self.df_principal_comments[columns].astype("string")

        self.df_principal_comments.rename({"permalink_url": "url"}, inplace=True)

        self.df_principal_comments["created_time"] = pd.to_datetime(self.df_principal_comments["created_time"])
        self.df_principal_comments["is_hidden"] = self.df_principal_comments["is_hidden"].astype("bool")

        self.df_principal_comments.drop(columns=['sub_comments'], inplace=True)
        self.df_principal_comments.drop_duplicates(inplace=True)

        # df_sub_comments section
        columns = ['like_count']
        self.df_sub_comments[columns] = self.df_sub_comments[columns].astype("int")

        columns = ['is_hidden', 'is_private', 'user_likes']
        self.df_sub_comments[columns] = self.df_sub_comments[columns].astype("bool")

        columns = ['message', 'comment_parent_id', 'id']
        self.df_sub_comments[columns] = self.df_sub_comments[columns].astype("string")

        self.df_sub_comments["created_time"] = pd.to_datetime(self.df_sub_comments["created_time"])
        self.df_sub_comments.drop_duplicates(inplace=True)

    def fn_save_data(self):
        """
        Saves the data from the DataFrames to BigQuery tables.
        """

        default_logger.info("\tSaving data on BigQuery")

        try:

            PROJECT = os.getenv("BQ_PROJECT")
            DATASET = 'meta_comments'
            TABLE_NAME_ADS = f'{self.country}_facebook_ads'
            TABLE_NAME_ADS_CREATIVES = f'{self.country}_facebook_ads_creatives'
            TABLE_NAME_ADS_CREATIVES_COMMENTS = f'{self.country}_facebook_ads_creatives_comments'
            TABLE_NAME_ADS_CREATIVES_SUBCOMMENTS = f'{self.country}_facebook_ads_creatives_sub_comments'

            save_table(df=self.df_ads, project=PROJECT, dataset=DATASET, table_name=TABLE_NAME_ADS)
            save_table(df=self.df_ads_creative, project=PROJECT, dataset=DATASET, table_name=TABLE_NAME_ADS_CREATIVES)
            save_table(df=self.df_principal_comments, project=PROJECT, dataset=DATASET, table_name=TABLE_NAME_ADS_CREATIVES_COMMENTS)
            save_table(df=self.df_sub_comments, project=PROJECT, dataset=DATASET, table_name=TABLE_NAME_ADS_CREATIVES_SUBCOMMENTS)

        except Exception as err:
            default_logger.error(f"\tError saving data: {err}")  # Otros errores