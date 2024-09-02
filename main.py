from ads import ads_class
from feedPost import feedpost_class
from instagram_media import media
from libraries.utils import default_logger
import datetime as dt
import pandas as pd
from libraries.sentry import sentry_setup


def get_facebook_ads_comments(country):
    """
    Extracts comments from Facebook Ads for the specified country.

    Args:
        country (str): The country code for which to extract Facebook Ads comments.

    This function creates an instance of the Ads class, retrieves ads data, extracts ad creatives,
    fetches all comments, extracts sub-comments, cleans the data, and saves it.
    """

    default_logger.info("Extracting Facebook Add's comments")

    adsObject = ads_class.Ads(country)
    adsObject.get_all_ads()
    adsObject.fn_extract_adcreatives()
    adsObject.get_all_comments()
    adsObject.fn_extract_sub_comments()
    adsObject.fn_clean_data()
    adsObject.fn_save_data()

def get_facebook_post_comments(country):
    """
    Extracts comments from Facebook Feed Posts for the specified country.

    Args:
        country (str): The country code for which to extract Facebook Feed Post comments.

    This function creates an instance of the FeedPost class, retrieves feed post data,
    fetches all comments, extracts sub-comments, cleans the data, and saves it.
    """

    default_logger.info("Extracting Facebook Feed Post's comments")

    feedObjet = feedpost_class.FeedPost(country=country)
    feedObjet.get_all_feed_post()
    feedObjet.get_all_comments()
    feedObjet.fn_extract_sub_comments()
    feedObjet.fn_clean_data()
    feedObjet.fn_save_data()

def get_instagram_comments(country):
    """
    Extracts comments from Instagram Media for the specified country.

    Args:
        country (str): The country code for which to extract Instagram Media comments.

    This function creates an instance of the InstragramMedia class, retrieves media items data,
    fetches all comments, extracts replies, cleans the data, and saves it.
    """

    default_logger.info("Extracting Instagram Media's comments")

    igMediaObj = media.InstragramMedia(country=country)

    igMediaObj.get_all_media_items()
    igMediaObj.get_all_comments()
    igMediaObj.fn_extract_replies()
    igMediaObj.fn_clean_data()
    igMediaObj.fn_save_data()

@sentry_setup  
def main():

    start_process = dt.datetime.now()

    """
        Get CO comments
    """
    default_logger.info("Gathering the whole CO data")

    get_facebook_ads_comments(country='CO')
    get_facebook_post_comments(country="CO")
    get_instagram_comments(country="CO")

    end_process = dt.datetime.now()

    default_logger.info(f"CO process duration: {end_process - start_process}")

    """
        Get MX comments
    """
    default_logger.info("Gathering the whole MX data")
    
    get_facebook_ads_comments(country='MX')
    get_facebook_post_comments(country="MX")
    get_instagram_comments(country="MX")

    end_process = dt.datetime.now()

    default_logger.info(f"Total process duration: {end_process - start_process}")

if __name__ == '__main__':
    main()