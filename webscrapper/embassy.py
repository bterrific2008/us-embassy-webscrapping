#!/usr/bin/env python3

import hashlib
import os
import string
import logging

import bs4
import requests
from bs4 import BeautifulSoup


def get_embassy_posts(url: str, page_number: int=1, page_count: int=10):
    """Retrieves the posts of an embassy website

    Args:
        url (str): the url of the embassy website

    Returns:
        (list) a list of all post urls for the embassy website
    """
    logging.info(f"[EMBASSY SCRAPE] Retrieving {page_count} posts of page {page_number} from the website {url}")
    posts_url = f"{url}/wp-json/wp/v2/posts?per_page={page_count}&page={page_number}"
    posts_request = requests.get(posts_url)
    total_page_number = posts_request.headers['X-WP-TotalPages']
    posts = posts_request.json()

    logging.info(f"[EMBASSY SCRAPE] Retrieved {len(posts)} posts from {url}")

    return posts, total_page_number


def clean_html(text: str):
    html_clean = BeautifulSoup(text, features='lxml').text
    encode_clean = html_clean.encode('ascii', 'ignore').decode()
    new_line_clean = encode_clean.replace('\n', "")
    return new_line_clean


def read_post_to_file(country_name: str, post: dict, data_path: str, order: int):
    """Extract the text of an embassy post

    Args:
        post (dict): the json representation of a post

    Returns:
        (list)
    """

    logging.info(f"[READ POST] Reading post")

    raw_title = post['title']['rendered']
    raw_content = post['content']['rendered']

    cleaned_title = clean_html(raw_title)
    cleaned_content = clean_html(raw_content)
    
    post_title = cleaned_title.translate(str.maketrans("", "", string.punctuation))
    file_name = f"{country_name}{order:03}_{post_title[:50]}.txt"
    file_path = os.path.join(
        data_path,
        file_name,
    )

    post_file = open(
        file_path,
        'w'
    )

    post_file.write(f"{cleaned_title}\n")
    post_file.write(f"{cleaned_content}")

    return file_name, file_path
