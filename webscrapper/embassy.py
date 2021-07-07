#!/usr/bin/env python3

import hashlib
import os
import string
import logging

import bs4
import requests
from bs4 import BeautifulSoup


def get_embassy_posts(url: str):
    """Retrieves the posts of an embassy website

    Args:
        url (str): the url of the embassy website

    Returns:
        (list) a list of all post urls for the embassy website
    """
    logging.info(f"[EMBASSY SCRAPE] Retrieving posts from the website {url}")
    sitemap_url = f"{url}/post-sitemap.xml"
    sitemap_request = requests.get(sitemap_url)
    sitemap_html = sitemap_request.content
    sitemap_soup = BeautifulSoup(sitemap_html, "lxml")

    embassy_posts = [loc.string for loc in sitemap_soup.find_all("loc")]

    logging.info(f"[EMBASSY SCRAPE] Retrieved {len(embassy_posts)} posts from {url}")

    return embassy_posts


def read_post_to_file(url: str, data_path: str, missing_file_handler=None):
    """Extract the text of an embassy post

    Args:
        url (str): the url of the embassy post
        data_path (str): the file path to missing files

    Returns:
        (list)
    """

    logging.info(f"[READ POST] Reading post from {url}")

    try:
        request_post = requests.get(url)
        html_post = request_post.content
        soup_post = BeautifulSoup(html_post, "lxml")

        post_title = soup_post.find(class_="mo-breadcrumbs").find("h1").string.strip()
        post_title = post_title.translate(str.maketrans("", "", string.punctuation))
        post_title_hash = hashlib.sha1(post_title.encode("utf-8")).hexdigest()

        post_file = open(
            os.path.join(
                data_path,
                post_title_hash,
            ),
            "w",
        )

        post_file.write(f"{post_title}\n")
        for sibling in (
            soup_post.find(class_="main")
            .article.find(class_="entry-content")
            .div.next_siblings
        ):
            if type(sibling) is bs4.element.Tag:
                if sibling.name == "p":
                    text = f"{sibling.text if sibling.text else ''} "
                    if sibling.attrs:
                        if "class" in sibling.attrs:
                            if "byline" not in sibling.attrs["class"]:
                                post_file.write(text)
                    else:
                        post_file.write(text)
        post_file.close()
    except:
        logging.warning("[READ POST] Failed to scrape", url)
        if missing_file_handler:
            missing_file_handler.write(f"Failed to scrape {url}\n")
