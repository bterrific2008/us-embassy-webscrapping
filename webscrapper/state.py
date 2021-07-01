#!/usr/bin/env python3

import time
import re

import requests
import json
import os

import bs4
from bs4 import BeautifulSoup
import tqdm

# defined urls
us_embassy_main_url = "https://www.usembassy.gov/post-sitemap.xml"

sleep_time = 3


def embassy_url_prefix(url):
    """Given a url, returns url if embassy link"""
    pattern_matched = re.match(
        r"https?://(?!www).*\.(usmission|usembassy|usconsulate).*",
        url["href"],
    )
    return pattern_matched.group() if pattern_matched else None


def extract_embassy_country_name(url):
    """Given a url pointing to an embassy website, extract the country name"""
    country_name = url.split("/")[-2]
    return country_name


def identify_embassy_posts():
    us_embassy_main_request = requests.get(us_embassy_main_url)
    us_embassy_main_html = us_embassy_main_request.content
    us_embassy_main_soup = BeautifulSoup(us_embassy_main_html, "lxml")

    embassy_url_list = [loc.string for loc in us_embassy_main_soup.find_all("loc")]
    return embassy_url_list


def identify_embassy_url(url):
    """Given the url to an embassy page, return the actual embassy website"""
    country_name = extract_embassy_country_name(url)

    embassy_request = requests.get(url)
    embassy_html = embassy_request.content
    embassy_soup = BeautifulSoup(embassy_html, "lxml")

    embassy_url_list = list(
        filter(
            None,
            list(
                map(
                    embassy_url_prefix,
                    embassy_soup.find_all("a"),
                )
            ),
        )
    )

    if len(embassy_url_list) > 0:
        country_website = embassy_url_list[0]
        country_website = "/".join(country_website.split("/")[:3])
        return country_name, country_website
    return None, None


def identify_missing_embassies(embassy_url_list: list, embassy_websites: dict):
    """Return all urls missing from the embassy_url_map"""
    missing_embassies = []

    for url in embassy_url_list:
        country_name = extract_embassy_country_name(url)
        if country_name not in set(embassy_websites.keys()):
            missing_embassies.append(url)
    return missing_embassies


def save_embassies(
    embassy_websites: dict, filepath="", filename="embassy_url_map.json"
):
    with open(os.path.join(filepath, filename), "w") as f:
        json.dump(embassy_websites, f, indent=6)


def load_embassies(filepath="", filename="embassy_url_map.json"):
    print(filepath, filename)
    with open(os.path.join(filepath, filename), "r") as f:
        embassy_url_map = json.load(f)
    return embassy_url_map


def run_all(filepath=""):
    embassy_posts_url_list = identify_embassy_posts()

    embassy_websites = {}
    for embassy_page_url in tqdm.tqdm(embassy_posts_url_list):
        country_name, country_url = identify_embassy_url(embassy_page_url)
        if country_name:
            embassy_websites[country_name] = country_url
        time.sleep(sleep_time)
    save_embassies(embassy_websites, filepath=filepath)
