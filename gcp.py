from google.cloud import storage
import requests
import json
from urllib.parse import urlparse

VALID_PATH_MATCHING_KEYS = ["prefix", "exact"]


def hello_gcs(event, context):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(event["bucket"])

    blob = bucket.blob(event["name"])
    blob = blob.download_as_string()

    adm_settings = json.loads(blob)
    for advertiser_key in adm_settings:
        advertiser = adm_settings.get(advertiser_key)
        is_valid_advertiser_urls(advertiser.get("advertiser_urls"))
        is_valid_host_list(advertiser.get("clicked_hosts", []))
        is_valid_host_list(advertiser.get("impression_hosts", []))


def validate_host(url):
    try:
        parsed_url = urlparse(url)
        if any([parsed_url.scheme, parsed_url.port, contains_path(url)]):
            print(f"host:{parsed_url} is invalid, host should not "
                  f"include protocols, ports or paths")
    except ValueError as e:
        print(f"host: {url} was unable to be parsed with error: {e}")


def contains_path(url):
    parsed_url = urlparse(url)
    netloc_is_empty = parsed_url.netloc != ""
    contains_no_slash = "/" in url

    return netloc_is_empty or contains_no_slash


def is_valid_host_list(hosts):
    for host in hosts:
        return validate_host(host)


def validate_paths(paths):
    for path in paths:
        try:
            match_term = path["matching"]
            path_value = path["value"]
            starts_with_slash = path_value.startswith("/")
            is_valid_match = match_term in VALID_PATH_MATCHING_KEYS
            if match_term == "prefix" and not path_value.endswith("/"):
                print(f"{path_value} is a prefix so it should end with /")

            if not starts_with_slash and is_valid_match:
                print(f"{path}: path value is invalid")

        except KeyError:
            print(f"{path} is missing a 'value' or 'matching' value")
            continue


def paths_overlap(paths):
    path_strings = [path["value"] for path in paths]
    path_strings.sort()
    for i in range(len(path_strings)):
        for j in range(i + 1, len(path_strings)):
            if path_strings[i] in path_strings[j]:
                print(
                    f"paths: {path_strings[i]} and {path_strings[j]} overlap")
            else:
                break


def is_valid_advertiser_urls(advertiser_urls):
    for entry in advertiser_urls:
        validate_host(entry.get("host"))
        validate_paths(entry.get("paths", []))
        paths_overlap(entry.get("paths", []))
