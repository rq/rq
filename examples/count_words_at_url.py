from gevent import monkey

monkey.patch_socket()

import requests

def count_words_at_url(url):
    resp = requests.get(url)
    return len(resp.text.split())
