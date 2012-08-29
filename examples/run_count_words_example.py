import os
import time
from rq import Queue, use_connection
from count_words_at_url import count_words_at_url

# Tell RQ what Redis connection to use
use_connection()

q = Queue()
urls = [
    'http://google.com',
    'http://yahoo.com',
    'http://bing.com',
    'http://duckduckgo.com/',
]
results = {}

for url in urls:
    results[url] = q.enqueue(count_words_at_url, url)

done = [False]

while not all(done):
    os.system('clear')

    for url in urls:
        result = results[url].return_value

        if result is None:
            print url, 'fetching...'
        else:
            print url, result, 'words'

        done = [x.return_value for x in results.values()]

    time.sleep(0.2)
