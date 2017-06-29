from os import remove
from time import sleep
from os.path import isfile


def write_number(number):
    with open('{}.txt'.format(number), 'wb') as f:
        f.write('{}'.format(number))
        print('WRITING {}'.format(number))
        sleep(1)


def merge(last_number):
    print("MERGING")
    with open('result.txt', 'ab') as output:
        for i in xrange(last_number):
            with open('{}.txt'.format(i), 'rb') as input:
                output.write(input.read())
    cleanup_partial_files(last_number)


def cleanup_partial_files(last_number=1):
    print("CLEANUP")
    for i in xrange(last_number):
        file_name = '{}.txt'.format(i)
        if isfile(file_name):
            remove(file_name)

