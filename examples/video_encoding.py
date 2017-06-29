from os import remove
from os.path import isfile
from subprocess import Popen, PIPE
import shlex


def ffmpeg_fragment(index):
    return execute(
        'ffmpeg -y -ss {} -t 10 -i {} -threads 1 {}.mp4'.format(index * 10, 'input.mp4', index))


def ffmpeg_concat(fragments):
    with open('fragments.txt', 'wb') as output:
        for i in xrange(fragments):
            output.write("file '{0}.mp4'\n".format(i))
    result = execute('ffmpeg -y -f concat -safe 0 -i fragments.txt -c copy output.mp4')
    cleanup_partial_files(fragments)
    return result


def execute(command):
    print('Executing command: {}'.format(command))
    print(shlex.split(command))
    sub = Popen(shlex.split(command), stdout=PIPE)
    try:
        out, err = sub.communicate()
        returncode = sub.returncode
        pid = sub.pid
    except SystemExit:
        sub.terminate()
        sub.kill()
        raise


def cleanup_partial_files(last_number=15):
    print("CLEANUP")

    file_fragments = 'fragments.txt'
    if isfile(file_fragments):
        remove(file_fragments)

    for i in xrange(last_number):
        file_name = '{}.mp4'.format(i)
        if isfile(file_name):
            remove(file_name)
