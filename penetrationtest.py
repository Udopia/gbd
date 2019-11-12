import time
from main.gbd_tool.http_client import post_request
import _thread


def main():
    test_query(50)
    time.sleep(5)
    test_cli_query(50)


def test_query(times):
    for n in range(times):
        _thread.start_new_thread(post_request,
                                 ("{}/results".format("localhost:5000"),
                                  {'query': "clauses+%3E+50"},
                                  {'User-Agent': "PENTESTER"}))


def test_cli_query(times):
    for n in range(times):
        _thread.start_new_thread(post_request,
                                 ("{}/results".format("localhost:5000"),
                                  {'query': "clauses+%3E+50"},
                                  {'User-Agent': "gbd_tool_cli"}))


if __name__ == '__main__':
    main()
