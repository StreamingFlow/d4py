import os

from redis import Redis


def connect(name=None):
    HOST = os.environ.get("REDIS_HOST", "localhost")
    PORT = os.environ.get("REDIS_PORT", 6379)
    USERNAME = os.environ.get("REDIS_USERNAME", None)
    PASSWORD = os.environ.get("REDIS_PASSWORD", None)

    client_kwars = {"host": HOST, "port": PORT, "decode_responses": True}

    if USERNAME:
        client_kwars["username"] = USERNAME

    if PASSWORD:
        client_kwars["password"] = PASSWORD

    redis = Redis(**client_kwars)

    if name is not None:
        redis.client_setname(name)

    return redis


# def parse_args(args, namespace):
#     parser = argparse.ArgumentParser(
#         prog='dispel4py',
#         description='Submit a dispel4py graph to zeromq multi processing')
#     parser.add_argument('-ct', '--consumer-timeout',
#                         help='stop consumers after timeout in ms',
#                         type=int)
#     parser.add_argument('-n', '--num', metavar='num_processes', required=True,
#                         type=int, help='number of processes to run')

#     result = parser.parse_args(args, namespace)
#     return result
