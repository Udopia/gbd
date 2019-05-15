import sys

from gbd_tool import config_manager


def main(args=None):
    if args is None:
        args = sys.argv[1:]
    print("||Installing config data||\n")
    config_manager.make_standard_configuration()
    print("||done||\n")


if __name__ == "__main__":
    main()
