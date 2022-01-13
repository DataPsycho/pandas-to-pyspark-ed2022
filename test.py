def hello():
    print("Hello")


def read_config():
    storage = dict()
    with open("config.txt") as f:
        for line in f:
            line = line.replace("\n", "")
            key, value = line.split("=")
            storage[key] = value
    print(storage)
    return storage


def execute():
    config = read_config()
    if config["PRIMARY_RUN"] == "GPU":
        print("Running form GPU")
        hello()
    else:
        print("Running form CPU")
        hello()


if __name__ == "__main__":
    read_config()