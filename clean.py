from pathlib import Path


def clean_directory(path):
    glob = Path(path).glob('**/*')
    for f in glob:
        if f.is_file():
            f.unlink()


if __name__ == '__main__':
    clean_directory('log')
    clean_directory('scan')