import hashlib
import sys

file_name = sys.argv[1]
with open(file_name, "rb") as file:
    file_bytes = file.read()
    checksum = hashlib.sha256(file_bytes).hexdigest()
    print(checksum)
