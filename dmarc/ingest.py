import os
from .db import db_insert
from .parser import parse_xml

def ingest_directory(engine, path):
    for e in os.scandir(path):
        if e.is_file():
            with open(e.path, "r") as f:
                parsed = parse_xml(f.read())
                db_ingest(engine, parsed[0], parsed[1])
