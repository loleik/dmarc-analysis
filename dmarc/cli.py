import argparse
from .db import db_connect, db_print
from .parser import parse_xml
from .ingest import ingest_directory

def main():
    parser = argparse.ArgumentParser(description = "DMARC report analysis pipeline")
    subparsers = parser.add_subparsers(dest = "command", required = True)

    ingest = subparsers.add_parser(
        "ingest",
        help = "Ingest DMARC XML reports"
    )
    ingest.add_argument(
        "db_url",
        help = "URL to PostgreSQL database ommitting version information - <user>@<address>/<name>"
    )
    ingest.add_argument(
        "path",
        help = "Directory containing XML reports"
    )

    show = subparsers.add_parser(
        "show",
        help = "Print database tables"
    )
    show.add_argument(
        "db_url",
        help = "URL to PostgreSQL database omitting version information - <user>@<address>/<name>"
    )
    show.add_argument(
        "table",
        choices = ["reports", "records"]
    )

    args = parser.parse_args()

    if args.command == 'ingest':
        url = "postgresql+psycopg2://{}".format(args.db_url)
        engine = db_connect(url)
        ingest_directory(engine, args.path)
    elif args.command == "show":
        url = "postgresql+psycopg2://{}".format(args.db_url)
        engine = db_connect(url)
        db_print(engine, args.table)

if __name__ == "__main__":
    main()
