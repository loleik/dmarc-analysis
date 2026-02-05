import argparse
from .db import db_connect, db_print, db_query_auth, db_query_sources, db_query_ips
from .parser import parse_xml
from .ingest import ingest_directory

def main():
    parser = argparse.ArgumentParser(description = "DMARC report analysis pipeline")
    subparsers = parser.add_subparsers(dest = "command", required = True)

    ingest = subparsers.add_parser(
        "ingest",
        help = "Ingest DMARC XML reports, parse them, and insert them into database tables"
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

    query = subparsers.add_parser(
        "query",
        help = "Send a demo query. auth : show dkim/spf auth rates | sources : show top 20 ip's with failed auth | ips : shows ipv4 vs ipv6 rates"
    )
    query.add_argument(
        "db_url",
        help = "URL to PostgreSQL database omitting version information - <user>@<address>/<name>"
    )
    query.add_argument(
        "type",
        choices = ["auth", "sources", "ips"]
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
    elif args.command == "query":
        url = "postgresql+psycopg2://{}".format(args.db_url)
        engine = db_connect(url)
        if args.type == "auth":
            db_query_auth(engine)
        elif args.type == "sources":
            db_query_sources(engine)
        elif args.type == "ips":
            db_query_ips(engine)


if __name__ == "__main__":
    main()
