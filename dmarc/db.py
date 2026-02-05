from sqlalchemy import create_engine, text
import pandas as pd

def db_connect(url):
    engine = create_engine(url)
    return engine


def db_insert(engine, xml_report, xml_records):
    insert_report = text("""
        INSERT INTO reports (
            org_name, begin_ts, end_ts, external_report_id
        )
        VALUES (
            :org_name, to_timestamp(:begin_ts), to_timestamp(:end_ts), :external_id
        )
        ON CONFLICT (external_report_id) DO NOTHING
        RETURNING report_id;
    """)

    insert_record = text("""
        INSERT INTO records (
            report_id, source_ip, msg_count, disposition, dkim_result, spf_result
        )
        VALUES (
            :internal_id, :source_ip, :count, :disposition, :dkim, :spf
        )
        ON CONFLICT DO NOTHING
    """)

    with engine.begin() as conn:
        result = conn.execute(insert_report, {
            "org_name"    : xml_report["org_name"],
            "begin_ts"    : int(xml_report["begin_ts"]),
            "end_ts"      : int(xml_report["end_ts"]),
            "external_id" : xml_report["external_id"]
        }).fetchone()

        if result:
            internal_id = result[0]
        else:
            internal_id = conn.execute(
                text("""
                    SELECT report_id FROM reports
                    WHERE external_report_id = :external_id;
                """),
                {"external_id" : xml_report["external_id"]}
            ).scalar()

        for r in xml_records:
            conn.execute(insert_record, {
                "internal_id" : internal_id,
                "source_ip"   : r["source_ip"],
                "disposition" : r["disposition"],
                "count"       : r["count"],
                "dkim"        : r["dkim"],
                "spf"         : r["spf"]
            })


def db_query_auth(engine):
    auth_query = """
        SELECT dkim_result, spf_result, SUM(msg_count) AS total_messages
        FROM records
        GROUP BY dkim_result, spf_result
        ORDER BY total_messages DESC;
    """
    print(auth_query)
    df_auth = pd.read_sql(auth_query, engine)
    print(df_auth)


def db_query_sources(engine):
    source_query = """
        SELECT source_ip, SUM(msg_count) AS failed_messages
        FROM records
        WHERE dkim_result = 'fail' OR spf_result = 'fail'
        GROUP BY source_ip
        ORDER BY failed_messages DESC
        LIMIT 10;
    """
    print(source_query)
    df_sources = pd.read_sql(source_query, engine)
    print(df_sources)


def db_query_ips(engine):
    ips_query = """
        SELECT
            CASE
                WHEN family(source_ip) = 6 THEN 'IPv6'
                ELSE 'IPv4'
            END AS ip_version,
            SUM(msg_count) AS total_messages
        FROM records
        GROUP BY
            CASE
                WHEN family(source_ip) = 6 THEN 'IPv6'
                ELSE 'IPv4'
            END;
    """
    print(ips_query)
    df_ips = pd.read_sql(ips_query, engine)
    print(df_ips)


def db_print(engine, table):
    query = "SELECT * FROM " + table + ";"
    print(query)
    df_report = pd.read_sql(query, engine)
    print(df_report)
