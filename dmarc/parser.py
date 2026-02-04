from bs4 import BeautifulSoup

def parse_xml(data):
    Bs_data = BeautifulSoup(data, "xml")
    b_report = Bs_data.find('report_metadata')
    b_records = Bs_data.find_all("record")

    report = {
        "org_name"    : b_report.find('org_name').text,
        "external_id" : b_report.find("report_id").text,
        "begin_ts"    : b_report.find("begin").text,
        "end_ts"      : b_report.find("end").text
    }

    records = []
    for r in b_records:
        records.append({
            "source_ip"   : r.find("source_ip").text,
            "count"       : r.find("count").text,
            "disposition" : r.find("disposition").text,
            "dkim"        : r.find("dkim").text,
            "spf"         : r.find("spf").text
        })

    return [report, records]
