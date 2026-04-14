# ================================================================================
# ACTC-Fronius2.py
# Fronius photovoltaic data pipeline - adapted for Colab, local, Flask and Render
# ================================================================================

import os
import sys
import datetime
import json
import socket
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import pandas as pd
import requests

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

import clts_pcp as clts

print("... imports done.")


# ================================================================================
# detect environment: colab, render or local
# ================================================================================
def detect_environment():
    if "COLAB_RELEASE_TAG" in os.environ:
        return "colab"
    elif "RENDER" in os.environ:
        return "render"
    else:
        return "local"


env = detect_environment()
print("Running in:", env)


# ================================================================================
# load environment-specific dependencies
# ================================================================================
if env == "colab":
    from google.colab import userdata  # type: ignore
    import ipynbname  # type: ignore

elif env == "local":
    from dotenv import load_dotenv
    load_dotenv()


# ================================================================================
# secret loaders - unified interface for all environments
# ================================================================================
def get_secret(name):
    return os.getenv(name)


def get_secret_json(name):
    if env == "colab":
        return json.loads(userdata.get(name))
    elif env == "render":
        with open(f"/etc/secrets/{name}") as f:
            return json.load(f)
    else:
        with open(f"secrets/{name}") as f:
            return json.load(f)


# ================================================================================
# clts profiling
# ================================================================================
tstart = clts.getts()
clts.elapt.clear()   # reset profiling table between runs
tstart = clts.getts()

DEFAULT_PARAMS = {
    "verbose": True,
    "destination": "-*-",
    "send_mail": True,
    "email_addresses": ["acatarinatc@gmail.com"],
    "start_date": os.getenv("START_DATE", "2026-04-14"),
    "days_back": int(os.getenv("DAYS_BACK", "10"))
}

hostname = socket.gethostname()
ip = requests.get("https://api.ipify.org").text
print("Server name:", hostname, "Public IP Address:", ip)


# ================================================================================
# context identification
# ================================================================================
if env == "colab":
    notebookname = requests.get("http://172.28.0.12:9000/api/sessions").json()[0]["name"]
    user = notebookname.split("-")[0]
    script = ipynbname.name()
else:
    user = os.getenv("USER", "ACTC")
    script = os.path.basename(__file__)

channel     = "fronius"
destination = DEFAULT_PARAMS["destination"]
verbose     = DEFAULT_PARAMS["verbose"]
send_mail   = DEFAULT_PARAMS["send_mail"]
email_addresses = DEFAULT_PARAMS["email_addresses"]

context = f"{hostname} ({ip}) | {user} | {channel} | {script} | {destination}"
clts.setcontext(context)
now  = str(datetime.datetime.now())[0:19]
hoje = now[:10]

clts.elapt[f"Environment detected: {env}"] = clts.deltat(tstart)

if verbose:
    print("context:", context)


# ================================================================================
# load github token and fetch file list from repository
# ================================================================================
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN") or get_secret_json(f"{user}-github_token.json")["key"]
print("GitHub token loaded")

url_api  = "https://api.github.com/repos/pedroccpimenta/datafiles/contents/Fronius"
headers  = {"Authorization": f"token {GITHUB_TOKEN}"}
response = requests.get(url_api, headers=headers)
files    = response.json()

clts.elapt["Fronius files list retrieved from GitHub"] = clts.deltat(tstart)
print(f"Total files found: {len(files)}")


# ================================================================================
# identify files within the date window
# ================================================================================
start_date = datetime.datetime.strptime(DEFAULT_PARAMS["start_date"], "%Y-%m-%d").date()
days_back  = DEFAULT_PARAMS["days_back"]

window_start = start_date - datetime.timedelta(days=days_back)
window_end   = start_date

recent_files = []
for f in files:
    try:
        date_str  = f["name"].split(" ")[1].split("_")[0]
        file_date = datetime.datetime.strptime(date_str, "%Y%m%d").date()
        if window_start <= file_date <= window_end:
            recent_files.append(f["name"])
    except Exception:
        pass

clts.elapt[f"Files identified ({window_start} → {window_end}, {days_back} days)"] = clts.deltat(tstart)

print(f"Window: {window_start} → {window_end}")
print(f"Files found in window: {len(recent_files)}")
for f in recent_files:
    print(f)


# ================================================================================
# download files into a temp folder to keep the repo clean
# wget is only available in colab - on render/local we use requests.get
# ================================================================================
os.makedirs("fronius_temp", exist_ok=True)

for filename in recent_files:
    filename_encoded = filename.replace(" ", "%20")
    local_path = os.path.join("fronius_temp", filename)

    if env == "colab":
        os.system(
            f'wget --header="Authorization: token {GITHUB_TOKEN}" '
            f'-N --quiet -P fronius_temp '
            f'https://raw.githubusercontent.com/pedroccpimenta/datafiles/master/Fronius/{filename_encoded}'
        )
    else:
        url_file = f"https://raw.githubusercontent.com/pedroccpimenta/datafiles/master/Fronius/{filename_encoded}"
        r = requests.get(url_file, headers=headers)
        with open(local_path, "wb") as fh:
            fh.write(r.content)

clts.elapt["Files downloaded"] = clts.deltat(tstart)
print(f"Files downloaded: {len(recent_files)}")


# ================================================================================
# load and combine all downloaded files into a single DataFrame
# ================================================================================
all_data = []
for filename in recent_files:
    local_path = os.path.join("fronius_temp", filename)
    df_temp = pd.read_excel(local_path, header=0, skiprows=[1])
    all_data.append(df_temp)

if not all_data:
    print("No files found in window. Nothing to process.")
    sys.exit(0)

df = pd.concat(all_data, ignore_index=True)

clts.elapt[f"Data loaded: {len(df)} records from {len(recent_files)} files"] = clts.deltat(tstart)

print("data loaded!")
print(df.shape)

df["Data e horário"] = pd.to_datetime(df["Data e horário"], format="%d.%m.%Y %H:%M")

clts.elapt["Timestamp column converted to datetime"] = clts.deltat(tstart)

print("Data types after conversion:")
print(df.dtypes)
print("\nNull values after conversion:")
print(df.isnull().sum())


# ================================================================================
# load database list and insert into each database
# ================================================================================
dblist = get_secret_json(f"{user}-dblist.json")
print(dblist)

for db in dblist:
    status = "nok"
    clts.elapt[f"Connecting to `{db}`"] = clts.deltat(tstart)
    if verbose:
        print("db in dblist:", db)
        print(f"connecting to `{db}`")

    try:
        print(f"Credentials in `{user}-{db}.json`")
        dbcreds = get_secret_json(f"{user}-{db}.json")

        if dbcreds["dbms"] == "sql":
            import pymysql
            print("... connecting to sql database...")
            timeout = dbcreds["timeout"]
            connection = pymysql.connect(
                host=dbcreds["dest_host"], port=dbcreds["port"],
                db=dbcreds["database"], user=dbcreds["username"],
                password=dbcreds["password"],
                cursorclass=pymysql.cursors.DictCursor, charset="utf8mb4",
                connect_timeout=timeout, write_timeout=timeout, read_timeout=timeout
            )
            cursor = connection.cursor()
            clts.elapt[f"... connected to `{db}`"] = clts.deltat(tstart)
            status = "ok"

        elif dbcreds["dbms"] == "sql_tls":
            import pymysql
            print("... connecting to sql_tls database...")
            timeout = dbcreds["timeout"]
            pem_content = get_secret(dbcreds["pem"])
            with open(f"/tmp/{user}.pem", "w") as fh:
                fh.write(pem_content)
            connection = pymysql.connect(
                host=dbcreds["dest_host"], port=dbcreds["port"],
                db=dbcreds["database"], user=dbcreds["username"],
                password=dbcreds["password"],
                cursorclass=pymysql.cursors.DictCursor, charset="utf8mb4",
                ssl={"ca": f"/tmp/{user}.pem"},
                connect_timeout=timeout, write_timeout=timeout,
                read_timeout=timeout, autocommit=True
            )
            cursor = connection.cursor()
            clts.elapt[f"... connected to `{db}`"] = clts.deltat(tstart)
            status = "ok"

        elif dbcreds["dbms"] == "crate":
            from crate import client
            print("... connecting to crate database...")
            connection = client.connect(
                dbcreds["dest_host"],
                username=dbcreds["username"],
                password=dbcreds["password"],
                verify_ssl_cert=True
            )
            cursor = connection.cursor()
            clts.elapt[f"... connected to `{db}`"] = clts.deltat(tstart)
            status = "ok"

        elif dbcreds["dbms"] == "influxdb":
            print("... connecting to influxdb database...")
            influx_client = InfluxDBClient(
                url=dbcreds["dest_host"],
                token=dbcreds["token"],
                org=dbcreds["org"]
            )
            write_api = influx_client.write_api(write_options=SYNCHRONOUS)
            clts.elapt[f"... connected to `{db}`"] = clts.deltat(tstart)
            status = "ok"

        else:
            clts.elapt[f"... `{dbcreds['dbms']}` dbms not ready"] = clts.deltat(tstart)
            status = "onerror"

    except Exception as e:
        print("Error:", e)
        clts.elapt[f"... error `{e}`"] = clts.deltat(tstart)
        status = "onerror"

    print("status:", status)

    if status == "ok":
        inserts = 0
        skipped = 0

        # --------------------------------------------------------------------
        # InfluxDB: fetch existing IDs, skip outdated points, write new ones
        # --------------------------------------------------------------------
        if dbcreds["dbms"] == "influxdb":
            bucket    = dbcreds["bucket"]
            org       = dbcreds["org"]
            query_api = influx_client.query_api()

            # retention cutoff: InfluxDB Cloud free tier has a 30-day retention
            # use 29 days as a safe margin to avoid boundary errors
            retention_cutoff = datetime.datetime.utcnow() - datetime.timedelta(days=29)

            try:
                check_query = f'''
                    from(bucket: "{bucket}")
                    |> range(start: 0)
                    |> filter(fn: (r) => r._measurement == "fronius")
                    |> keep(columns: ["id"])
                    |> distinct(column: "id")
                '''
                result = query_api.query(org=org, query=check_query)
                existing_ids = set(
                    record.values["id"]
                    for table in result
                    for record in table.records
                    if "id" in record.values
                )
                clts.elapt[f"... {len(existing_ids)} existing records fetched from influxdb"] = clts.deltat(tstart)

                for _, row in df.iterrows():
                    tstamp = row["Data e horário"]
                    row_id = f"{hostname}_{tstamp.strftime('%Y-%m-%d %H:%M:%S')}"

                    # skip duplicates
                    if row_id in existing_ids:
                        skipped += 1
                        continue

                    # skip points older than retention period
                    if tstamp.replace(tzinfo=None) < retention_cutoff:
                        skipped += 1
                        continue

                    # skip rows with NaN values to avoid write errors
                    if pd.isna(row["Consumida diretamente"]) or \
                       pd.isna(row["Consumo"]) or \
                       pd.isna(row["Energia obtida da rede elétrica"]):
                        skipped += 1
                        continue

                    point = (
                        Point("fronius")
                        .tag("id", row_id)
                        .tag("hostname", hostname)
                        .field("consumida_diretamente", float(row["Consumida diretamente"]))
                        .field("consumo", float(row["Consumo"]))
                        .field("energia_rede", float(row["Energia obtida da rede elétrica"]))
                        .time(tstamp, WritePrecision.S)
                    )
                    write_api.write(bucket=bucket, org=org, record=point)
                    inserts += 1

                clts.elapt[f"... {inserts} inserted, {skipped} skipped @ {db}"] = clts.deltat(tstart)
                print(f"... {inserts} inserted, {skipped} skipped @ {db}")

            except Exception as e:
                print("Error:", e)
                if hasattr(e, "body"):
                    print("Error body:", e.body)
                clts.elapt[f"... error inserting into `{db}`: `{e}`"] = clts.deltat(tstart)

        # --------------------------------------------------------------------
        # SQL / CrateDB: fetch existing IDs, bulk insert only new rows
        # --------------------------------------------------------------------
        else:
            try:
                # normalize ID format with strftime to match what is stored in the DB
                all_ids = [
                    f"{hostname}_{row['Data e horário'].strftime('%Y-%m-%d %H:%M:%S')}"
                    for _, row in df.iterrows()
                ]
                placeholders = ", ".join(["?" for _ in all_ids])
                sql_check    = f"SELECT id FROM fronius WHERE id IN ({placeholders})"
                cursor.execute(sql_check, all_ids)
                existing_ids = set(
                    row[0] if dbcreds["dbms"] == "crate" else row["id"]
                    for row in cursor.fetchall()
                )
                clts.elapt[f"... {len(existing_ids)} existing records fetched from {db}"] = clts.deltat(tstart)

                values_to_insert = []
                for _, row in df.iterrows():
                    tstamp = row["Data e horário"]
                    row_id = f"{hostname}_{tstamp.strftime('%Y-%m-%d %H:%M:%S')}"

                    if row_id in existing_ids:
                        skipped += 1
                    else:
                        values_to_insert.append((
                            row_id,
                            tstamp.strftime("%Y-%m-%d %H:%M:%S"),
                            float(row["Consumida diretamente"]),
                            float(row["Consumo"]),
                            float(row["Energia obtida da rede elétrica"])
                        ))
                        inserts += 1

                if values_to_insert:
                    sql = (
                        "INSERT INTO fronius "
                        "(id, tstamp, consumida_diretamente, consumo, energia_rede) "
                        "VALUES (?, ?, ?, ?, ?)"
                    )
                    cursor.executemany(sql, values_to_insert)
                    connection.commit()
                    # CrateDB requires explicit refresh so next SELECT sees new rows
                    if dbcreds["dbms"] == "crate":
                        cursor.execute("REFRESH TABLE fronius")

                clts.elapt[f"... {inserts} inserted, {skipped} skipped @ {db}"] = clts.deltat(tstart)
                print(f"... {inserts} inserted, {skipped} skipped @ {db}")

            except Exception as e:
                print("Error:", e)
                clts.elapt[f"... error inserting into `{db}`: `{e}`"] = clts.deltat(tstart)

    print("Connection closing....")
    if dbcreds["dbms"] != "influxdb":
        connection.close()
        clts.elapt[f"... connection to `{db}` closed"] = clts.deltat(tstart)
    else:
        influx_client.close()
        clts.elapt[f"... connection to `{db}` closed"] = clts.deltat(tstart)


# ================================================================================
# send profiling email
# render uses resend api because smtp ports are blocked on the free tier
# local and colab use smtp directly
# ================================================================================
clts.elapt["Overall (before email):"] = clts.deltat(tstart)

if send_mail and email_addresses:
    toem = clts.listtimes()

    if env == "render":
        try:
            import resend
            resend.api_key = os.getenv("RESEND_API_KEY")
            resend.Emails.send({
                "from": "onboarding@resend.dev",
                "to": email_addresses,
                "subject": context,
                "html": f"""
                <html>
                    <body style='font-family:Montserrat;'>
                        {toem}
                        <hr color='orange'>
                        This message is an automated notification from {context}
                    </body>
                </html>
                """
            })
            print("Notification sent.")
            clts.elapt["After sending email"] = clts.deltat(tstart)
        except Exception as e:
            print("Notification not sent:", e)
            clts.elapt[f"email not sent ({e})"] = clts.deltat(tstart)

    else:
        try:
            if env == "colab":
                credsgmail = json.loads(userdata.get(f"configGMail_{user}.json"))
            else:
                with open("./secrets/configGMail_ACTC.json", "r") as fh:
                    credsgmail = json.loads(fh.read())

            message = MIMEMultipart("alternative")
            message["Subject"] = context
            message["From"]    = credsgmail["UserFrom"]
            message["To"]      = ", ".join(email_addresses)

            text = f"{toem}\nThis is an automated notification."
            html = f"""
            <html>
                <body style='font-family:Montserrat;'>
                    {toem}
                    <hr color='orange'>
                    This message is an automated notification from {context}
                </body>
            </html>
            """
            message.attach(MIMEText(text, "plain"))
            message.attach(MIMEText(html, "html"))

            port        = 465
            ssl_context = ssl.create_default_context()
            with smtplib.SMTP_SSL("smtp.gmail.com", port, context=ssl_context) as server:
                server.login(credsgmail["UserName"], credsgmail["UserPwd"])
                server.sendmail(credsgmail["UserFrom"], email_addresses, message.as_string())

            print("Notification sent.")
            clts.elapt["After sending email"] = clts.deltat(tstart)

        except Exception as e:
            print("Notification not sent:", e)
            clts.elapt[f"email not sent ({e})"] = clts.deltat(tstart)