# ================================================================================
# ACTC-Fronius2.py
# Fronius photovoltaic data pipeline - adapted for Colab, local and Render
# ================================================================================

# imports
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
# load environment-specific dependencies and secrets loader
# ================================================================================
if env == "colab":
    from google.colab import userdata

    def get_secret(name):
        return userdata.get(name)

    def get_secret_json(name):
        return json.loads(userdata.get(name))

elif env == "render":
    # on render, secrets are uploaded as secret files and saved in /etc/secrets/
    def get_secret(name):
        return os.getenv(name)

    def get_secret_json(name):
        with open(f"/etc/secrets/{name}") as f:
            return json.load(f)

else:
    # local: load from .env file and secrets/ folder
    from dotenv import load_dotenv
    load_dotenv()

    def get_secret(name):
        return os.getenv(name)

    def get_secret_json(name):
        with open(f"secrets/{name}") as f:
            return json.load(f)


# ================================================================================
# clts profiling
# ================================================================================
tstart = clts.getts()


# default configuration
DEFAULT_PARAMS = {
    "verbose": True,
    "destination": "-*-",
    "send_mail": True,
    "email_addresses": ["acatarinatc@gmail.com"],
    "start_date": "2026-03-25",  # (format YYYY-MM-DD)
    "days_back": 10
}


# get hostname and public IP of the machine running the script
hostname = socket.gethostname()
ip = requests.get('https://api.ipify.org').text
print("Server name:", hostname, "Public IP Address:", ip)


# ================================================================================
# context: user and script identification
# on colab, user is extracted from the notebook name
# on render/local, user is loaded from environment variable USER
# ================================================================================
if env == "colab":
    import ipynbname
    notebookname = requests.get("http://172.28.0.12:9000/api/sessions").json()[0]["name"]
    user = notebookname.split("-")[0]
    script = ipynbname.name()
else:
    user = os.getenv("USER", "ACTC")
    script = os.path.basename(__file__)

channel = "fronius"
destination = DEFAULT_PARAMS['destination']
verbose = DEFAULT_PARAMS['verbose']
send_mail = DEFAULT_PARAMS['send_mail']
email_addresses = DEFAULT_PARAMS['email_addresses']

context = f'{hostname} ({ip}) | {user} | {channel} | {script} | {destination}'
clts.setcontext(context)
now = str(datetime.datetime.now())[0:19]
hoje = now[:10]

clts.elapt[f"Environment detected: {env}"] = clts.deltat(tstart)

if verbose:
    print("context:", context)


# ================================================================================
# load github token and fetch file list from repository
# ================================================================================
GITHUB_TOKEN = get_secret_json(f'{user}-github_token.json')['key']
print("GitHub token loaded")

url_api = "https://api.github.com/repos/pedroccpimenta/datafiles/contents/Fronius"
headers = {"Authorization": f"token {GITHUB_TOKEN}"}
response = requests.get(url_api, headers=headers)
files = response.json()

clts.elapt[f"Fronius files list retrieved from GitHub"] = clts.deltat(tstart)
print(f"Total files found: {len(files)}")


# define the date window: from (start_date - days_back) up to start_date
start_date = datetime.datetime.strptime(DEFAULT_PARAMS["start_date"], "%Y-%m-%d").date()
days_back  = DEFAULT_PARAMS["days_back"]

window_start = start_date - datetime.timedelta(days=days_back)
window_end   = start_date

recent_files = []
for f in files:
    try:
        date_str  = f['name'].split(' ')[1].split('_')[0]
        file_date = datetime.datetime.strptime(date_str, '%Y%m%d').date()
        if window_start <= file_date <= window_end:
            recent_files.append(f['name'])
    except Exception:
        pass

clts.elapt[
    f"Files identified ({window_start} → {window_end}, {days_back} days)"
] = clts.deltat(tstart)

print(f"Window: {window_start} → {window_end}")
print(f"Files found in window: {len(recent_files)}")
for f in recent_files:
    print(f)


# ================================================================================
# download files
# wget is only available in colab - on render/local we use requests.get instead
# ================================================================================
for filename in recent_files:
    filename_encoded = filename.replace(' ', '%20')  # filenames have spaces - encode for URL

    if env == "colab":
        os.system(
            f'wget --header="Authorization: token {GITHUB_TOKEN}" '
            f'-N --quiet '
            f'https://raw.githubusercontent.com/pedroccpimenta/datafiles/master/Fronius/{filename_encoded}'
        )
    else:
        # on render/local, download with requests and save to current folder
        url_file = f"https://raw.githubusercontent.com/pedroccpimenta/datafiles/master/Fronius/{filename_encoded}"
        r = requests.get(url_file, headers=headers)
        with open(filename, 'wb') as f:
            f.write(r.content)

clts.elapt[f"Files downloaded"] = clts.deltat(tstart)
print(f"Files downloaded: {len(recent_files)}")


# load each downloaded file and combine into a single DataFrame
all_data = []
for filename in recent_files:
    df_temp = pd.read_excel(filename, header=0, skiprows=[1])
    all_data.append(df_temp)

df = pd.concat(all_data, ignore_index=True)

clts.elapt[f"Data loaded: {len(df)} records from {len(recent_files)} files"] = clts.deltat(tstart)

print("data loaded!")
print(df.shape)


# convert timestamp column to datetime format
df['Data e horário'] = pd.to_datetime(df['Data e horário'], format='%d.%m.%Y %H:%M')

clts.elapt["Timestamp column converted to datetime"] = clts.deltat(tstart)

# verify no null values were introduced during conversion
print("Data types after conversion:")
print(df.dtypes)
print("\nNull values after conversion:")
print(df.isnull().sum())


# ================================================================================
# load database list and connect to each database
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
                host=dbcreds["dest_host"],
                port=dbcreds["port"],
                db=dbcreds["database"],
                user=dbcreds["username"],
                password=dbcreds["password"],
                cursorclass=pymysql.cursors.DictCursor,
                charset="utf8mb4",
                connect_timeout=timeout,
                write_timeout=timeout,
                read_timeout=timeout
            )
            cursor = connection.cursor()
            clts.elapt[f"... connected to `{db}`"] = clts.deltat(tstart)
            status = "ok"

        elif dbcreds["dbms"] == "sql_tls":
            import pymysql
            print("... connecting to sql_tls database...")
            timeout = dbcreds["timeout"]
            pem_content = get_secret(dbcreds["pem"])
            with open(f"/tmp/{user}.pem", "w") as f:
                f.write(pem_content)
            connection = pymysql.connect(
                host=dbcreds["dest_host"],
                port=dbcreds["port"],
                db=dbcreds["database"],
                user=dbcreds["username"],
                password=dbcreds["password"],
                cursorclass=pymysql.cursors.DictCursor,
                charset="utf8mb4",
                ssl={"ca": f"/tmp/{user}.pem"},
                connect_timeout=timeout,
                write_timeout=timeout,
                read_timeout=timeout,
                autocommit=True
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
        skipped_list = []  # collect skipped timestamps for the summary

        if dbcreds["dbms"] == "influxdb":
            # InfluxDB does not enforce unique constraints like sql databases
            # so we query all existing ids at once before writing to avoid duplicate points
            bucket = dbcreds["bucket"]
            org = dbcreds["org"]
            query_api = influx_client.query_api()

            try:
                # fetch all existing ids in a single query instead of one query per row
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
                    row_id = f"{hostname}_{tstamp}"

                    if row_id in existing_ids:
                        skipped += 1
                        skipped_list.append(str(tstamp))
                    else:
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

                # only the summary is logged - not each individual record
                clts.elapt[f"... {inserts} inserted, {skipped} skipped @ {db}"] = clts.deltat(tstart)
                print(f"... {inserts} inserted, {skipped} skipped @ {db}")
                if skipped_list:
                    print(f"... skipped timestamps @ {db}: " + ", ".join(skipped_list))

            except Exception as e:
                print("Error:", e)
                clts.elapt[f"... error inserting into `{db}`: `{e}`"] = clts.deltat(tstart)

        else:
            # for sql, sql_tls and crate databases
            # id is built as hostname_timestamp and used as a unique key per row
            # we check which ids already exist before inserting - no updates,
            # fronius inversor data does not change after being recorded
            try:
                # fetch all existing ids in a single query
                all_ids = [f"{hostname}_{row['Data e horário']}" for _, row in df.iterrows()]
                placeholders = ", ".join(["?" for _ in all_ids])
                sql_check = f"SELECT id FROM fronius WHERE id IN ({placeholders})"
                cursor.execute(sql_check, all_ids)
                existing_ids = set(row[0] if dbcreds["dbms"] == "crate" else row["id"] for row in cursor.fetchall())
                clts.elapt[f"... {len(existing_ids)} existing records fetched from {db}"] = clts.deltat(tstart)

                # build list of rows to insert (only new ones)
                values_to_insert = []
                for _, row in df.iterrows():
                    tstamp = row["Data e horário"]
                    row_id = f"{hostname}_{tstamp}"

                    if row_id in existing_ids:
                        skipped += 1
                        skipped_list.append(str(tstamp))
                    else:
                        values_to_insert.append((
                            row_id,
                            tstamp.strftime('%Y-%m-%d %H:%M:%S'),
                            float(row["Consumida diretamente"]),
                            float(row["Consumo"]),
                            float(row["Energia obtida da rede elétrica"])
                        ))
                        inserts += 1

                # bulk insert all new rows in a single query
                if values_to_insert:
                    sql = (
                        "INSERT INTO fronius "
                        "(id, tstamp, consumida_diretamente, consumo, energia_rede) "
                        "VALUES (?, ?, ?, ?, ?)"
                    )
                    cursor.executemany(sql, values_to_insert)
                    connection.commit()

                # only the summary is logged - not each individual record
                clts.elapt[f"... {inserts} inserted, {skipped} skipped @ {db}"] = clts.deltat(tstart)
                print(f"... {inserts} inserted, {skipped} skipped @ {db}")
                if skipped_list:
                    print(f"... skipped timestamps @ {db}: " + ", ".join(skipped_list))

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

if send_mail and email_addresses != []:
    toem = clts.listtimes()

    if env == "render":
        # resend api - required on render because smtp (465, 587) is blocked
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
        # smtp - used on colab and local
        try:
            if env == "colab":
                credsgmail = json.loads(userdata.get(f'configGMail_{user}.json'))
            else:
                with open(f'./secrets/configGMail_ACTC.json', 'r') as fh:
                    credsgmail = json.loads(fh.read())

            assunto = f"{context}"
            message = MIMEMultipart("alternative")
            message["Subject"] = assunto
            message["From"] = credsgmail['UserFrom']
            message["To"] = ", ".join(email_addresses)

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
            part1 = MIMEText(text, "plain")
            part2 = MIMEText(html, "html")
            message.attach(part1)
            message.attach(part2)

            port = 465  # SSL
            ssl_context = ssl.create_default_context()
            with smtplib.SMTP_SSL("smtp.gmail.com", port, context=ssl_context) as server:
                server.login(credsgmail['UserName'], credsgmail['UserPwd'])
                sender_email = credsgmail['UserFrom']
                server.sendmail(sender_email, email_addresses, message.as_string())

            print("Notification sent.")
            clts.elapt["After sending email"] = clts.deltat(tstart)

        except Exception as e:
            print("Notification not sent:", e)
            clts.elapt[f"email not sent ({e})"] = clts.deltat(tstart)