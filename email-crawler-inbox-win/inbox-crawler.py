
import os, uuid,re,traceback,shutil,base64,gzip,psutil
import json,sys,platform,re
import time
import boto3, hashlib, urllib3
import numpy as np
from collections import Counter
import win32com.client as win32
from datetime import date, datetime
import pgSQL

sql_w = pgSQL.SQL(random_host=False)
sql_r = pgSQL.SQL(random_host=True)

REGEX_CASE_ID = r"(?<!\d)\d{10}(?!\d)"
REGEX_10_ALPHANUMERIC = r'(?<!\w)[A-Za-z0-9]{10}(?!\w)'
REGEX_7_ALPHANUMERIC = r'(?<!\w)[A-Za-z0-9]{7}(?!\w)'
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

path_separator = '/' if 'linux' in str(platform.system()).lower() else '\\'
win_user = os.getlogin()
cred_path = os.path.abspath(os.path.join(os.getcwd(), os.pardir)) + path_separator +'cred_files'
reg_name ='us-west-2'

def get_jobs_to_run(source_id):
    try:
        q = "select email_crawler.get_jobs('{}')".format(source_id)
        sql_w.conn()
        rs = sql_w.executeAndReturn(q)
        sql_w.close_conn()
        jobs = rs[0][0]
        #print(jobs)
        return jobs
    except Exception:
        print(str(traceback.format_exc().splitlines()))


def db_query(rs,typ=None):
    try:
        if typ == 'config':
            q = "select email_crawler.job_get_config('{}')".format(json.dumps(rs))
        else:
            q = "select email_crawler.do_updates('{}')".format(str(json.dumps(rs)).replace("'", "''"))
        #print(q)
        sql_w.conn()
        rs = sql_w.executeAndReturn(q)
        sql_w.close_conn()
        return rs[0][0]
    except Exception:
        print(str(traceback.format_exc().splitlines()))
        # update logs
        rs = {}
        rs = {"update_task": 'update_log', "job_id": job_id, "source_id": source_id, "source": source,
              "log_type": "ERROR",
              "log_details": [{"status": "error", "msg": str(traceback.format_exc().splitlines())}]}
        db_query(rs)
        # update status
        rs = {}
        rs = {"update_task": 'crawler_exec_status', "job_id": job_id, "data": "Error"}
        db_query(rs)

def send_email():
    global env
    with open(cred_path + path_separator + "aws_cred.txt") as f:
        aws_cr = json.loads(base64.b64decode(f.readline()).decode())

    SENDER = "auto.cpat@hp.com"
    RECIPIENT = ([str(i) for i in contact.split(",")])
    CHARSET = "UTF-8"
    SUBJECT = "Email Inbox Crawler Error"

    BODY_HTML = """<html>
            <head></head>
            <body>Hello,
            </br></br>There was an error during the crawler execution for crawler_id - {} , job_id - {}. 
            </br>Please check database for error details.
            </br></br> 
            </br></br>Best regards
            </body>
            </html>""".format(source_id,job_id)

    client = boto3.client('ses', region_name='us-west-2'
                          ,aws_access_key_id=aws_cr['username']
                          ,aws_secret_access_key=aws_cr['password'])
    # Try to send the email.
    try:
        # Provide the contents of the email.
        response = client.send_email(
            Destination={
                'ToAddresses': RECIPIENT
            },
            Message={
                'Body': {
                    'Html': {
                        'Charset': CHARSET,
                        'Data': BODY_HTML,
                    }
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': SUBJECT,
                },
            },
            Source=SENDER,
        )
    # Display an error if something goes wrong.
    except Exception as e:
        print(e.response['Error']['Message'])
        # update logs
        rs = {}
        rs = {"update_task": 'update_log', "job_id": job_id, "source_id": source_id, "source": source,
              "log_type": "ERROR", "log_details": [{"status": "error", "msg": str(e.response['Error']['Message'])}]}
        db_query(rs)
    else:
        print("Email sent! Message ID:")
        print(response['MessageId'])


def send_email_notification():
    global env
    with open(cred_path + path_separator + "aws_cred.txt") as f:
        aws_cr = json.loads(base64.b64decode(f.readline()).decode())

    SENDER = "auto.cpat@hp.com"
    RECIPIENT = ([str(i) for i in business_contact.split(",")])
    CHARSET = "UTF-8"
    SUBJECT = "Email Notification Inbox Crawler"

    BODY_HTML = """<html>
            <head></head>
            <body>Hello,
            </br></br>There is a new mail for '{}' subject keyword and '{}' file name keyword. 
            </br>Please check digitalanalytics mail.
            </br></br> 
            </br></br>Best regards
            </body>
            </html>""".format(mail_notification_details["SUBJECT"], mail_notification_details["KEYWORD"])

    client = boto3.client('ses', region_name='us-west-2'
                          ,aws_access_key_id=aws_cr['username']
                          ,aws_secret_access_key=aws_cr['password'])

    try:
        # Provide the contents of the email.
        response = client.send_email(
            Destination={
                'ToAddresses': RECIPIENT
            },
            Message={
                'Body': {
                    'Html': {
                        'Charset': CHARSET,
                        'Data': BODY_HTML,
                    }
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': SUBJECT,
                },
            },
            Source=SENDER,
        )
        print(response)
    # Display an error if something goes wrong.
    except Exception as e:
        print(e.response['Error']['Message'])
        # update logs
        rs = {}
        rs = {"update_task": 'update_log', "job_id": job_id, "source_id": source_id, "source": source,
              "log_type": "ERROR", "log_details": [{"status": "error", "msg": str(e.response['Error']['Message'])}]}
        db_query(rs)
    else:
        print("Email sent! Message ID:")
        print(response['MessageId'])


def validate_string(string):
    """Check that a string contains letters and numbers
    """
    letter_flag = False
    number_flag = False
    for i in string:
        if i.isalpha():
            letter_flag = True
        if i.isdigit():
            number_flag = True
    return letter_flag and number_flag

def file_push_to_s3(doc):

    with open(cred_path + path_separator + "aws_cred.txt") as f:
        aws_cr = json.loads(base64.b64decode(f.readline()).decode())
    dt = str(date.today()).split('-')
    mail_storage = config['crawler_config']['email_storage']['root_path']
    bucket = config['crawler_config']['email_storage']['bucket']
    if '#yyyy#' in mail_storage or '#mm#' in mail_storage or '#dd#' in mail_storage:
        mail_storage = mail_storage.replace('#yyyy#', dt[0]).replace('#mm#', dt[1]).replace('#dd#', dt[2])
    if '#received_date#' in mail_storage:
        mail_storage = mail_storage.replace('#received_date#', received_dt)
    if '#subject_date#' in mail_storage:
        mail_storage = mail_storage.replace('#subject_date#', subject_dt)
    key = mail_storage.replace('#jobid#',job_id).replace('#emailid#',email_id) +'/'+ doc.split(path_separator)[-1]
    print(f"Key : {key}")

    if bucket == 'a-coe-email-crw':
        #zip file
        with open(doc, 'rb') as f_in, gzip.open(doc+'.gz', 'wb') as f_out:
            f_out.writelines(f_in)
        key = key + '.gz'
        doc = doc + '.gz'
    try:
        s3_client = boto3.client('s3'
                    ,aws_access_key_id=aws_cr['username']
                    ,aws_secret_access_key=aws_cr['password'])
        s3_client.upload_file(doc, bucket, key)
        print(key.split('/')[-1] + ' pushed to s3')
        return True
    except Exception as e:
        print(e)
        # update logs
        rs = {}
        rs = {"update_task": 'update_log', "job_id": job_id, "source_id": source_id, "source": source,
              "log_type": "ERROR","log_details": [{"status": "error", "msg": str(traceback.format_exc().splitlines())}]}
        db_query(rs)
        # update status
        rs = {}
        rs = {"update_task": 'crawler_exec_status', "job_id": job_id, "data": "Error"}
        db_query(rs)
        return False

def get_serial(string):
    """
    Get first string that is at serial number format in a specific string
    """
    potential_sn = re.findall(REGEX_10_ALPHANUMERIC, string)
    potential_sn = [sn for sn in potential_sn if validate_string(sn)]

    if not potential_sn:
        return np.nan
    else:
        return potential_sn[0]

def parse_body(body, d_info):

    """Parse the body email, retrieving case and unit info
    """
    d_info['body_raw'] = body

    try:
        d_info['case_id_body'] = re.findall(REGEX_CASE_ID, body)[0]
    except IndexError:
        d_info['case_id_body'] = np.nan

    potential_sn = re.findall(REGEX_10_ALPHANUMERIC, body)
    potential_sn = [sn for sn in potential_sn if validate_string(sn)]

    try:
        d_info['serial_number'] = potential_sn[0]
        #print("A serial number was found")
        #print("Serial number is {}".format(potential_sn[0]))
    except IndexError:
        d_info['serial_number'] = np.nan

    potential_pn = re.findall(REGEX_7_ALPHANUMERIC, body)
    potential_pn = [pn for pn in potential_pn if validate_string(pn)]

    try:
        d_info['product_number'] = potential_pn[0]
    except IndexError:
        d_info['product_number'] = np.nan

    return d_info

def email_get_summary(msg):
    global received_dt
    global subject_dt
    r_list = []
    a_list = []
    sum = {}

    try:
        sum['subject'] = msg.Subject
        sum['received'] = str(datetime.strptime(str(msg.senton.date()) +'T'+ str(msg.senton.time()), "%Y-%m-%dT%H:%M:%S"))
        msg_from = msg.Sender.GetExchangeUser().PrimarySmtpAddress if msg.SenderEmailType == 'EX' else msg.SenderEmailAddress
        sum['from_domain'] = msg_from.split('@')[-1].replace('>', '')
        received_dt = str(datetime.strptime(str(msg.senton.date()), "%Y-%m-%d")).split(' ')[0]
        if "Week of " in sum['subject']:
            subject_dt = str(datetime.strptime(sum['subject'].split("Week of ")[-1], "%B %d, %Y"))

        # Get and save attachment details
        if not os.path.exists(attachment_path):
            os.makedirs(attachment_path)
        for a in msg.Attachments:
            a.SaveAsFile(os.path.join(attachment_path, a.Filename))
            attach = {}
            attach['name'] = str(a.Filename)
            attach['size'] = os.path.getsize(attachment_path + path_separator + a.Filename)
            a_list.append(attach)
        sum['attachements'] = a_list

        for r in msg.Recipients:
            r_mail = None
            if r.AddressEntry.GetExchangeUser() is not None:
                r_mail = r.AddressEntry.GetExchangeUser().PrimarySmtpAddress
                if '@' in str(r_mail):
                    r_list.append(str(r_mail).split('@')[-1].replace('>', ''))
        to_domain = dict(Counter(r_list))
        sum['to_domains'] = to_domain
        #print(sum)
        return sum

    except Exception as err:
        print("error in email get summary")
        # update logs
        rs = {}
        rs = {"update_task": 'update_log', "job_id": job_id, "source_id": source_id, "source": source,
              "log_type": "ERROR",
              "log_details": [{"status": "error", "msg": str(traceback.format_exc().splitlines())}]}
        db_query(rs)
        # update status
        rs = {}
        rs = {"update_task": 'crawler_exec_status', "job_id": job_id, "data": "Error"}
        db_query(rs)
        return False

def parse_email(msg):
    try:
        d_info ={}
        request_id = str(uuid.uuid4())

        d_info['REQUEST_ID'] = request_id
        d_info['SUBJECT'] = msg.Subject
        d_info['SUBMITTED_BY'] = msg.Sender.GetExchangeUser().PrimarySmtpAddress if msg.SenderEmailType == 'EX' else msg.SenderEmailAddress
        d_info['SUBMITTED_ON_DATE'] = (msg.senton.date())
        d_info['KEYWORD'] = ''
        for a in msg.Attachments:
            regex = re.compile(config['crawler_config']['email_lookup']['KEYWORD'])
            if re.match(regex, str(a.Filename)):
                d_info['KEYWORD'] = a.Filename
            #if config['crawler_config']['email_lookup']['KEYWORD'] in str(a.Filename):
                #d_info['KEYWORD'] = a.Filename
        #print(d_info)

        body = msg.Body
        d_info = parse_body(body, d_info)

        # If no SN found in e-mail body, check if in the subject
        body_sn = d_info['serial_number']

        d_info['subject_serial_number'] = get_serial(d_info['SUBJECT'])
        subject_sn = d_info['subject_serial_number']

        if subject_sn == subject_sn:  # True only if subject_sn is not NaN

            d_info['serial_number'] = subject_sn

        # Remove key to not alter schema
        d_info.pop('subject_serial_number', None)

        # Get and save attachments
        attachments = msg.Attachments
        d_info['ATTACHMENTS'] = len(attachments)
        return d_info

    except Exception:
        print("Error in get parse email")
        print(str(traceback.format_exc().splitlines()))

def is_outlook_running():
    for p in psutil.process_iter(attrs=['pid', 'name']):
        if "OUTLOOK.EXE" in p.info['name']:
            print(p.info['name'], "is running")
            os.system("taskkill /f /im  OUTLOOK.EXE")
            break

def get_outlook_mail():
    global email_id, attachment_path, mail_notification_details
    mail_lookup = config['crawler_config']['email_lookup']
    mailbox = config['crawler_config']['work_directory']
    mailbox_name = config['crawler_config']['mailbox']
    last_email_index = config['last_email_index']
    try:
        try:
            outlook = win32.Dispatch("Outlook.Application").GetNamespace("MAPI")
        except:
            try:
                is_outlook_running()
                # update logs
                rs = {}
                rs = {"update_task": 'update_log', "job_id": job_id, "source_id": source_id, "source": source,
                      "log_type": "ERROR",
                      "log_details": [{"status": "error", "msg": "outlook forcefully terminated"}]}
                db_query(rs)
                outlook = win32.Dispatch("Outlook.Application").GetNamespace("MAPI")
            except:
                rs = {}
                rs = {"update_task": 'update_log', "job_id": job_id, "source_id": source_id, "source": source,
                      "log_type": "ERROR",
                      "log_details": [{"status": "error", "msg": str(traceback.format_exc().splitlines())}]}
                db_query(rs)
                send_email()

        folder = outlook.Folders.Item(mailbox_name)
        inbox = folder.Folders.Item(mailbox)
        messages = inbox.Items
        messages.Sort("[ReceivedTime]", True) # True - new to old / False - old to new
        #print(messages)
        new_mails = False
        msg_dt_latest = datetime.strptime(str(messages[0].senton.date()) + ' ' + str(messages[0].senton.time()),"%Y-%m-%d %H:%M:%S")
        for i in range(0, len(messages)):
            if messages[i].Class == 43:
                message = messages[i]
                last_email_dt = datetime.strptime(last_email_index, "%Y-%m-%d %H:%M:%S")
                msg_dt = datetime.strptime(str(message.senton.date()) + ' ' + str(message.senton.time()), "%Y-%m-%d %H:%M:%S")
                if last_email_dt < msg_dt:
                    # update logs
                    if new_mails is False:
                        rs = {}
                        rs = {"update_task": 'update_log', "job_id": job_id, "source_id": source_id, "source": source,
                              "log_type": "INFO", "log_details": [{"status": "ok", "msg": "processing new emails"}]}
                        db_query(rs)
                    new_mails = True
                    # Parse email
                    d_info = parse_email(message)
                    mail_details = {'SUBJECT': d_info['SUBJECT'], 'SUBMITTED_BY': d_info['SUBMITTED_BY'],'KEYWORD':d_info['KEYWORD']}
                    mail_notification_details = {'SUBJECT': d_info['SUBJECT'], 'SUBMITTED_BY': d_info['SUBMITTED_BY'],
                                                 'KEYWORD': d_info['KEYWORD']}
                    rs = []
                    for c in mail_lookup.keys():
                        if re.match(re.compile(mail_lookup[c]), mail_details[c]):
                            rs.append(True)
                        else:
                            rs.append(False)
                        #rs.append(mail_lookup[c] in mail_details[c])
                    if len(list(set(rs)))==1 and list(set(rs))[0]:
                        mail_notification_details = {'SUBJECT': d_info['SUBJECT'], 'SUBMITTED_BY': d_info['SUBMITTED_BY'],'KEYWORD':d_info['KEYWORD']}
                        send_email_notification()
                        #print('processing mail')
                        email_id = hashlib.md5(d_info['SUBJECT'].encode("utf-8") + d_info['KEYWORD'].encode("utf-8") + str(msg_dt).encode("utf-8")).hexdigest()
                        #print(email_id)
                        # register new email
                        rs = {}
                        rs = {"update_task": "register_new_email", "job_id": job_id, "data": email_id}
                        register_new_email = db_query(rs)

                        #get email summary and save attachments
                        attachment_path = os.getcwd() + path_separator + email_id
                        sum = email_get_summary(message)

                        # check if the register_new_email as successful
                        if register_new_email['email_registered']:
                            # push attachment to s3
                            file_push_to_s3(attachment_path + path_separator + d_info['KEYWORD'])

                        #update email summary
                        # update db with email summary
                        rs = {}
                        rs = {"update_task": 'set_email_summary', "job_id": job_id, "email_id": email_id, "data": sum}
                        db_query(rs)
                        shutil.rmtree(attachment_path)
                else:
                    break
                    #continue
        if new_mails:
            # post the current max mail datetime after processing new mails
            rs = {}
            rs = {"update_task": 'last_email_index', "job_id": job_id, "data": str(msg_dt_latest)}
            register_email_index = db_query(rs)
        else:
            print('no new mails')
            # update logs
            rs = {}
            rs = {"update_task": 'update_log', "job_id": job_id, "source_id": source_id, "source": source,
                  "log_type": "INFO", "log_details": [{"status": "ok", "msg": "no new emails"}]}
            db_query(rs)

    except Exception:
        print("Error in get outlook")
        print(str(traceback.format_exc().splitlines()))
        # update logs
        rs = {}
        rs = {"update_task": 'update_log', "job_id": job_id, "source_id": source_id, "source": source,
              "log_type": "ERROR","log_details": [{"status": "error", "msg": str(traceback.format_exc().splitlines())}]}
        db_query(rs)
        # update status
        rs = {}
        rs = {"update_task": 'crawler_exec_status', "job_id": job_id, "data": "Error"}
        db_query(rs)
        send_email()

def main():
    global config, job_id, source, source_id, contact, business_contact
    job_id = ''
    source = 'crawler'
    jobid_list = []
    try:
        with open(cred_path + path_separator + "crawler_config.txt") as f:
            source_id = f.readline()
        # update heart_beat
        rs = {}
        rs = {"update_task":'update_log', "source_id": source_id, "source": source,
              "log_type": "INFO", "log_details": [{"status": "ok", "msg": "heart_beat", "user": win_user}]}
        db_query(rs)

        #get scheduled jobs
        jobs = get_jobs_to_run(source_id)
        #print(jobs)
        if jobs is not None:
            # update logs
            jobid_list = [j["job_id"] for j in jobs]
            rs = {}
            rs = {"update_task":'update_log', "source_id": source_id, "source": source,
                  "log_type": "INFO", "log_details": [{"status": "ok", "msg": "job_id_list", "job_ids": jobid_list}]}
            db_query(rs)
            for jb in jobs:
                job_id = jb["job_id"]
                qr={"job_id":jb["job_id"], "source_id": source_id, "source": source}
                config = db_query(qr,'config')
                print(config)

                if config['response']['status'] == 'ok':
                    contact = config["it_contact"]
                    business_contact = config["business_contact"]
                    # update status
                    rs = {}
                    rs = {"update_task": 'crawler_exec_status',"job_id":job_id, "source_id": source_id,"data": "Running"}
                    db_query(rs)
                    # update logs
                    rs = {}
                    rs = {"update_task": 'update_log' ,"job_id":job_id, "source_id": source_id, "source": source,
                          "log_type": "INFO", "log_details": [{"status":"ok", "msg":"process started"}]}
                    db_query(rs)

                else:
                    print('error in config response')
                    # update logs
                    rs = {}
                    rs = {"update_task": 'update_log', "job_id": job_id, "source_id": source_id, "source": source,
                          "log_type": "ERROR","log_details": [{"status": "error", "msg": "error in config response"}]}
                    db_query(rs)
                    # update status
                    rs = {}
                    rs = {"update_task": 'crawler_exec_status', "job_id": job_id, "data": "Error"}
                    db_query(rs)
                    sys.exit(0)

                #Start outlook check
                get_outlook_mail()

                # update logs
                rs = {}
                rs = {"update_task": 'update_log', "job_id": job_id, "source_id": source_id, "source": source,
                      "log_type": "INFO", "log_details": [{"status": "ok", "msg": "process completed"}]}
                db_query(rs)
                # update status
                rs = {}
                rs = {"update_task": 'crawler_exec_status', "job_id": job_id, "data": "COMPLETED"}
                db_query(rs)

    except Exception as er:
        print("-" * 30)
        print(er)
        print(str(traceback.format_exc().splitlines()))
        print("-" * 30)
        # update logs
        rs = {}
        rs = {"update_task": 'update_log',"job_id": job_id,  "source_id": source_id, "source": source,
              "log_type": "ERROR", "log_details": [{"status": "error", "msg": str(traceback.format_exc().splitlines())}]}
        db_query(rs)
        # update status
        rs = {}
        rs = {"update_task": 'crawler_exec_status', "job_id": job_id,"data": "Error"}
        db_query(rs)
        # send mail
        send_email()
    finally:
        if jobs is not None:
            for j in jobs:
                # update status
                rs = {}
                rs = {"update_task": 'crawler_exec_status_finally', "job_id": j['job_id']}
                db_query(rs)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print(str(traceback.format_exc().splitlines()))
