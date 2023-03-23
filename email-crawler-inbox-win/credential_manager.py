import boto3,json,traceback,os,platform
import base64
path_separator = '/' if 'linux' in str(platform.system()).lower() else '\\'
path = os.path.abspath(os.path.join(os.getcwd(), os.pardir)) + path_separator +'cred_files'

def read_aws_cred():
    global aCred
    with open(path + path_separator +"aws_cred.txt") as f:
        aCred = json.loads(base64.b64decode(f.readline()).decode())

def update_cred_files(s_name):
    try:
        secrets_client = boto3.client('secretsmanager', region_name='us-west-2',
                    aws_access_key_id=aCred['username'],
                    aws_secret_access_key=aCred['password'])
        sec_details = secrets_client.get_secret_value(SecretId=s_name)
        credentials = json.loads(sec_details['SecretString'])
        #update file with creadentials
        if s_name == 'email_crawler_db':
            filename = "db_cred.txt"
        elif s_name == 'crawler_email_auth':
            filename = "aws_cred.txt"
        # update the file with  cred
        with open(path + path_separator +filename, 'wb') as outfile:
            outfile.write(base64.b64encode(json.dumps(credentials).encode('utf-8')))

    except Exception as e:
        print('error in update cred files')
        print(e)
        return None
def main():
    read_aws_cred()
    update_cred_files('crawler_email_auth')
    update_cred_files('email_crawler_db')

if __name__ == "__main__":
    try:
        main()
    except Exception:
        print(str(traceback.format_exc().splitlines()))
