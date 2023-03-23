import pg8000, json, boto3
from random import randrange

class SQL:

    def __init__(self,random_host=False,host_id=0):
        self.con_data = None
        try:
            session = boto3.session.Session()
            client = session.client(service_name='secretsmanager', region_name='us-west-2')
            self.con_details = client.get_secret_value(SecretId='email_crawler_db')
            self.con_data = json.loads(self.con_details['SecretString'])
        except Exception:
            session = boto3.session.Session()
            client = session.client(service_name='secretsmanager', region_name='us-west-2')
            self.con_details = client.get_secret_value(SecretId='email_crawler_db')
            self.con_data = json.loads(self.con_details['SecretString'])

        self.rhost = random_host
        self.hid = host_id

    def conn(self):

        rhid =randrange(len(self.con_data['host']))
        #print(rhid)
        try:
            self.connection = pg8000.connect(
                user=self.con_data['user'],
                password=self.con_data['pwd'],
                host=self.con_data['host'][rhid] if self.rhost else self.con_data['host'][self.hid],
                port=self.con_data['port'],
                database=self.con_data['db'])
        except Exception:
            self.connection = pg8000.connect(
                user=self.con_data['user'],
                password=self.con_data['pwd'],
                host=self.con_data['host'][rhid] if self.rhost else self.con_data['host'][self.hid],
                port=self.con_data['port'],
                database=self.con_data['db'])

        self.cursor = self.connection.cursor()

    def select(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def execute(self, query):
        self.cursor.execute(query)
        self.connection.commit()

    def executeAndReturn(self, query):
        self.cursor.execute(query)
        self.connection.commit()
        return self.cursor.fetchall()

    def close_conn(self):
        self.connection.close()
