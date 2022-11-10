'''import ibm_db
conn = ibm_db.connect("DATABASE=bludb; HOSTNAME=b70af05b-76e4-4bca-a1f5-23dbb4c6a74e.c1ogj3sd0tgtu0lqde00.databases.appdomain.cloud; PORT=32716; SECURITY=SSL;SSLServerCertificate=DigiCertGlobalRootCA.crt;UID=xnt96883;PWD=9264toUt1JvksMo6",'','')
print(conn)
print("connection successfull...")

app = Flask(__name__)'''

from flask import Flask,redirect,url_for,render_template,request
import ibm_boto3
from ibm_botocore.client import Config, ClientError

COS_ENDPOINT="https://s3.jp-tok.cloud-object-storage.appdomain.cloud"
COS_API_KEY_ID="_kPfkf1cOMTulYGejQgBE3Rq2PQfC9uRJf9wkOHQcLeX"
COS_INSTANCE_CRN="crn:v1:bluemix:public:cloud-object-storage:global:a/9d2fdf95509049dd923815de404bce6d:2a174862-e3e7-40a8-b346-d5b76fcde86a::"

cos = ibm_boto3.resource("s3",
    ibm_api_key_id = COS_API_KEY_ID,
    ibm_service_instance_id = COS_INSTANCE_CRN,
    config = Config(signature_version = "oauth"),
    endpoint_url = COS_ENDPOINT)

app = Flask(__name__)

def get_bucket_contents(bucket_name):
    print("Retrieving bucket contents from: {0}".format(bucket_name))
    try:
        files = cos.Bucket(bucket_name).objects.all()
        files_names = []
        for file in files:
            files_names.append(file.key)
            print("Item: {0} ({1} bytes).".format(file.key, file.size))
        return files_names
    except ClientError as be:
        print("CLIENT ERROR: {0}\n".format(be))
    except Exception as e:
        print("Unable to retrieve bucket contents: {0}".format(e))

def get_item(bucket_name, item_name):
    print("Retrieving item from bucket: {0}, key: {1}".format(bucket_name, item_name))
    try:
        file = cos.Object(bucket_name, item_name).get()
        print("File Contents: {0}".format(file["Body"].read()))
    except ClientError as be:
        print("CLIENT ERROR: {0}\n".format(be))
    except Exception as e:
        print("Unable to retrieve file contents: {0}".format(e))

def delete_item(bucket_name, object_name):
    try:
        cos.delete_object(Bucket = bucket_name, Key = object_name)
        print("Item: {0} deleted!\n".format(object_name))
    except ClientError as be:
        print("CLIENT ERROR: {0}\n".format(be))
    except Exception as e:
        print("Unable to delete object: {0}".format(e))

def multi_part_upload(bucket_name, item_name, file_path):
    try:
        print("Starting file transfer for {0} to bucket: {1}\n".format(item_name, bucket_name))
        part_size = 1024*1024*5
        file_threshold = 1024*1024*15

        transfer_config = ibm_boto3.s3.transfer.TransferConfig(
            multipart_threshold = file_threshold,
            multipart_chunksize = part_size
        )

        with open(file_path, "rb") as file_data:
            cos.Object(bucket_name, item_name).upload_fileobj(
            Fileobj = file_data,
            Config = transfer_config
        )

    except ClientError as be:
        print("CLIENT ERROR: {0}\n".format(be))
    except Exception as e:
        print("Unable to complete miulti-part upload: {0}".format(e)) 

@app.route("/")
def index():
    files = get_bucket_contents("assignment3db2")
    return render_template("index.html", files=files)


if __name__=='__main__':
    app.run(debug=True)