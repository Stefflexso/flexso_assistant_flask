import subprocess
import time
import json
import pandas as pd 
import boto3
import io
from dateutil import parser
from dateutil.tz import tzutc
from concurrent.futures import ThreadPoolExecutor
from fpdf import FPDF
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfbase import pdfmetrics
from reportlab.lib.units import mm


with open("s3_bucket_secrets.json", "r") as f:
    amazon_s3_secrets = json.load(f)

# S3 client using root credentials (or IAM user credentials)
# Create S3 client with root user credentials
s3_client = boto3.client(
    's3',
    aws_access_key_id=amazon_s3_secrets["aws_access_key_id"],
    aws_secret_access_key=amazon_s3_secrets["aws_secret_access_key"],
    region_name='eu-central-1'  # Change if needed
)

pdf = FPDF()

#general run command function
# variables defined in original extract
# MAX_RETRIES = 3
# CLI_TIMEOUT = 30
# RETRY_DELAY = 10
def run_cli(command: str) -> str:
    count = 0
    while count < 1: #MAX_RETRIES
        proc = subprocess.run(command, shell=True, capture_output=True, text=True, encoding="utf-8", timeout=30) #CLI_TIMEOUT
        if proc.returncode == 0:
            return proc.stdout.strip()
        if proc.returncode == 1:
            return proc.stdout.strip()
        time.sleep(10) #retry delay
        count += 1
        print(count)
    return subprocess.run(command + " --verbose", shell=True, capture_output=True, text=True, encoding="utf-8", timeout=30)

#command list datasphere
def command_datasphere_list_metadata_object(object: str, space: str, skip: int, necessary_metadata: str, top=200):
    metadata = run_cli(f"datasphere objects {object} list --space {space} --top {top} --skip {skip} --select {necessary_metadata}")
    return json.loads(metadata)

#get the metadata from all objects in a space with previously decided necessary metadata
def get_object_level_metadata(object: str, space: str, skip: int, necessary_metadata: str, top=200):

    all_metadata_from_object = []

    while True:
        metadata_json = command_datasphere_list_metadata_object(object=object, space=space, top=top, necessary_metadata=necessary_metadata, skip=skip)
        if not metadata_json:
            break

        all_metadata_from_object.extend(metadata_json)

        if len(metadata_json) < top:
            break  # Last page
        skip += top
    
    boolean_true_if_metadata = bool(all_metadata_from_object) #boolean is true if there is metadata
    
    return all_metadata_from_object, boolean_true_if_metadata

#stores the metadata extracted from datasphere cli as a csv file in s3 bucket
def store_json_object_level_metadata_in_s3_as_csv(metadata_json, object_type, space, bucket):
    
    # Convert JSON to DataFrame
    df = pd.DataFrame(metadata_json)

    # Convert DataFrame to CSV in memory
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    # Create S3 key
    s3_key = f"object_level_metadata_csv/{space}/{object_type}.csv"

    # Store in S3
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=csv_buffer.getvalue().encode("utf-8")
    )


#for this you first need to run get_object_level_metadata and use this output for the function
#returns a dataframe with columns technical_name and changed_on based on datasphere metadata
def get_changed_date_entity_level_metadata_ds(metadata_json):

    # If metadata_json is empty, return an empty DataFrame with the right columns
    if not metadata_json or not metadata_json[0]:
        return pd.DataFrame(columns=["technical_name", "changed_on"])
    df = pd.DataFrame([
        {
            "technical_name": item["technicalName"],
            "changed_on": parser.isoparse(item["changedOn"]).astimezone(tzutc())
        }
        for item in metadata_json
    ])

    return df

#extracts metadata directly from bucket based on space and object path and does this for all the files within this directory
#returns a dataframe with columns technical_name, last_modified and path_s3 based on s3 metadata
def get_changed_date_entity_level_metadata_s3_json(bucket, space, object_type):
    # initialize empty dataframe
    df = pd.DataFrame(columns=["technical_name", "last_modified", "path_s3"])

    prefix = f'entity_level_metadata_json/{space}/{object_type}'

    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

    
    for obj in response.get("Contents", []): 
        technical_name = obj['Key'].split('/')[-1].replace('.json', '')
        last_modified = obj['LastModified']
        path_s3 = obj["Key"]

        
        df = pd.concat([
            df,
            pd.DataFrame([{
                'technical_name': technical_name,
                'last_modified': last_modified,
                'path_s3': path_s3
            }])
        ], ignore_index=True)

    return df

def get_changed_date_entity_level_metadata_s3_pdf(bucket, space, object_type):
    # initialize empty dataframe
    df = pd.DataFrame(columns=["technical_name", "last_modified", "path_s3"])
    
    prefix = f'entity_level_metadata_pdf/{space}/{object_type}' 

    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

    for obj in response.get("Contents", []):
        technical_name = obj['Key'].split('/')[-1].replace('.pdf', '')
        last_modified = obj['LastModified']
        path_s3 = obj["Key"]

        df = pd.concat([
            df,
            pd.DataFrame([{
                'technical_name': technical_name,
                'last_modified': last_modified,
                'path_s3': path_s3
            }])
        ], ignore_index=True)

    return df

#returns technical names of objects to renew or to add as a list
#based on outcome of get_changed_date_entity_level_metadata_ds and get_changed_date_entity_level_metadata_s3 respectively
#get everything before point to get technical name
def what_objects_to_add_renew(entity_level_metadata_ds_df, json_metadata_s3_df):

    object_to_add = entity_level_metadata_ds_df[
    ~entity_level_metadata_ds_df["technical_name"].isin(json_metadata_s3_df["technical_name"])
    ]

    #update
    comparing_changed_dates = pd.merge(
        entity_level_metadata_ds_df,
        json_metadata_s3_df,
        on="technical_name",
        how="inner"
    )

    # Keep rows where changed_on > last_modified
    objects_changed = comparing_changed_dates[comparing_changed_dates["changed_on"] > comparing_changed_dates["last_modified"]]

    list1 = object_to_add["technical_name"].tolist()
    list2 = objects_changed["technical_name"].tolist()

    list_of_technical_names_to_read = set(list1 + list2)

    return list_of_technical_names_to_read


def command_datasphere_read_metadata_object(object_type, space, technical_name):
    enitity_level_metadata = run_cli(f"datasphere objects {object_type} read --space {space} --technical-name {technical_name}")
    return json.loads(enitity_level_metadata)


def store_json_entity_level_metadata_in_s3(metadata_json, technical_name, object_type, space, bucket):
    s3_key = f"entity_level_metadata_json/{space}/{object_type}/{technical_name}.json"
    s3_client.put_object(Bucket=bucket, Key=s3_key, Body=json.dumps(metadata_json, indent=4).encode("utf-8"))


def store_pdf_entity_level_metadata_in_s3(metadata_json, technical_name, object_type, space, bucket):

    s3_key = f"entity_level_metadata_pdf/{space}/{object_type}/{technical_name}.pdf"
    
    pretty_json = json.dumps(metadata_json, indent=2, ensure_ascii=False)

    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4

    
    pdfmetrics.registerFont(TTFont('DejaVu', 'DejaVuSansMono.ttf'))
    c.setFont('DejaVu', 12)

    # Title
    c.drawCentredString(width / 2, height - 20*mm, technical_name)

    # JSON text
    text_object = c.beginText(10*mm, height - 30*mm)
    text_object.setFont('DejaVu', 10)

    for line in pretty_json.splitlines():
        if text_object.getY() < 15*mm:  # bottom margin
            c.drawText(text_object)
            c.showPage()
            text_object = c.beginText(10*mm, height - 20*mm)
            text_object.setFont('DejaVu', 10)
        text_object.textLine(line)
    c.drawText(text_object)

    c.showPage()
    c.save()

    pdf_bytes = buffer.getvalue()

    s3_client.put_object(Bucket=bucket, Key=s3_key, Body=pdf_bytes, ContentType="application/pdf")

#returns a list of technical names to delete
def what_objects_to_delete(entity_level_metadata_ds_df, json_metadata_s3_df):

    # Filter technical names present in S3 but not in Datasphere
    only_in_s3 = json_metadata_s3_df[
        ~json_metadata_s3_df["technical_name"].isin(entity_level_metadata_ds_df["technical_name"])
    ]

    # Return as a list
    return only_in_s3["path_s3"].to_list()

#deletes object from s3
def delete_object_from_s3(bucket_name, object_path_s3):

    s3_client.delete_object(Bucket=bucket_name, Key=object_path_s3)