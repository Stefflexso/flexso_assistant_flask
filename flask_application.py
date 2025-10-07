from flask import Flask, request, render_template_string
import subprocess
import os
from initialize_datasphere_cli import set_host, cache_initialization
from extract_helper_functions import *
from llm_ask_helper_functions import *

app = Flask(__name__)
port = int(os.environ.get('PORT', 3000))

#load secrets of datasphere cli
secret_cli_json_path = "secrets_file.json"

with open(secret_cli_json_path, "r") as f:
    login_ds_cli_information = json.load(f)

client_id_cli = login_ds_cli_information.get("client_id")
client_secret_cli = login_ds_cli_information.get("client_secret")
authorization_url_cli = login_ds_cli_information.get("authorization_url")
token_url_cli = login_ds_cli_information.get("token_url")
access_token_cli = login_ds_cli_information.get("access_token")
refresh_token_cli = login_ds_cli_information.get("refresh_token")
host_cli = login_ds_cli_information.get("host")

#load secrets of sap ai core
ai_core_secrets_path = "ai_core_secret_orchestration.json"

with open(ai_core_secrets_path, encoding="utf-8") as f:
    secrets_ai_core = json.load(f)

auth_url_ai_core = secrets_ai_core["auth_url"]
client_id_ai_core = secrets_ai_core["client_id"]
client_secret_ai_core = secrets_ai_core["client_secret"]
genai_url = secrets_ai_core["genai_url"]

#define spaces and object_types to be searched
SPACES = ["DEV_CENTRAL_SPC"]
OBJECT_TYPES = ['remote-tables',
 'local-tables',
 'er-models',
 'views',
 'analytic-models',
 'task-chains',
 'data-flows',
 'replication-flows',
 'transformation-flows',
 'data-access-controls',
 'business-entities',
 'fact-models',
 'consumption-models',
 'intelligent-lookups']

# OBJECT_TYPES = ["transformation-flows"]

SKIP = 0
BUCKET = "hcp-8b40cf9e-4ef4-4eb6-8da8-32f9f911183e"

list_necessary_metadata = [
    "technicalName",
    "businessName",
    "type",
    "semanticUsage",
    "status",
    "createdOn",
    "changedOn",
    "deployedOn",
    "createdBy",
    "changedBy"
]
necessary_metadata = ",".join(list_necessary_metadata)

HTML = """
<!doctype html>
<title>Ask question to grounded llm</title>
<h2>Enter a question to the llm</h2>
<form method="POST" action="/">
  Question: 
  <textarea name="question" style="width: 600px; height: 150px; resize: both;"></textarea>
  <br><br>
  <input type="submit" name="action" value="Ask question">
  <input type="submit" name="action" value="Extract Function">
</form>

{% if output is not none %}
  <h3>Output:</h3>
  <pre>{{ output }}</pre>
{% endif %}

{% if error is not none %}
  <h3 style="color: red;">Error:</h3>
  <pre>{{ error }}</pre>
{% endif %}
"""

#########################
#general functions
#########################
def extract_function():
  outputstring = ""
  for space in SPACES:
      for object_type in OBJECT_TYPES:
          #get metadata from datasphere
          object_level_metadata_ds, boolean = get_object_level_metadata(object=object_type, space=space, skip=SKIP, necessary_metadata=necessary_metadata)
          
          #store metadata from ds as csv in s3 bucket
          store_json_object_level_metadata_in_s3_as_csv(metadata_json=object_level_metadata_ds, object_type=object_type, space=space, bucket=BUCKET)

          #create dataframes for cross referencing objects s3 and ds
          metadata_ds_df = get_changed_date_entity_level_metadata_ds(object_level_metadata_ds)

          metadata_s3_df_json = get_changed_date_entity_level_metadata_s3_json(bucket=BUCKET, space=space, object_type=object_type)

          metadata_s3_df_pdf = get_changed_date_entity_level_metadata_s3_pdf(bucket=BUCKET, space=space, object_type=object_type)

          #adding or updating existing objects
          list_objects_add_renew_tech_name_json = what_objects_to_add_renew(metadata_ds_df, metadata_s3_df_json)

          list_objects_add_renew_tech_name_pdf = what_objects_to_add_renew(metadata_ds_df, metadata_s3_df_pdf)

          list_objects_add_renew_tech_name = list_objects_add_renew_tech_name_json.union(list_objects_add_renew_tech_name_pdf)

          def function_for_concurrent_storing(tech_name):
              
              #print(f"working on {tech_name}")
              entity_level_metadata = command_datasphere_read_metadata_object(
              object_type=object_type,
              space=space,
              technical_name=tech_name
              )

              store_json_entity_level_metadata_in_s3(metadata_json=entity_level_metadata, technical_name=tech_name,
                                                      object_type=object_type, space=space, bucket=BUCKET)
              
              store_pdf_entity_level_metadata_in_s3(metadata_json=entity_level_metadata, technical_name=tech_name,
                                                      object_type=object_type, space=space, bucket=BUCKET)
              #print(f"finished with {tech_name}")

          with ThreadPoolExecutor(max_workers=5) as executor:
              list(
                  executor.map(
                      function_for_concurrent_storing,
                      list_objects_add_renew_tech_name
                  )
              )
          #deleting objects from s3 if non existant in datasphere
          list_objects_to_delete_s3_path_json = what_objects_to_delete(metadata_ds_df, metadata_s3_df_json)

          list_objects_to_delete_s3_path_pdf = what_objects_to_delete(metadata_ds_df, metadata_s3_df_pdf)

          list_objects_to_delete_s3_path = list_objects_to_delete_s3_path_json.extend(list_objects_to_delete_s3_path_pdf)

          if list_objects_to_delete_s3_path:
              for s3_path in list_objects_to_delete_s3_path:
                  delete_object_from_s3(bucket_name=BUCKET, object_path_s3=s3_path)

          outputstring = outputstring + f"space {space} object_type {object_type}\n\nadded or updated {list_objects_add_renew_tech_name}\n\ndeleted {list_objects_to_delete_s3_path}\n\n"

  
  return outputstring

gen_ai_token = get_acces_token_ai_core(client_id=client_id_ai_core, client_secret=client_secret_ai_core, auth_url=auth_url_ai_core)

@app.route("/", methods=["GET", "POST"])
def run_command():
    output = None
    error = None

    if request.method == "POST":
        action = request.form.get("action")

        if action == "Ask question":
            question = request.form.get("question")
            output = ask_llm(question=question, token=gen_ai_token, genai_url=genai_url)

        if action == "Extract Function":
            output = extract_function()

    return render_template_string(HTML, output=output, error=error)


if __name__ == '__main__':
    set_host(host_cli)
    cache_initialization(host_cli, secret_cli_json_path)
    app.run(host='0.0.0.0', port=port)
    #app.run(host='127.0.0.1', port=5000, debug=True)
