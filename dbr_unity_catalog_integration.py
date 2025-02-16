# Databricks notebook source
import json, os
import requests
import time
from concurrent.futures import ThreadPoolExecutor

# COMMAND ----------

user_identity_bearer_token = "user_identity_bearer_token"

# COMMAND ----------

tenant_id = "your tenant id"

spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set(
    "fs.azure.account.oauth.provider.type",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
)
spark.conf.set("fs.azure.account.oauth2.client.id", fabric_client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret", fabric_client_secret)
spark.conf.set(
    "fs.azure.account.oauth2.client.endpoint",
    f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
)

# COMMAND ----------

def get_databricks_uc_tables(databricks_config):    
    all_tables = []

    dbx_workspace = databricks_config['dbx_workspace']
    dbx_token = databricks_config['dbx_token']
    dbx_uc_catalog = databricks_config['dbx_uc_catalog']
    dbx_uc_schemas = databricks_config['dbx_uc_schemas']

    for schema in dbx_uc_schemas:
        url = f"{dbx_workspace}/api/2.1/unity-catalog/tables?catalog_name={dbx_uc_catalog}&schema_name={schema}"
        payload = {}
        headers = {
            'Authorization': f'Bearer {dbx_token}',
            'Content-Type': 'application/json'
        }
        response = requests.get(url, data=payload, headers=headers)

        if response.status_code == 200:
            response_json = json.loads(response.text)
            all_tables.extend(response_json['tables'])
        else:
            print(f"Error Occurred: [{response.status_code}] Cannot connect to Unity Catalog. Please review configs.")
            return None
    return all_tables

# COMMAND ----------

FABRIC_API_ENDPOINT = "api.fabric.microsoft.com"
ONELAKE_API_ENDPOINT = "onelake.dfs.fabric.microsoft.com"

def get_lakehouse_shortcuts(fabric_config):
    workspace_id = fabric_config['workspace_id']
    lakehouse_id = fabric_config['lakehouse_id']

    table_infos = dbutils.fs.ls(f"abfss://{workspace_id}@{ONELAKE_API_ENDPOINT}/{lakehouse_id}/Tables")
    current_shortcut_names = [table_info.name for table_info in table_infos]
    return current_shortcut_names

def create_shortcuts(fabric_config, tables):    
    sc_created, sc_failed, sc_skipped = 0, 0, 0

    workspace_id = fabric_config['workspace_id']
    lakehouse_id = fabric_config['lakehouse_id']
    shortcut_connection_id = fabric_config['shortcut_connection_id']
    #shortcut_name = fabric_config['shortcut_name']
    skip_if_shortcut_exists = True #fabric_config['skip_if_shortcut_exists']

    max_retries = 3
    max_threads = 2

    def create_shortcut(table):        
        nonlocal sc_created, sc_failed, sc_skipped
        catalog_name = table['catalog_name']
        schema_name = table['schema_name']
        operation = table['operation']
        #table_name = shortcut_name.format(schema=schema_name, table=table['name'], catalog=catalog_name)
        #table_name = f"{schema_name}_{table['name']}"
        table_name = f"{table['name']}"
        table_type = table['table_type']

        if operation == "create":

            if table_type in {"EXTERNAL"}:

                data_source_format = table['data_source_format']
                table_location = table['storage_location']

                if data_source_format == "DELTA":
                    # Remove the 'abfss://' scheme from the path
                    without_scheme = table_location.replace("abfss://", "", 1)

                    # Extract the storage account name and the rest of the path
                    container_end = without_scheme.find("@")
                    container = without_scheme[:container_end]
                    remainder = without_scheme[container_end + 1:]

                    account_end = remainder.find("/")
                    storage_account = remainder[:account_end]
                    path = remainder[account_end + 1:]
                    https_path = f"https://{storage_account}/{container}"

                    url = f"https://{FABRIC_API_ENDPOINT}/v1/workspaces/{workspace_id}/items/{lakehouse_id}/shortcuts"

                    if not skip_if_shortcut_exists:
                        url += "?shortcutConflictPolicy=GenerateUniqueName"

                    token = user_identity_bearer_token

                    payload = {
                        "path": f"Tables/{schema_name}",
                        "name": table_name,
                        "target": {
                            "adlsGen2": {
                                "location": https_path,
                                "subpath": path,
                                "connectionId": shortcut_connection_id
                            }
                        }
                    }
                    headers = {
                        'Authorization': f'Bearer {token}',
                        'Content-Type': 'application/json',
                    }

                    for attempt in range(max_retries):
                        try:
                            response = requests.post(url, json=payload, headers=headers)
                            if response.status_code == 429:
                                retry_after_seconds = 60
                                if 'Retry-After' in response.headers:
                                    retry_after_seconds = int(response.headers['Retry-After']) + 5
                                print(f"! Upps [429] Exceeded the amount of calls while creating '{table_name}', sleeping for {retry_after_seconds} seconds.")
                                time.sleep(retry_after_seconds)
                            elif response.status_code in [200, 201]:
                                data = json.loads(response.text)
                                print(f"Shortcut created successfully with name:'{data['name']}'")
                                sc_created += 1
                                break
                            elif response.status_code == 400:
                                data = json.loads(response.text)
                                error_details = data.get('moreDetails', [])
                                error_message = error_details[0].get('message', 'No error message found')
                                if error_message == "Copy, Rename or Update of shortcuts are not supported by OneLake.":
                                    print(f"Skipped shortcut creation for '{table_name}'. Shortcut with the SAME NAME exists.")
                                    sc_skipped += 1
                                    break
                                elif "Unauthorized. Access to target location" in error_message:
                                    print(f"status_code [400] Cannot create shortcut for '{table_name}'. Access denied, please review.")
                                else:
                                    print(f"! Upps [{response.status_code}] Failed to create shortcut. Response details: {response.text}")
                            elif response.status_code == 403:
                                print(f"! Upps [403] Cannot create shortcut for '{table_name}'. Access forbidden, please review.")
                            else:
                                print(f"! Upps [{response.status_code}] Failed to create shortcut '{table_name}'. Response details: {response.text}")
                        except requests.RequestException as e:
                            print(f"Request failed: {e}")
                        if attempt < 2:
                            sleep_time = 2 ** attempt
                            print(f"___ Retrying in {sleep_time} seconds for '{table_name}'...")
                            time.sleep(sleep_time)
                        else:
                            print(f"! Max retries reached for '{table_name}'. Exiting.")
                            sc_failed += 1
                            break
                else:
                    print(f"Skipped shortcut creation for '{table_name}'. Format not supported: {data_source_format}.")
                    sc_skipped += 1
            else:
                print(f"∟ Skipped shortcut creation for '{table_name}'. Table type is not EXTERNAL.")
                sc_skipped += 1
        else:
            print(f"∟ Skipped shortcut creation for '{table_name}'. Shortcut with the SAME NAME exists.")
            sc_skipped += 1 
        
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        executor.map(create_shortcut, tables)
    
    return sc_created, sc_skipped, sc_failed

# COMMAND ----------

def dbr_uc_tables_to_onelake(databricks_config, fabric_config):
    tables = get_databricks_uc_tables(databricks_config)
    all_shortcuts = get_lakehouse_shortcuts(fabric_config)

    fabric_shortcuts = [name for name in all_shortcuts]

    uc_table_names = [f"{entry['schema_name']}_{entry['name']}" for entry in tables]

    skipped_required = [name for name in fabric_shortcuts if name in uc_table_names]
    #deletions_required = [name for name in fabric_shortcuts if name not in uc_table_names]
    creations_required = [name for name in uc_table_names if name not in fabric_shortcuts]

    print(f"Started syncing from Unity Catalog to Fabric...")

    # Delete shortcuts if not exist in UC tables
    '''
    consider_dbx_uc_table_changes = fabric_config['consider_dbx_uc_table_changes']
    sc_deleted, sc_failed_delete = 0, 0
    if(consider_dbx_uc_table_changes): 
        sc_deleted, sc_failed_delete = delete_shortcuts(fabric_config, deletions_required)
    '''

    # Create shortcuts if not exist in Lakehouse
    if tables is not None:
        for entry in tables:
            if f"{entry['schema_name']}_{entry['name']}" in creations_required:
                entry['operation'] = 'create'
            else:
                entry['operation'] = 'skip'
        sc_created, sc_skipped, sc_failed_create = create_shortcuts(fabric_config, tables)

    #total_failed = sc_failed_delete + sc_failed_create
    #print(f"\nSync finished. {sc_created} shortcuts created, {sc_skipped} skipped, {total_failed} failed, {sc_deleted} deleted.")
    print(f"\nSync finished. {sc_created} shortcuts created, {sc_skipped} skipped.")

# COMMAND ----------

# Databricks workspace
dbx_workspace = your_databricks_workspace
dbx_token = your_databricks_pat_token

# Unity Catalog
dbx_uc_catalog = "databricks_catalog_name"
dbx_uc_schemas = '["schema1", "schema2","schema3"]'

# Fabric
fab_workspace_id = "fabric_workspace_id"
fab_lakehouse_id = "fabric_lakehouse_id"
fab_shortcut_connection_id = "cloud_connection_id"
# If True, UC table renames and deletes will be considered
fab_consider_dbx_uc_table_changes = True

databricks_config = {
    'dbx_workspace': dbx_workspace,
    'dbx_token': dbx_token,
    'dbx_uc_catalog': dbx_uc_catalog,
    'dbx_uc_schemas': json.loads(dbx_uc_schemas)
}

fabric_config = {
    'workspace_id': fab_workspace_id,
    'lakehouse_id': fab_lakehouse_id,
    'shortcut_connection_id': fab_shortcut_connection_id,
    "consider_dbx_uc_table_changes": fab_consider_dbx_uc_table_changes
}

dbr_uc_tables_to_onelake(databricks_config, fabric_config)