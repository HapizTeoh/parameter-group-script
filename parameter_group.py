import os
import logging
import csv
from urllib import response
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level='INFO')

data = {}
sheet_list = {}
source_parameter_group = os.getenv("source_file")
target_parameter_group = os.getenv("target_parameter_group")
parameter_group_family = os.getenv("parameter_group_family")
access_profile = os.getenv("aws_profile")
new_postgres_version = os.getenv("new_postgres_version")
old_postgres_version = os.getenv("old_postgres_version")
found_flag = False

def init_aws_session():
    """ Create a new AWS session"""
    return boto3.setup_default_session(profile_name=f'{access_profile}')

def rds_get_parameter_group_list():
    """Get all parameter groups"""
    rds = boto3.client('rds')
    responses = rds.describe_db_parameter_groups()
    return responses["DBParameterGroups"]

def rds_update_parameters(parameter_group_name, update_parameters):
    """
    Updates parameters in a custom DB parameter group.

    :param parameter_group_name: The name of the parameter group to update.
    :param update_parameters: The parameters to update in the group.
    :return: Data about the modified parameter group.
    """
    rds = boto3.client('rds')
    try:
        responses = rds.modify_db_parameter_group(
            DBParameterGroupName=parameter_group_name, Parameters=update_parameters)
        return responses
    except ClientError as err:
        logging.error(
            "Couldn't update parameters in %s. Here's why: %s: %s", parameter_group_name,
            err.response['Error']['Code'], err.response['Error']['Message'])
        raise

def get_parameters(parameter_group_name, name_prefix='', source=None):
    """
    Gets the parameters that are contained in a DB parameter group.

    :param parameter_group_name: The name of the parameter group to query.
    :param name_prefix: When specified, the retrieved list of parameters is filtered
                        to contain only parameters that start with this prefix.
    :param source: When specified, only parameters from this source are retrieved.
                    For example, a source of 'user' retrieves only parameters that
                    were set by a user.
    :return: The list of requested parameters.
    """
    rds = boto3.client('rds')
    try:
        kwargs = {'DBParameterGroupName': parameter_group_name}
        if source is not None:
            kwargs['Source'] = source
        parameters = []
        paginator = rds.get_paginator('describe_db_parameters')
        for page in paginator.paginate(**kwargs):
            parameters += [
                p for p in page['Parameters'] if p['ParameterName'].startswith(name_prefix)]
        return parameters
    except ClientError as err:
        logging.error(
            "Couldn't get parameters for %s. Here's why: %s: %s", parameter_group_name,
            err.response['Error']['Code'], err.response['Error']['Message'])
        raise

def read_csv(file_path):
    """Read CSV file and create a dictionary"""
    logging.info("Reading CSV file: %s", file_path)
    with open(file_path, mode='r', encoding="utf-8") as csv_file:
        csv_reader = csv.DictReader(csv_file)
        line_count = 0
        try:
            for row in csv_reader:
                if line_count == 0:
                    logging.info(f'Column names are {", ".join(row)}')
                    line_count += 1
                if row["Name"] != "":
                    data.update({row["Name"] : row["Value"]})
                line_count += 1
            logging.info('Processed %s rows including header.', line_count)
        except KeyError:
            logging.info("Column names are not correct!")

if __name__ == "__main__":
    read_csv(f'{source_parameter_group}')
    for key,value in data.items():
        logging.info('Parameter_name: %s," values: "%s"', key, value)
        sheet_list.update( {key : value} )

    session = init_aws_session()
    pg_groups = rds_get_parameter_group_list()
    #loop all parameter groups
    for x in pg_groups:
        #print parameter groups that is in postgres family
        if parameter_group_family in x["DBParameterGroupFamily"]:
            if target_parameter_group in x["DBParameterGroupName"]:
                logging.info("Target parameter group found: %s",x["DBParameterGroupName"])
                found_flag = True
                #print the parameter group parameters
                parameters = get_parameters(x["DBParameterGroupName"])
                #print the parameters and values
                for i in parameters:
                    #Find the matching parameter group in source csv
                    if i["ParameterName"] in sheet_list:
                        parameters_name = i["ParameterName"]
                        parameters_apply_type = i["ApplyType"]
                        for key,value in sheet_list.items():
                            if parameters_name == key:
                                print("\nParameter name from pg %s: %s" %(old_postgres_version,key))
                                print("Parameter name from pg %s: %s" %(new_postgres_version,parameters_name))
                                print("Value to copy: %s" % value)
                                if parameters_apply_type == "static":
                                    Parameters_args=[{'ParameterName': parameters_name,
                                                      'ParameterValue': value,'ApplyMethod': 'pending-reboot' }]
                                else:
                                    Parameters_args=[{'ParameterName': parameters_name,
                                                      'ParameterValue': value,'ApplyMethod': 'immediate' }]
                                #rds_update_parameters(target_parameter_group,Parameters_args)
    if found_flag is not True:
        logging.info("Target parameter group not found!\n")