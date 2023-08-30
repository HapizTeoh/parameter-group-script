import os
from urllib import response
import boto3
import pprint, logging
import csv
from botocore.exceptions import ClientError

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level='INFO')

data = {}
sheet_list = {}
source_parameter_group = "payments-master-pg10"
target_parameter_group = "payments-master-pg11"
access_profile = "prod-readonly"

def init_aws_session():
    return boto3.setup_default_session(profile_name=f'{access_profile}')

def rds_get_parameter_group_list():
    rds = boto3.client('rds')
    response = rds.describe_db_parameter_groups()
    return response["DBParameterGroups"]

def rds_update_parameters(parameter_group_name, update_parameters):
        rds = boto3.client('rds')
        """
        Updates parameters in a custom DB parameter group.

        :param parameter_group_name: The name of the parameter group to update.
        :param update_parameters: The parameters to update in the group.
        :return: Data about the modified parameter group.
        """
        try:
            response = rds.modify_db_parameter_group(
                DBParameterGroupName=parameter_group_name, Parameters=update_parameters)
        except ClientError as err:
            logging.error(
                "Couldn't update parameters in %s. Here's why: %s: %s", parameter_group_name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response

def get_parameters(parameter_group_name, name_prefix='', source=None):
    rds = boto3.client('rds')
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
    try:
        kwargs = {'DBParameterGroupName': parameter_group_name}
        if source is not None:
            kwargs['Source'] = source
        parameters = []
        paginator = rds.get_paginator('describe_db_parameters')
        for page in paginator.paginate(**kwargs):
            parameters += [
                p for p in page['Parameters'] if p['ParameterName'].startswith(name_prefix)]
    except ClientError as err:
        logging.error(
            "Couldn't get parameters for %s. Here's why: %s: %s", parameter_group_name,
            err.response['Error']['Code'], err.response['Error']['Message'])
        raise
    else:
        return parameters

def read_csv(file_path):
    logging.info("Reading CSV file: "+file_path)
    with open(file_path, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                logging.info(f'Column names are {", ".join(row)}')
                line_count += 1
            if row["Name"] != "":
                data.update({row["Name"] : row["Values"]})
            line_count += 1
        logging.info(f'Processed {line_count} lines.')
        
if __name__ == "__main__":
    read_csv(f'{source_parameter_group}.csv')
    for key,value in data.items():
        print("Parameter_name: "+key," values: "+value)
        sheet_list.update( {key : value} )
    
    init_aws_session()
    pg_groups = rds_get_parameter_group_list()
    #loop all parameter groups
    for x in pg_groups:                                                
        #print parameter groups that is in postgres family
        if "postgres" in x["DBParameterGroupFamily"]:  
            if target_parameter_group in x["DBParameterGroupName"]:
                #print the paramater group parameters
                parameters = get_parameters(x["DBParameterGroupName"])
                #print the parameters and values
                for i in parameters:
                    if i["ParameterName"] in sheet_list.keys():
                        parameters_name = i["ParameterName"]
                        parameters_apply_type = i["ApplyType"]
                        for n in sheet_list:
                            if parameters_name == n:
                                print("\nParameter name from pg10: "+n,"\nParameter name from pg11: "+parameters_name,"\nValue to copy: "+sheet_list[n])
                                if parameters_apply_type == "static":
                                    Parameters_args=[{'ParameterName': parameters_name,'ParameterValue': sheet_list[n],'ApplyMethod': 'pending-reboot' }]
                                else:
                                    Parameters_args=[{'ParameterName': parameters_name,'ParameterValue': sheet_list[n],'ApplyMethod': 'immediate' }]
                                rds_update_parameters(target_parameter_group,Parameters_args)