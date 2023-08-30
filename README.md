## What is it ?
Will update the parameter group's parameter value according to a csv file.

This script is used to update a new parameter group according to the old parameter group (.csv file).

## What you need ?
1. csv file with the last change
2. AWS profile with the right permission to update the parameter group
3. .env file with the following format
```
aws_profile = profile_name
target_parameter_group = parameter_group_you_want_to_update
parameter_group_family = postgres/mysql/oracle
source_file = csv_file_name.csv
```
4. Install requirements.txt

## Program Flow
This script will loop through a csv file, and list the content of each line in the csv.

It will also connect according to the AWS profile mentioned, and loop through the parameter groups in the account.

Then, it will get all the parameter groups that belong to postgres. It will then find the parameter group mentioned in target_parameter_group var.

Then it will loop through all the parameters variable in the target parameter group.

Then it will compare with the parameter group list from the source csv file.

If the parameter_value apply_type is static, then the apply method will be "pending-reboot". If its dynamic, then the apply method will be "immediate".

Then it will update the parameter group according to the parameter value in the csv file.