import os
import secrets
import string

from aws_lambda_powertools import Logger
import boto3

logger = Logger()
COGNITO_USER_POOL_ID = os.environ["COGNITO_USER_POOL_ID"]


def get_random_password_string(length):
    pass_chars = string.ascii_letters + string.digits + string.punctuation
    while True:
        password = "".join(secrets.choice(pass_chars) for x in range(length))
        upper = sum(c.isupper() for c in password)
        lower = sum(c.islower() for c in password)
        digit = sum(c.isdigit() for c in password)
        punctuation = sum(c in string.punctuation for c in password)
        if upper >= 1 and lower >= 1 and digit >= 1 and punctuation >= 1:
            break
    return password


def create_cognito_user(email_address):
    client = boto3.client(
        "cognito-idp",
        region_name=os.environ.get("AWS_REGION"),
        endpoint_url=os.environ.get("endpoint_url"),
    )
    # if True:  # TODO ローカル確認用（localstackがCognito未対応のため）
    #     return "bd6fff86-88f1-4ebe-ab02-2e37b8ce51a2"
    response = client.admin_create_user(
        UserPoolId=COGNITO_USER_POOL_ID,
        Username=email_address,
        DesiredDeliveryMediums=["EMAIL"],
        TemporaryPassword=get_random_password_string(8),  # TODO パスワード桁数を要確認
        UserAttributes=[
            {
                "Name": "email",
                "Value": email_address,
            },
            {
                "Name": "email_verified",
                "Value": "true",
            },
        ],
    )

    auth_id = response["User"]["Username"]

    client.admin_update_user_attributes(
        UserPoolId=COGNITO_USER_POOL_ID,
        Username=auth_id,
        UserAttributes=[
            {
                "Name": "custom:auth_id",
                "Value": auth_id,
            },
        ],
    )

    return auth_id


def update_cognito_user(auth_id, email_address):
    # if True:  # TODO ローカル確認用（localstackがCognito未対応のため）
    #     return "bd6fff86-88f1-4ebe-ab02-2e37b8ce51a2"
    client = boto3.client(
        "cognito-idp",
        region_name=os.environ.get("AWS_REGION"),
        endpoint_url=os.environ.get("endpoint_url"),
    )
    response = client.admin_update_user_attributes(
        UserPoolId=COGNITO_USER_POOL_ID,
        Username=auth_id,
        UserAttributes=[
            {
                "Name": "email",
                "Value": email_address,
            }
        ],
    )
    return response["User"]["Username"]


def clear_cognito_mfa(email_address):
    client = boto3.client(
        "cognito-idp",
        region_name=os.environ.get("AWS_REGION"),
        endpoint_url=os.environ.get("endpoint_url"),
    )
    client.admin_set_user_mfa_preference(
        Username=email_address,
        UserPoolId=COGNITO_USER_POOL_ID,
        SMSMfaSettings={"Enabled": False, "PreferredMfa": False},
        SoftwareTokenMfaSettings={"Enabled": False, "PreferredMfa": False},
    )
    client.admin_update_user_attributes(
        UserPoolId=COGNITO_USER_POOL_ID,
        Username=email_address,
        UserAttributes=[
            {
                "Name": "phone_number",
                "Value": "",
            }
        ],
    )
