#!/bin/bash

# terraformインストール
apt-get update && apt-get install -y vim wget unzip sudo
wget https://releases.hashicorp.com/terraform/1.1.7/terraform_1.1.7_linux_amd64.zip
unzip ./terraform_1.1.7_linux_amd64.zip -d /usr/local/bin/
rm ./terraform_1.1.7_linux_amd64.zip
sudo pip install terraform-local

# terraformコードコピー
cp -r /usr/local/terraform /usr/local/terraform-wk

export AWS_DEFAULT_REGION=ap-northeast-1

# DynamoDB構築
cd /usr/local/terraform-wk/DDB
tflocal init
tflocal apply -var-file db.tfvars -auto-approve

# SSM構築
cd /usr/local/terraform-wk/SSM
tflocal init
tflocal apply -var-file ssm.tfvars -auto-approve

# 動作確認用データ登録
awslocal dynamodb put-item --table-name lmonosc-ddb-m-office-accounts --item '{"salesforce_id": {"S": "dummy"},"contract_id": {"S": "a261dadc-a4a7-4a5e-909a-0a9ee2ace39e"},"service": {"S": "monosc"},"contract_data": {"S": ""},"user_list": {"L": [{"S": "ed20f13d-3b86-4d37-aadd-2190f47990c6"}]},"device_list": {"L": []},"group_list": {"L": []}}'

awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-users --item '{"user_id": {"S": "ed20f13d-3b86-4d37-aadd-2190f47990c6"},"user_type": {"S": "admin"},"user_data": {"M": {"config": {"M": {"mail_address": {"S": "mun-yamashita@design.secom.co.jp"}}}}}}'

