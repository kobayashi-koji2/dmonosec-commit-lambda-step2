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

# m-office-accounts
awslocal dynamodb put-item --table-name lmonosc-ddb-m-office-accounts --item '{"account_id": {"S": "a261dadc-a4a7-4a5e-909a-0a9ee2ace39e"},"email_address": {"S": "mun-yamashita@design.secom.co.jp"},"auth_id": {"S": "ed20f13d-3b86-4d37-aadd-2190f47990c6"},"user_data": {"M": {"config": {"M": {"user_name": {"S": "山下 宗史"}, "password_update_datetime": {"N": "12345"}, "del_datetime": {"N": "12345"}, "auth_status": {"S": "1"}, "auth_period": {"N": "12345"}}}}}}'

# t-monosec-users
awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-users --item '{"user_id": {"S": "ed20f13d-3b86-4d37-aadd-2190f47990c6"},"account_id": {"S": "c74a5ce9-437a-4fd1-a8fc-81885ec65a52"},"contract_id": {"S": "a261dadc-a4a7-4a5e-909a-0a9ee2ace39e"},"user_type": {"S": "admin"},"user_data": {"M": {"config": {"M": {"mail_address": {"S": "mun-yamashita@design.secom.co.jp"}}}}}}'

# m-office-contracts
awslocal dynamodb put-item --table-name lmonosc-ddb-m-office-contracts --item '{"contract_id": {"S": "a261dadc-a4a7-4a5e-909a-0a9ee2ace39e"},"service": {"S": "monosc"},"contract_data": {"M": {"user_list": {"L": [{"S": "ed20f13d-3b86-4d37-aadd-2190f47990c6"}]},"device_list": {"L": [{"S": "869411fe-200a-4d2d-9eeb-7506e49c0a50"},{"S": "a2be194e-64fc-42ce-977e-d8dcf7109825"}]},"group_list": {"L": []}}}}'

# m-device-relation
awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-device-relation --item '{"key1": {"S": "u-ed20f13d-3b86-4d37-aadd-2190f47990c6"},"key2": {"S": "d-869411fe-200a-4d2d-9eeb-7506e49c0a50"}}'

# t-monosec-hist-list
awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-hist-list --item '{"device_id": {"S": "869411fe-200a-4d2d-9eeb-7506e49c0a50"},"hist_id": {"S": "920c19ba-f86c-45c0-ac8d-f1319ef9de28"},"event_datetime": {"N": "1702263329000"},"recv_datetime": {"N": "1702263421000"},"hist_data": {"M": {"device_name": {"S": "SC1Fアダプター"},"imei": {"S": "1234"},"event_type": {"S": "di_change"},"terminal_name": {"S": "入力1"},"control_trigger": {"S": "検知"},"do_di_return": {"S": "トリガー1"},"terminal_state_name": {"S": "ON"},"terminal_state_icon": {"S": "icon-on"},"notification_hist": {"S": "通知あり"}}}}'
awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-hist-list --item '{"device_id": {"S": "869411fe-200a-4d2d-9eeb-7506e49c0a50"},"hist_id": {"S": "cc5e1da9-21bd-40db-95e5-341a692dc9e0"},"event_datetime": {"N": "1702263328000"},"recv_datetime": {"N": "1702263422000"},"hist_data": {"M": {"device_name": {"S": "SC1Fアダプター"},"imei": {"S": "1234"},"event_type": {"S": "do_change"},"terminal_no": {"N": "1"},"control_trigger": {"S": "検知"},"do_di_return": {"S": "トリガー1"},"terminal_state_name": {"S": "ON"},"terminal_state_icon": {"S": "icon-on"},"notification_hist": {"S": "通知あり"}}}}'
awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-hist-list --item '{"device_id": {"S": "869411fe-200a-4d2d-9eeb-7506e49c0a50"},"hist_id": {"S": "b2556612-252d-40ab-a6bd-c30bfd2dcdce"},"event_datetime": {"N": "1702263327000"},"recv_datetime": {"N": "1702263423000"},"hist_data": {"M": {"device_name": {"S": "SC1Fアダプター"},"imei": {"S": "1234"},"event_type": {"S": "battery_near"},"occurrence_flag": {"N": "1"},"notification_hist": {"S": "通知あり"}}}}'
awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-hist-list --item '{"device_id": {"S": "869411fe-200a-4d2d-9eeb-7506e49c0a50"},"hist_id": {"S": "13d2adf7-58ff-4d92-9324-6046e3152bfa"},"event_datetime": {"N": "1702263326000"},"recv_datetime": {"N": "1702263424000"},"hist_data": {"M": {"device_name": {"S": "SC1Fアダプター"},"imei": {"S": "1234"},"event_type": {"S": "battery_near"},"occurrence_flag": {"N": "0"},"notification_hist": {"S": "通知あり"}}}}'
awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-hist-list --item '{"device_id": {"S": "869411fe-200a-4d2d-9eeb-7506e49c0a50"},"hist_id": {"S": "aabb45af-7371-483f-b41c-87527f29d0d1"},"event_datetime": {"N": "1702263325000"},"recv_datetime": {"N": "1702263425000"},"hist_data": {"M": {"device_name": {"S": "SC1Fアダプター"},"imei": {"S": "1234"},"event_type": {"S": "power_on"},"notification_hist": {"S": "通知あり"}}}}'
awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-hist-list --item '{"device_id": {"S": "869411fe-200a-4d2d-9eeb-7506e49c0a50"},"hist_id": {"S": "1c1e957a-51a7-4c69-b8ad-1a92f6df736e"},"event_datetime": {"N": "1702263324000"},"recv_datetime": {"N": "1702263426000"},"hist_data": {"M": {"device_name": {"S": "SC1Fアダプター"},"imei": {"S": "1234"},"event_type": {"S": "on_timer_control"},"terminal_name": {"S": "入力1"},"control_trigger": {"S": "検知"},"do_di_return": {"S": "トリガー1"},"terminal_state_name": {"S": "ON"},"terminal_state_icon": {"S": "icon-on"},"notification_hist": {"S": "通知あり"}}}}'
awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-hist-list --item '{"device_id": {"S": "869411fe-200a-4d2d-9eeb-7506e49c0a50"},"hist_id": {"S": "380b4d55-0709-4e68-a80f-6a9ec3c4cdc7"},"event_datetime": {"N": "1702263323000"},"recv_datetime": {"N": "1702263427000"},"hist_data": {"M": {"device_name": {"S": "SC1Fアダプター"},"imei": {"S": "1234"},"event_type": {"S": "off_timer_control"},"terminal_name": {"S": "入力1"},"control_trigger": {"S": "検知"},"do_di_return": {"S": "トリガー1"},"terminal_state_name": {"S": "ON"},"terminal_state_icon": {"S": "icon-on"},"notification_hist": {"S": "通知あり"}}}}'

