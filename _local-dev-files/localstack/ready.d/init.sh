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
awslocal dynamodb put-item --table-name lmonosc-ddb-m-office-accounts --item '{"account_id": {"S": "8965a9aa-1ed3-4856-b1f4-47a8a86771a1"},"email_address": {"S": "mun-yamashita+monosec-worker@design.secom.co.jp"},"auth_id": {"S": "bd6fff86-88f1-4ebe-ab02-2e37b8ce51a2"},"user_data": {"M": {"config": {"M": {"user_name": {"S": "山下 作業者"}, "password_update_datetime": {"N": "12345"}, "del_datetime": {"N": "12345"}, "auth_status": {"S": "1"}, "auth_period": {"N": "12345"}}}}}}'
### remote-controlテスト用
awslocal dynamodb put-item --table-name lmonosc-ddb-m-office-accounts --item '{"account_id": {"S": "cadbd920-3545-b390-b132-3480e9696fe8"},"email_address": {"S": "sts03858+worker@design.secom.co.jp"},"auth_id": {"S": "527c3745-f16c-4d21-896a-1acfcea00350"},"user_data": {"M": {"config": {"M": {"user_name": {"S": "斉藤 作業者"}, "password_update_datetime": {"N": "12345"}, "del_datetime": {"N": "12345"}, "auth_status": {"S": "1"}, "auth_period": {"N": "12345"}}}}}}'

# t-monosec-users
awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-users --item '{"user_id": {"S": "ed20f13d-3b86-4d37-aadd-2190f47990c6"},"account_id": {"S": "a261dadc-a4a7-4a5e-909a-0a9ee2ace39e"},"contract_id": {"S": "1ea207eb-dd2e-401f-918d-ac293581cd4c"},"user_type": {"S": "admin"},"user_data": {"M": {"config": {"M": {"mail_address": {"S": "mun-yamashita@design.secom.co.jp"}}}}}}'
awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-users --item '{"user_id": {"S": "bd6fff86-88f1-4ebe-ab02-2e37b8ce51a2"},"account_id": {"S": "8965a9aa-1ed3-4856-b1f4-47a8a86771a1"},"contract_id": {"S": "1ea207eb-dd2e-401f-918d-ac293581cd4c"},"user_type": {"S": "worker"},"user_data": {"M": {"config": {"M": {"mail_address": {"S": "mun-yamashita@design.secom.co.jp"}}}}}}'
### remote-control-listテスト用
awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-users --item '{"user_id":{"S":"527c3745-f16c-4d21-896a-1acfcea00350"},"account_id":{"S":"cadbd920-3545-b390-b132-3480e9696fe8"},"contract_id":{"S":"a322ae61-9763-c168-b2b2-1794ca906724"},"user_type":{"S":"worker"},"user_data":{"M":{"config":{"M":{"device_order":{"L":[{"S":"2eb40f09-9fa3-e1a6-d1d6-dfdb1971ed14"},{"S":"510a3458-9b5d-7999-9811-e23526d1531d"},{"S":"6b028849-6ae1-cc57-4306-0956dac08dca"}]},"last_page":{"S":"device_list"},"del_datetime":{"N":"0"}}}}}}'

# m-office-contracts
awslocal dynamodb put-item --table-name lmonosc-ddb-m-office-contracts --item '{"contract_id": {"S": "1ea207eb-dd2e-401f-918d-ac293581cd4c"},"service": {"S": "monosc"},"contract_data": {"M": {"user_list": {"L": [{"S": "ed20f13d-3b86-4d37-aadd-2190f47990c6"},{"S": "bd6fff86-88f1-4ebe-ab02-2e37b8ce51a2"}]},"device_list": {"L": [{"S": "869411fe-200a-4d2d-9eeb-7506e49c0a50"},{"S": "a2be194e-64fc-42ce-977e-d8dcf7109825"}]},"group_list": {"L": [{"S": "6868b7a4-eec6-46e0-b392-9021f021193d"}]}}}}'
### remote-controlテスト用
awslocal dynamodb put-item --table-name lmonosc-ddb-m-office-contracts --item '{"contract_id": {"S": "a322ae61-9763-c168-b2b2-1794ca906724"},"service": {"S": "monosc"},"contract_data": {"M": {"user_list": {"L": [{"S": "ed20f13d-3b86-4d37-aadd-2190f47990c6"}]},"device_list": {"L": [{"S": "2eb40f09-9fa3-e1a6-d1d6-dfdb1971ed14"},{"S": "510a3458-9b5d-7999-9811-e23526d1531d"},{"S": "6b028849-6ae1-cc57-4306-0956dac08dca"}]},"group_list": {"L": [{"S": "ba870b6d-e2b4-6d23-e0f8-dda0299f2d43"}]}}}}'

# t-monosec-devices
awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-devices --item '{"device_id": {"S": "a2be194e-64fc-42ce-977e-d8dcf7109825"},"imei": {"S": "imei1"},"contract_state": {"N": "1"},"device_type": {"N": "1"},"device_data": {"M": {"device_data": {"M": {}},"config": {"M": {"device_name": {"S": "正面玄関"}}}}}}'
awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-devices --item '{"device_id": {"S": "869411fe-200a-4d2d-9eeb-7506e49c0a50"},"imei": {"S": "imei4"},"contract_state": {"N": "1"},"device_type": {"N": "2"},"device_data": {"M": {"param": {"M": {"iccid": {"S": "39011acf-c9ff-42c4-8fd4-eae7367f8875"},"device_code": {"S": ""}}},"config": {"M": {"device_name": {"S": "裏口"}, "terminal_settings": {"M": {}}}}}}}'
### remote-control-listテスト用
# awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-devices --item '{"device_id": {"S": "a2be194e-64fc-42ce-977e-d8dcf7109825"},"imei": {"S": "imei1"},"contract_state": {"N": "1"},"device_type": {"N": "1"},"device_data": {"M": {"param": {"M": {"contract_id": {"S": "1ea207eb-dd2e-401f-918d-ac293581cd4c"},"iccid": {"S": ""},"device_code": {"S": ""},"dev_reg_datetime": {"N": "0"},"dev_user_reg_datetime": {"N": "0"},"service": {"S": ""},"init_datetime": {"N": "0"},"del_datetime": {"N": "0"},"user_type": {"N": "0"},"coverage_url": {"S": ""}}},"config": {"M": {"device_name": {"S": "正面玄関"},"terminal_settings": {"M": {"di_list": {"L": [{"M": {"di_no": {"N": "1"},"di_name": {"S": ""},"di_on_name": {"S": "di1_on_name"},"di_on_icon": {"S": "di1_on_icon"},"di_off_name": {"S": "di1_off_name"},"di_off_icon": {"S": "di1_off_icon"}}},{"M": {"di_no": {"N": "2"},"di_name": {"S": "di2"},"di_on_name": {"S": "di2_on_name"},"di_on_icon": {"S": "di2_on_icon"},"di_off_name": {"S": "di2_off_name"},"di_off_icon": {"S": "di2_off_icon"}}}]},"do_list": {"L": [{"M": {"do_no": {"N": "1"},"do_name": {"S": "do1"},"do_on_name": {"S": "do1_on_name"},"do_on_icon": {"S": "do1_on_icon"},"do_off_name": {"S": "do1_off_name"},"do_off_icon": {"S": "do1_off_icon"},"do_control": {"S": "open"},"do_specified_time": {"N": "0"},"do_di_return": {"N": "1"},"do_time_list": {"L": [{"M": {"do_onoff_control": {"N": "1"},"do_timer_time": {"S": "10:00"}}}]}}},{"M": {"do_no": {"N": "2"},"do_name": {"S": "do2"},"do_on_name": {"S": "do2_on_name"},"do_on_icon": {"S": "do2_on_icon"},"do_off_name": {"S": "do2_off_name"},"do_off_icon": {"S": "do2_off_icon"},"do_control": {"S": "open"},"do_specified_time": {"N": "0"},"do_di_return": {"N": "0"},"do_time_list": {"L": [{"M": {"do_onoff_control": {"N": "1"},"do_timer_time": {"S": "10:00"}}}]}}}]}}}}}}}}'
# awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-devices --item '{"device_id": {"S": "869411fe-200a-4d2d-9eeb-7506e49c0a50"},"imei": {"S": "imei2"},"contract_state": {"N": "1"},"device_type": {"N": "1"},"device_data": {"M": {"param": {"M": {"contract_id": {"S": "1ea207eb-dd2e-401f-918d-ac293581cd4c"},"iccid": {"S": ""},"device_code": {"S": ""},"dev_reg_datetime": {"N": "0"},"dev_user_reg_datetime": {"N": "0"},"service": {"S": ""},"init_datetime": {"N": "0"},"del_datetime": {"N": "0"},"user_type": {"N": "0"},"coverage_url": {"S": ""}}},"config": {"M": {"device_name": {"S": "裏口"},"terminal_settings": {"M": {"di_list": {"L": [{"M": {"di_no": {"N": "1"},"di_name": {"S": ""},"di_on_name": {"S": "di1_on_name"},"di_on_icon": {"S": "di1_on_icon"},"di_off_name": {"S": "di1_off_name"},"di_off_icon": {"S": "di1_off_icon"}}},{"M": {"di_no": {"N": "2"},"di_name": {"S": "di2"},"di_on_name": {"S": "di2_on_name"},"di_on_icon": {"S": "di2_on_icon"},"di_off_name": {"S": "di2_off_name"},"di_off_icon": {"S": "di2_off_icon"}}}]},"do_list": {"L": [{"M": {"do_no": {"N": "1"},"do_name": {"S": "do1"},"do_on_name": {"S": "do1_on_name"},"do_on_icon": {"S": "do1_on_icon"},"do_off_name": {"S": "do1_off_name"},"do_off_icon": {"S": "do1_off_icon"},"do_control": {"S": "open"},"do_specified_time": {"N": "0"},"do_di_return": {"N": "1"},"do_time_list": {"L": [{"M": {"do_onoff_control": {"N": "1"},"do_timer_time": {"S": "10:00"}}}]}}},{"M": {"do_no": {"N": "2"},"do_name": {"S": "do2"},"do_on_name": {"S": "do2_on_name"},"do_on_icon": {"S": "do2_on_icon"},"do_off_name": {"S": "do2_off_name"},"do_off_icon": {"S": "do2_off_icon"},"do_control": {"S": "open"},"do_specified_time": {"N": "0"},"do_di_return": {"N": "0"},"do_time_list": {"L": [{"M": {"do_onoff_control": {"N": "1"},"do_timer_time": {"S": "10:00"}}}]}}}]}}}}}}}}'
awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-devices --item '{"device_id": {"S": "2eb40f09-9fa3-e1a6-d1d6-dfdb1971ed14"},"imei": {"S": "imei1"},"contract_state": {"N": "1"},"device_type": {"N": "1"},"device_data": {"M": {"param": {"M": {"contract_id": {"S": "a322ae61-9763-c168-b2b2-1794ca906724"},"iccid": {"S": "9e6d06e1-07f1-a08c-e367-0c80400296a4"},"device_code": {"S": ""},"dev_reg_datetime": {"N": "0"},"dev_user_reg_datetime": {"N": "0"},"service": {"S": ""},"init_datetime": {"N": "0"},"del_datetime": {"N": "0"},"user_type": {"N": "0"},"coverage_url": {"S": ""}}},"config": {"M": {"device_name": {"S": "正面玄関"},"terminal_settings": {"M": {"di_list": {"L": [{"M": {"di_no": {"N": "1"},"di_name": {"S": ""},"di_on_name": {"S": "di1_on_name"},"di_on_icon": {"S": "di1_on_icon"},"di_off_name": {"S": "di1_off_name"},"di_off_icon": {"S": "di1_off_icon"}}},{"M": {"di_no": {"N": "2"},"di_name": {"S": "di2"},"di_on_name": {"S": "di2_on_name"},"di_on_icon": {"S": "di2_on_icon"},"di_off_name": {"S": "di2_off_name"},"di_off_icon": {"S": "di2_off_icon"}}}]},"do_list": {"L": [{"M": {"do_no": {"N": "1"},"do_name": {"S": "do1"},"do_on_name": {"S": "do1_on_name"},"do_on_icon": {"S": "do1_on_icon"},"do_off_name": {"S": "do1_off_name"},"do_off_icon": {"S": "do1_off_icon"},"do_control": {"S": "open"},"do_specified_time": {"N": "10"},"do_di_return": {"N": "1"},"do_time_list": {"L": [{"M": {"do_onoff_control": {"N": "1"},"do_timer_time": {"S": "10:00"}}}]}}},{"M": {"do_no": {"N": "2"},"do_name": {"S": "do2"},"do_on_name": {"S": "do2_on_name"},"do_on_icon": {"S": "do2_on_icon"},"do_off_name": {"S": "do2_off_name"},"do_off_icon": {"S": "do2_off_icon"},"do_control": {"S": "open"},"do_specified_time": {"N": "0"},"do_di_return": {"N": "0"},"do_time_list": {"L": [{"M": {"do_onoff_control": {"N": "1"},"do_timer_time": {"S": "10:00"}}}]}}}]}}}}}}}}'
awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-devices --item '{"device_id": {"S": "510a3458-9b5d-7999-9811-e23526d1531d"},"imei": {"S": "imei2"},"contract_state": {"N": "1"},"device_type": {"N": "1"},"device_data": {"M": {"param": {"M": {"contract_id": {"S": "a322ae61-9763-c168-b2b2-1794ca906724"},"iccid": {"S": "0126e4a3-9938-7faf-64ba-6615c4ea017a"},"device_code": {"S": ""},"dev_reg_datetime": {"N": "0"},"dev_user_reg_datetime": {"N": "0"},"service": {"S": ""},"init_datetime": {"N": "0"},"del_datetime": {"N": "0"},"user_type": {"N": "0"},"coverage_url": {"S": ""}}},"config": {"M": {"device_name": {"S": "裏口"},"terminal_settings": {"M": {"di_list": {"L": [{"M": {"di_no": {"N": "1"},"di_name": {"S": ""},"di_on_name": {"S": "di1_on_name"},"di_on_icon": {"S": "di1_on_icon"},"di_off_name": {"S": "di1_off_name"},"di_off_icon": {"S": "di1_off_icon"}}},{"M": {"di_no": {"N": "2"},"di_name": {"S": "di2"},"di_on_name": {"S": "di2_on_name"},"di_on_icon": {"S": "di2_on_icon"},"di_off_name": {"S": "di2_off_name"},"di_off_icon": {"S": "di2_off_icon"}}}]},"do_list": {"L": [{"M": {"do_no": {"N": "1"},"do_name": {"S": "do1"},"do_on_name": {"S": "do1_on_name"},"do_on_icon": {"S": "do1_on_icon"},"do_off_name": {"S": "do1_off_name"},"do_off_icon": {"S": "do1_off_icon"},"do_control": {"S": "open"},"do_specified_time": {"N": "0"},"do_di_return": {"N": "0"},"do_time_list": {"L": [{"M": {"do_onoff_control": {"N": "1"},"do_timer_time": {"S": "10:00"}}}]}}},{"M": {"do_no": {"N": "2"},"do_name": {"S": "do2"},"do_on_name": {"S": "do2_on_name"},"do_on_icon": {"S": "do2_on_icon"},"do_off_name": {"S": "do2_off_name"},"do_off_icon": {"S": "do2_off_icon"},"do_control": {"S": "close"},"do_specified_time": {"N": "0"},"do_di_return": {"N": "2"},"do_time_list": {"L": [{"M": {"do_onoff_control": {"N": "1"},"do_timer_time": {"S": "10:00"}}}]}}}]}}}}}}}}'
awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-devices --item '{"device_id": {"S": "6b028849-6ae1-cc57-4306-0956dac08dca"},"imei": {"S": "imei3"},"contract_state": {"N": "1"},"device_type": {"N": "1"},"device_data": {"M": {"param": {"M": {"contract_id": {"S": "a322ae61-9763-c168-b2b2-1794ca906724"},"iccid": {"S": "293b9f42-612a-100e-e80e-9788cad5a790"},"device_code": {"S": ""},"dev_reg_datetime": {"N": "0"},"dev_user_reg_datetime": {"N": "0"},"service": {"S": ""},"init_datetime": {"N": "0"},"del_datetime": {"N": "0"},"user_type": {"N": "0"},"coverage_url": {"S": ""}}},"config": {"M": {"device_name": {"S": "正門前"},"terminal_settings": {"M": {"di_list": {"L": [{"M": {"di_no": {"N": "1"},"di_name": {"S": ""},"di_on_name": {"S": "di1_on_name"},"di_on_icon": {"S": "di1_on_icon"},"di_off_name": {"S": "di1_off_name"},"di_off_icon": {"S": "di1_off_icon"}}}]},"do_list": {"L": [{"M": {"do_no": {"N": "1"},"do_name": {"S": "do1"},"do_on_name": {"S": "do1_on_name"},"do_on_icon": {"S": "do1_on_icon"},"do_off_name": {"S": "do1_off_name"},"do_off_icon": {"S": "do1_off_icon"},"do_control": {"S": "toggle"},"do_specified_time": {"N": "100"},"do_di_return": {"N": "1"},"do_time_list": {"L": [{"M": {"do_onoff_control": {"N": "1"},"do_timer_time": {"S": "10:00"}}}]}}}]}}}}}}}}'

# t-monosec-groups
awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-groups --item '{"group_id": {"S": "6868b7a4-eec6-46e0-b392-9021f021193d"},"group_data": {"M": {"config": {"M": {"contract_id": {"S": "1ea207eb-dd2e-401f-918d-ac293581cd4c"},"group_name": {"S": "SCセンター1F"},"del_datetime": {"N": "12345"}}}}}}'
### remote-controlテスト用
awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-groups --item '{"group_id": {"S": "ba870b6d-e2b4-6d23-e0f8-dda0299f2d43"},"group_data": {"M": {"config": {"M": {"contract_id": {"S": "a322ae61-9763-c168-b2b2-1794ca906724"},"group_name": {"S": "自宅"},"del_datetime": {"N": "12345"}}}}}}'

# m-device-relation
awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-device-relation --item '{"key1": {"S": "u-ed20f13d-3b86-4d37-aadd-2190f47990c6"},"key2": {"S": "d-869411fe-200a-4d2d-9eeb-7506e49c0a50"}}'
awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-device-relation --item '{"key1": {"S": "u-ed20f13d-3b86-4d37-aadd-2190f47990c6"},"key2": {"S": "g-6868b7a4-eec6-46e0-b392-9021f021193d"}}'
awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-device-relation --item '{"key1": {"S": "g-6868b7a4-eec6-46e0-b392-9021f021193d"},"key2": {"S": "d-a2be194e-64fc-42ce-977e-d8dcf7109825"}}'
awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-device-relation --item '{"key1": {"S": "u-bd6fff86-88f1-4ebe-ab02-2e37b8ce51a2"},"key2": {"S": "d-869411fe-200a-4d2d-9eeb-7506e49c0a50"}}'
### remote-control-listテスト用
awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-device-relation --item '{"key1": {"S":"u-527c3745-f16c-4d21-896a-1acfcea00350"},"key2": {"S":"d-2eb40f09-9fa3-e1a6-d1d6-dfdb1971ed14"}}'
awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-device-relation --item '{"key1": {"S":"u-527c3745-f16c-4d21-896a-1acfcea00350"},"key2": {"S":"d-510a3458-9b5d-7999-9811-e23526d1531d"}}'
awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-device-relation --item '{"key1": {"S":"u-527c3745-f16c-4d21-896a-1acfcea00350"},"key2": {"S":"g-ba870b6d-e2b4-6d23-e0f8-dda0299f2d43"}}'
awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-device-relation --item '{"key1": {"S":"g-ba870b6d-e2b4-6d23-e0f8-dda0299f2d43"},"key2": {"S":"d-510a3458-9b5d-7999-9811-e23526d1531d"}}'
awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-device-relation --item '{"key1": {"S":"g-ba870b6d-e2b4-6d23-e0f8-dda0299f2d43"},"key2": {"S":"d-6b028849-6ae1-cc57-4306-0956dac08dca"}}'

# t-monosec-hist-list
awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-hist-list --item '{"device_id": {"S": "869411fe-200a-4d2d-9eeb-7506e49c0a50"},"hist_id": {"S": "920c19ba-f86c-45c0-ac8d-f1319ef9de28"},"event_datetime": {"N": "1702263329000"},"recv_datetime": {"N": "1702263421000"},"hist_data": {"M": {"device_name": {"S": "SC1Fアダプター"},"imei": {"S": "1234"},"event_type": {"S": "di_change"},"terminal_name": {"S": "入力1"},"control_trigger": {"S": "検知"},"do_di_return": {"S": "トリガー1"},"terminal_state_name": {"S": "ON"},"terminal_state_icon": {"S": "icon-on"},"notification_hist": {"S": "通知あり"}}}}'
awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-hist-list --item '{"device_id": {"S": "869411fe-200a-4d2d-9eeb-7506e49c0a50"},"hist_id": {"S": "cc5e1da9-21bd-40db-95e5-341a692dc9e0"},"event_datetime": {"N": "1702263328000"},"recv_datetime": {"N": "1702263422000"},"hist_data": {"M": {"device_name": {"S": "SC1Fアダプター"},"imei": {"S": "1234"},"event_type": {"S": "do_change"},"terminal_no": {"N": "1"},"control_trigger": {"S": "検知"},"do_di_return": {"S": "トリガー1"},"terminal_state_name": {"S": "ON"},"terminal_state_icon": {"S": "icon-on"},"notification_hist": {"S": "通知あり"}}}}'
awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-hist-list --item '{"device_id": {"S": "869411fe-200a-4d2d-9eeb-7506e49c0a50"},"hist_id": {"S": "b2556612-252d-40ab-a6bd-c30bfd2dcdce"},"event_datetime": {"N": "1702263327000"},"recv_datetime": {"N": "1702263423000"},"hist_data": {"M": {"device_name": {"S": "SC1Fアダプター"},"imei": {"S": "1234"},"event_type": {"S": "battery_near"},"occurrence_flag": {"N": "1"},"notification_hist": {"S": "通知あり"}}}}'
awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-hist-list --item '{"device_id": {"S": "869411fe-200a-4d2d-9eeb-7506e49c0a50"},"hist_id": {"S": "13d2adf7-58ff-4d92-9324-6046e3152bfa"},"event_datetime": {"N": "1702263326000"},"recv_datetime": {"N": "1702263424000"},"hist_data": {"M": {"device_name": {"S": "SC1Fアダプター"},"imei": {"S": "1234"},"event_type": {"S": "battery_near"},"occurrence_flag": {"N": "0"},"notification_hist": {"S": "通知あり"}}}}'
awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-hist-list --item '{"device_id": {"S": "869411fe-200a-4d2d-9eeb-7506e49c0a50"},"hist_id": {"S": "aabb45af-7371-483f-b41c-87527f29d0d1"},"event_datetime": {"N": "1702263325000"},"recv_datetime": {"N": "1702263425000"},"hist_data": {"M": {"device_name": {"S": "SC1Fアダプター"},"imei": {"S": "1234"},"event_type": {"S": "power_on"},"notification_hist": {"S": "通知あり"}}}}'
awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-hist-list --item '{"device_id": {"S": "869411fe-200a-4d2d-9eeb-7506e49c0a50"},"hist_id": {"S": "1c1e957a-51a7-4c69-b8ad-1a92f6df736e"},"event_datetime": {"N": "1702263324000"},"recv_datetime": {"N": "1702263426000"},"hist_data": {"M": {"device_name": {"S": "SC1Fアダプター"},"imei": {"S": "1234"},"event_type": {"S": "on_timer_control"},"terminal_name": {"S": "入力1"},"control_trigger": {"S": "検知"},"do_di_return": {"S": "トリガー1"},"terminal_state_name": {"S": "ON"},"terminal_state_icon": {"S": "icon-on"},"notification_hist": {"S": "通知あり"}}}}'
awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-hist-list --item '{"device_id": {"S": "869411fe-200a-4d2d-9eeb-7506e49c0a50"},"hist_id": {"S": "380b4d55-0709-4e68-a80f-6a9ec3c4cdc7"},"event_datetime": {"N": "1702263323000"},"recv_datetime": {"N": "1702263427000"},"hist_data": {"M": {"device_name": {"S": "SC1Fアダプター"},"imei": {"S": "1234"},"event_type": {"S": "off_timer_control"},"terminal_name": {"S": "入力1"},"control_trigger": {"S": "検知"},"do_di_return": {"S": "トリガー1"},"terminal_state_name": {"S": "ON"},"terminal_state_icon": {"S": "icon-on"},"notification_hist": {"S": "通知あり"}}}}'

# t-monosec-device-state
### remote-control-listテスト用
awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-device-state --item '{"device_id": {"S": "869411fe-200a-4d2d-9eeb-7506e49c0a50"},"di1_state": {"N": "0"},"di2_state": {"N": "0"},"signal_last_update_datetime": {"S": "1703728800"}}'
awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-device-state --item '{"device_id": {"S": "a2be194e-64fc-42ce-977e-d8dcf7109825"},"di1_state": {"N": "1"},"di2_state": {"N": "0"}}'
awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-device-state --item '{"device_id": {"S": "2eb40f09-9fa3-e1a6-d1d6-dfdb1971ed14"},"di1_state": {"N": "0"},"di2_state": {"N": "1"}}'
awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-device-state --item '{"device_id": {"S": "510a3458-9b5d-7999-9811-e23526d1531d"},"di1_state": {"N": "1"},"di2_state": {"N": "0"}}'
awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-device-state --item '{"device_id": {"S": "6b028849-6ae1-cc57-4306-0956dac08dca"},"di1_state": {"N": "1"}}'

# t-monosec-req_no_counter
### remote-controlテスト用
awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-req-no-counter-2 --item '{"simid": {"S": "0126e4a3-9938-7faf-64ba-6615c4ea017a"},"num": {"N": "65536"}}'

# t-monosec-remote-controls
### remote-controlテスト用
awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-remote-controls --item '{"do_no": {"N": "2"},"control_exec_email_address": {"S": "sts03858+worker@design.secom.co.jp"},"device_id": {"S": "510a3458-9b5d-7999-9811-e23526d1531d"},"contract_id": {"S": "a322ae61-9763-c168-b2b2-1794ca906724"},"control": {"S": "close"},"req_datetime": {"N": "1703138511555"},"link_di_no": {"S": "2"},"iccid": {"S": "0126e4a3-9938-7faf-64ba-6615c4ea017a"},"device_req_no": {"S": "0126e4a3-9938-7faf-64ba-6615c4ea017a-0x01"},"event_datetime": {"N": "1703138511555"},"control_trigger": {"S": "manual_control"},"control_exec_user_name": {"S": "斉藤 作業者"},"recv_datetime": {"N": "1703138511581"}}'
awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-remote-controls --item '{"device_req_no": {"S": "39011acf-c9ff-42c4-8fd4-eae7367f8875-0x01"},"event_datetime": {"N": "1703514796000"},"device_id": {"S": "869411fe-200a-4d2d-9eeb-7506e49c0a50"},"req_datetime": {"N": "1703514797000"},"link_di_no": {"N": "2"},"iccid": {"S": "39011acf-c9ff-42c4-8fd4-eae7367f8875"},"recv_datetime": {"N": "1703514797000"}}'

# t-monosec-cnt-hist-2
awslocal dynamodb put-item --table-name lmonosc-ddb-t-monosec-cnt-hist-2 --item '{"cnt_hist_id": {"N": "123456"},"simid": {"S": "39011acf-c9ff-42c4-8fd4-eae7367f8875"},"event_datetime": {"N": "1703514798000"},"recv_datetime": {"N": "1703514799000"},"di_trigger": {"N": "2"}}'