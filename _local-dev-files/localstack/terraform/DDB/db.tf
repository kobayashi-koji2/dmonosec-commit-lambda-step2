variable "global_name" {}
variable "tags" {}
variable "num"{}

#アカウント管理テーブル
resource "aws_dynamodb_table" "account" {
  name           = "${var.global_name}-ddb-m-office-accounts"
  hash_key       = "account_id"
  stream_enabled = "false"
  table_class    = "STANDARD"

  attribute {
    name = "account_id"
    type = "S"
  }

  attribute {
    name = "email_address"
    type = "S"
  }

  attribute {
    name = "auth_id"
    type = "S"
  }

  billing_mode = "PAY_PER_REQUEST"

  global_secondary_index {
    hash_key        = "auth_id"
    name            = "auth_id_index"
    projection_type = "ALL"
  }

  global_secondary_index {
    hash_key        = "email_address"
    name            = "email_address_index"
    projection_type = "ALL"
  }

  point_in_time_recovery {
    enabled = "true"
  }

  server_side_encryption {
    enabled = true 
  }

  tags = var.tags

}

#IMEI管理テーブル
resource "aws_dynamodb_table" "imei" {
  name           = "${var.global_name}-ddb-m-office-imei"
  hash_key       = "imei"
  stream_enabled = "false"
  table_class    = "STANDARD"

  attribute {
    name = "imei"
    type = "S"
  }

  billing_mode = "PAY_PER_REQUEST"

  point_in_time_recovery {
    enabled = "true"
  }

  server_side_encryption {
    enabled = true 
  }

  tags = var.tags

}

#ICCID管理テーブル
resource "aws_dynamodb_table" "iccid" {
  name           = "${var.global_name}-ddb-m-office-iccid"
  hash_key       = "iccid"
  stream_enabled = "false"
  table_class    = "STANDARD"

  attribute {
    name = "iccid"
    type = "S"
  }

  billing_mode = "PAY_PER_REQUEST"

  point_in_time_recovery {
    enabled = "true"
  }

  server_side_encryption {
    enabled = true 
  }

  tags = var.tags

}

#契約管理テーブル
resource "aws_dynamodb_table" "contract" {
  name           = "${var.global_name}-ddb-m-office-contracts"
  hash_key       = "contract_id"
  stream_enabled = "false"
  table_class    = "STANDARD"

  attribute {
    name = "contract_id"
    type = "S"
  }

  billing_mode = "PAY_PER_REQUEST"

  point_in_time_recovery {
    enabled = "true"
  }

  server_side_encryption {
    enabled = true 
  }

  tags = var.tags

}

#モノセコムユーザ管理テーブル
resource "aws_dynamodb_table" "user" {
  name           = "${var.global_name}-ddb-t-monosec-users"
  hash_key       = "user_id"
  stream_enabled = "false"
  table_class    = "STANDARD"

  attribute {
    name = "user_id"
    type = "S"
  }

  attribute{
    name = "account_id"
    type = "S"
  }

  attribute{
    name = "contract_id"
    type = "S"
  }

  global_secondary_index {
    hash_key        = "account_id"
    range_key       = "contract_id" 
    name            = "account_id_index"
    projection_type = "ALL"
  }

  billing_mode = "PAY_PER_REQUEST"

  point_in_time_recovery {
    enabled = "true"
  }

  server_side_encryption {
    enabled = true 
  }

  tags = var.tags

}

#デバイス管理テーブル
resource "aws_dynamodb_table" "device" {
  name           = "${var.global_name}-ddb-t-monosec-devices"
  hash_key       = "device_id"
  range_key      = "imei"
  stream_enabled = "false"
  table_class    = "STANDARD"

  attribute {
    name = "device_id"
    type = "S"
  }

  attribute {
    name = "imei"
    type = "S"
  }

  attribute {
    name = "contract_state"
    type = "N"
  }

  billing_mode = "PAY_PER_REQUEST"

  global_secondary_index {
    hash_key        = "contract_state"
    name            = "contract_state_index"
    projection_type = "ALL"
  }

  point_in_time_recovery {
    enabled = "true"
  }

  server_side_encryption {
    enabled = true 
  }

  tags = var.tags

}

#グループ管理テーブル
resource "aws_dynamodb_table" "group" {
  name           = "${var.global_name}-ddb-t-monosec-groups"
  hash_key       = "group_id"
  stream_enabled = "false"
  table_class    = "STANDARD"

  attribute {
    name = "group_id"
    type = "S"
  }

  billing_mode = "PAY_PER_REQUEST"

  point_in_time_recovery {
    enabled = "true"
  }

  server_side_encryption {
    enabled = true 
  }

  tags = var.tags

}

#デバイス関係テーブル
resource "aws_dynamodb_table" "device_relation" {
  name           = "${var.global_name}-ddb-t-monosec-device-relation"
  hash_key       = "key1"
  range_key      = "key2"
  stream_enabled = "false"
  table_class    = "STANDARD"

  attribute {
    name = "key1"
    type = "S"
  }

  attribute {
    name = "key2"
    type = "S"
  }

  global_secondary_index {
    hash_key        = "key2"
    range_key       = "key1"
    name            = "key2_index"
    projection_type = "ALL"
  }

  billing_mode = "PAY_PER_REQUEST"

  point_in_time_recovery {
    enabled = "true"
  }

  server_side_encryption {
    enabled = true 
  }

  tags = var.tags

}


#現状態テーブル
resource "aws_dynamodb_table" "state" {
  name           = "${var.global_name}-ddb-t-monosec-device-state"
  hash_key       = "device_id"
  stream_enabled = "false"
  table_class    = "STANDARD"

  attribute {
    name = "device_id"
    type = "S"
  }

  billing_mode = "PAY_PER_REQUEST"

  point_in_time_recovery {
    enabled = "true"
  }

  server_side_encryption {
    enabled = true 
  }

  tags = var.tags

}

#履歴一覧テーブル
resource "aws_dynamodb_table" "hist_list" {
  name           = "${var.global_name}-ddb-t-monosec-hist-list"
  hash_key       = "device_id"
  range_key      = "hist_id"
  stream_enabled = "false"
  table_class    = "STANDARD"

  attribute {
    name = "device_id"
    type = "S"
  }

  attribute {
    name = "hist_id"
    type = "S"
  }

  attribute {
    name = "event_datetime"
    type = "N"
  }

  attribute {
    name = "recv_datetime"
    type = "N"
  }


  billing_mode = "PAY_PER_REQUEST"

  local_secondary_index {
    range_key        = "event_datetime"
    name            = "event_datetime_index"
    projection_type = "ALL"
  }

  local_secondary_index {
    range_key        = "recv_datetime"
    name            = "recv_datetime_index"
    projection_type = "ALL"
  }

  point_in_time_recovery {
    enabled = "true"
  }

  server_side_encryption {
    enabled = true 
  }

  tags = var.tags

}

#操作ログテーブル
resource "aws_dynamodb_table" "operation_log" {
  name           = "${var.global_name}-ddb-t-monosec-operation-log"
  hash_key       = "log_id"
  stream_enabled = "false"
  table_class    = "STANDARD"

  attribute {
    name = "log_id"
    type = "S"
  }

  attribute {
    name = "contract_id"
    type = "S"
  }

  attribute {
    name = "user_id"
    type = "S"
  }

  attribute {
    name = "event_datetime"
    type = "N"
  }

  global_secondary_index {
    hash_key        = "contract_id"
    range_key       = "user_id"
    name            = "contract_id_index"
    projection_type = "ALL"
  }

  global_secondary_index {
    hash_key        = "event_datetime"
    name            = "event_datetime_index"
    projection_type = "ALL"
  }

  billing_mode = "PAY_PER_REQUEST"

  point_in_time_recovery {
    enabled = "true"
  }

  server_side_encryption {
    enabled = true 
  }

  tags = var.tags

}

#通知履歴テーブル
resource "aws_dynamodb_table" "notification_hist" {
  name           = "${var.global_name}-ddb-t-monosec-notification-hist"
  hash_key       = "notification_hist_id"
  stream_enabled = "false"
  table_class    = "STANDARD"

  attribute {
    name = "notification_hist_id"
    type = "S"
  }

  attribute {
    name = "notification_datetime"
    type = "N"
  }

  attribute {
    name = "contract_id"
    type = "S"
  }


  billing_mode = "PAY_PER_REQUEST"

  global_secondary_index {
    hash_key        = "contract_id"
    range_key       = "notification_datetime"  
    name            = "contract_id_index"
    projection_type = "ALL"
  }

  point_in_time_recovery {
    enabled = "true"
  }

  server_side_encryption {
    enabled = true 
  }

  tags = var.tags

}

#登録前デバイス管理テーブル
resource "aws_dynamodb_table" "pre_register_devices" {
  name           = "${var.global_name}-ddb-t-monosec-pre-register-devices"
  hash_key       = "imei"
  stream_enabled = "false"
  table_class    = "STANDARD"

  attribute {
    name = "imei"
    type = "S"
  }

  attribute {
    name = "contract_id"
    type = "S"
  }

  billing_mode = "PAY_PER_REQUEST"

  global_secondary_index {
    hash_key        = "contract_id"
    name            = "contract_id_index"
    projection_type = "ALL"
  }

  point_in_time_recovery {
    enabled = "true"
  }

  server_side_encryption {
    enabled = true 
  }

  tags = var.tags

}


#履歴情報テーブル
resource "aws_dynamodb_table" "cnt_hist" {
  name           = "${var.global_name}-ddb-t-monosec-cnt-hist-${var.num}"
  hash_key       = "cnt_hist_id"
  stream_enabled = "false"
  table_class    = "STANDARD"

  attribute {
    name = "cnt_hist_id"
    type = "S"
  }

  attribute {
    name = "event_datetime"
    type = "N"
  }

  attribute {
    name = "simid"
    type = "S"
  }

  billing_mode = "PAY_PER_REQUEST"

  global_secondary_index {
    hash_key        = "simid"
    range_key       = "event_datetime"
    name            = "simid_index"
    projection_type = "ALL"
  }

  point_in_time_recovery {
    enabled = "true"
  }

  server_side_encryption {
    enabled = true 
  }

  tags = var.tags

}

#制御状況テーブル
resource "aws_dynamodb_table" "control_status" {
  name           = "${var.global_name}-ddb-t-monosec-control-status"
  hash_key       = "device_id"
  range_key      = "do_no"
  stream_enabled = "false"
  table_class    = "STANDARD"

  attribute {
    name = "device_id"
    type = "S"
  }

  attribute {
    name = "do_no"
    type = "N"
  }

  ttl {
    attribute_name = "del_datetime"
    enabled        = true
  }

  billing_mode = "PAY_PER_REQUEST"

  point_in_time_recovery {
    enabled = "true"
  }

  server_side_encryption {
    enabled = true 
  }

  tags = var.tags
}

#接点出力制御応答テーブル
resource "aws_dynamodb_table" "remote_controls" {
  name           = "${var.global_name}-ddb-t-monosec-remote-controls"
  hash_key       = "device_req_no"
  range_key      = "req_datetime"
  stream_enabled = "false"
  table_class    = "STANDARD"

  attribute {
    name = "device_req_no"
    type = "S"
  }

  attribute {
    name = "req_datetime"
    type = "N"
  }

  attribute {
    name = "recv_datetime"
    type = "N"
  }

  attribute {
    name = "device_id"
    type = "S"
  }

  billing_mode = "PAY_PER_REQUEST"

  global_secondary_index {
    hash_key        = "device_id"
    range_key       = "recv_datetime" 
    name            = "device_id_index"
    projection_type = "ALL"
  }

  global_secondary_index {
    hash_key        = "device_id"
    range_key       = "req_datetime" 
    name            = "device_id_req_datetime_index"
    projection_type = "ALL"
  }

  point_in_time_recovery {
    enabled = "true"
  }

  server_side_encryption {
    enabled = true 
  }

  tags = var.tags

}

#要求番号カウンタテーブル
resource "aws_dynamodb_table" "req_no_counter" {
  name           = "${var.global_name}-ddb-t-monosec-req-no-counter-${var.num}"
  hash_key       = "simid"
  stream_enabled = "false"
  table_class    = "STANDARD"

  attribute {
    name = "simid"
    type = "S"
  }

  billing_mode = "PAY_PER_REQUEST"

  point_in_time_recovery {
    enabled = "true"
  }

  server_side_encryption {
    enabled = true 
  }

  tags = var.tags

}

#OPIDテーブル
resource "aws_dynamodb_table" "operator" {
  name           = "${var.global_name}-ddb-m-operator-${var.num}"
  hash_key       = "service"
  stream_enabled = "false"
  table_class = "STANDARD"

  attribute {
    name = "service"
    type = "S"
  }

  billing_mode = "PAY_PER_REQUEST"

  point_in_time_recovery {
    enabled = "true"
  }

  server_side_encryption {
    enabled = true 
  }

  tags = var.tags
}

#連動制御設定管理テーブル
resource "aws_dynamodb_table" "automations" {
  name           = "${var.global_name}-ddb-t-monosec-automations"
  hash_key       = "automation_id"
  stream_enabled = "false"
  table_class    = "STANDARD"

  attribute {
    name = "automation_id"
    type = "S"
  }

  attribute {
    name = "trigger_device_id"
    type = "S"
  }

  attribute {
    name = "control_device_id"
    type = "S"
  }

  billing_mode = "PAY_PER_REQUEST"

  global_secondary_index {
    hash_key        = "trigger_device_id"
    name            = "trigger_device_id_index"
    projection_type = "ALL"
  }

  global_secondary_index {
    hash_key        = "control_device_id"
    name            = "control_device_id_index"
    projection_type = "ALL"
  }

  point_in_time_recovery {
    enabled = "true"
  }

  server_side_encryption {
    enabled = true 
  }

  tags = var.tags
}