variable "global_name" {}
variable "tags" {}
variable "num"{}

#アカウント管理テーブル
resource "aws_dynamodb_table" "account" {
  name           = "${var.global_name}-ddb-m-office-accounts"
  hash_key       = "salesforce_id"
  range_key      = "contract_id"
  stream_enabled = "false"
  table_class    = "STANDARD"

  attribute {
    name = "salesforce_id"
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

  billing_mode = "PAY_PER_REQUEST"

  global_secondary_index {
    hash_key        = "user_id"
    name            = "user_id_index"
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

#ユーザ管理テーブル
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
    name = "email"
    type = "S"
  }

  global_secondary_index {
    hash_key        = "email"
    name            = "email_index"
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
    hash_key        = "device_id"
    range_key       = "contract_state"
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

#通知履歴テーブル
resource "aws_dynamodb_table" "notification_list" {
  name           = "${var.global_name}-ddb-t-monosec-notification-hist"
  hash_key       = "notification_address"
  range_key      = "notification_hist_id"
  stream_enabled = "false"
  table_class    = "STANDARD"

  attribute {
    name = "notification_address"
    type = "S"
  }

  attribute {
    name = "notification_hist_id"
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


#履歴受信テーブル
resource "aws_dynamodb_table" "cnt_hist" {
  name           = "${var.global_name}-ddb-t-monosec-cnt-hist-${var.num}"
  hash_key       = "simid"
  range_key      = "event_datetime"
  stream_enabled = "false"
  table_class    = "STANDARD"

  attribute {
    name = "event_datetime"
    type = "N"
  }

  attribute {
    name = "recv_datetime"
    type = "N"
  }

  attribute {
    name = "simid"
    type = "S"
  }

  billing_mode = "PAY_PER_REQUEST"

  global_secondary_index {
    hash_key        = "simid"
    name            = "simid-recv_datetime-index"
    projection_type = "ALL"
    range_key       = "recv_datetime"
  }

  point_in_time_recovery {
    enabled = "true"
  }

  server_side_encryption {
    enabled = true 
  }

  tags = var.tags

}

#接点出力制御応答テーブル
resource "aws_dynamodb_table" "remote_control" {
  name           = "${var.global_name}-ddb-t-monosec-remote-control"
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
    name = "client_req_no"
    type = "S"
  }

  billing_mode = "PAY_PER_REQUEST"

  global_secondary_index {
    hash_key        = "client_req_no"
    name            = "client_req_no-index"
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

#ユーザ_デバイス_グループ中間テーブル
resource "aws_dynamodb_table" "user_device_group" {
  name           = "${var.global_name}-ddb-t-monosec-user-device-group"
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
