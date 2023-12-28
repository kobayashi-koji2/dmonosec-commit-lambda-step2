resource "aws_ssm_parameter" "dynamodb_table_name"{
  name      = "${var.global_name}-ssm-dynamodb-table-names" 
  value     = jsonencode({
        ACCOUNT_TABLE    = "${var.account_table_name}"
        IMEI_TABLE       = "${var.imei_table_name}"
        ICCID_TABLE      = "${var.iccid_table_name}"
        CONTRACT_TABLE   = "${var.contract_table_name}"
        OPERATOR_TABLE   = "${var.operator_table_name}"
        USER_TABLE       = "${var.user_table_name}"
        DEVICE_TABLE     = "${var.device_table_name}"
        GROUP_TABLE      = "${var.group_table_name}"
        STATE_TABLE      = "${var.state_table_name}"
        HIST_LIST_TABLE  = "${var.hist_list_table_name}"
        NOTIFICATION_HIST_TABLE = "${var.notification_hist_table_name}"
        PRE_REGISTER_DEVICE_TABLE = "${var.pre_register_devices_table_name}"
        CNT_HIST_TABLE   = "${var.cnt_hist_table_name}"
        REMOTE_CONTROL_TABLE = "${var.remote_control_name}"
        DEVICE_RELATION_TABLE = "${var.device_relation_name}"
        REQ_NO_COUNTER_TABLE = "${var.req_no_counter_table}"
  }) 
  description = "DynamoDB Table Name"
  data_type = "text"
  tier      = "Standard"
  type      = "String"
  tags      = var.tags
}

resource "aws_ssm_parameter" "soracom_authkey"{
  name      = "${var.global_name}-ssm-soracom-authkey-${var.num}"
  value     = "${var.soracom_authkey}" 
  description = "Soracom AuthKey"
  data_type = "text"
  tier      = "Standard"
  type      = "String"
  tags      = var.tags
}

resource "aws_ssm_parameter" "soracom_secret"{
  name      = "${var.global_name}-ssm-soracom-secret-${var.num}"
  value     = "${var.soracom_secret}" 
  description = "Soracom Secret"
  data_type = "text"
  tier      = "Standard"
  type      = "String"
  tags      = var.tags
}
