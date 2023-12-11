resource "aws_ssm_parameter" "dynamodb_table_name"{
  name      = "${var.global_name}-ssm-dynamodb-table-names" 
  value     = jsonencode({
        ACCOUNT_TABLE    = "${var.account_table_name}"
        IMEI_TABLE       = "${var.imei_table_name}"
        ICCID_TABLE      = "${var.iccid_table_name}"
        CONTRACT_TABLE   = "${var.contract_table_name}"
        USER_TABLE       = "${var.user_table_name}"
        DEVICE_TABLE     = "${var.device_table_name}"
        GROUP_TABLE      = "${var.group_table_name}"
        STATE_TABLE      = "${var.state_table_name}"
        HIST_LIST_TABLE  = "${var.hist_list_table_name}"
        NOTIFICATION_LIST_TABLE = "${var.notification_list_table_name}"
        PRE_REGISTER_DEVICE_TABLE = "${var.pre_register_devices_table_name}"
        CNT_HIST_TABLE   = "${var.cnt_hist_table_name}"
        REMOTE_CONTROL_TABLE = "${var.remote_control_name}"
        USER_DEVICE_GROUP_TABLE = "${var.user_device_group_name}"
  }) 
  description = "DynamoDB Table Name"
  data_type = "text"
  tier      = "Standard"
  type      = "String"
  tags      = var.tags
}