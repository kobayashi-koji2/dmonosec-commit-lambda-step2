variable "global_name" {}
variable "domain_name" {}
variable "tags" {}


provider "aws" {
  region     = "ap-northeast-1"
}

resource "aws_route53_zone" "primary" {
  name = "${var.domain_name}"
}

resource "aws_ses_domain_identity" "ses" {
  domain = "${var.domain_name}"
}

resource "aws_route53_record" "ses_record" {
  zone_id = "${aws_route53_zone.primary.zone_id}"
  name    = "_amazonses.${aws_route53_zone.primary.name}"
  type    = "TXT"
  ttl     = "600"
  records = ["${aws_ses_domain_identity.ses.verification_token}"]
}

# resource "aws_ses_domain_dkim" "dkim" {
#   domain = "${var.domain_name}"
# }

# resource "aws_route53_record" "dkim_record" {
#   count   = 3
#   zone_id = "${aws_route53_zone.primary.zone_id}"
#   name    = "${element(aws_ses_domain_dkim.dkim.dkim_tokens, count.index)}._domainkey.${aws_route53_zone.primary.name}"
#   type    = "CNAME"
#   ttl     = "600"
#   records = ["${element(aws_ses_domain_dkim.dkim.dkim_tokens, count.index)}.dkim.amazonses.com"]
# }

resource "aws_ses_template" "DITemplate" {
  name    = "${var.global_name}-sest-di"
  subject = "イベントが発生しました"
  html    = ""
  text    = "■発生日時：{{event_datetime}}\n　受信日時:{{recv_datetime}}\n\n■グループ：{{group_name}}\n　デバイス：{{device_name}}\n\n■イベント内容\n　【接点入力変化】\n　　{{terminal_name}}が{{terminal_state_name}}に変化しました。\n"
}

resource "aws_ses_template" "DOTemplate" {
  name    = "${var.global_name}-sest-di"
  subject = "イベントが発生しました"
  html    = ""
  text    = "■発生日時：{{event_datetime}}\n　受信日時:{{recv_datetime}}\n\n■グループ：{{group_name}}\n　デバイス：{{device_name}}\n\n■イベント内容\n　【接点出力変化】\n　　{{terminal_name}}が{{terminal_state_name}}に変化しました。\n"
}

resource "aws_ses_template" "BatteryAbnormalityTemplate" {
  name    = "${var.global_name}-sest-battery-near-occurrence"
  subject = "イベントが発生しました"
  html    = ""
  text    = "■発生日時：{{event_datetime}}\n　受信日時:{{recv_datetime}}\n\n■グループ：{{group_name}}\n　デバイス：{{device_name}}\n\n■イベント内容\n　【電池残量変化(少ない)】\n　　デバイスの電池残量が少ない状態に変化しました。\n"
}

resource "aws_ses_template" "BatteryRecoveryTemplate" {
  name    = "${var.global_name}-sest-battery-near-recovery"
  subject = "イベントが発生しました"
  html    = ""
  text    = "■発生日時：{{event_datetime}}\n　受信日時:{{recv_datetime}}\n\n■グループ：{{group_name}}\n　デバイス：{{device_name}}\n\n■イベント内容\n　【電池残量変化(十分)】\n　　デバイスの電池残量が十分な状態に変化しました。\n"
}

resource "aws_ses_template" "DeviceAbnormalityTemplate" {
  name    = "${var.global_name}-sest-device-abnormality-occurrence"
  subject = "イベントが発生しました"
  html    = ""
  text    = "■発生日時：{{event_datetime}}\n　受信日時:{{recv_datetime}}\n\n■グループ：{{group_name}}\n　デバイス：{{device_name}}\n\n■イベント内容\n　【機器異常(発生)】\n　　機器異常が発生しました。\n"
}

resource "aws_ses_template" "DeviceRecoveryTemplate" {
  name    = "${var.global_name}-sest-device-abnormality-recovery"
  subject = "イベントが発生しました"
  html    = ""
  text    = "■発生日時：{{event_datetime}}\n　受信日時:{{recv_datetime}}\n\n■グループ：{{group_name}}\n　デバイス：{{device_name}}\n\n■イベント内容\n　【機器異常(復旧)】\n　　機器異常が復旧しました。\n"
}

resource "aws_ses_template" "ParamAbnormalityTemplate" {
  name    = "${var.global_name}-sest-parameter-abnormality-occurrence"
  subject = "イベントが発生しました"
  html    = ""
  text    = "■発生日時：{{event_datetime}}\n　受信日時:{{recv_datetime}}\n\n■グループ：{{group_name}}\n　デバイス：{{device_name}}\n\n■イベント内容\n　【パラメータ異常(発生)】\n　　パラメータ異常が発生しました。\n"
}

resource "aws_ses_template" "ParamRecoveryTemplate" {
  name    = "${var.global_name}-sest-parameter-abnormality-recovery"
  subject = "イベントが発生しました"
  html    = ""
  text    = "■発生日時：{{event_datetime}}\n　受信日時:{{recv_datetime}}\n\n■グループ：{{group_name}}\n　デバイス：{{device_name}}\n\n■イベント内容\n　【パラメータ異常(復旧)】\n　　パラメータ異常が復旧しました。\n"
}

resource "aws_ses_template" "FwAbnormalityTemplate" {
  name    = "${var.global_name}-sest-fw-update-abnormality-occurrence"
  subject = "イベントが発生しました"
  html    = ""
  text    = "■発生日時：{{event_datetime}}\n　受信日時:{{recv_datetime}}\n\n■グループ：{{group_name}}\n　デバイス：{{device_name}}\n\n■イベント内容\n　【FW更新異常(発生)】\n　　FW更新異常が発生しました。\n"
}

resource "aws_ses_template" "FwRecoveryTemplate" {
  name    = "${var.global_name}-sest-fw-update-abnormality-recovery"
  subject = "イベントが発生しました"
  html    = ""
  text    = "■発生日時：{{event_datetime}}\n　受信日時:{{recv_datetime}}\n\n■グループ：{{group_name}}\n　デバイス：{{device_name}}\n\n■イベント内容\n　【FWこうしん異常(復旧)】\n　　FW更新異常が復旧しました。\n"
}

resource "aws_ses_template" "TurnRecoveryTemplate" {
  name    = "${var.global_name}-sest-turn-on"
  subject = "イベントが発生しました"
  html    = ""
  text    = "■発生日時：{{event_datetime}}\n　受信日時:{{recv_datetime}}\n\n■グループ：{{group_name}}\n　デバイス：{{device_name}}\n\n■イベント内容\n　【電源ON】\n　　デバイスの電源がONになりました。\n"
}

resource "aws_ses_template" "RemoteControlTemplate" {
  name    = "${var.global_name}-sest-remote-control"
  subject = "イベントが発生しました"
  html    = ""
  text    = "■発生日時：{{event_datetime}}\n　受信日時:{{recv_datetime}}\n\n■グループ：{{group_name}}\n　デバイス：{{device_name}}\n\n■イベント内容\n　【画面操作による制御(成功)】\n　　{{terminal_name}}の制御信号がデバイスに届きました。\n　※{{user_name}}が操作を行いました。\n"
}

resource "aws_ses_template" "ManualControl" {
  name    = "${var.global_name}-sest-manual-control"
  subject = "イベントが発生しました"
  html    = ""
  text    = "■発生日時：{{event_datetime}}\n　受信日時:{{recv_datetime}}\n\n■グループ：{{group_name}}\n　デバイス：{{device_name}}\n\n■イベント内容\n　【画面操作による制御(成功)】\n　　制御信号（{{do_terminal_name}}）がデバイスに届き、{{di_terminal_name}}が{{terminal_state_name}}に変化しました。\n　※{{user_name}}が操作を行いました。\n"
}

resource "aws_ses_template" "TimerControl" {
  name    = "${var.global_name}-sest-timer-control"
  subject = "イベントが発生しました"
  html    = ""
  text    = "■発生日時：{{event_datetime}}\n　受信日時:{{recv_datetime}}\n\n■グループ：{{group_name}}\n　デバイス：{{device_name}}\n\n■イベント内容\n　【タイマー設定による制御(成功)】\n　　制御信号（{{do_terminal_name}}）がデバイスに届き、{{di_terminal_name}}が{{terminal_state_name}}に変化しました。\n　※タイマー設定「{{control_trigger}}　{{timer_time}}により制御信号を送信しました。\n"
}
