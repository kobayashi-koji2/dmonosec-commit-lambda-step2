import os
import ddb
import uuid
import boto3
import time
import json
import logging
from datetime import datetime

"""
MONOSC_MAIL_FROM = os.environ["MONOSC_MAIL_FROM"]
OCCURRENCE_FLAG_ON = os.environ["OCCURRENCE_FLAG_ON"]
DI_MAIL_TEMPLATE = os.environ["DI_MAIL_TEMPLATE"]
DO_MAIL_TEMPLATE = os.environ["DO_MAIL_TEMPLATE"]
DEVICE_ABNORMALITY_OCCURRENCE_MAIL_TEMPLATE = os.environ["DEVICE_ABNORMALITY_OCCURRENCE_MAIL_TEMPLATE"]
DEVICE_ABNORMALITY_RECOVERY_MAIL_TEMPLATE = os.environ["DEVICE_ABNORMALITY_RECOVERY_MAIL_TEMPLATE"]
BATTERY_NEAR_OCCURRENCE_MAIL_TEMPLATE = os.environ["BATTERY_NEAR_OCCURRENCE_MAIL_TEMPLATE"]
BATTERY_NEAR_RECOVERY_MAIL_TEMPLATE = os.environ["BATTERY_NEAR_RECOVERY_MAIL_TEMPLATE"]
PARAMETER_ABNORMALITY_OCCURRENCE_MAIL_TEMPLATE = os.environ["PARAMETER_ABNORMALITY_OCCURRENCE_MAIL_TEMPLATE"]
PARAMETER_ABNORMALITY_RECOVERY_MAIL_TEMPLATE = os.environ["PARAMETER_ABNORMALITY_RECOVERY_MAIL_TEMPLATE"]
FW_UPDATE_ABNORMALITY_OCCURRENCE_MAIL_TEMPLATE = os.environ["FW_UPDATE_ABNORMALITY_OCCURRENCE_MAIL_TEMPLATE"]
FW_UPDATE_ABNORMALITY_RECOVERY_MAIL_TEMPLATE = os.environ["FW_UPDATE_ABNORMALITY_RECOVERY_MAIL_TEMPLATE"]
TURN_ON_MAIL_TEMPLATE = os.environ["TURN_ON_MAIL_TEMPLATE"]
REMOTE_CONTROL_MAIL_TEMPLATE = os.environ["REMOTE_CONTROL_MAIL_TEMPLATE"]
"""

logger = logging.getLogger()

def diNameToState(terminal_state_name, device_info):
	di_list = device_info['device_data']['param']['config']['terminal_settings']['di_list']
	for di in di_list:
		if di['di_on_name'] == terminal_state_name:
			di_state = 1
			break
		elif di['di_off_name'] == terminal_state_name:
			di_state = 0
			break
	return di_state


def doNameToState(terminal_state_name, device_info):
	do_list = device_info['device_data']['param']['config']['terminal_settings']['do_list']
	for do in do_list:
		if do['do_on_name'] == terminal_state_name:
			do_state = 1
			break
		elif do['do_off_name'] == terminal_state_name:
			do_state = 0
			break
	return do_state


def mailNotice(hist_list, device_info, user_table, account_table, notification_hist_table):
	logger.debug(f'mailNotice開始 hist_list={hist_list} device_info={device_info}')
	"""
	# 通知設定チェック
	if device_info['device_data']['param']['config']['notification_settings'] is None or\
		  len(device_info['device_data']['param']['config']['notification_settings']) == 0:
		# 通知設定が存在しない場合、通知無し応答
		return hist_list
	notification_settings_list = device_info['device_data']['param']['config']['notification_settings']

	remote_control_list = ["manual_control", "on_timer_control", "off_timer_control"]
	# メール通知設定チェック
	for notification_settings in notification_settings_list:
		(event_trigger, change_detail, notification_target_list) = notification_settings

		# 通知先チェック
		if len(notification_target_list) == 0:
			# 通知先が存在しないため、通知無し応答
			continue

		for i, hist_data in enumerate(hist_list):
			mail_send_flg = False
			mail_template = ""
			template_data = {
				'event_datetime': hist_data['event_datetime'],
				'recv_datetime'	: hist_data['recv_datetime'],
				'group_name': hist_data['recv_datetime']['group_list'][0]['group_name'],
				'device_name': hist_data['device_name']
			}
			# 接点入力
			if hist_data['hist_data']['event_type'] == "di_change" and event_trigger == "di_change":
				di_state = diNameToState(hist_data['hist_data']['terminal_state_name'], device_info)
				if change_detail == di_state:
					mail_template = DI_MAIL_TEMPLATE
					template_data['terminal_name'] = hist_data['hist_data']['terminal_name']
					template_data['terminal_state_name'] = hist_data['hist_data']['terminal_state_name']
					mail_send_flg = True

			# デバイス状態（バッテリーニアエンド）
			elif hist_data['hist_data']['event_type'] == "battery_near" and event_trigger == "device_abnormality":
				if hist_data['hist_data']['occurrence_flag'] == OCCURRENCE_FLAG_ON:
					mail_template = BATTERY_NEAR_OCCURRENCE_MAIL_TEMPLATE
				else:
					mail_template = BATTERY_NEAR_RECOVERY_MAIL_TEMPLATE
				mail_send_flg = True

			# デバイス状態（機器異常）
			elif hist_data['hist_data']['event_type'] == "device_abnormality" and event_trigger == "device_abnormality":
				if hist_data['hist_data']['occurrence_flag'] == OCCURRENCE_FLAG_ON:
					mail_template = DEVICE_ABNORMALITY_OCCURRENCE_MAIL_TEMPLATE
				else:
					mail_template = DEVICE_ABNORMALITY_RECOVERY_MAIL_TEMPLATE
				mail_send_flg = True

			# デバイス状態（パラメータ異常）
			elif hist_data['hist_data']['event_type'] == "parameter_abnormality" and event_trigger == "device_abnormality":
				if hist_data['hist_data']['occurrence_flag'] == OCCURRENCE_FLAG_ON:
					mail_template = PARAMETER_ABNORMALITY_OCCURRENCE_MAIL_TEMPLATE
				else:
					mail_template = PARAMETER_ABNORMALITY_RECOVERY_MAIL_TEMPLATE
				mail_send_flg = True

			# デバイス状態（FW更新異常）
			elif hist_data['hist_data']['event_type'] == "fw_update_abnormality" and event_trigger == "device_abnormality":
				if hist_data['hist_data']['occurrence_flag'] == OCCURRENCE_FLAG_ON:
					mail_template = FW_UPDATE_ABNORMALITY_OCCURRENCE_MAIL_TEMPLATE
				else:
					mail_template = FW_UPDATE_ABNORMALITY_RECOVERY_MAIL_TEMPLATE
				mail_send_flg = True

			# 電源ON
			elif hist_data['hist_data']['event_type'] == "power_on" and event_trigger == "power_on":
				mail_template = TURN_ON_MAIL_TEMPLATE
				mail_send_flg = True

			# 遠隔制御応答
			elif hist_data['hist_data']['event_type'] in remote_control_list and\
				event_trigger == "do_change":
				if "link_terminal_no" in hist_data['hist_data']:
					di_state = diNameToState(hist_data['hist_data']['link_terminal_state_name'], device_info)
					if change_detail == di_state:
						mail_template = DO_MAIL_TEMPLATE
				else:
					do_state = doNameToState(hist_data['hist_data']['terminal_state_name'], device_info)
					if change_detail == do_state:
						mail_template = REMOTE_CONTROL_MAIL_TEMPLATE
				mail_send_flg = True

			# メール通知
			if (mail_send_flg):
				mail_address_list = ddb.get_notice_mailaddress(notification_target_list, user_table, account_table)
				now = datetime.now()
				szNoticeDatetime = int(time.mktime(now.timetuple()) * 1000) + int(now.microsecond / 1000)

				# 招待メール送信
				ses_client = boto3.client("ses")
				response = ses_client.send_templated_email(
					Source=MONOSC_MAIL_FROM,
					Destination={
						"ToAddresses": mail_address_list,
					},
					Template=mail_template,
					TemplateData=json.dumps(template_data),
				)

				# 通知履歴保存
				notice_hist_info = {
					'notification_hist_id': str(uuid.uuid4()),
					'contract_id': device_info['device_data']['param']['contract_id'],
					'notification_datetime': szNoticeDatetime,
					'notification_user_list': notification_target_list
				}
				ddb.put_notice_hist(notice_hist_info, notification_hist_table)

				# 履歴一覧編集
				hist_data[i]['hist_data']['notification_hist_id'] = notice_hist_info['notification_hist_id']
	"""
	logger.debug(f'mailNotice終了 hist_list={hist_list}')
	return hist_list
