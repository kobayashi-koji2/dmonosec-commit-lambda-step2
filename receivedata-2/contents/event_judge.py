import os
import ddb
import uuid
import logging

logger = logging.getLogger()

RSSI_HIGH_MIN = int(os.environ["RSSI_HIGH_MIN"])
RSSI_HIGH_MAX = int(os.environ["RSSI_HIGH_MAX"])
RSSI_MID_MIN = int(os.environ["RSSI_MID_MIN"])
RSSI_MID_MAX = int(os.environ["RSSI_MID_MAX"])
RSSI_LOW_MIN = int(os.environ["RSSI_LOW_MIN"])
RSSI_LOW_MAX = int(os.environ["RSSI_LOW_MAX"])
SINR_HIGH_MIN = int(os.environ["SINR_HIGH_MIN"])
SINR_HIGH_MAX = int(os.environ["SINR_HIGH_MAX"])
SINR_MID_MIN = int(os.environ["SINR_MID_MIN"])
SINR_MID_MAX = int(os.environ["SINR_MID_MAX"])
SINR_LOW_MIN = int(os.environ["SINR_LOW_MIN"])
SINR_LOW_MAX = int(os.environ["SINR_LOW_MAX"])
SIGNAL_HIGH = int(os.environ["SIGNAL_HIGH"])
SIGNAL_MID = int(os.environ["SIGNAL_MID"])
SIGNAL_LOW = int(os.environ["SIGNAL_LOW"])
NO_SIGNAL = int(os.environ["NO_SIGNAL"])


def createHistListData(recv_data, device_info, event_info, device_relation_table, group_table):
	logger.debug(f'createHistListData開始 recv_data={recv_data}, device_info={device_info}, event_info={event_info}')
	# グループ情報取得
	group_list = []
	group_list = ddb.get_device_group_list(device_info['device_id'], device_relation_table, group_table)
	logger.debug(f'group_list={group_list}')

	# 共通部
	hist_list_data = {
		'device_id': device_info['device_id'],
		'hist_id': str(uuid.uuid4()),
		'event_datetime': event_info['event_datetime'],
		'recv_datetime'	: recv_data['recv_datetime'],
		'hist_data'	: {
			'device_name': device_info['device_data']['config']['device_name'],
			'group_list': group_list,
			'imei': device_info['imei'],
			'event_type': event_info['event_type']
		}
	}

	if recv_data['message_type'] in ["0001", "0011", "0012"]:
		hist_list_data['hist_data']['cnt_hist_id'] = recv_data['cnt_hist_id']

	# 接点入力部
	if event_info['event_type'] == "di_change":
		terminal_no = event_info['terminal_no']
		for di_list in device_info['device_data']['config']['terminal_settings']['di_list']:
			if int(di_list['di_no']) == int(terminal_no):
				terminal_name = di_list['di_name']
				if event_info['di_state'] == 0:
					terminal_state_name = di_list['di_off_name']
				else:
					terminal_state_name = di_list['di_on_name']
				break;
		hist_list_data['hist_data']['terminal_no'] = terminal_no
		hist_list_data['hist_data']['terminal_name'] = terminal_name
		hist_list_data['hist_data']['terminal_state_name'] = terminal_state_name

	# 接点出力部
	elif event_info['event_type'] == "do_change":
		terminal_no = event_info['terminal_no']
		for do_list in device_info['device_data']['config']['terminal_settings']['do_list']:
			if int(do_list['do_no']) == int(terminal_no):
				terminal_name = do_list['do_name']
				if event_info['do_state'] == 0:
					terminal_state_name = do_list['do_off_name']
				else:
					terminal_state_name = do_list['do_on_name']
				break;
		hist_list_data['hist_data']['terminal_no'] = terminal_no
		hist_list_data['hist_data']['terminal_name'] = terminal_name
		hist_list_data['hist_data']['terminal_state_name'] = terminal_state_name

	# デバイス状態
	elif event_info['event_type'] in ["battery_near", "device_abnormality", "parameter_abnormality", "fw_update_abnormality"]:
		hist_list_data['hist_data']['occurrence_flag'] = event_info['occurrence_flag']

	# 接点出力制御応答
	elif event_info['event_type'] in ["manual_control", "on_timer_control", "off_timer_control"]:
		if "link_di_no" in event_info:
			hist_list_data['hist_data']['link_terminal_no'] = event_info['link_di_no']
			for di_list in device_info['device_data']['config']['terminal_settings']['di_list']:
				if int(di_list['di_no']) == int(event_info['link_di_no']):
					di_terminal_name = di_list['di_name']
					break;
			hist_list_data['hist_data']['link_terminal_name'] = di_terminal_name
			hist_list_data['hist_data']['control_trigger'] = event_info['control_trigger']
			hist_list_data['hist_data']['terminal_no'] = int(event_info['do_no'])
			for do_list in device_info['device_data']['config']['terminal_settings']['do_list']:
				if int(do_list['do_no']) == int(event_info['do_no']):
					do_terminal_name = do_list['do_name']
					break;
			hist_list_data['hist_data']['terminal_name'] = do_terminal_name
			hist_list_data['hist_data']['control_exec_user_name'] = event_info['control_exec_user_name']
			hist_list_data['hist_data']['control_exec_user_email_address'] = event_info['control_exec_user_email_address']
			hist_list_data['hist_data']['cntrol_result'] = event_info['cntrol_result']
			hist_list_data['hist_data']['device_req_no'] = event_info['device_req_no']
			if event_info['event_type'] in ["on_timer_control", "off_timer_control"]:
				hist_list_data['hist_data']['timer_time'] = event_info['timer_time']
		else:
			hist_list_data['hist_data']['control_trigger'] = event_info['control_trigger']
			for do_list in device_info['device_data']['config']['terminal_settings']['do_list']:
				if int(do_list['do_no']) == int(event_info['do_no']):
					terminal_name = do_list['do_name']
					break;
			hist_list_data['hist_data']['terminal_no'] = int(event_info['do_no'])
			hist_list_data['hist_data']['control_exec_user_name'] = event_info['control_exec_user_name']
			hist_list_data['hist_data']['control_exec_user_email_address'] = event_info['control_exec_user_email_address']
			hist_list_data['hist_data']['cntrol_result'] = event_info['cntrol_result']
			hist_list_data['hist_data']['device_req_no'] = event_info['device_req_no']
			if event_info['event_type'] in ["on_timer_control", "off_timer_control"]:
				hist_list_data['hist_data']['timer_time'] = event_info['timer_time']
	logger.debug(f'createHistListData終了 hist_list_data={hist_list_data}')
	return hist_list_data


def initCurrentStateInfo(recv_data, device_current_state, device_info, init_state_flg):
	logger.debug(f'initCurrentStateInfo開始 recv_data={recv_data}, device_current_state={device_current_state}, \
	   device_info={device_info}, init_state_flg={init_state_flg}')
	if init_state_flg == 1:
		di_list = list(reversed(list(recv_data['di_state'])))
		do_list = list(reversed(list(recv_data['do_state'])))

		current_state_info = {
			'device_id': device_info['device_id'],
			'signal_last_update_datetime': recv_data['recv_datetime'],
			'battery_near_last_update_datetime': recv_data['recv_datetime'],
			'device_abnormality_last_update_datetime': recv_data['recv_datetime'],
			'parameter_abnormality_last_update_datetime': recv_data['recv_datetime'],
			'fw_update_abnormality_last_update_datetime': recv_data['recv_datetime'],
			'di1_last_update_datetime': recv_data['recv_datetime'],
			'di2_last_update_datetime': recv_data['recv_datetime'],
			'di3_last_update_datetime': recv_data['recv_datetime'],
			'di4_last_update_datetime': recv_data['recv_datetime'],
			'di5_last_update_datetime': recv_data['recv_datetime'],
			'di6_last_update_datetime': recv_data['recv_datetime'],
			'di7_last_update_datetime': recv_data['recv_datetime'],
			'di8_last_update_datetime': recv_data['recv_datetime'],
			'do1_last_update_datetime': recv_data['recv_datetime'],
			'do2_last_update_datetime': recv_data['recv_datetime'],
			'ai1_last_update_datetime': recv_data['recv_datetime'],
			'ai2_last_update_datetime': recv_data['recv_datetime'],
			'ai1_threshold_last_update_datetime': recv_data['recv_datetime'],
			'ai2_threshold_last_update_datetime': recv_data['recv_datetime'],
			'signal_state': 0,
			'battery_near_state': 0,
			'device_abnormality': 0,
			'parameter_abnormality': 0,
			'fw_update_abnormality': 0,
			'di1_state': int(di_list[0]),
			'di2_state': int(di_list[1]),
			'di3_state': int(di_list[2]),
			'di4_state': int(di_list[3]),
			'di5_state': int(di_list[4]),
			'di6_state': int(di_list[5]),
			'di7_state': int(di_list[6]),
			'di8_state': int(di_list[7]),
			'do1_state': int(do_list[0]),
			'do2_state': int(do_list[1]),
			'ai1_state': recv_data['analogv1'],
			'ai2_state': recv_data['analogv2']
		}
	else:
		current_state_info = device_current_state
		recv_datetime = recv_data['recv_datetime']
		current_state_info['signal_last_update_datetime'] = recv_datetime
		current_state_info['battery_near_last_update_datetime'] = recv_datetime
		current_state_info['device_abnormality_last_update_datetime'] = recv_datetime
		current_state_info['parameter_abnormality_last_update_datetime'] = recv_datetime
		current_state_info['fw_update_abnormality_last_update_datetime'] = recv_datetime
		current_state_info['di1_last_update_datetime'] = recv_datetime
		current_state_info['di2_last_update_datetime'] = recv_datetime
		current_state_info['di3_last_update_datetime'] = recv_datetime
		current_state_info['di4_last_update_datetime'] = recv_datetime
		current_state_info['di5_last_update_datetime'] = recv_datetime
		current_state_info['di6_last_update_datetime'] = recv_datetime
		current_state_info['di7_last_update_datetime'] = recv_datetime
		current_state_info['di8_last_update_datetime'] = recv_datetime
		current_state_info['do1_last_update_datetime'] = recv_datetime
		current_state_info['do2_last_update_datetime'] = recv_datetime

	logger.debug(f'initCurrentStateInfo終了 current_state_info={current_state_info}')
	return current_state_info


def updateCurrentStateInfo(current_state_info, event_info, event_datetime):

	logger.debug(f'updateCurrentStateInfo開始 current_state_info={current_state_info}, event_info={event_info}, event_datetime={event_datetime}')
	di_state = ["di1_state", "di2_state", "di3_state", "di4_state", "di5_state", "di6_state", "di7_state", "di8_state"]
	di_change_datetime = ["di1_last_change_datetime", "di2_last_change_datetime", "di3_last_change_datetime", "di4_last_change_datetime",\
					    "di5_last_change_datetime", "di6_last_change_datetime", "di7_last_change_datetime", "di8_last_change_datetime"]
	do_state = ["do1_state", "do2_state"]
	do_change_datetime = ["do1_last_change_datetime", "do2_last_change_datetime"]

	# イベント判定結果をもとに現状態情報を更新
	# 接点入力部
	if event_info['event_type'] == "di_change":
		list_num = int(event_info['terminal_no']) - 1
		state_key = di_state[list_num]
		change_datetime_key = di_change_datetime[list_num]
		current_state_info[state_key] = event_info['di_state']
		current_state_info[change_datetime_key] = event_datetime

	# 接点出力部
	elif event_info['event_type'] == "do_change":
		list_num = int(event_info['terminal_no']) - 1
		state_key = do_state[list_num]
		change_datetime_key = do_change_datetime[list_num]
		current_state_info[state_key] = event_info['do_state']
		current_state_info[change_datetime_key] = event_datetime

	# デバイス状態（バッテリーニアエンド）
	elif event_info['event_type'] == "battery_near":
		current_state_info['battery_near'] = event_info['occurrence_flag']
		current_state_info['battery_near_last_change_datetime'] = event_datetime

	# デバイス状態（機器異常）
	elif event_info['event_type'] == "device_abnormality":
		current_state_info['device_abnormality'] = event_info['occurrence_flag']
		current_state_info['device_abnormality_last_change_datetime'] = event_datetime

	# デバイス状態（パラメータ異常）
	elif event_info['event_type'] == "parameter_abnormality":
		current_state_info['parameter_abnormality'] = event_info['occurrence_flag']
		current_state_info['parameter_abnormality_last_change_datetime'] = event_datetime

	# デバイス状態（FW更新異常）
	elif event_info['event_type'] == "fw_update_abnormality":
		current_state_info['fw_update_abnormality'] = event_info['occurrence_flag']
		current_state_info['fw_update_abnormality_last_change_datetime'] = event_datetime

	# 電波状態
	elif event_info['event_type'] == "signal_state":
		current_state_info['signal_state'] = event_info['signal_state']
		current_state_info['signal_last_change_datetime'] = event_datetime

	logger.debug(f'updateCurrentStateInfo終了 current_state_info={current_state_info}')
	return current_state_info


def signalStateJedge(rssi, sinr):
	logger.debug(f'signalStateJedge開始 rssi={rssi}, sinr={sinr}')
	signal_state_matrix = [
		["high", "mid", "low", "no_signal"],
		["mid", "mid", "low", "no_signal"],
		["low", "low", "low", "no_signal"],
		["no_signal",  "no_signal", "no_signal", "no_signal"]
	]

	# RSSI判定
	if RSSI_HIGH_MIN <= rssi <= RSSI_HIGH_MAX:
		rssi_revel = SIGNAL_HIGH
	elif RSSI_MID_MIN <= rssi <= RSSI_MID_MAX:
		rssi_revel = SIGNAL_MID
	elif RSSI_LOW_MIN <= rssi <= RSSI_LOW_MAX:
		rssi_revel = SIGNAL_LOW
	else:
		rssi_revel = NO_SIGNAL

	# SINR判定
	if SINR_HIGH_MIN <= sinr <= SINR_HIGH_MAX:
		sinr_revel = SIGNAL_HIGH
	elif SINR_MID_MIN <= sinr <= SINR_MID_MAX:
		sinr_revel = SIGNAL_MID
	elif SINR_LOW_MIN <= sinr <= SINR_LOW_MAX:
		sinr_revel = SIGNAL_LOW
	else:
		sinr_revel = NO_SIGNAL

	signl_state = signal_state_matrix[sinr_revel][rssi_revel]

	logger.debug(f'signalStateJedge終了 signl_state={signl_state}')
	return signl_state


def eventJudge(recv_data, device_current_state, device_info, device_relation_table, group_table, remote_control_table):
	logger.debug(f'eventJudge開始 recv_data={recv_data}, \
	   device_current_state={device_current_state}, device_info={device_info}')

	# 履歴リスト作成
	hist_list = []
	event_datetime = recv_data['event_datetime']

	# 現状態設定
	init_state_flg = False
	if device_current_state is None or len(device_current_state) == 0:
		init_state_flg = True
	current_state_info = initCurrentStateInfo(recv_data, device_current_state, device_info, init_state_flg)
	logger.debug(f'init_state_flg={init_state_flg}')

	# 接点入力変化判定
	if recv_data['message_type'] in ["0001", "0011", "0012"]:
		event_info = {}
		event_info['event_type'] = "di_change"
		event_info['event_datetime'] = recv_data['event_datetime']
		di_list = list(reversed(list(recv_data['di_state'])))
		if recv_data['message_type'] == "0001":
			di_trigger = list(reversed(list(format(recv_data['di_trigger'], '08b'))))
		for i in range(8):
			event_info['terminal_no'] = i + 1
			event_info['di_state'] = int(di_list[i])
			if recv_data['message_type'] == "0001":
				if di_trigger[i] == "1":
					hist_list_data = createHistListData(recv_data, device_info, event_info, device_relation_table, group_table)
					hist_list.append(hist_list_data)
			if not init_state_flg:
				terminal_key = "di" + str(i+1) + "_state"
				current_di = device_current_state[terminal_key]
			if (init_state_flg) or (not init_state_flg and int(di_list[i]) != current_di):
				current_state_info = updateCurrentStateInfo(current_state_info, event_info, event_datetime)

	# 接点出力変化判定
	if recv_data['message_type'] in ["0001", "0011", "0012"]:
		event_info = {}
		event_info['event_type'] = "do_change"
		event_info['event_datetime'] = recv_data['event_datetime']
		do_list = list(reversed(list(recv_data['do_state'])))
		if recv_data['message_type'] == "0001":
			do_trigger = list(reversed(list(format(recv_data['do_trigger'], '08b'))))
		for i in range(2):
			event_info['terminal_no'] = i + 1
			event_info['do_state'] = int(do_list[i])
			if recv_data['message_type'] == "0001":
				if do_trigger[i] == "1":
					hist_list_data = createHistListData(recv_data, device_info, event_info, device_relation_table, group_table)
					hist_list.append(hist_list_data)
			if not init_state_flg:
				terminal_key = "do" + str(i+1) + "_state"
				current_do = device_current_state[terminal_key]
			if (init_state_flg) or (not init_state_flg and int(do_list[i]) != current_do):
				current_state_info = updateCurrentStateInfo(current_state_info, event_info, event_datetime)

	# バッテリーニアエンド判定
	if recv_data['message_type'] in ["0001", "0011", "0012"]:
		check_digit = 0b00000001
		event_info = {}
		event_info['event_type'] = "battery_near"
		event_info['event_datetime'] = recv_data['event_datetime']
		hist_battery_near = recv_data['device_state'] & check_digit
		if not init_state_flg:
			current_battery_near = device_current_state['battery_near_state']

		if (init_state_flg) or (not init_state_flg and hist_battery_near != current_battery_near):
			if hist_battery_near == check_digit:
				event_info['occurrence_flag'] = 1
			else:
				event_info['occurrence_flag'] = 0

			if not (init_state_flg == 1 and event_info['occurrence_flag'] == 0):
				hist_list_data = createHistListData(recv_data, device_info, event_info, device_relation_table, group_table)
				hist_list.append(hist_list_data)
				current_state_info = updateCurrentStateInfo(current_state_info, event_info, event_datetime)

	# 機器異常判定
	if recv_data['message_type'] in ["0001", "0011", "0012"]:
		check_digit = 0b00000100
		event_info = {}
		event_info['event_type'] = "device_abnormality"
		event_info['event_datetime'] = recv_data['event_datetime']
		hist_device_abnormality = recv_data['device_state'] & check_digit
		if not init_state_flg:
			current_device_abnormality = device_current_state['device_abnormality']

		if (init_state_flg) or (not init_state_flg and hist_device_abnormality != current_device_abnormality):
			if hist_device_abnormality == check_digit:
				event_info['occurrence_flag'] = 1
			else:
				event_info['occurrence_flag'] = 0

			if not (init_state_flg == 1 and event_info['occurrence_flag'] == 0):
				hist_list_data = createHistListData(recv_data, device_info, event_info, device_relation_table, group_table)
				hist_list.append(hist_list_data)
				current_state_info = updateCurrentStateInfo(current_state_info, event_info, event_datetime)

	# パラメータ異常判定
	if recv_data['message_type'] in ["0001", "0011", "0012"]:
		check_digit = 0b01000000
		event_info = {}
		event_info['event_type'] = "parameter_abnormality"
		event_info['event_datetime'] = recv_data['event_datetime']
		hist_parameter_abnormality = recv_data['device_state'] & check_digit
		if not init_state_flg:
			current_parameter_abnormality = device_current_state['parameter_abnormality']

		if (init_state_flg) or (not init_state_flg and hist_parameter_abnormality != current_parameter_abnormality):
			if hist_parameter_abnormality == check_digit:
				event_info['occurrence_flag'] = 1
			else:
				event_info['occurrence_flag'] = 0

			if not (init_state_flg == 1 and event_info['occurrence_flag'] == 0):
				hist_list_data = createHistListData(recv_data, device_info, event_info, device_relation_table, group_table)
				hist_list.append(hist_list_data)
				current_state_info = updateCurrentStateInfo(current_state_info, event_info, event_datetime)

	# FW更新異常判定
	if recv_data['message_type'] in ["0001", "0011", "0012"]:
		check_digit = 0b10000000
		event_info = {}
		event_info['event_type'] = "fw_update_abnormality"
		event_info['event_datetime'] = recv_data['event_datetime']
		hist_fw_update_abnormality = recv_data['device_state'] & check_digit
		if not init_state_flg:
			current_fw_update_abnormality = device_current_state['fw_update_abnormality']

		if (init_state_flg) or (not init_state_flg and hist_fw_update_abnormality != current_fw_update_abnormality):
			if hist_fw_update_abnormality == check_digit:
				event_info['occurrence_flag'] = 1
			else:
				event_info['occurrence_flag'] = 0

			if not (init_state_flg == True and event_info['occurrence_flag'] == 0):
				hist_list_data = createHistListData(recv_data, device_info, event_info, device_relation_table, group_table)
				hist_list.append(hist_list_data)
				current_state_info = updateCurrentStateInfo(current_state_info, event_info, event_datetime)

	# 電源ON
	if recv_data['message_type'] in ["0011"]:
		check_digit = 0b10000000
		event_info = {}
		event_info['event_type'] = "power_on"
		event_info['event_datetime'] = recv_data['event_datetime']
		hist_list_data = createHistListData(recv_data, device_info, event_info, device_relation_table, group_table)
		hist_list.append(hist_list_data)
		current_state_info = updateCurrentStateInfo(current_state_info, event_info, event_datetime)

	# 電波状態
	if recv_data['message_type'] in ["0001", "0011", "0012"]:
		hist_signal_state = signalStateJedge(recv_data['rssi'], recv_data['sinr'])
		event_info = {}
		event_info['event_type'] = "signal_state"
		event_info['event_datetime'] = recv_data['event_datetime']
		event_info['signal_state'] = hist_signal_state
		if (init_state_flg) or (not init_state_flg and hist_signal_state != device_current_state['signal_state']):
			current_state_info = updateCurrentStateInfo(current_state_info, event_info, event_datetime)

	# 遠隔制御（接点出力制御応答）
	if recv_data['message_type'] in ["8002"]:
		event_info = {}
		remote_control_info = ddb.get_remote_control_info(recv_data['device_req_no'], remote_control_table)
		if remote_control_info is None:
			return hist_list, current_state_info
		logger.debug(f'remote_control_info={remote_control_info}')
		event_info['event_datetime'] = remote_control_info['req_datetime']
		event_info['do_no'] = remote_control_info['do_no']
		event_info['control_exec_user_name'] = remote_control_info['control_exec_user_name']
		event_info['control_exec_user_email_address'] = remote_control_info['control_exec_user_email_address']
		event_info['control_trigger'] = remote_control_info['control_trigger']
		event_info['event_type'] = remote_control_info['control_trigger']
		event_info['device_req_no'] = recv_data['device_req_no']
		if recv_data['cntrol_result'] == 0:
			event_info['cntrol_result'] = "success"
		else:
			event_info['cntrol_result'] = "failure"

		# 制御トリガー判定
		if remote_control_info['control_trigger'] in ["on_timer_control", "off_timer_control"]:
			event_info['timer_time'] = remote_control_info['timer_time']
		hist_list_data = createHistListData(recv_data, device_info, event_info, device_relation_table, group_table)
		hist_list.append(hist_list_data)

	# 遠隔制御（状態変化通知）
	if recv_data['message_type'] in ["0001"]:
		event_info = {}
		remote_control_info = ddb.get_remote_control_info_by_device_id(device_info['device_id'], recv_data['recv_datetime'], remote_control_table)
		logger.debug(f'remote_control_info={remote_control_info}')
		if remote_control_info is not None and "link_di_no" in remote_control_info:
			di_trigger = list(reversed(list(format(recv_data['di_trigger'], '08b'))))
			list_di_no = int(remote_control_info['link_di_no']) - 1
			if di_trigger[list_di_no] == "1":
				event_info['event_datetime'] = remote_control_info['req_datetime']
				event_info['do_no'] = remote_control_info['do_no']
				event_info['control_exec_user_name'] = remote_control_info['control_exec_user_name']
				event_info['control_exec_user_email_address'] = remote_control_info['control_exec_user_email_address']
				event_info['link_di_no'] = remote_control_info['link_di_no']
				event_info['device_req_no'] = remote_control_info['device_req_no']
				event_info['control_trigger'] = remote_control_info['control_trigger']
				event_info['event_type'] = remote_control_info['control_trigger']
				if remote_control_info['cntrol_result'] == 0:
					event_info['cntrol_result'] = "success"
				else:
					event_info['cntrol_result'] = "failure"

				# 制御トリガー判定
				if remote_control_info['control_trigger'] in ["on_timer_control", "off_timer_control"]:
					event_info['timer_time'] = remote_control_info['timer_time']
				hist_list_data = createHistListData(recv_data, device_info, event_info, device_relation_table, group_table)
				hist_list.append(hist_list_data)

	logger.debug(f'eventJudge終了 hist_list={hist_list}, current_state_info={current_state_info}')
	return hist_list, current_state_info
