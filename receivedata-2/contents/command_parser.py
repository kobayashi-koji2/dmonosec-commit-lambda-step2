import ddb
import logging
import uuid
from event_judge import eventJudge
from mail_notice import mailNotice

logger = logging.getLogger()


def getByteArray(Payload, index, len):
	start = index[0]
	end	 = index[0] + len
	index[0] += len
	return Payload[start:end]


def signed_hex2int( signed_hex, digit ):
	signed = 0x01 << (digit-1)
	mask = 0x00
	for num in range(digit):
		mask = mask | (0x01<<num)
	signed_int = (int(signed_hex^mask)*-1)-1	if (signed_hex & signed) else int(signed_hex)
	return signed_int


def commandParser(szSimid, szRecvDatetime, Payload, device_info, stray_flag, hist_table,
				  hist_list_table, state_table, group_table, notification_hist_table,
				  device_relation_table, user_table, account_table, remote_control_table):
	logger.debug(f'commandParser開始 szSimid={szSimid}, szRecvDatetime={szRecvDatetime}, Payload={Payload}')
    ### コマンド共通部 ###
	index = [0]
	# メッセージ長
	nlen = int.from_bytes(getByteArray(Payload, index, 2), 'big')
	# デバイス種別
	nDeviceType	= int.from_bytes(getByteArray(Payload, index, 2), 'big')
	# ファームウェアVersion
	nVer = int.from_bytes(getByteArray(Payload, index, 2), 'big')
	# メッセージ種別
	nMsgType = int.from_bytes(getByteArray(Payload, index, 2), 'big')

	# 要求番号
	if nMsgType == 0x8002:
		szReqNo = getByteArray(Payload, index, 4).hex()
		logger.debug(f'szReqNo={szReqNo}')
	else:
		szReqNo = None

	# イベント発生日時
	nEventTime = int.from_bytes(getByteArray(Payload, index, 8), 'big')
	logger.debug(f'nEventTime={nEventTime}')

    # デバイス状態
	nState = int.from_bytes(getByteArray(Payload, index, 1), 'big')
	if nMsgType == 0x0001:
		nStatetrg = int.from_bytes(getByteArray(Payload, index, 1), 'big')
		logger.debug(f'nStatetrg={nStatetrg}')
	else:
		nStatetrg = None

	# 供給電圧(1/10V)
	nVolt = int.from_bytes(getByteArray(Payload, index, 1), 'big')
	# RSSI
	nRssi = signed_hex2int(int.from_bytes(getByteArray(Payload, index, 1), 'big'), 8)
	# SINR
	nSinr = signed_hex2int(int.from_bytes(getByteArray(Payload, index, 1), 'big'), 8)
	logger.debug(f'nState={nState}, nVolt={nVolt}, nRssi={nRssi}, nSinr={nSinr}')

    ### コマンドデータ部 ###
	nDIState = None
	nDItrg = None
	nDOState = None
	nDOtrg = None
	nAI1 = None
	nAI2 = None
	nAItrg = None
	hist_flg = False

    # 状態変化通知
	if nMsgType == 0x0001:
		nDIState = int.from_bytes(getByteArray(Payload, index, 1), 'big')
		nDItrg = int.from_bytes(getByteArray(Payload, index, 1), 'big')
		nDOState = int.from_bytes(getByteArray(Payload, index, 1), 'big')
		nDOtrg = int.from_bytes(getByteArray(Payload, index, 1), 'big')
		nAI1 = signed_hex2int(int.from_bytes(getByteArray(Payload, index, 2), 'big'), 16)
		nAI2 = signed_hex2int(int.from_bytes(getByteArray(Payload, index, 2), 'big'), 16)
		nAItrg = int.from_bytes(getByteArray(Payload, index, 1), 'big')
		hist_flg = True
		logger.debug(f'状態変化通知 nDIState={nDIState}, nDItrg={nDItrg}, nDOState={nDOState}, nDOtrg={nDOtrg}')
		logger.debug(f'nAI1={nAI1}, nAI2={nAI2}, nAItrg={nAItrg}, nAItrg={nAItrg}')

		recv_data = {
			'cnt_hist_id': str(uuid.uuid4()),
			'simid': szSimid,
			'event_datetime': nEventTime,
			'recv_datetime': szRecvDatetime,
			'dev_type': nDeviceType,
			'fw_version': nVer,
			'message_type': format(nMsgType, '04x'),
			'power_voltage': nVolt,
			'rssi': nRssi,
			'device_state': nState,
			'device_trigger': nStatetrg,
			'di_state': format(nDIState, '08b'),
			'di_trigger': nDItrg,
			'do_state': format(nDOState, '08b'),
			'do_trigger': nDOtrg,
			'analogv1': nAI1,
			'analogv2': nAI2,
			'ad_trigger': nAItrg,
			'sinr': nSinr
		}

	# 現状態通知
	elif nMsgType in [0x0011, 0x0012]:
		nDIState = int.from_bytes(getByteArray(Payload, index, 1), 'big')
		nDOState = int.from_bytes(getByteArray(Payload, index, 1), 'big')
		nAI1 = signed_hex2int(int.from_bytes(getByteArray(Payload, index, 2), 'big'), 16)
		nAI2 = signed_hex2int(int.from_bytes(getByteArray(Payload, index, 2), 'big'), 16)
		hist_flg = True
		logger.debug(f'現状態通知 nDIState={nDIState}, nDOState={nDOState}, nAI1={nAI1}, nAI2={nAI2}')

		recv_data = {
			'cnt_hist_id': str(uuid.uuid4()),
			'simid': szSimid,
			'event_datetime': nEventTime,
			'recv_datetime': szRecvDatetime,
			'dev_type': nDeviceType,
			'fw_version': nVer,
			'message_type': format(nMsgType, '04x'),
			'power_voltage': nVolt,
			'rssi': nRssi,
			'sinr': nSinr,
			'device_state': nState,
			'di_state': format(nDIState, '08b'),
			'do_state': format(nDOState, '08b'),
			'analogv1': nAI1,
			'analogv2': nAI2
		}

	# 接点出力制御応答
	elif nMsgType == 0x8002:
		nControlResult = int.from_bytes(getByteArray(Payload, index, 1), 'big')
		nDOState = int.from_bytes(getByteArray(Payload, index, 1), 'big')
		logger.debug(f'接点出力制御応答 cntrol_result={nControlResult}, nDOState={nDOState}')

		recv_data = {
			'device_req_no': szSimid + '-' + szReqNo,
			'event_datetime': nEventTime,
			'recv_datetime': szRecvDatetime,
			'device_type': nDeviceType,
			'fw_version': nVer,
			'message_type': format(nMsgType, '04x'),
			'power_voltage': nVolt,
			'rssi': nRssi,
			'sinr': nSinr,
			'cntrol_result': nControlResult,
			'device_state': nState,
			'do_state': format(nDOState, '08b'),
			'iccid': szSimid,
		}


	if not stray_flag:
		# 現状態取得
		device_id = device_info['device_id']
		device_current_state = ddb.get_device_state(device_id, state_table)
		logger.debug(f'device_current_state={device_current_state}')

		# イベント判定 履歴一覧、現状態作成
		hist_list, current_state_info = eventJudge(recv_data, device_current_state, device_info,
												device_relation_table, group_table, remote_control_table)
		logger.debug(f'hist_list={hist_list}, current_state_info={current_state_info}')

		# メール通知
		# hist_list = mailNotice(hist_list, device_info, user_table, account_table, notification_hist_table)
		logger.debug(f'hist_list2={hist_list}')

    # DB登録データ編集
    # 履歴情報テーブル
	if (hist_flg):
		logger.debug(f'履歴情報テーブル dbItem={recv_data}')
		ddb.put_cnt_hist(recv_data, hist_table)
	else:
		logger.debug(f'接点出力制御応答テーブル dbItem={recv_data}')
		ddb.update_control_res(recv_data, remote_control_table)

	if not stray_flag:
		# 履歴一覧テーブル
		logger.debug(f'履歴一覧 dbItem={hist_list}')
		ddb.put_cnt_hist_list(hist_list, hist_list_table)

		# 現状態テーブル
		if (hist_flg):
			logger.debug(f'現状態テーブル dbItem={current_state_info}')
			ddb.update_current_state(current_state_info, state_table)

	logger.debug('commandParser終了')
	return bytes([1])