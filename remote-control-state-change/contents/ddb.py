from aws_lambda_powertools import Logger
from boto3.dynamodb.conditions import Attr, Key

logger = Logger()


def get_cnt_hist_list_by_sim_id(
    sim_id, cnt_hist_table, recv_datetime_low=None, recv_datetime_high=None
):
    query_params = {
        "IndexName": "simid_index",
        "KeyConditionExpression": Key("simid").eq(sim_id),
    }
    if recv_datetime_low and recv_datetime_high:
        query_params["FilterExpression"] = Attr("recv_datetime").between(
            recv_datetime_low, recv_datetime_high
        )
    elif recv_datetime_low:
        query_params["FilterExpression"] = Attr("recv_datetime").gte(recv_datetime_low)
    elif recv_datetime_high:
        query_params["FilterExpression"] = Attr("recv_datetime").lte(recv_datetime_high)

    cnt_hist_list = cnt_hist_table.query(**query_params)
    return cnt_hist_list["Items"]
