def update_account_auth_period(account_id, auth_period, account_table):
    account_table.update_item(
        Key={"account_id": account_id},
        UpdateExpression="set user_data.config.auth_period = :auth_period",
        ExpressionAttributeValues={":auth_period": auth_period},
    )
