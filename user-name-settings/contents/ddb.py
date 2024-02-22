def update_account_user_name(account_id, user_name, account_table):
    result = account_table.update_item(
        Key={"account_id": account_id},
        UpdateExpression="set user_data.config.user_name = :user_name",
        ExpressionAttributeValues={":user_name": user_name},
        ReturnValues="UPDATED_NEW"
    )
    return result["Attributes"]["user_data"]["config"]["user_name"]
