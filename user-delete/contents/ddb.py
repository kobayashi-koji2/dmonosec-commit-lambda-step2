def update_user_data(user_id, user_data, user_table):
    user_table.update_item(
        Key={"user_id": user_id},
        UpdateExpression="set #s = :s",
        ExpressionAttributeNames={"#s": "user_data"},
        ExpressionAttributeValues={":s": user_data},
    )
