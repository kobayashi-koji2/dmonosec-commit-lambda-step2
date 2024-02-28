def update_user_display_information(user_id, display_information, user_table):
    result = user_table.update_item(
        Key={"user_id": user_id},
        UpdateExpression="set user_data.display_information = :display_information",
        ExpressionAttributeValues={":display_information": display_information},
        ReturnValues="UPDATED_NEW"
    )
    return result["Attributes"]["user_data"]["display_information"]
