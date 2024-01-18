def update_account_email(account_id, email, account_table):
    account_table.update_item(
        Key={"account_id": account_id},
        UpdateExpression="set email_address = :e",
        ExpressionAttributeValues={":e": email},
    )
