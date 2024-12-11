import os
import boto3
import textwrap
from aws_lambda_powertools import Logger

logger = Logger()

region_name = os.environ.get("AWS_REGION")
from_address = os.environ.get("MAIL_FROM_ADDRESS")
endpoint_url = os.environ.get("endpoint_url")
monosc_web_url = os.environ.get("MONOSC_WEB_URL")

def send_email(to_address_list, subject, body):
    client = boto3.client("ses", region_name=region_name, endpoint_url=endpoint_url)

    text_body = textwrap.dedent(f"""
        モノセコムをご利用いただきありがとうございます。
        下記の通り、検知したイベントをお知らせします。

        {body}

        ■モノセコムWEBログインページはこちら

        ――――――――――――――――――――
        ・本メールはモノセコムWEBにご登録いただいたメールアドレス宛にお送りしております。
        ・本メールの送信アドレスは送信専用となっております。返信によるお問い合わせは承っておりませんので、あらかじめご了承ください。
        ・本メールの配信停止はモノセコムWEBのメール通知設定にてご変更ください。
    """).strip()

    # HTML形式のメール本文を作成
    html_body = textwrap.dedent(f"""
        <html>
        <body>
            <p>
                モノセコムをご利用いただきありがとうございます。<br>
                下記の通り、検知したイベントをお知らせします。<br>
                <br>
                {body.replace('\n', '<br>')}<br>
                <br>
                ■モノセコムWEBログインページは<a href="{monosc_web_url}">こちら</a><br>
                <br>
                ――――――――――――――――――――<br>
                ・本メールはモノセコムWEBにご登録いただいたメールアドレス宛にお送りしております。<br>
                ・本メールの送信アドレスは送信専用となっております。返信によるお問い合わせは承っておりませんので、あらかじめご了承ください。<br>
                ・本メールの配信停止はモノセコムWEBのメール通知設定にてご変更ください。<br>
            </p>
        </body>
        </html>
    """).strip()

    for to_address in to_address_list:
        client.send_email(
            Source=from_address,
            Destination={"ToAddresses": [to_address]},
            Message={
                "Subject": {
                    "Data": subject,
                },
                "Body": {
                    "Text": {
                        "Data": text_body,
                    },
                    "Html": {
                        "Data": html_body,
                    },
                },
            },
        )