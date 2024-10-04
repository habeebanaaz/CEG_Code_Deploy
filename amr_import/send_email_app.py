
import msal
import os
from dotenv import load_dotenv
import requests;
load_dotenv()

# app_dict = {'client_id': os.getenv('DV_CLIENT_ID'), 'client_secret': os.getenv('DV_CLIENT_SECRET'), 'tenant_id': os.getenv('DV_TENANT_ID')}
app_dict = {'client_id': os.getenv('APP_CLIENT_ID'), 'client_secret': os.getenv('APP_SECRET_VALUE'), 'tenant_id': os.getenv('APP_TENANT_ID')}

def acquire_token():
    try:
        authority_url = f'https://login.microsoftonline.com/{app_dict["tenant_id"]}'
        app = msal.ConfidentialClientApplication(
            authority=authority_url,
            client_id=app_dict["client_id"],
            client_credential=app_dict["client_secret"]
        )
        token = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
        return token
    except Exception as e:
        error_message=f"Issue in obtaining access token.\n{e}"
        raise Exception(error_message)


def send_email_notification(subject, body, logger):
    try:
        result = acquire_token()
        if "access_token" in result:
            # print("Access token created.",result["access_token"])
            sender_email = os.getenv('SENDER_EMAIL')
            recipients = os.getenv('RECIPIENTS').split(',')
            to_recipients = [{'EmailAddress': {'Address': email.strip()}} for email in recipients]

            endpoint = f'https://graph.microsoft.com/v1.0/users/{sender_email}/sendMail'
            email_msg = {'Message': {'Subject': subject,
                                    'Body': {'ContentType': 'Text', 'Content': body},
                                    'ToRecipients': to_recipients
                                    },
                        'SaveToSentItems': 'true'}
            try:
                r = requests.post(endpoint,headers={'Authorization': 'Bearer ' + result['access_token']},json=email_msg)
                if r.ok:
                    logger.info('Sent email successfully')
                else:
                    logger.error(f"Unable to send email.\n{r.json()}")
            except Exception as e:
                    error_message=f"Issue in API request for sending email.\n{e}"
                    logger.error(error_message)
                    raise Exception(error_message)
    except Exception as e:
        error_message=f"Issue in sending email.\n{e}"
        logger.error(error_message)
