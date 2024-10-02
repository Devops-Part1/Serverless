import boto3
import requests
import os
import uuid
from google.cloud import storage
from botocore.exceptions import BotoCoreError
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Initialize AWS DynamoDB client
dynamo_client = boto3.client('dynamodb')

# Helper function to send email
def send_email(api_key, domain, user_email, file_size, table_name, submission_url, error_message=None, success_path=None):
    subject = 'Submission Status'
    
    if error_message:
        text = f"Hey,\n\nWe encountered an issue while processing your submission:\n\n{error_message}\n\nBest regards,\nVidya"
        download_status = 'No Content'
    elif file_size == '0':
        text = f"Hey,\n\nYour submission was empty and not processed.\n\nBest regards,\nVidya"
        download_status = 'No Content'
    else:
        text = f"Hey,\n\nYour submission was {file_size} bytes and successfully uploaded to Google Cloud Storage.\n\nGCP file path: {success_path}\n\nThank you for your submission.\n\nBest regards,\nVidya"
        download_status = 'Success'

    success_path = success_path or 'N/A'
    email_data = {
        "from": f"noreply@{domain}",
        "to": user_email,
        "subject": subject,
        "text": text,
    }
    
    mailgun_url = f"https://api.mailgun.net/v3/{domain}/messages"
    try:
        response = requests.post(mailgun_url, auth=("api", api_key), data=email_data)
        response.raise_for_status()
        print("Email sent successfully")
    except requests.RequestException as e:
        print(f"Error sending email: {e}")

    # Log submission details to DynamoDB
    dynamo_item = {
        'id': {'S': str(uuid.uuid4())},
        'userEmail': {'S': user_email},
        'submissionUrl': {'S': submission_url},
        'downloadStatus': {'S': download_status},
        'emailSent': {'S': 'Yes' if not error_message else 'No'},
        'successPath': {'S': success_path},
        'Timestamp': {'N': str(int(datetime.now().timestamp()))},
    }
    try:
        dynamo_client.put_item(TableName=table_name, Item=dynamo_item)
    except BotoCoreError as e:
        print(f"Error logging to DynamoDB: {e}")

# Main Lambda handler
def lambda_handler(event, context):
    try:
        sns_message = event['Records'][0]['Sns']['Message']
        submission_data = json.loads(sns_message)
        submission_url = submission_data['submission_url']
        user_email = submission_data['email']
        table_name = os.getenv('DYNAMODB_TABLE_NAME')
        
        google_private_key = os.getenv('GOOGLE_PRIVATE_KEY')
        bucket_name = os.getenv('BUCKET_NAME')
        mailgun_api_key = os.getenv('MAILGUN_API_KEY')
        mailgun_domain = os.getenv('MAILGUN_DOMAIN')

        # Validate file type
        if not submission_url.lower().endswith('.zip'):
            send_email(mailgun_api_key, mailgun_domain, user_email, None, table_name, submission_url, "Invalid file format. Only ZIP files are supported.")
            return

        # Download the file
        response = requests.get(submission_url, stream=True)
        if response.status_code != 200:
            send_email(mailgun_api_key, mailgun_domain, user_email, None, table_name, submission_url, "The URL does not exist.")
            return

        zip_file_path = f"/tmp/{uuid.uuid4()}.zip"
        with open(zip_file_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)

        # Check file size
        file_size = os.path.getsize(zip_file_path)
        if file_size == 0:
            send_email(mailgun_api_key, mailgun_domain, user_email, '0', table_name, submission_url)
            return

        # Upload to Google Cloud Storage
        client = storage.Client.from_service_account_json(google_private_key)
        bucket = client.bucket(bucket_name)
        blob_name = f"submissions/{user_email}_{int(datetime.now().timestamp())}_submission.zip"
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(zip_file_path)
        success_path = f"gs://{bucket_name}/{blob_name}"

        # Send success email
        send_email(mailgun_api_key, mailgun_domain, user_email, str(file_size), table_name, submission_url, success_path=success_path)
        print(f"Successfully processed submission from {user_email}")

    except Exception as e:
        print(f"Error: {e}")
        raise
