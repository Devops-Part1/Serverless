# Serverless

# Serverless Submission Processor (Python)

This project implements a serverless architecture to handle and process user submissions using AWS Lambda. The functionality includes downloading a file from a provided URL, validating the file format, uploading it to Google Cloud Storage (GCS), sending a status email to the user, and logging the results in DynamoDB.

---

## How It Works

1. **Triggered by SNS**:
   - The Lambda function is invoked through an AWS SNS notification, which contains submission details (URL, user email).

2. **Download and Validation**:
   - The function checks if the provided URL points to a `.zip` file. If not, an error email is sent, and the process terminates.
   - If the file is valid, it is downloaded and stored temporarily in the `/tmp` directory.

3. **Upload to Google Cloud Storage**:
   - The downloaded file is uploaded to a GCS bucket using the Google Cloud Storage client.

4. **Send Email Notification**:
   - Depending on the outcome (success or error), the function sends an email to the user using the Mailgun API.
   - The email includes details like file size and status.

5. **Log to DynamoDB**:
   - The function logs submission details (user email, URL, download status, email status, etc.) to a DynamoDB table.

---

## Environment Variables

The following environment variables need to be configured for the Lambda function:

- **`GOOGLE_PRIVATE_KEY`**: Path to the Google Cloud private key JSON file.
- **`MAILGUN_API_KEY`**: API key for Mailgun.
- **`MAILGUN_DOMAIN`**: Domain for sending emails via Mailgun.
- **`BUCKET_NAME`**: Name of the GCS bucket.
- **`DYNAMODB_TABLE_NAME`**: Name of the DynamoDB table for logging.

---

## Installation and Deployment

1. **Set Up AWS Lambda**:
   - Create a Lambda function and configure it to trigger via SNS.

2. **Dependencies**:
   - Install the required Python packages using `pip`:
     ```bash
     pip install -r requirements.txt
     ```

3. **Environment Variables**:
   - Add the required environment variables to the Lambda function.

4. **Permissions**:
   - Grant necessary permissions to:
     - Read from SNS.
     - Write to DynamoDB.
     - Access GCS buckets.

5. **Deploy**:
   - Package the function and dependencies and deploy it to AWS Lambda.

---

## File Structure

```plaintext
.
├── lambda_function.py    # Main Lambda function
├── requirements.txt      # Python dependencies
├── README.md             # Documentation for the project
```

---

## Error Handling

- If the submission URL is invalid or the file is empty, an error email is sent to the user.
- Errors are logged to DynamoDB for tracking purposes.

---

## Example Email Notifications

1. **Success**:
   ```
   Subject: Submission Status
   Hey,

   Your submission was 1024 bytes and successfully uploaded to Google Cloud Storage.

   GCP file path: gs://<BUCKET_NAME>/submissions/<FILE_NAME>

   Thank you for your submission.

   Best regards,  
   Vidya
   ```

2. **Error**:
   ```
   Subject: Submission Status
   Hey,

   We encountered an issue while processing your submission:

   Invalid file format. Only ZIP files are supported.

   Best regards,  
   Vidya
   ```

---

## Technologies Used

- **AWS Lambda**: For serverless function execution.
- **SNS**: To trigger the Lambda function.
- **Google Cloud Storage**: To store uploaded files.
- **Mailgun API**: To send email notifications.
- **DynamoDB**: To log and track submissions.
- **Python**: Runtime for the Lambda function.
