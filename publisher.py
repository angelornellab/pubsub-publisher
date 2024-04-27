import requests
import base64
import json
import io
import json
import os
import time

from google.auth import crypt
from google.auth import jwt

# Publish messages to the cloud pub/sub REST endpoints

# Service account key path

credential_path = "payload/service_account.json"

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

sa_keyfile = credential_path

# Get service account email and load the json data from the service account key file.

with io.open(credential_path, "r", encoding="utf-8") as json_file:
    data = json.loads(json_file.read())
    sa_email = data['client_email']

# Audience

audience = "https://pubsub.googleapis.com/google.pubsub.v1.Publisher"

# Topic - rest end point

url = "https://pubsub.googleapis.com/v1/projects/linio-staging/topics/account-gateway.settlement.payment-received-topic:publish"

# Message data

with open('payload/message.json', 'r') as file:
    json_content = file.read()

message = json.loads(json_content)

# print(message)

# Generate the Json Web Token

def generate_jwt(sa_keyfile,
                 sa_email,
                 audience,
                 expiry_length=3600):
    """Generates a signed JSON Web Token using a Google API Service Account."""

    now = int(time.time())

    # Build payload

    payload = {
        'iat': now,
        # Expires after 'expiry_length' seconds.
        "exp": now + expiry_length,
        # iss must match 'issuer' in the security configuration in your
        # Swagger spec (e.g. service account email). It can be any string.
        'iss': sa_email,
        # Aud must be either your Endpoints service name, or match the value
        # specified as the 'x-google-audience' in the OpenAPI document.
        'aud': audience,
        # Sub and email should match the service account's email address
        'sub': sa_email,
        'email': sa_email
    }

    # Sign with keyfile

    signer = crypt.RSASigner.from_service_account_file(sa_keyfile)

    jwt_token = jwt.encode(signer, payload)

    # print(jwt_token.decode('utf-8'))

    return jwt_token

# Publish the message

def publish_with_jwt_request(signed_jwt, encoded_element, url):
    # Request Headers

    headers = {
        'Authorization': 'Bearer {}'.format(signed_jwt.decode('utf-8')),
        'content-type': 'application/json'
    }

    # Request json data

    json_data = {
        "messages": [
            {
                "data": encoded_element,
                "ordering_key": "first order",
                "attributes": {
                    "status": "200",
                    "code": "0",
                    "country": "cl",
                }
            }
        ]
    }

    # Get response data

    response = requests.post(url, headers=headers, json=json_data)

    print(response.status_code, response.content)

    response.raise_for_status()

signed_jwt = generate_jwt(sa_keyfile, sa_email, audience)

# Encode the data and publish it

for element in message:
    dumped_element = json.dumps(element)
    message_bytes = dumped_element.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    encoded_element = base64_bytes.decode('ascii')
    print('---debug---')
    print(encoded_element)
    publish_with_jwt_request(signed_jwt, encoded_element, url)