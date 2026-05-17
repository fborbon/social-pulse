"""
AWS Bedrock client wrapper.
Uses boto3 with IAM role credentials (no API key required).
Model: amazon.nova-micro-v1:0 via eu. cross-region inference profile.
No use-case form required — available immediately in any AWS account.
"""
import json
import logging

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

log = logging.getLogger(__name__)

# Amazon Nova Micro: text-only, ~28x cheaper than Claude Haiku, no use-case form needed
# eu. prefix = cross-region inference profile (routes within EU region group)
MODEL_ID = "eu.amazon.nova-micro-v1:0"
REGION   = "eu-west-1"

_client = None


def get_client():
    global _client
    if _client is None:
        _client = boto3.client("bedrock-runtime", region_name=REGION)
    return _client


def invoke(system: str, user: str, max_tokens: int = 300) -> str:
    """
    Call Amazon Nova Micro on Bedrock and return the text response.
    Falls back to None on any error so callers can use template fallback.
    """
    body = json.dumps({
        "messages": [{"role": "user", "content": [{"text": user}]}],
        "system":   [{"text": system}],
        "inferenceConfig": {"maxTokens": max_tokens},
    })
    try:
        response = get_client().invoke_model(
            modelId=MODEL_ID,
            body=body,
            contentType="application/json",
            accept="application/json",
        )
        result = json.loads(response["body"].read())
        return result["output"]["message"]["content"][0]["text"]
    except NoCredentialsError:
        log.warning("Bedrock: no AWS credentials — attach an IAM role to the EC2 instance")
    except ClientError as e:
        log.warning("Bedrock ClientError: %s", e)
    except Exception as e:
        log.warning("Bedrock error: %s", e)
    return None
