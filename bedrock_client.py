"""
AWS Bedrock client wrapper.
Uses boto3 with IAM role credentials (no API key required).
Model: anthropic.claude-3-haiku-20240307-v1:0 in eu-west-1.
"""
import json
import logging

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

log = logging.getLogger(__name__)

MODEL_ID = "anthropic.claude-3-haiku-20240307-v1:0"
REGION   = "eu-west-1"

_client = None


def get_client():
    global _client
    if _client is None:
        _client = boto3.client("bedrock-runtime", region_name=REGION)
    return _client


def invoke(system: str, user: str, max_tokens: int = 300) -> str:
    """
    Call Claude 3 Haiku on Bedrock and return the text response.
    Falls back to None on any error so callers can use template fallback.
    """
    body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens":        max_tokens,
        "system":            system,
        "messages": [{"role": "user", "content": user}],
    })
    try:
        response = get_client().invoke_model(
            modelId=MODEL_ID,
            body=body,
            contentType="application/json",
            accept="application/json",
        )
        result = json.loads(response["body"].read())
        return result["content"][0]["text"]
    except NoCredentialsError:
        log.warning("Bedrock: no AWS credentials — attach an IAM role to the EC2 instance")
    except ClientError as e:
        log.warning("Bedrock ClientError: %s", e)
    except Exception as e:
        log.warning("Bedrock error: %s", e)
    return None
