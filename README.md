# Agent Agri AI – Setup and Run Guide (Windows/PowerShell)

This guide gets the project running locally and prepares AWS resources so the system is functional.

## Prerequisites
- Python 3.12+ on Windows
- AWS account with credentials configured (Administrator or a role/user with at least: DynamoDB, Secrets Manager, CloudWatch Logs, EventBridge, Lambda, Bedrock, and SES send permissions)
- OpenWeather API key (free or paid)

## 1) Create and activate a virtual environment
```powershell
# from root directory of the project
python -m venv aws_agent_venv
.\aws_agent_venv\Scripts\activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## 2) Configure AWS credentials and default region
- If you haven’t already, run the AWS CLI configuration and pick your default region (most code assumes us-east-1 for Bedrock Agent):
```powershell
aws configure
```
- Or set a profile/region via environment variables as you prefer.

## 3) Create Secrets Manager entry for OpenWeather
- Create a secret named `openweather-api-key` with a JSON payload that includes the key `openweather-api-key`.
- Example with AWS CLI (replace YOUR_KEY):
```powershell
aws secretsmanager create-secret `
  --name openweather-api-key `
  --secret-string '{"openweather-api-key":"YOUR_KEY"}'
```

## 4) Create DynamoDB tables and seed data
Run the seeding script; it will create the tables if they don’t exist and populate example data.
```powershell
.\aws_agent_venv\Scripts\activate
python -m src.scripts.populate_dynamodb
```
Tables created/populated:
- FarmRegistry
- PestDiseaseKB
- AlertHistory


## 5) Bedrock Agent and action groups
The Bedrock Agent should be configured with action groups that match the OpenAPI specs under:
- `src/bedrock_agent/action_groups/`
  - `weather_actions.json`
  - `satellite_actions.json`
  - `alert_actions.json`
  - `pest_kb_actions.json`

Ensure the Agent can invoke the corresponding Lambda ARNs.

## 6) Package and deploy Lambdas (example)
- Create lambda functions for satellite fetcher, weather fetcher, pest data fetcher, alert sender, and daily orchestrator.
- set timeout and memory as needed (e.g., 300 seconds, 3008 MB for orchestrator).
- In layers for each Lambda, include `AWSSDKPandas-Python312` for Pandas support.
- In Configuration-Permission for each lambda(except daily orchestrator), ensure the Bedrock Agent can invoke them.

## 7) Configure Lambda environments
- For the Daily Orchestrator (`src/lambdas/daily_orchestrator/`): set environment variables in the Lambda configuration:
  - `AGENT_ID` = your Bedrock Agent ID
  - `AGENT_ALIAS_ID` = your Bedrock Agent alias ID
  - set timeout and memory as needed (e.g., 15 minutes, 3008 MB for orchestrator).
- For `alert_sender`: ensure the SES identity `alerts@agri-ai.com` (or the address you configure in code) is verified in the same region; otherwise, change the `Source` email in the code or verify an identity.

## 8) Schedule the daily run
- Create an EventBridge (CloudWatch) rule to trigger the Daily Orchestrator Lambda at 06:00 daily (UTC or your timezone as needed). The code is designed for a schedule like `cron(0 6 * * ? *)`.

## 9) Troubleshooting quick notes
- Region mismatches: The orchestrator explicitly uses Bedrock in `us-east-1`. Ensure your AWS credentials and Agent are in the same region.
- Secrets Manager: The secret must be named `openweather-api-key` with JSON key `openweather-api-key`.
- DynamoDB numeric types: Code uses `Decimal`; this is normal—no action required when seeding.
- SES sandbox: If your SES account is in sandbox, you must verify both sender and recipient emails.