# tap-microsoft-graph

Singer tap for [Microsoft Graph](https://learn.microsoft.com/graph) mail endpoints. Pulls every message and attachment metadata from a shared Outlook mailbox into a Singer-compatible target — built for the Vertex Service Partners **Marketing Spend Agent** email-invoice ingest pipeline.

Two streams:

| Stream        | Endpoint                                                      | Replication |
| ------------- | ------------------------------------------------------------- | ----------- |
| `messages`    | `GET /users/{mailbox}/messages`                               | Incremental on `receivedDateTime` |
| `attachments` | `GET /users/{mailbox}/messages/{message_id}/attachments`      | Full table per parent message (child stream) |

The `attachments` stream emits **metadata only**. Attachment bytes are large (tens of MB) and don't belong in the Singer protocol's JSON Lines envelope. Instead, when `stage_upload_enabled=true`, the tap streams the bytes directly into a Snowflake internal stage via `snowflake-connector-python` and records the resulting `stage_path` on the metadata row. Downstream `SNOWFLAKE.CORTEX.PARSE_DOCUMENT(@stage, path)` reads from that stage.

## Configuration

| Setting               | Required | Default                              | Description |
| --------------------- | -------- | ------------------------------------ | ----------- |
| `client_id`           | yes      | —                                    | Azure AD app registration client ID. |
| `client_secret`       | yes      | —                                    | Azure AD app registration client secret. |
| `tenant_id`           | yes      | —                                    | Azure AD tenant GUID. |
| `shared_mailbox_id`   | yes      | —                                    | UPN or object id of the shared mailbox (e.g. `marketingagent@vertexservicepartners.com`). |
| `api_url`             | no       | `https://graph.microsoft.com/v1.0`   | Graph API root. Use `/beta` only for beta-only fields. |
| `start_date`          | no       | `2024-01-01T00:00:00Z`               | Earliest `receivedDateTime` to sync on the first run. |
| `stage_upload_enabled`| no       | `false`                              | Enable the Snowflake-stage PUT side-channel for attachment bytes. Requires the stage to exist. |

See [.env.example](.env.example) for the full list.

## Auth

OAuth2 client_credentials flow against Azure AD:

```
POST https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token
  client_id={client_id}
  client_secret={client_secret}
  scope=https://graph.microsoft.com/.default
  grant_type=client_credentials
```

The app registration must have **application permission** `Mail.Read` granted admin consent, and (strongly recommended) be scoped to the single shared mailbox via Exchange Online's [`ApplicationAccessPolicy`](https://learn.microsoft.com/graph/auth-limit-mailbox-access):

```powershell
# Run as Exchange administrator in PowerShell:
New-ApplicationAccessPolicy `
  -AppId <client_id> `
  -PolicyScopeGroupId marketingagent@vertexservicepartners.com `
  -AccessRight RestrictAccess `
  -Description "Limit tap-microsoft-graph to the marketing-invoices mailbox only"
```

Without the policy, `Mail.Read` grants the app read access to **every mailbox in the tenant** — a least-privilege violation.

## Local development

```bash
cd ~/code/tap-microsoft-graph
uv venv && source .venv/bin/activate
uv pip install -e .

# Populate .env from .env.example with real values, then:
uvx meltano install
uvx meltano invoke tap-microsoft-graph --about

# Smoke test with bytes going to a JSONL file:
TAP_MICROSOFT_GRAPH_CLIENT_ID=... \
TAP_MICROSOFT_GRAPH_CLIENT_SECRET=... \
TAP_MICROSOFT_GRAPH_TENANT_ID=... \
TAP_MICROSOFT_GRAPH_SHARED_MAILBOX_ID=marketingagent@vertexservicepartners.com \
  uvx meltano run tap-microsoft-graph target-jsonl
```

## Usage in dagster-vertex

Pinned via git+https in `dagster-vertex/requirements/requirements.in`:

```
tap-microsoft-graph @ git+https://github.com/Vertex-Service-Partners/tap-microsoft-graph.git@v0.0.1
```

Wired through `dagster_meltano_pipelines.Extractor` in `dagster_vertex/common/marketing_invoice_inbox.py`. See that file for the per-stream selection and Dagster job/schedule definitions.

## Why the cookiecutter slug differs

This tap was bootstrapped from the singer-sdk cookiecutter, which slugified `MicrosoftGraph` → `tap-microsoftgraph` (no separator). The package, module, and CLI were renamed to the more conventional `tap-microsoft-graph` / `tap_microsoft_graph` to match `tap-service-titan` and the broader Singer ecosystem.

## License

Apache 2.0
