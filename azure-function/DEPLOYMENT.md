# Cygenix — Azure Function Deployment Guide

## What you're deploying
A Node.js Azure Function App called `cygenix-db-api` that:
- Connects to your Azure SQL database using Managed Identity (no passwords)
- Exposes an HTTPS endpoint your Netlify dashboard calls
- Never stores credentials anywhere

---

## Step 1 — Create the Function App in Azure Portal

1. Go to **portal.azure.com** → **Create a resource** → search **Function App**
2. Click **Create** and fill in:

| Field | Value |
|---|---|
| Subscription | Your subscription |
| Resource Group | Create new: `cygenix-rg` |
| Function App name | `cygenix-db-api` (must be globally unique) |
| Runtime stack | **Node.js** |
| Version | **20 LTS** |
| Region | Same region as your Azure SQL database |
| OS | **Windows** or Linux (either works) |
| Plan type | **Consumption (Serverless)** — free tier |

3. Click **Review + create** → **Create**
4. Wait ~2 minutes for deployment

---

## Step 2 — Enable System-Assigned Managed Identity

1. Go to your new Function App → **Settings** → **Identity**
2. Under **System assigned** tab, set Status to **On**
3. Click **Save** → **Yes** to confirm
4. Note the **Object (principal) ID** — you'll need it shortly

---

## Step 3 — Set Environment Variables

1. Function App → **Settings** → **Environment variables**
2. Click **+ Add** for each of these:

| Name | Value |
|---|---|
| `SQL_SERVER` | `cygenix.database.windows.net` |
| `SQL_DATABASE` | Your database name (e.g. `CygenixDB`) |
| `USE_MANAGED_IDENTITY` | `true` |
| `ALLOWED_ORIGINS` | `https://cygenix.netlify.app` |

3. Click **Apply** → **Confirm**

---

## Step 4 — Grant the Managed Identity access to Azure SQL

Connect to your Azure SQL database in SSMS or the Azure Portal Query Editor and run:

```sql
-- Create a database user for the Managed Identity
-- The name must match your Function App name exactly
CREATE USER [cygenix-db-api] FROM EXTERNAL PROVIDER;
GO

-- Grant read and write access
ALTER ROLE db_datareader ADD MEMBER [cygenix-db-api];
ALTER ROLE db_datawriter ADD MEMBER [cygenix-db-api];
GO

-- Optional: allow schema reading (needed for introspection)
GRANT VIEW DEFINITION TO [cygenix-db-api];
GO

-- Verify it worked
SELECT name, type_desc, create_date
FROM sys.database_principals
WHERE name = 'cygenix-db-api';
```

> **Note**: The Azure SQL server must have **Azure Active Directory authentication enabled**.
> Portal → SQL Server (not database) → Settings → Azure Active Directory → Set admin

---

## Step 5 — Deploy the Function code

### Option A: Deploy via ZIP (easiest — no CLI needed)

1. Open this `cygenix-azure-fn` folder
2. Select all files (package.json, host.json, src/ folder) and ZIP them
3. In Azure Portal → Function App → **Deployment Center**
4. Select **External Git** or use **ZIP Deploy**:
   - Go to `https://cygenix-db-api.scm.azurewebsites.net/ZipDeployUI`
   - Drag and drop your ZIP file
5. Wait for deployment to complete (~1 minute)

### Option B: Deploy via Azure Functions Core Tools (CLI)

```bash
# Install Azure Functions Core Tools
npm install -g azure-functions-core-tools@4

# In the cygenix-azure-fn folder:
npm install
func azure functionapp publish cygenix-db-api
```

### Option C: Deploy via VS Code
1. Install the **Azure Functions** VS Code extension
2. Open the `cygenix-azure-fn` folder
3. Click the Azure icon → Functions → Deploy to Function App
4. Select `cygenix-db-api`

---

## Step 6 — Get your Function URL and Key

1. Function App → **Functions** → click **db**
2. Click **Get function URL**
3. Select **default (Function key)** from the dropdown
4. Copy the full URL — it looks like:
   ```
   https://cygenix-db-api.azurewebsites.net/api/db?code=AbCdEfGh...
   ```
5. Split this into two parts for the dashboard:
   - **URL**: `https://cygenix-db-api.azurewebsites.net/api/db`
   - **Key**: everything after `?code=`

---

## Step 7 — Configure CORS on the Function App

1. Function App → **API** → **CORS**
2. Remove the `*` wildcard if present
3. Add: `https://cygenix.netlify.app`
4. Add: `http://localhost:3000` (for local testing)
5. Check **Enable Access-Control-Allow-Credentials**
6. Click **Save**

---

## Step 8 — Test in the Cygenix dashboard

1. Go to `https://cygenix.netlify.app/connect.html`
2. Paste the Function URL (without ?code=)
3. Paste the function key separately
4. Click **Test connection**
5. You should see: `Connected via Managed Identity`

---

## Troubleshooting

**"Login failed for user '<token-identified principal>'"**
→ You haven't run the `CREATE USER` SQL in Step 4, or the Function App name in the SQL doesn't match exactly.

**"Cannot reach the SQL server"**
→ Check that your Azure SQL server has **Allow Azure services** enabled:
  Portal → SQL Server → Networking → Allow Azure services and resources to access this server → ✓

**"401 Unauthorized" from Function**
→ The function key is wrong. Go back to Step 6 and copy it fresh.

**"Managed Identity is not enabled"**
→ Check Step 2 — System assigned identity must be On.

**Function shows "Error: DefaultAzureCredential"**
→ The Managed Identity isn't enabled (Step 2), or the environment variable `USE_MANAGED_IDENTITY=true` isn't set (Step 3).

---

## Security notes

- The Function key keeps the endpoint private — only Cygenix can call it
- The Managed Identity has `db_datareader` + `db_datawriter` only — it cannot drop tables or databases
- No credentials are stored anywhere — the Managed Identity token is fetched fresh and cached in memory
- CORS is restricted to your Netlify domain

---

## Local development

To test locally before deploying:

```bash
# Install deps
cd cygenix-azure-fn
npm install

# Login to Azure CLI (uses your personal credentials locally)
az login

# Start the function locally
func start

# It will run at: http://localhost:7071/api/db
# No function key needed locally
```

Update `connect.html` temporarily to use `http://localhost:7071/api/db` for local testing.
