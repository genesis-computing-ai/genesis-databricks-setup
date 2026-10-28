# Genesis setup notebook for Databricks

`genesis_lifecycle.py` prepares a Databricks workspace for the Genesis App from the Databricks
Marketplace, checks it after installing, and removes it.

This is the notebook of Genesis **1.19.100** (built from genesis-agents `d0e5cfd7dfde076e5c2f1b33f1a1a214505647f6`).
`main` holds the newest release published here.

## Install Genesis in three steps

1. **Prepare.** In your workspace: Workspace -> Import -> URL, and paste

   https://raw.githubusercontent.com/genesis-computing-ai/genesis-databricks-setup/main/genesis_lifecycle.py

   (or `https://raw.githubusercontent.com/genesis-computing-ai/genesis-databricks-setup/v1.19.100/genesis_lifecycle.py` for exactly this release). Run it with `action`
   set to `preinstall`. It creates the Lakebase database, a Unity Catalog schema and volume
   and a secret scope, then prints what to enter in each install field.
2. **Install** Genesis from its Databricks Marketplace listing, entering the values the
   notebook printed.
3. **Finish.** Run the notebook again with `action` set to `postinstall`.

The full guide, including granting Genesis your existing data and uninstalling:
https://docs.genesiscomputing.com/docs/deployments/Databricks/Genesis-On-Databricks#marketplace-install-and-removal
