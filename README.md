# Genesis setup notebook for Databricks

`genesis_lifecycle.py` prepares a Databricks workspace for the Genesis App from the Databricks
Marketplace, checks it after installing, and removes it.

This is the notebook of the Genesis **1.20.2** release candidate (built from genesis-agents
`f7765a271b`).
`main` holds the newest release published here.

## If your workspace restricts serverless egress

Installing builds the Genesis App in your workspace, and the build downloads Python 3.12 and
Genesis's Python packages. If a serverless network policy restricts egress in your workspace,
an account admin adds these domains to it under Allowed domains before you install, and
waits about ten minutes for the change to take effect: `github.com`,
`release-assets.githubusercontent.com`, `objects.githubusercontent.com`, `pypi.org` and
`files.pythonhosted.org`. Without them the install's build fails, and the App's build log
names the domain it could not reach. A connection you add later to a public host outside
Databricks needs that host allowed too; when Test Connection finds the connection blocked, it
names the hosts to allow. A connection routed through a Genesis Outpost needs no allowed
domain, and a private or on-premises host cannot be reached through Allowed domains at all:
it needs a private connection from the workspace or an Outpost.

## Install Genesis in three steps

1. **Prepare.** In your workspace: Workspace -> Import -> URL, and paste

   https://raw.githubusercontent.com/genesis-computing-ai/genesis-databricks-setup/main/genesis_lifecycle.py

   Run it with `action` set to `preinstall`. It creates the Lakebase database, a Unity Catalog
   schema and volume and a secret scope, then prints what to enter in each install field.
2. **Install** Genesis from its Databricks Marketplace listing, entering the values the
   notebook printed.
3. **Finish.** Run the notebook again with `action` set to `postinstall`.

The full guide, including granting Genesis your existing data and uninstalling:
https://docs.genesiscomputing.com/docs/deployments/Databricks/Genesis-On-Databricks#marketplace-install-and-removal
