# Genesis setup notebook for Databricks

`genesis_lifecycle.py` prepares a Databricks workspace for the Genesis App from the Databricks
Marketplace, checks it after installing, and removes it.

This is the notebook of Genesis **2.0.1** (built from genesis-agents `00443ef3334d98219f91d6b7a5c81f42e6530b52`).
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

Importing this notebook by URL (step 1 below) fetches it from `raw.githubusercontent.com`.
Allow that domain too, or download the notebook and import it as a file.

## Install Genesis in three steps

1. **Prepare.** Bring the notebook into your workspace in one of these ways:

   - **Git folder (recommended).** Workspace -> Create -> Git folder, with
     `https://github.com/genesis-computing-ai/genesis-databricks-setup` as the repository URL, then open `genesis_lifecycle` in the
     new folder. Nothing is uploaded. Pull only when upgrading Genesis, then run `postinstall`
     again; the restart job runs this notebook as it is in the folder.
   - **Import from a URL.** Open Workspace -> Users -> your email, then use that folder's ⋮
     menu -> Import -> URL, and paste

     https://raw.githubusercontent.com/genesis-computing-ai/genesis-databricks-setup/main/genesis_lifecycle.py

     (or `https://raw.githubusercontent.com/genesis-computing-ai/genesis-databricks-setup/v2.0.1/genesis_lifecycle.py` for exactly this release). If Import -> URL fails (a
     workspace with restricted egress), download `genesis_lifecycle.py` from that address and use
     Import -> File from the same folder menu instead.
   - **Databricks CLI.** From a copy of this repository, with your own email in the path:

     ```
     databricks workspace import /Users/<your email>/genesis_lifecycle --file genesis_lifecycle.py --format SOURCE --language PYTHON
     ```

   Run it with `action` set to `preinstall`. It creates the Lakebase database, a Unity
   Catalog schema and volume and a secret scope, then prints what to enter in each install
   field.
2. **Install** Genesis from its Databricks Marketplace listing, entering the values the
   notebook printed.
3. **Finish.** Run the notebook again with `action` set to `postinstall`.

## Troubleshooting

**`File upload failed with error: requirement failed: undefined/genesis_lifecycle is not absolute`.**
The Import dialog was opened without a target folder (from the workspace home page, Recents,
or the top-level New -> Import), so Databricks had no folder to put the notebook in. The file
is fine. Open a folder first, such as Workspace -> Users -> your email, and import from that
folder's ⋮ menu, or use the Git folder or the Databricks CLI (step 1).

The full guide, including granting Genesis your existing data and uninstalling:
https://docs.genesiscomputing.com/docs/deployments/Databricks/Genesis-On-Databricks#marketplace-install-and-removal
