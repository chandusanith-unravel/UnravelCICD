# Unravel Github Integration

The [Unravel Github Integration](.github/workflows/upload-repo-zip-to-databricks.yml) GitHub Action copies a ZIP of a repository to a Unity Catalog volume. It runs only when someone selects **Run workflow** in the GitHub Actions tab. This repository is a tested reference; each customer must install the workflow and configure secrets in their own repository.

For complete setup, operation, troubleshooting, and spaces to add screenshots, use the [editable Word guide](docs/Unravel_Github_Integration_Setup_Guide.docx).

## Install in a customer repository

1. In the customer repository, create `.github/workflows/upload-repo-zip-to-databricks.yml` with the contents of this repository's [workflow YAML](.github/workflows/upload-repo-zip-to-databricks.yml). Commit or merge it into the customer's default branch. No separate script file is needed.
2. Create or choose a Databricks Unity Catalog volume. The Databricks identity behind the token needs permission to write files there.
3. Open the **customer repository** on GitHub, then go to **Settings → Secrets and variables → Actions → New repository secret**.
4. Add these three repository secrets in that customer repository:

   | Secret | Value |
   | --- | --- |
   | `DBX_URL` | Databricks workspace URL, such as `https://<workspace>.azuredatabricks.net` |
   | `DBX_TOKEN` | Databricks access token for the identity with volume write access |
   | `DBX_VOLUME_PATH` | Destination directory, such as `/Volumes/<catalog>/<schema>/<volume>/<folder>/`. The `dbfs:/Volumes/...` form also works. |

   Keep the token in GitHub Actions secrets; do not put it in the repository or a workflow input.

## Run the upload

1. Open **Actions** in the customer repository.
2. Select **Unravel Github Integration** in the workflow list.
3. Select **Run workflow**, choose the branch to archive, and select the green **Run workflow** button.
4. Open the new run and wait for the `upload` job to complete. The job summary displays the ZIP filename on success.

## What the workflow does

1. Checks out the selected commit.
2. Installs the Databricks CLI.
3. Runs `git archive` to ZIP every tracked file at that commit. The ZIP contains repository content, including this document and the workflow, but not the `.git` history or untracked runner files.
4. Accepts either `/Volumes/...` or `dbfs:/Volumes/...` for `DBX_VOLUME_PATH` and checks that catalog, schema, and volume names are present.
5. Creates the destination folder if needed, then copies the ZIP into it with the Databricks CLI. A run produces a name in the form `<repository>-<commit>-<run-id>-<attempt>.zip`.

## Verify the result

1. Confirm that all steps in the GitHub Actions `upload` job have green check marks.
2. In Databricks Catalog Explorer, open the configured catalog, schema, volume, and folder. Find the ZIP filename shown in the job summary.
3. If you use the Databricks CLI, list the destination with `databricks fs ls dbfs:/Volumes/<catalog>/<schema>/<volume>/<folder>/` and confirm that filename appears.

If the upload fails, check that all three secrets exist, the URL is a workspace URL, the token is valid, and its identity can write to the configured volume. The workflow reports an invalid volume path before attempting the copy.

## Manual browser alternative

If the customer cannot install or run the workflow, open their repository's **Code** tab, choose the intended branch, then select **Code → Download ZIP**. In Azure Databricks, open **Catalog**, browse to the target catalog, schema, and volume, and select **Upload to this volume**. Choose the downloaded ZIP and its destination directory, complete the upload, then confirm the ZIP appears in the volume with a nonzero size. This browser route requires GitHub read access and Databricks volume write access, but no GitHub Actions workflow or secrets. Repeat it for each new snapshot. The [Word guide](docs/Unravel_Github_Integration_Setup_Guide.docx) has every step and screenshot placeholders.

References: [GitHub manual workflow runs](https://docs.github.com/en/actions/how-tos/manage-workflow-runs/manually-run-a-workflow), [GitHub source ZIP downloads](https://docs.github.com/en/repositories/working-with-files/using-files/downloading-source-code-archives), and [Databricks volume file uploads](https://learn.microsoft.com/en-us/azure/databricks/ingestion/file-upload/upload-to-volume).
