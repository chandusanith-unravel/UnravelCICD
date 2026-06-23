# This workflow deploys DDL scripts to stage environment
name: 'deploy-ddl-stage'
permissions:
  id-token: write    # Required for Azure OIDC authentication
  contents: read

# Ensure that only a single job or workflow using the same concurrency group
# runs at a time. Multiple runs will queue instead of running concurrently.
# Uses base.ref for PRs so all PRs targeting main share one queue.
concurrency:
  group: deploy-ddl-stage-${{ github.event.pull_request.base.ref || github.ref_name }}
  cancel-in-progress: false  # Queue runs instead of cancelling previous runs

# Trigger this workflow only when DDL files are added/modified (not when PR is first opened)
on:
  pull_request:
    types:
      - synchronize
    branches:
      - main
    paths:
      - 'bronze/**/schema/ddl/stage/**/*.sql'
      - 'bronze/**/schema/ddl/stage/**/*.vw'
      - 'brz/**/schema/ddl/stage/**/*.sql'
      - 'brz/**/schema/ddl/stage/**/*.vw'
      - 'silver/schema/ddl/stage/**/*.sql'
      - 'silver/schema/ddl/stage/**/*.vw'
      - 'slv/schema/ddl/stage/**/*.sql'
      - 'slv/schema/ddl/stage/**/*.vw'

jobs:
  # The job that runs the Python script to deploy DDL
  deploy_ddl_stage_primary:
    name: 'Deploy DDL to Stage'
    runs-on: ubuntu-latest
    environment: stage_primary

    steps:
      # Check out first (no auth needed) so we can detect incremental DDL changes
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0  # Fetch full history for git diff

      # Detect DDL files changed in THIS push only (not full PR diff).
      # GitHub's paths filter for pull_request:synchronize checks the full PR
      # diff (all commits vs base), which causes this workflow to re-trigger
      # when non-DDL files are pushed after an earlier commit changed DDL files.
      # By comparing only the incremental push, we skip unnecessary re-deploys.
      - name: Get Changed DDL Files
        id: changed-files
        run: |
          BEFORE="${{ github.event.before }}"
          AFTER="${{ github.event.pull_request.head.sha }}"

          if [ -z "$BEFORE" ] || [ "$BEFORE" = "0000000000000000000000000000000000000000" ] || ! git cat-file -e "$BEFORE" 2>/dev/null; then
            echo "[INFO] Incremental comparison unavailable (first push or force-push) - using full PR diff"
            git fetch origin ${{ github.base_ref }}
            CHANGED_FILES=$(git diff --name-only --diff-filter=AMR origin/${{ github.base_ref }}...HEAD | grep -E '\.(sql|vw)$' | grep 'schema/ddl/stage/' | tr '\n' ' ' || echo "")
          else
            echo "[INFO] Checking incremental changes only (${BEFORE:0:8}..${AFTER:0:8})"
            CHANGED_FILES=$(git diff --name-only --diff-filter=AMR "$BEFORE".."$AFTER" | grep -E '\.(sql|vw)$' | grep 'schema/ddl/stage/' | tr '\n' ' ' || echo "")
          fi

          if [ -z "$CHANGED_FILES" ]; then
            echo "[INFO] No DDL files changed in this push - subsequent steps will be skipped"
          else
            echo "[INFO] DDL files to deploy: $CHANGED_FILES"
          fi

          echo "files=$CHANGED_FILES" >> $GITHUB_OUTPUT

      - name: Azure CLI Login
        if: steps.changed-files.outputs.files != ''
        uses: azure/login@v2
        with:
          client-id: ${{ vars.SP_CLIENT_ID }}
          tenant-id: ${{ vars.AZ_TENANT_ID }}
          allow-no-subscriptions: true
          # Remove client-secret for OIDC authentication
          # Federated identity credentials must be configured in Azure AD

      - name: 'Get Token'
        id: get_token
        if: steps.changed-files.outputs.files != ''
        run: |
          TOKEN=$(az account get-access-token --resource "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d" --query accessToken -o tsv)
          if [ -z "$TOKEN" ]; then
            echo "[ERROR] Failed to retrieve Databricks access token from Azure CLI"
            echo "[ERROR] Ensure OIDC federated credentials are configured for this environment's service principal"
            exit 1
          fi
          echo "token=$TOKEN" >> $GITHUB_OUTPUT

      # Set up Python environment.
      - name: Set up Python
        if: steps.changed-files.outputs.files != ''
        uses: actions/setup-python@v5
        with:
          python-version: '3.x'

      # Install dependencies if a requirements.txt file exists.
      - name: Install dependencies
        if: steps.changed-files.outputs.files != ''
        run: |
          if [ -f "requirements.txt" ]; then
            python -m pip install --upgrade pip
            pip install -r requirements.txt
          fi

      # Print the parent directory.
      - name: Print Parent Directory
        if: steps.changed-files.outputs.files != ''
        run: |
          echo "Parent Directory: $(dirname $(pwd))"

      - name: List Files in Directory
        if: steps.changed-files.outputs.files != ''
        run: ls -la

      # Run the Python script to deploy DDL.
      - name: Execute DDL Deployment Script
        if: steps.changed-files.outputs.files != ''
        env:
          DATABRICKS_TOKEN: ${{ steps.get_token.outputs.token }}
          SQL_SERVER_HOSTNAME: ${{ vars.SQL_SERVER_HOSTNAME }}
          SQL_SERVER_HTTP_PATH: ${{ vars.SQL_SERVER_HTTP_PATH }}
        run: |
          if [ -z "$DATABRICKS_TOKEN" ]; then
            echo "[ERROR] DATABRICKS_TOKEN is empty - Azure CLI login or token fetch may have failed"
            exit 1
          fi
          FILES="${{ steps.changed-files.outputs.files }}"
          if [ -n "$FILES" ]; then
            python ./databricks-sql/deploy-ddl.py stage --files $FILES
          else
            echo "[INFO] No DDL files changed - skipping execution"
          fi

      # Summary of deployment
      - name: Deployment Summary (Primary)
        if: always() && steps.changed-files.outputs.files != ''
        run: |
          echo "================================================================================"
          echo "  ✅ DEPLOYMENT SUMMARY (Stage Primary)"
          echo "================================================================================"
          echo ""
          echo "Environment:  stage_primary"
          echo "File Count:   $(echo '${{ steps.changed-files.outputs.files }}' | wc -w)"
          echo ""
          echo "Files Deployed:"
          INDEX=1
          for FILE in ${{ steps.changed-files.outputs.files }}; do
            echo "  $INDEX. $FILE"
            INDEX=$((INDEX + 1))
          done
          echo ""
          echo "Status:       Deployment completed"
          echo "Next:         Deployment to stage_secondary (DR) will start automatically"
          echo "================================================================================"
          echo ""

  # Deploy DDL to Secondary (DR) region after primary completes
  deploy_ddl_stage_secondary:
    name: 'Deploy DDL to Stage Secondary (DR)'
    runs-on: ubuntu-latest
    needs: [deploy_ddl_stage_primary]  # Wait for primary region to complete
    if: |
      !cancelled() &&
      needs.deploy_ddl_stage_primary.result != 'failure'
    environment: stage_secondary
    
    steps:
      # Check out first (no auth needed) so we can detect incremental DDL changes
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0  # Fetch full history for git diff

      # Detect DDL files changed in THIS push only (not full PR diff)
      - name: Get Changed DDL Files
        id: changed-files
        run: |
          BEFORE="${{ github.event.before }}"
          AFTER="${{ github.event.pull_request.head.sha }}"

          if [ -z "$BEFORE" ] || [ "$BEFORE" = "0000000000000000000000000000000000000000" ] || ! git cat-file -e "$BEFORE" 2>/dev/null; then
            echo "[INFO] Incremental comparison unavailable (first push or force-push) - using full PR diff"
            git fetch origin ${{ github.base_ref }}
            CHANGED_FILES=$(git diff --name-only --diff-filter=AMR origin/${{ github.base_ref }}...HEAD | grep -E '\.(sql|vw)$' | grep 'schema/ddl/stage/' | tr '\n' ' ' || echo "")
          else
            echo "[INFO] Checking incremental changes only (${BEFORE:0:8}..${AFTER:0:8})"
            CHANGED_FILES=$(git diff --name-only --diff-filter=AMR "$BEFORE".."$AFTER" | grep -E '\.(sql|vw)$' | grep 'schema/ddl/stage/' | tr '\n' ' ' || echo "")
          fi

          if [ -z "$CHANGED_FILES" ]; then
            echo "[INFO] No DDL files changed in this push - subsequent steps will be skipped"
          else
            echo "[INFO] DDL files to deploy: $CHANGED_FILES"
          fi

          echo "files=$CHANGED_FILES" >> $GITHUB_OUTPUT

      - name: Azure CLI Login
        if: steps.changed-files.outputs.files != ''
        uses: azure/login@v2
        with:
          client-id: ${{ vars.SP_CLIENT_ID }}
          tenant-id: ${{ vars.AZ_TENANT_ID }}
          allow-no-subscriptions: true

      - name: 'Get Token'
        id: get_token
        if: steps.changed-files.outputs.files != ''
        run: |
          TOKEN=$(az account get-access-token --resource "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d" --query accessToken -o tsv)
          if [ -z "$TOKEN" ]; then
            echo "[ERROR] Failed to retrieve Databricks access token from Azure CLI"
            echo "[ERROR] Ensure OIDC federated credentials are configured for this environment's service principal"
            exit 1
          fi
          echo "token=$TOKEN" >> $GITHUB_OUTPUT

      # Preview which DDL files will be deployed
      - name: Preview DDL Files
        if: steps.changed-files.outputs.files != ''
        run: |
          echo "================================================================================"
          echo "  📋 PREVIEW: DDL Files to Deploy (Stage Secondary DR)"
          echo "================================================================================"
          echo ""
          echo "Environment:  stage_secondary (DR)"
          echo "Trigger:      After stage_primary completes"
          echo "File Count:   $(echo '${{ steps.changed-files.outputs.files }}' | wc -w)"
          echo ""
          echo "Files to Deploy:"
          INDEX=1
          for FILE in ${{ steps.changed-files.outputs.files }}; do
            echo "  $INDEX. $FILE"
            INDEX=$((INDEX + 1))
          done
          echo ""
          echo "⚠️  These DDL files will be deployed to Stage Secondary DR region"
          echo "================================================================================"
          echo ""

      - name: Set up Python
        if: steps.changed-files.outputs.files != ''
        uses: actions/setup-python@v5
        with:
          python-version: '3.x'

      - name: Install dependencies
        if: steps.changed-files.outputs.files != ''
        run: |
          if [ -f "requirements.txt" ]; then
            python -m pip install --upgrade pip
            pip install -r requirements.txt
          fi

      - name: Print Parent Directory
        if: steps.changed-files.outputs.files != ''
        run: |
          echo "Parent Directory: $(dirname $(pwd))"

      - name: List Files in Directory
        if: steps.changed-files.outputs.files != ''
        run: ls -la

      - name: Execute DDL Deployment Script
        if: steps.changed-files.outputs.files != ''
        env:
          DATABRICKS_TOKEN: ${{ steps.get_token.outputs.token }}
          SQL_SERVER_HOSTNAME: ${{ vars.SQL_SERVER_HOSTNAME }}
          SQL_SERVER_HTTP_PATH: ${{ vars.SQL_SERVER_HTTP_PATH }}
        run: |
          if [ -z "$DATABRICKS_TOKEN" ]; then
            echo "[ERROR] DATABRICKS_TOKEN is empty - Azure CLI login or token fetch may have failed"
            exit 1
          fi
          FILES="${{ steps.changed-files.outputs.files }}"
          if [ -n "$FILES" ]; then
            python ./databricks-sql/deploy-ddl.py stage --files $FILES
          else
            echo "[INFO] No DDL files changed - skipping execution"
          fi

      # Summary of deployment
      - name: Deployment Summary (Secondary)
        if: always() && steps.changed-files.outputs.files != ''
        run: |
          echo "================================================================================"
          echo "  ✅ DEPLOYMENT SUMMARY (Stage Secondary DR)"
          echo "================================================================================"
          echo ""
          echo "Environment:  stage_secondary (DR)"
          echo "File Count:   $(echo '${{ steps.changed-files.outputs.files }}' | wc -w)"
          echo ""
          echo "Files Deployed:"
          INDEX=1
          for FILE in ${{ steps.changed-files.outputs.files }}; do
            echo "  $INDEX. $FILE"
            INDEX=$((INDEX + 1))
          done
          echo ""
          echo "Status:       Deployment completed to DR region"
          echo "Result:       Both stage_primary and stage_secondary deployments complete"
          echo "================================================================================"
          echo ""