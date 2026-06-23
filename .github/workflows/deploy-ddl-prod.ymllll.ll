# This workflow deploys DDL scripts to production environment
name: 'deploy-ddl-prod'
permissions:
  id-token: write    # Required for Azure OIDC authentication
  contents: read

# Ensure that only a single job or workflow using the same concurrency group
# runs at a time. Multiple runs will queue instead of running concurrently.
# Uses base.ref for PRs so all PRs targeting main share one queue.
concurrency:
  group: deploy-ddl-prod-${{ github.event.pull_request.base.ref || github.ref_name }}
  cancel-in-progress: false  # Queue runs instead of cancelling previous runs

# Trigger this workflow only when DDL files are added/modified on main (after PR merge)
on:
  push:
    branches:
      - main
    paths:
      - 'bronze/**/schema/ddl/prod/**/*.sql'
      - 'bronze/**/schema/ddl/prod/**/*.vw'
      - 'brz/**/schema/ddl/prod/**/*.sql'
      - 'brz/**/schema/ddl/prod/**/*.vw'
      - 'silver/schema/ddl/prod/**/*.sql'
      - 'silver/schema/ddl/prod/**/*.vw'
      - 'slv/schema/ddl/prod/**/*.sql'
      - 'slv/schema/ddl/prod/**/*.vw'

jobs:
  # STEP 0: Call Tower API for deployment approval/tracking
  tower_api:
    runs-on: ubuntu-24.04
    outputs:
      jobstart: ${{ steps.jobstart.outputs.jobstart }}
    steps:
      - name: Fetch Repository Custom Properties
        id: repo_props
        env:
          GH_TOKEN: ${{ github.token }}
        run: |
          echo "Fetching custom properties for ${{ github.repository }}..."
          
          if ! PROPS=$(gh api "repos/${{ github.repository }}/properties/values"); then
            echo "::error::Failed to fetch repository custom properties via GitHub API. Verify GITHUB_TOKEN permissions and API availability."
            exit 1
          fi
          
          # Extract app-shortname (maps to Tower archer_short_name)
          ARCHER_SHORT_NAME=$(echo "$PROPS" | jq -r '.[] | select(.property_name == "app-shortname") | .value // empty')
          if [ -z "$ARCHER_SHORT_NAME" ]; then
            echo "::error::Custom property 'app-shortname' not found on repository. Set it via GitHub repo settings or deploy_to_repo.py."
            exit 1
          fi
          
          # Extract squad-id (maps to Tower squad360_id)
          SQUAD360_ID=$(echo "$PROPS" | jq -r '.[] | select(.property_name == "squad-id") | .value // empty')
          if [ -z "$SQUAD360_ID" ]; then
            echo "::error::Custom property 'squad-id' not found on repository. Set it via GitHub repo settings or deploy_to_repo.py."
            exit 1
          fi
          
          echo "archer_short_name=${ARCHER_SHORT_NAME}" >> $GITHUB_OUTPUT
          echo "squad360_id=${SQUAD360_ID}" >> $GITHUB_OUTPUT
          echo "Custom Properties:"
          echo "  archer_short_name: ${ARCHER_SHORT_NAME}"
          echo "  squad360_id: ${SQUAD360_ID}"

      - name: Call TnT Tower
        uses: AAInternal/tower-action@v1
        with:
          tower_url: ${{ secrets.TOWER_URL }}
          token_url: ${{ secrets.TOWER_TOKEN_URL }}
          client_id: ${{ secrets.TOWER_USR }}
          client_secret: ${{ secrets.TOWER_PWD }}
          archer_short_name: ${{ steps.repo_props.outputs.archer_short_name }}
          squad360_id: ${{ steps.repo_props.outputs.squad360_id }}
          description: 'Prod DDL Deployment: ${{ github.event.head_commit.message }}'
          deployment_environment: prod

      - name: Capture Job Start Time
        id: jobstart
        run: |
          jobstart=$(date +'%Y-%m-%dT%H:%M:%S%z')
          echo "jobstart=$jobstart"
          echo "jobstart=$jobstart" >> $GITHUB_OUTPUT

  # The job that runs the Python script to deploy DDL
  deploy_ddl_prod_primary:
    name: 'Deploy DDL to Production'
    needs: [tower_api]
    runs-on: ubuntu-latest
    environment:
      name: prod_primary

    steps:
      - name: Azure CLI Login
        uses: azure/login@v2
        with:
          client-id: ${{ vars.SP_CLIENT_ID }}
          tenant-id: ${{ vars.AZ_TENANT_ID }}
          allow-no-subscriptions: true
          # Remove client-secret for OIDC authentication
          # Federated identity credentials must be configured in Azure AD

      - name: 'Get Token'
        id: get_token
        run: |
          TOKEN=$(az account get-access-token --resource "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d" --query accessToken -o tsv)
          if [ -z "$TOKEN" ]; then
            echo "[ERROR] Failed to retrieve Databricks access token from Azure CLI"
            echo "[ERROR] Ensure OIDC federated credentials are configured for this environment's service principal"
            exit 1
          fi
          echo "token=$TOKEN" >> $GITHUB_OUTPUT

      # Check out this repo, so that this workflow can access it.
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0  # Fetch full history for git diff

      # Detect added/modified/renamed SQL/view files (exclude deleted files)
      - name: Get Changed DDL Files
        id: changed-files
        run: |
          # Find added/modified/renamed SQL/view files in prod DDL directories (exclude deleted files)
          CHANGED_FILES=$(git diff --name-only --diff-filter=AMR ${{ github.event.before }} ${{ github.sha }} | grep -E '\.(sql|vw)$' | grep 'schema/ddl/prod/' | tr '\n' ' ' || echo "")
          
          # Display changed files (one per line for readability)
          FILE_COUNT=$(echo "$CHANGED_FILES" | wc -w)
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          echo "Changed DDL Files ($FILE_COUNT):"
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          if [ -n "$CHANGED_FILES" ]; then
            i=1
            for f in $CHANGED_FILES; do
              echo "  $i. $f"
              i=$((i + 1))
            done
          else
            echo "  (none)"
          fi
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          echo "files=$CHANGED_FILES" >> $GITHUB_OUTPUT

      # Set up Python environment.
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.x'

      # Install dependencies if a requirements.txt file exists.
      - name: Install dependencies
        run: |
          if [ -f "requirements.txt" ]; then
            python -m pip install --upgrade pip
            pip install -r requirements.txt
          fi

      # Print the parent directory.
      - name: Print Parent Directory
        run: |
          echo "Parent Directory: $(dirname $(pwd))"

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
            python ./databricks-sql/deploy-ddl.py prod --files $FILES
          else
            echo "[INFO] No DDL files changed - skipping execution"
          fi

      # Summary of deployment
      - name: Deployment Summary (Primary)
        if: always() && steps.changed-files.outputs.files != ''
        run: |
          echo "================================================================================"
          echo "  ✅ DEPLOYMENT SUMMARY (Prod Primary)"
          echo "================================================================================"
          echo ""
          echo "Environment:  prod_primary"
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
          echo "Next:         Deployment to prod_secondary (DR) will start automatically"
          echo "================================================================================"
          echo ""

  # Deploy DDL to Secondary (DR) region after primary completes
  deploy_ddl_prod_secondary:
    name: 'Deploy DDL to Production Secondary (DR)'
    runs-on: ubuntu-latest
    needs: [tower_api, deploy_ddl_prod_primary]  # Wait for Tower API and primary region to complete
    if: |
      !cancelled() &&
      needs.deploy_ddl_prod_primary.result != 'failure'
    environment:
      name: prod_secondary

    steps:
      - name: Azure CLI Login
        uses: azure/login@v2
        with:
          client-id: ${{ vars.SP_CLIENT_ID }}
          tenant-id: ${{ vars.AZ_TENANT_ID }}
          allow-no-subscriptions: true

      - name: 'Get Token'
        id: get_token
        run: |
          TOKEN=$(az account get-access-token --resource "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d" --query accessToken -o tsv)
          if [ -z "$TOKEN" ]; then
            echo "[ERROR] Failed to retrieve Databricks access token from Azure CLI"
            echo "[ERROR] Ensure OIDC federated credentials are configured for this environment's service principal"
            exit 1
          fi
          echo "token=$TOKEN" >> $GITHUB_OUTPUT

      - uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - name: Get Changed DDL Files
        id: changed-files
        run: |
          CHANGED_FILES=$(git diff --name-only --diff-filter=AMR ${{ github.event.before }} ${{ github.sha }} | grep -E '\.(sql|vw)$' | grep 'schema/ddl/prod/' | tr '\n' ' ' || echo "")
          
          echo "Changed files: $CHANGED_FILES"
          echo "files=$CHANGED_FILES" >> $GITHUB_OUTPUT

      # Preview which DDL files will be deployed
      - name: Preview DDL Files
        if: steps.changed-files.outputs.files != ''
        run: |
          echo "================================================================================"
          echo "  📋 PREVIEW: DDL Files to Deploy (Prod Secondary DR)"
          echo "================================================================================"
          echo ""
          echo "Environment:  prod_secondary (DR)"
          echo "Trigger:      After prod_primary completes"
          echo "File Count:   $(echo '${{ steps.changed-files.outputs.files }}' | wc -w)"
          echo ""
          echo "Files to Deploy:"
          INDEX=1
          for FILE in ${{ steps.changed-files.outputs.files }}; do
            echo "  $INDEX. $FILE"
            INDEX=$((INDEX + 1))
          done
          echo ""
          echo "⚠️  These DDL files will be deployed to PRODUCTION Secondary DR region"
          echo "================================================================================"
          echo ""

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.x'

      - name: Install dependencies
        run: |
          if [ -f "requirements.txt" ]; then
            python -m pip install --upgrade pip
            pip install -r requirements.txt
          fi

      - name: Print Parent Directory
        run: |
          echo "Parent Directory: $(dirname $(pwd))"

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
            python ./databricks-sql/deploy-ddl.py prod --files $FILES
          else
            echo "[INFO] No DDL files changed - skipping execution"
          fi

      # Summary of deployment
      - name: Deployment Summary (Secondary)
        if: always() && steps.changed-files.outputs.files != ''
        run: |
          echo "================================================================================"
          echo "  ✅ DEPLOYMENT SUMMARY (Prod Secondary DR)"
          echo "================================================================================"
          echo ""
          echo "Environment:  prod_secondary (DR)"
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
          echo "Result:       Both prod_primary and prod_secondary deployments complete"
          echo "================================================================================"
          echo ""

  # STEP FINAL: Record ServiceNow Change Request (post-deployment)
  servicenow:
    needs: [tower_api, deploy_ddl_prod_primary, deploy_ddl_prod_secondary]
    if: |
      always() &&
      needs.tower_api.result == 'success'
    runs-on: ubuntu-24.04
    steps:
      - name: Capture Job Stop Time
        id: jobstop
        run: |
          jobstop=$(date +'%Y-%m-%dT%H:%M:%S%z')
          echo "jobstop=$jobstop"
          echo "jobstop=$jobstop" >> $GITHUB_ENV

      - name: ServiceNow Change Request
        id: servicenow
        continue-on-error: true
        uses: AAInternal/enterprise-change-action@v1
        with:
          appName: ${{ vars.SNOW_APP_NAME }}
          badgeNumber: ${{ vars.SNOW_FALLBACK_BADGE }}
          enableDynamicBadgeNumber: true
          team: ${{ vars.SNOW_TEAM }}
          description: |
            Prod DDL Deployment
            Repository: ${{ github.repository }}
            Commit: ${{ github.event.head_commit.message }}
            Author: ${{ github.event.head_commit.author.name }}
            Run: ${{ github.run_id }}
          changeProposedStartDateTime: ${{ needs.tower_api.outputs.jobstart }}
          changeProposedEndDateTime: ${{ env.jobstop }}
          changeArtifactLocation: 'repo: https://github.com/${{ github.repository }} ref: ${{ github.ref }} sha: ${{ github.sha }}'
          affectedConfigItems: '${{ github.event.repository.name }},MINERVA Framework,Databricks Pipelines,tables,views'
          applicationLocation: 'Azure East'

      - name: ServiceNow Failure Notification
        if: steps.servicenow.outcome == 'failure'
        uses: AAInternal/emailMe@v1
        with:
          emailTo: ${{ vars.SNOW_EMAIL_TO }}
          subject: "ServiceNow Change Request Failed - ${{ github.event.repository.name }}"
          message: |
            <h2>ServiceNow Change Request Failed</h2>
            <ul>
              <li><b>Repository:</b> ${{ github.repository }}</li>
              <li><b>Commit:</b> ${{ github.event.head_commit.message }}</li>
              <li><b>Author:</b> ${{ github.event.head_commit.author.name }}</li>
              <li><b>Pipeline Run:</b> <a href="https://github.com/${{ github.repository }}/actions/runs/${{ github.run_id }}">View Pipeline</a></li>
            </ul>
          secretConfig: ${{ secrets.EMAILME_SECRET_CONFIG }}

      - name: ServiceNow Success Notification
        if: steps.servicenow.outcome == 'success'
        uses: AAInternal/emailMe@v1
        with:
          emailTo: ${{ vars.SNOW_EMAIL_TO }}
          subject: "ServiceNow: ${{ steps.servicenow.outputs.change_request_id }} - ${{ github.event.repository.name }} - Run ${{ github.run_number }}"
          message: |
            <h2>ServiceNow Change Request Created</h2>
            <ul>
              <li><b>Repository:</b> ${{ github.repository }}</li>
              <li><b>Commit:</b> ${{ github.event.head_commit.message }}</li>
              <li><b>Author:</b> ${{ github.event.head_commit.author.name }}</li>
              <li><b>Pipeline Run:</b> <a href="https://github.com/${{ github.repository }}/actions/runs/${{ github.run_id }}">View Pipeline</a></li>
              <li><b>ServiceNow Ticket:</b> <a href="${{ steps.servicenow.outputs.change_request_url }}">${{ steps.servicenow.outputs.change_request_id }}</a></li>
            </ul>
          secretConfig: ${{ secrets.EMAILME_SECRET_CONFIG }}