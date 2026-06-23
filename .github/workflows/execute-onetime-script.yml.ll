# =============================================================================
# ONE-TIME SCRIPT EXECUTION WORKFLOW (Unified: All Environments)
# =============================================================================
#
# PURPOSE:
#   Execute one-time data scripts (DML, data corrections) that should NEVER be
#   re-executed. Unlike DDL workflows that can run repeatedly, one-time scripts
#   are locked-in after execution to prevent accidental re-runs.
#
# LIFECYCLE (Merge-After-Prod Strategy):
#   1. Create feature branch from main
#   2. Add scripts under schema/migrations/{environment}/ with numeric prefixes
#      - Stage scripts: schema/migrations/stage/001_test_data.sql
#      - Prod scripts:  schema/migrations/prod/001_critical_fix.sql
#   3. Execute to target environment and region (dry_run: true first)
#   4. Verify results, execute to other regions as needed
#   5. Create PR to main (DO NOT MERGE YET)
#   6. Team reviews/approves PR
#   7. Execute to all production regions (prod_primary → prod_secondary)
#   8. Merge PR to main (marks scripts as executed - prevents re-execution)
#
# ENVIRONMENT SUBFOLDERS:
#   - /schema/migrations/stage/  → Stage-only scripts (testing, validation)
#   - /schema/migrations/prod/   → Prod-only scripts (critical fixes, compliance)
#   - Independent numbering per environment (both can have 001, 002, etc.)
#   - Prevents cross-environment contamination (stage scripts won't run in prod)
#
# KEY DIFFERENCES FROM DDL:
#   - Manual trigger only (workflow_dispatch) vs automatic (pull_request)
#   - --diff-filter=A (Add only) vs AMR (Add/Modify/Rename)
#   - Always compares to main branch vs incremental push detection
#   - Merge AFTER execution (lock-in) vs merge anytime
#   - Environment subfolder filtering (stage/ or prod/)
#
# EXECUTION TRACKING:
#   - main branch = "executed" state
#   - feature branch = "pending" state
#   - Merge to main = permanent lock-in (git diff returns empty, no re-execution)
#
# SAFETY FEATURES:
#   - dry_run: true (default) - preview SQL without executing
#   - Branch validation (prod only) - prevents execution from main
#   - Environment subfolder filtering - prevents cross-environment execution
#   - Region selection (primary/secondary) prevents accidental dual execution
#   - Numeric prefix sorting (001_*.sql before 002_*.sql)
#
# ENVIRONMENT SELECTION:
#   - User selects: environment (stage/prod) + region (primary/secondary)
#   - GitHub dynamically loads: {environment}_{region} environment variables
#   - Example: stage + primary → stage_primary environment → correct Databricks host
#
# REQUIRED GITHUB ENVIRONMENTS & VARIABLES:
#   Each of these 4 environments MUST be configured in GitHub with required variables:
#
#   1. stage_primary
#      - SP_CLIENT_ID (Service Principal client ID)
#      - AZ_TENANT_ID (Azure Tenant ID)
#      - SQL_SERVER_HOSTNAME (Databricks workspace hostname)
#      - SQL_SERVER_HTTP_PATH (Databricks SQL warehouse HTTP path)
#
#   2. stage_secondary (DR region for stage)
#      - SP_CLIENT_ID
#      - AZ_TENANT_ID
#      - SQL_SERVER_HOSTNAME (different workspace)
#      - SQL_SERVER_HTTP_PATH
#
#   3. prod_primary
#      - SP_CLIENT_ID
#      - AZ_TENANT_ID
#      - SQL_SERVER_HOSTNAME (production workspace)
#      - SQL_SERVER_HTTP_PATH
#
#   4. prod_secondary (DR region for prod)
#      - SP_CLIENT_ID
#      - AZ_TENANT_ID
#      - SQL_SERVER_HOSTNAME (different workspace)
#      - SQL_SERVER_HTTP_PATH
#
# ENVIRONMENT MATRIX & EXECUTION PATHS:
#   ┌─────────────┬──────────────┬────────────────────────┬─────────────┐
#   │ Environment │ Region       │ Folder Filter          │ Repository  │
#   ├─────────────┼──────────────┼────────────────────────┼─────────────┤
#   │ stage       │ primary      │ schema/migrations/stage│ AAInternal/ │
#   │             │              │ (East region)          │ omegasd1    │
#   ├─────────────┼──────────────┼────────────────────────┼─────────────┤
#   │ stage       │ secondary    │ schema/migrations/stage│ AAInternal/ │
#   │             │              │ (West DR region)       │ omegasd1    │
#   ├─────────────┼──────────────┼────────────────────────┼─────────────┤
#   │ prod        │ primary      │ schema/migrations/prod │ AAInternal/ │
#   │             │              │ (East region)          │ omegasd1    │
#   │             │              │ [Branch check: not main│            │
#   ├─────────────┼──────────────┼────────────────────────┼─────────────┤
#   │ prod        │ secondary    │ schema/migrations/prod │ AAInternal/ │
#   │             │              │ (West DR region)       │ omegasd1    │
#   │             │              │ [Branch check: not main│            │
#   └─────────────┴──────────────┴────────────────────────┴─────────────┘
#
# VERIFICATION CHECKLIST:
#   Before running this workflow, ensure:
#
#   Environment Setup:
#   ☐ GitHub org has 4 environments: stage_primary, stage_secondary, 
#     prod_primary, prod_secondary
#   ☐ Each environment has 4 required variables configured
#   ☐ Variables are non-sensitive (host, client_id, tenant_id)
#   ☐ OIDC federated credentials configured for each environment
#   ☐ Azure Service Principal has permissions to target Databricks workspace
#
#   Repository Setup:
#   ☐ Repository branch protection: main branch protected
#   ☐ Folder structure exists: schema/migrations/stage/ and schema/migrations/prod/
#   ☐ databricks-sql/deploy-ddl.py script present and executable
#   ☐ Python requirements.txt includes databricks-sql-connector
#
#   Permissions:
#   ☐ GitHub runner: ubuntu-latest available
#   ☐ Azure OIDC: Service Principal roles assigned (Contributor minimum)
#   ☐ Databricks: Warehouse access for SPN (SQL warehouse must be accessible)
#
# =============================================================================

name: 'Execute One-Time Scripts'
permissions:
  id-token: write    # Required for Azure OIDC authentication
  contents: read

concurrency:
  group: execute-onetime-script-${{ github.ref_name }}-${{ github.event.inputs.environment }}-${{ github.event.inputs.region }}
  cancel-in-progress: false  # Queue runs instead of cancelling

# Manual trigger only (no automatic execution)
on:
  workflow_dispatch:
    inputs:
      environment:
        description: 'Target environment (stage or prod)'
        required: true
        type: choice
        options:
          - stage
          - prod
        default: 'stage'
      region:
        description: 'Target region (primary=East, secondary=West DR)'
        required: true
        type: choice
        options:
          - primary
          - secondary
        default: 'primary'
      dry_run:
        description: 'Dry run mode (true = preview only, false = actual execution)'
        required: true
        type: boolean
        default: true

jobs:
  # STEP 0: Call Tower API for deployment approval/tracking (prod only, skip dry-run)
  tower_api:
    if: github.event.inputs.environment == 'prod' && github.event.inputs.dry_run == 'false'
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
          description: 'Prod One-Time Script Execution: ${{ github.event.inputs.region }} (${{ github.ref_name }})'
          deployment_environment: prod

      - name: Capture Job Start Time
        id: jobstart
        run: |
          jobstart=$(date +'%Y-%m-%dT%H:%M:%S%z')
          echo "jobstart=$jobstart"
          echo "jobstart=$jobstart" >> $GITHUB_OUTPUT

  execute_scripts:
    name: 'Execute Scripts [${{ github.event.inputs.environment }}-${{ github.event.inputs.region }}]'
    needs: [tower_api]
    # Always run: for prod, tower_api succeeds first; for stage, it is skipped
    if: |
      always() &&
      (needs.tower_api.result == 'success' || needs.tower_api.result == 'skipped')
    runs-on: ubuntu-latest
    environment: ${{ github.event.inputs.environment }}_${{ github.event.inputs.region }}

    steps:
      # PROD SAFETY: Prevent execution from main branch (scripts already executed)
      - name: Validate Branch (Prod Only)
        if: github.event.inputs.environment == 'prod' && github.ref == 'refs/heads/main'
        run: |
          echo "================================================================================"
          echo "  ❌ ERROR: Cannot execute PROD scripts from main branch"
          echo "================================================================================"
          echo ""
          echo "REASON:"
          echo "  - Main branch represents 'executed' state"
          echo "  - Scripts on main have already been executed in production"
          echo "  - Re-execution would violate one-time script contract"
          echo ""
          echo "SOLUTION:"
          echo "  1. Ensure you are on a feature branch (not main)"
          echo "  2. Feature branch should contain NEW scripts (not yet on main)"
          echo "  3. Execute from feature branch BEFORE merging to main"
          echo ""
          echo "LIFECYCLE:"
          echo "  feature branch → execute all 4 regions → THEN merge to main"
          echo ""
          echo "================================================================================"
          exit 1

      - uses: actions/checkout@v4
        with:
          fetch-depth: 0  # Fetch full history for git diff vs main

      # Detect NEW script files only (--diff-filter=A prevents re-execution)
      # Filters to environment-specific subfolder (stage/ or prod/)
      - name: Get New Script Files
        id: changed-files
        run: |
          git fetch origin main
          
          # Only detect ADDED files in environment-specific subfolder
          # Example: schema/migrations/stage/*.sql or schema/migrations/prod/*.sql
          CHANGED_FILES=$(git diff --name-only --diff-filter=A origin/main...HEAD \
            | grep -E '\.(sql|vw)$' \
            | grep "schema/migrations/${{ github.event.inputs.environment }}/" \
            | tr '\n' ' ')
          
          if [ -z "$CHANGED_FILES" ]; then
            echo "[INFO] No new scripts detected in schema/migrations/${{ github.event.inputs.environment }}/"
            echo "[INFO] All scripts in this environment are already on main branch"
            echo "[INFO] To execute new scripts, add files to schema/migrations/${{ github.event.inputs.environment }}/"
            
            # Special message for prod execution from main
            if [ "${{ github.event.inputs.environment }}" = "prod" ] && [ "${{ github.ref }}" = "refs/heads/main" ]; then
              echo "[WARNING] Running from main branch - no scripts will execute (already executed)"
            fi
          else
            echo "[INFO] New scripts detected: $CHANGED_FILES"
            
            # Count files for logging
            FILE_COUNT=$(echo "$CHANGED_FILES" | wc -w)
            echo "[INFO] Total new scripts: $FILE_COUNT"
            echo "[INFO] Environment: ${{ github.event.inputs.environment }}"
            echo "[INFO] Region: ${{ github.event.inputs.region }}"
            
            # Dry-run mode check
            if [ "${{ github.event.inputs.dry_run }}" = "true" ]; then
              echo "[SAFETY] DRY RUN MODE ENABLED - SQL will be previewed but not executed"
            else
              echo "[WARNING] ACTUAL EXECUTION MODE - SQL will be executed in ${{ github.event.inputs.environment }}_${{ github.event.inputs.region }}"
            fi
          fi
          
          echo "files=$CHANGED_FILES" >> $GITHUB_OUTPUT

      # Initialize audit log file
      - name: Initialize Audit Log
        id: init-log
        if: steps.changed-files.outputs.files != ''
        run: |
          LOG_FILE="execution-log-${{ github.event.inputs.environment }}-${{ github.event.inputs.region }}-$(date +%Y%m%d-%H%M%S).log"
          echo "log_file=${LOG_FILE}" >> $GITHUB_OUTPUT
          
          {
            echo "================================================================================"
            echo "  ONE-TIME SCRIPT EXECUTION LOG"
            echo "================================================================================"
            echo ""
            echo "WORKFLOW METADATA:"
            echo "  Run ID:           ${{ github.run_id }}"
            echo "  Run Number:       ${{ github.run_number }}"
            echo "  Triggered By:     ${{ github.actor }}"
            echo "  Repository:       ${{ github.repository }}"
            echo "  Branch:           ${{ github.ref_name }}"
            echo "  Commit SHA:       ${{ github.sha }}"
            echo "  Timestamp (UTC):  $(date -u +"%Y-%m-%d %H:%M:%S")"
            echo ""
            echo "EXECUTION PARAMETERS:"
            echo "  Environment:      ${{ github.event.inputs.environment }}"
            echo "  Region:           ${{ github.event.inputs.region }}"
            echo "  Dry Run:          ${{ github.event.inputs.dry_run }}"
            echo "  Target:           ${{ github.event.inputs.environment }}_${{ github.event.inputs.region }}"
            echo ""
            echo "================================================================================"
            echo ""
          } > "${LOG_FILE}"

      # Preview which scripts will be executed
      - name: Preview Scripts
        if: steps.changed-files.outputs.files != ''
        run: |
          LOG_FILE="${{ steps.init-log.outputs.log_file }}"
          
          {
            echo "================================================================================"
            echo "  📋 PREVIEW: Scripts to Execute"
            echo "================================================================================"
            echo ""
            echo "Target:       ${{ github.event.inputs.environment }}_${{ github.event.inputs.region }}"
            echo "Mode:         ${{ github.event.inputs.dry_run == 'true' && 'DRY RUN (preview only)' || 'EXECUTION (actual run)' }}"
            echo "Script Count: $(echo '${{ steps.changed-files.outputs.files }}' | wc -w)"
            echo ""
            echo "Files to Execute:"
            INDEX=1
            for FILE in ${{ steps.changed-files.outputs.files }}; do
              echo "  $INDEX. $FILE"
              INDEX=$((INDEX + 1))
            done
            echo ""
            
            if [ "${{ github.event.inputs.dry_run }}" = "true" ]; then
              echo "⚠️  DRY RUN MODE - SQL will be previewed but not executed"
            else
              echo "⚠️  EXECUTION MODE - SQL will be executed in Databricks"
              echo "    Review the file list above carefully before proceeding"
            fi
            echo "================================================================================"
            echo ""
          } | tee -a "${LOG_FILE}"

      - name: Azure CLI Login
        if: steps.changed-files.outputs.files != '' && github.event.inputs.dry_run != 'true'
        uses: azure/login@v2
        with:
          client-id: ${{ vars.SP_CLIENT_ID }}
          tenant-id: ${{ vars.AZ_TENANT_ID }}
          allow-no-subscriptions: true

      - name: 'Get Token'
        id: get_token
        if: steps.changed-files.outputs.files != '' && github.event.inputs.dry_run != 'true'
        run: |
          TOKEN=$(az account get-access-token --resource "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d" --query accessToken -o tsv)
          if [ -z "$TOKEN" ]; then
            echo "[ERROR] Failed to retrieve Databricks access token from Azure CLI"
            echo "[ERROR] Ensure OIDC federated credentials are configured for ${{ github.event.inputs.environment }}_${{ github.event.inputs.region }} environment"
            exit 1
          fi
          echo "token=$TOKEN" >> $GITHUB_OUTPUT

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

      # Execute scripts with dry-run support and environment filtering
      - name: Execute Scripts
        if: steps.changed-files.outputs.files != '' && github.event.inputs.dry_run != 'true'
        env:
          DATABRICKS_TOKEN: ${{ steps.get_token.outputs.token }}
          SQL_SERVER_HOSTNAME: ${{ vars.SQL_SERVER_HOSTNAME }}
          SQL_SERVER_HTTP_PATH: ${{ vars.SQL_SERVER_HTTP_PATH }}
        run: |
          LOG_FILE="${{ steps.init-log.outputs.log_file }}"
          
          # Determine dry-run flag
          DRY_RUN_FLAG=""
          if [ "${{ github.event.inputs.dry_run }}" = "true" ]; then
            DRY_RUN_FLAG="--dry-run"
            echo "[INFO] Running in DRY RUN mode - no SQL will be executed" | tee -a "${LOG_FILE}"
          else
            echo "[WARNING] Running in EXECUTION mode - SQL will be executed" | tee -a "${LOG_FILE}"
          fi
          
          echo "" | tee -a "${LOG_FILE}"
          echo "===============================================================================" | tee -a "${LOG_FILE}"
          echo "  EXECUTION OUTPUT" | tee -a "${LOG_FILE}"
          echo "===============================================================================" | tee -a "${LOG_FILE}"
          echo "" | tee -a "${LOG_FILE}"
          
          # Execute scripts from environment-specific subfolder
          # Pass detected new files to ensure ONLY new scripts are executed (prevents re-execution)
          # Capture all output (stdout + stderr) to both console and log file
          python databricks-sql/deploy-ddl.py \
            --schema bronze \
            --environment ${{ github.event.inputs.environment }} \
            --migration-folder schema/migrations/${{ github.event.inputs.environment }} \
            $DRY_RUN_FLAG \
            --files ${{ steps.changed-files.outputs.files }} 2>&1 | tee -a "${LOG_FILE}"

      # Summary output
      - name: Execution Summary
        if: always() && steps.changed-files.outputs.files != ''
        run: |
          LOG_FILE="${{ steps.init-log.outputs.log_file }}"
          
          {
            echo ""
            echo "================================================================================"
            if [ "${{ github.event.inputs.dry_run }}" = "true" ]; then
              echo "  DRY RUN PREVIEW SUMMARY"
            else
              echo "  EXECUTION SUMMARY"
            fi
            echo "================================================================================"
            echo ""
            echo "Environment:  ${{ github.event.inputs.environment }}"
            echo "Region:       ${{ github.event.inputs.region }}"
            echo "Dry Run:      ${{ github.event.inputs.dry_run }}"
            echo "Branch:       ${{ github.ref_name }}"
            echo "Script Count: $(echo '${{ steps.changed-files.outputs.files }}' | wc -w)"
            echo ""
            if [ "${{ github.event.inputs.dry_run }}" = "true" ]; then
              echo "Files Previewed:"
            else
              echo "Files Executed:"
            fi
            # Print each file with numeric index
            INDEX=1
            for FILE in ${{ steps.changed-files.outputs.files }}; do
              echo "  $INDEX. $FILE"
              INDEX=$((INDEX + 1))
            done
            echo ""
            
            if [ "${{ github.event.inputs.dry_run }}" = "true" ]; then
              echo "STATUS: Preview completed (no execution)"
              echo ""
              echo "NEXT STEPS:"
              echo "  1. Review SQL preview output above"
              echo "  2. If correct, re-run with dry_run: false"
            else
              echo "STATUS: Execution completed"
              echo ""
              echo "NEXT STEPS:"
              if [ "${{ github.event.inputs.environment }}" = "stage" ]; then
                echo "  1. Verify results in ${{ github.event.inputs.environment }}_${{ github.event.inputs.region }}"
                echo "  2. Execute to other stage region if needed"
                echo "  3. Create PR to main (DO NOT MERGE YET)"
                echo "  4. Execute to PROD environments (prod_primary → prod_secondary)"
                echo "  5. THEN merge PR to main (locks-in execution state)"
              else
                echo "  1. Verify results in ${{ github.event.inputs.environment }}_${{ github.event.inputs.region }}"
                echo "  2. Execute to other prod region if needed"
                echo "  3. Once all 4 regions complete, MERGE PR to main"
              fi
            fi
            echo ""
            echo "================================================================================"
            echo ""
            echo "AUDIT LOG SAVED: ${LOG_FILE}"
            echo "  - Download from workflow artifacts for compliance/auditing"
            echo "  - Retention: 90 days"
            echo "================================================================================"
          } | tee -a "${LOG_FILE}"

      # Post-execution reminder for prod
      - name: Post-Execution Reminder (Prod Only)
        if: github.event.inputs.environment == 'prod' && github.event.inputs.dry_run == 'false' && steps.changed-files.outputs.files != ''
        run: |
          LOG_FILE="${{ steps.init-log.outputs.log_file }}"
          
          {
            echo ""
            echo "================================================================================"
            echo "  ⚠️  CRITICAL REMINDER - DO NOT MERGE YET"
            echo "================================================================================"
            echo ""
            echo "CURRENT STATUS:"
            echo "  ✅ Scripts executed in prod_${{ github.event.inputs.region }}"
            echo ""
            echo "BEFORE MERGING TO MAIN:"
            echo "  ☐ Execute scripts to prod_primary (East region)"
            echo "  ☐ Execute scripts to prod_secondary (West DR region)"
            echo "  ☐ Verify results in BOTH prod regions"
            echo "  ☐ Confirm no errors or data issues"
            echo ""
            echo "THEN (and only then):"
            echo "  → Merge PR to main branch"
            echo "  → This locks-in execution state (prevents re-execution)"
            echo ""
            echo "================================================================================"
          } | tee -a "${LOG_FILE}"

      # Upload execution logs as workflow artifact for audit trail
      - name: Upload Execution Logs
        if: always() && steps.changed-files.outputs.files != ''
        uses: actions/upload-artifact@v4
        with:
          name: execution-logs-${{ github.event.inputs.environment }}-${{ github.event.inputs.region }}-run-${{ github.run_number }}
          path: execution-log-*.log
          retention-days: 90
          if-no-files-found: warn

  # STEP FINAL: Record ServiceNow Change Request (post-deployment, prod only)
  servicenow:
    needs: [tower_api, execute_scripts]
    if: |
      always() &&
      needs.tower_api.result == 'success' &&
      github.event.inputs.environment == 'prod' && github.event.inputs.dry_run == 'false'
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
            Prod One-Time Script Execution
            Repository: ${{ github.repository }}
            Environment: ${{ github.event.inputs.environment }}
            Region: ${{ github.event.inputs.region }}
            Branch: ${{ github.ref_name }}
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
              <li><b>Workflow:</b> One-Time Script Execution</li>
              <li><b>Environment:</b> ${{ github.event.inputs.environment }}</li>
              <li><b>Region:</b> ${{ github.event.inputs.region }}</li>
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
              <li><b>Workflow:</b> One-Time Script Execution</li>
              <li><b>Environment:</b> ${{ github.event.inputs.environment }}</li>
              <li><b>Region:</b> ${{ github.event.inputs.region }}</li>
              <li><b>Pipeline Run:</b> <a href="https://github.com/${{ github.repository }}/actions/runs/${{ github.run_id }}">View Pipeline</a></li>
              <li><b>ServiceNow Ticket:</b> <a href="${{ steps.servicenow.outputs.change_request_url }}">${{ steps.servicenow.outputs.change_request_id }}</a></li>
            </ul>
          secretConfig: ${{ secrets.EMAILME_SECRET_CONFIG }}