name: DR Switch

on:
  workflow_dispatch:
    inputs:
      environment:
        description: 'Environment'
        required: true
        type: choice
        options:
          - 'stage'
          - 'prod'
      
      operation_mode:
        description: 'Operation Mode'
        required: true
        type: choice
        options:
          - 'all-in-scope'    # All jobs SP has access to (based on scope_code)
          - 'selected-jobs'   # Specific job pairs (comma-separated)
      
      scope_code:
        description: 'Scope code (all-in-scope mode): Subdomain (bronze/silver) or data product (gold). Examples: omegasd1, sttrm, customer-360'
        required: false
        type: string
      
      job_names:
        description: 'Job names (selected-jobs mode): Semicolon-separated list (e.g., job-name-1;job-name-2;job-name-3). Names are identical in both regions.'
        required: false
        type: string
      
      action:
        description: 'Action'
        required: true
        type: choice
        options:
          - 'status'
          - 'failover'
          - 'failback'
      
      dry_run:
        description: 'Dry-run mode: Preview impact without executing (shows what would be affected)'
        required: false
        type: boolean
        default: false
      
      validate_pairing:
        description: 'Validate primary/secondary job pairing (recommended for all-in-scope mode)'
        required: false
        type: boolean
        default: true
      
      disaster_mode:
        description: 'Disaster mode: Skip primary region operations during failover (used when primary region is DOWN/UNREACHABLE). Only applies to failover action.'
        required: false
        type: boolean
        default: false

permissions:
  contents: read
  id-token: write    # Required for Azure OIDC authentication

jobs:
  # STEP 0: Call Tower API for deployment approval/tracking
  # Only for prod failover/failback with dry_run disabled
  tower_api:
    if: |
      github.event.inputs.environment == 'prod' &&
      (github.event.inputs.action == 'failover' || github.event.inputs.action == 'failback') &&
      github.event.inputs.dry_run == 'false'
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
          description: 'Prod DR ${{ github.event.inputs.action }}: ${{ github.event.inputs.operation_mode }} (${{ github.ref_name }})'
          deployment_environment: prod

      - name: Capture Job Start Time
        id: jobstart
        run: |
          jobstart=$(date +'%Y-%m-%dT%H:%M:%S%z')
          echo "jobstart=$jobstart"
          echo "jobstart=$jobstart" >> $GITHUB_OUTPUT

  # Step 1: Validate inputs
  validate-inputs:
    needs: [tower_api]
    if: |
      always() &&
      (needs.tower_api.result == 'success' || needs.tower_api.result == 'skipped')
    runs-on: ubuntu-latest
    outputs:
      operation_mode: ${{ github.event.inputs.operation_mode }}
    steps:
      - name: Display Configuration
        run: |
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          echo "🎯 DR SWITCH CONFIGURATION"
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          echo "Mode        : ${{ github.event.inputs.operation_mode }}"
          echo "Environment : ${{ github.event.inputs.environment }}"
          echo "Action      : ${{ github.event.inputs.action }}"
          echo "Dry Run     : ${{ github.event.inputs.dry_run }}"
          echo "Validation  : ${{ github.event.inputs.validate_pairing }}"
          
          if [ "${{ github.event.inputs.operation_mode }}" = "all-in-scope" ]; then
            echo "Scope Code  : ${{ github.event.inputs.scope_code }}"
          else
            echo "Job Names   : ${{ github.event.inputs.job_name_pairs }}"
          fi
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  # Step 2a: Discover secondary workspace jobs (both modes - needed to resolve job names)
  discover-secondary:
    runs-on: ubuntu-latest
    needs: validate-inputs
    if: always()
    environment: ${{ github.event.inputs.environment }}_secondary
    outputs:
      job_count: ${{ steps.discover.outputs.job_count }}
      jobs_json: ${{ steps.discover.outputs.jobs_json }}
    
    steps:
      - name: Azure CLI Login (OIDC)
        uses: azure/login@v2
        with:
          client-id: ${{ vars.SP_CLIENT_ID }}
          tenant-id: ${{ vars.AZ_TENANT_ID }}
          audience: api://AzureADTokenExchange
          allow-no-subscriptions: true

      - name: Set up Databricks CLI
        uses: databricks/setup-cli@main

      - name: Discover Secondary Jobs
        id: discover
        env:
          DATABRICKS_HOST: ${{ vars.DATABRICKS_HOST }}
          ARM_CLIENT_ID: ${{ vars.SP_CLIENT_ID }}
          ARM_TENANT_ID: ${{ vars.AZ_TENANT_ID }}
          DATABRICKS_AUTH_TYPE: azure-cli
        run: |
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          echo "🔍 DISCOVERING SECONDARY WORKSPACE JOBS"
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          echo ""
          echo "Workspace: ${{ github.event.inputs.environment }}_secondary"
          echo "Scope: All jobs accessible to Service Principal"
          echo ""
          
          JOBS_JSON=$(databricks jobs list --output json)
          
          # Handle both response formats: {"jobs": [...]} or [...]
          if echo "$JOBS_JSON" | jq -e '.jobs' > /dev/null 2>&1; then
            # Format: {"jobs": [...]}
            JOBS_ARRAY=$(echo "$JOBS_JSON" | jq -c '.jobs')
            JOB_COUNT=$(echo "$JOBS_JSON" | jq '.jobs | length')
          else
            # Format: [...]
            JOBS_ARRAY=$(echo "$JOBS_JSON" | jq -c '.')
            JOB_COUNT=$(echo "$JOBS_JSON" | jq '. | length')
          fi
          
          echo "Found ${JOB_COUNT} job(s):"
          echo "$JOBS_ARRAY" | jq -r '.[] | "  ✓ \(.settings.name) (ID: \(.job_id))"'
          echo ""
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          
          # Extract minimal job data (name + ID only) for cross-job transfer
          MINIMAL_JOBS=$(echo "$JOBS_ARRAY" | jq -c '[.[] | {name: .settings.name, job_id: .job_id}]')
          echo "job_count=${JOB_COUNT}" >> $GITHUB_OUTPUT
          {
            echo "jobs_json<<__JOBS_EOF__"
            echo "$MINIMAL_JOBS"
            echo "__JOBS_EOF__"
          } >> $GITHUB_OUTPUT

  # Step 2b: Discover primary workspace jobs (both modes - needed to resolve job names)
  discover-primary:
    runs-on: ubuntu-latest
    needs: validate-inputs
    if: always()
    environment: ${{ github.event.inputs.environment }}_primary
    outputs:
      job_count: ${{ steps.discover.outputs.job_count }}
      jobs_json: ${{ steps.discover.outputs.jobs_json }}
    
    steps:
      - name: Azure CLI Login (OIDC)
        uses: azure/login@v2
        with:
          client-id: ${{ vars.SP_CLIENT_ID }}
          tenant-id: ${{ vars.AZ_TENANT_ID }}
          audience: api://AzureADTokenExchange
          allow-no-subscriptions: true

      - name: Set up Databricks CLI
        uses: databricks/setup-cli@main

      - name: Discover Primary Jobs
        id: discover
        env:
          DATABRICKS_HOST: ${{ vars.DATABRICKS_HOST }}
          ARM_CLIENT_ID: ${{ vars.SP_CLIENT_ID }}
          ARM_TENANT_ID: ${{ vars.AZ_TENANT_ID }}
          DATABRICKS_AUTH_TYPE: azure-cli
        run: |
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          echo "🔍 DISCOVERING PRIMARY WORKSPACE JOBS"
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          echo ""
          echo "Workspace: ${{ github.event.inputs.environment }}_primary"
          echo "Scope: All jobs accessible to Service Principal"
          echo ""
          
          JOBS_JSON=$(databricks jobs list --output json)
          
          # Handle both response formats: {"jobs": [...]} or [...]
          if echo "$JOBS_JSON" | jq -e '.jobs' > /dev/null 2>&1; then
            # Format: {"jobs": [...]}
            JOBS_ARRAY=$(echo "$JOBS_JSON" | jq -c '.jobs')
            JOB_COUNT=$(echo "$JOBS_JSON" | jq '.jobs | length')
          else
            # Format: [...]
            JOBS_ARRAY=$(echo "$JOBS_JSON" | jq -c '.')
            JOB_COUNT=$(echo "$JOBS_JSON" | jq '. | length')
          fi
          
          echo "Found ${JOB_COUNT} job(s):"
          echo "$JOBS_ARRAY" | jq -r '.[] | "  ✓ \(.settings.name) (ID: \(.job_id))"'
          echo ""
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          
          # Extract minimal job data (name + ID only) for cross-job transfer
          MINIMAL_JOBS=$(echo "$JOBS_ARRAY" | jq -c '[.[] | {name: .settings.name, job_id: .job_id}]')
          echo "job_count=${JOB_COUNT}" >> $GITHUB_OUTPUT
          {
            echo "jobs_json<<__JOBS_EOF__"
            echo "$MINIMAL_JOBS"
            echo "__JOBS_EOF__"
          } >> $GITHUB_OUTPUT

  # Step 3: Build job pair matrix
  build-matrix:
    runs-on: ubuntu-latest
    needs: [validate-inputs, discover-secondary, discover-primary]
    if: always() && needs.discover-secondary.result == 'success' && needs.discover-primary.result == 'success'
    outputs:
      job_matrix: ${{ steps.build.outputs.matrix }}
      job_count: ${{ steps.build.outputs.count }}
      matched_count: ${{ steps.build.outputs.matched_count }}
      unmatched_secondary: ${{ steps.build.outputs.unmatched_secondary }}
      unmatched_primary: ${{ steps.build.outputs.unmatched_primary }}
    
    steps:
      - name: Build Matrix
        id: build
        run: |
          MODE="${{ github.event.inputs.operation_mode }}"
          
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          echo "🔧 BUILDING JOB PAIR MATRIX"
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          echo "Mode: ${MODE}"
          echo ""
          
          if [ "$MODE" = "selected-jobs" ]; then
            # Parse user-provided job names and resolve to IDs
            JOB_NAMES="${{ github.event.inputs.job_names }}"
            
            # Read discovered jobs from both workspaces
            SECONDARY_JOBS='${{ needs.discover-secondary.outputs.jobs_json }}'
            PRIMARY_JOBS='${{ needs.discover-primary.outputs.jobs_json }}'
            
            echo "Processing user-provided job names..."
            echo "Secondary workspace: ${{ needs.discover-secondary.outputs.job_count }} jobs discovered"
            echo "Primary workspace: ${{ needs.discover-primary.outputs.job_count }} jobs discovered"
            echo ""
            
            MATRIX='{"include":['
            INDEX=0
            UNMATCHED_COUNT=0
            
            IFS=';' read -ra NAME_ARRAY <<< "$JOB_NAMES"
            for JOB_NAME in "${NAME_ARRAY[@]}"; do
              # Trim whitespace
              JOB_NAME=$(echo "$JOB_NAME" | xargs)
              
              # Find job in secondary workspace
              SEC_JOB=$(echo "$SECONDARY_JOBS" | jq -c --arg name "$JOB_NAME" '.[] | select(.name == $name)' | head -n 1)
              # Find job in primary workspace
              PRI_JOB=$(echo "$PRIMARY_JOBS" | jq -c --arg name "$JOB_NAME" '.[] | select(.name == $name)' | head -n 1)
              
              if [ -n "$SEC_JOB" ] && [ -n "$PRI_JOB" ]; then
                SEC_ID=$(echo "$SEC_JOB" | jq -r '.job_id')
                PRI_ID=$(echo "$PRI_JOB" | jq -r '.job_id')
                
                if [ $INDEX -gt 0 ]; then
                  MATRIX="${MATRIX},"
                fi
                
                MATRIX="${MATRIX}{\"name\":\"${JOB_NAME}\",\"primary_id\":\"${PRI_ID}\",\"secondary_id\":\"${SEC_ID}\"}"
                echo "  ✅ Resolved: ${JOB_NAME}"
                echo "     Primary ID: ${PRI_ID}"
                echo "     Secondary ID: ${SEC_ID}"
                echo ""
                INDEX=$((INDEX + 1))
              else
                echo "  ❌ NOT FOUND: ${JOB_NAME}"
                if [ -z "$SEC_JOB" ]; then
                  echo "     Missing in secondary workspace"
                fi
                if [ -z "$PRI_JOB" ]; then
                  echo "     Missing in primary workspace"
                fi
                echo ""
                UNMATCHED_COUNT=$((UNMATCHED_COUNT + 1))
              fi
            done
            
            MATRIX="${MATRIX}]}"
            COUNT=$INDEX
            
            if [ $COUNT -eq 0 ]; then
              echo "❌ ERROR: No valid job pairs found!"
              echo "   None of the specified jobs exist in both workspaces."
              echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
              exit 1
            fi
            
            echo ""
            echo "✅ Built matrix with ${COUNT} job pair(s)"
            if [ $UNMATCHED_COUNT -gt 0 ]; then
              echo "⚠️  ${UNMATCHED_COUNT} job name(s) could not be resolved"
            fi
            echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            
            echo "matrix=${MATRIX}" >> $GITHUB_OUTPUT
            echo "count=${COUNT}" >> $GITHUB_OUTPUT
            echo "matched_count=${COUNT}" >> $GITHUB_OUTPUT
            echo "unmatched_secondary=${UNMATCHED_COUNT}" >> $GITHUB_OUTPUT
            echo "unmatched_primary=0" >> $GITHUB_OUTPUT
            
          else
            # all-in-scope mode: Read minimal job data (name + ID) from discover outputs
            SECONDARY_JOBS='${{ needs.discover-secondary.outputs.jobs_json }}'
            PRIMARY_JOBS='${{ needs.discover-primary.outputs.jobs_json }}'
            
            echo "Secondary workspace: ${{ needs.discover-secondary.outputs.job_count }} jobs"
            echo "Primary workspace: ${{ needs.discover-primary.outputs.job_count }} jobs"
            echo ""
            
            # Debug: Check if job data was received
            echo "🔍 Debug - Checking job data..."
            if [ -z "$SECONDARY_JOBS" ]; then
              echo "❌ ERROR: SECONDARY_JOBS is empty!"
            else
              echo "✅ SECONDARY_JOBS received (length: ${#SECONDARY_JOBS} chars)"
              echo "   First 100 chars: ${SECONDARY_JOBS:0:100}"
            fi
            
            if [ -z "$PRIMARY_JOBS" ]; then
              echo "❌ ERROR: PRIMARY_JOBS is empty!"
            else
              echo "✅ PRIMARY_JOBS received (length: ${#PRIMARY_JOBS} chars)"
              echo "   First 100 chars: ${PRIMARY_JOBS:0:100}"
            fi
            echo ""
            
            echo "Matching jobs by name..."
            echo ""
            
            # Build matched pairs matrix
            MATRIX='{"include":['
            MATCHED_COUNT=0
            UNMATCHED_SEC=0
            UNMATCHED_PRI=0
            
            # Iterate through secondary jobs and find matches in primary
            while IFS= read -r SEC_JOB; do
              SEC_NAME=$(echo "$SEC_JOB" | jq -r '.name')
              SEC_ID=$(echo "$SEC_JOB" | jq -r '.job_id')
              
              # Look for matching job in primary by name
              PRI_JOB=$(echo "$PRIMARY_JOBS" | jq -c --arg name "$SEC_NAME" '.[] | select(.name == $name)' | head -n 1)
              
              if [ -n "$PRI_JOB" ]; then
                PRI_ID=$(echo "$PRI_JOB" | jq -r '.job_id')
                
                # Found a match!
                if [ $MATCHED_COUNT -gt 0 ]; then
                  MATRIX="${MATRIX},"
                fi
                
                MATRIX="${MATRIX}{\"name\":\"${SEC_NAME}\",\"primary_id\":\"${PRI_ID}\",\"secondary_id\":\"${SEC_ID}\"}"
                echo "  ✅ MATCHED: ${SEC_NAME}"
                echo "     Secondary ID: ${SEC_ID}"
                echo "     Primary ID: ${PRI_ID}"
                echo ""
                MATCHED_COUNT=$((MATCHED_COUNT + 1))
              else
                echo "  ⚠️  UNMATCHED (secondary only): ${SEC_NAME} (ID: ${SEC_ID})"
                echo ""
                UNMATCHED_SEC=$((UNMATCHED_SEC + 1))
              fi
            done < <(echo "$SECONDARY_JOBS" | jq -c '.[]')
            
            # Check for primary jobs without secondary matches
            while IFS= read -r PRI_JOB; do
              PRI_NAME=$(echo "$PRI_JOB" | jq -r '.name')
              PRI_ID=$(echo "$PRI_JOB" | jq -r '.job_id')
              
              # Check if this primary job was already matched
              SEC_JOB=$(echo "$SECONDARY_JOBS" | jq -c --arg name "$PRI_NAME" '.[] | select(.name == $name)' | head -n 1)
              
              if [ -z "$SEC_JOB" ]; then
                echo "  ⚠️  UNMATCHED (primary only): ${PRI_NAME} (ID: ${PRI_ID})"
                echo ""
                UNMATCHED_PRI=$((UNMATCHED_PRI + 1))
              fi
            done < <(echo "$PRIMARY_JOBS" | jq -c '.[]')
            
            MATRIX="${MATRIX}]}"
            
            echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            echo "📊 MATCHING SUMMARY"
            echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            echo "✅ Matched pairs: ${MATCHED_COUNT}"
            echo "⚠️  Unmatched (secondary only): ${UNMATCHED_SEC}"
            echo "⚠️  Unmatched (primary only): ${UNMATCHED_PRI}"
            echo ""
            
            if [ $MATCHED_COUNT -eq 0 ]; then
              echo "❌ ERROR: No matched job pairs found!"
              echo "   Operations would affect no jobs."
              echo ""
              echo "Possible causes:"
              echo "  - Job names don't match between workspaces"
              echo "  - Service Principal lacks permissions"
              echo "  - Wrong scope_code specified"
              echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
              exit 1
            fi
            
            echo "ℹ️  Only matched pairs will be processed in DR operations"
            echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            
            echo "matrix=${MATRIX}" >> $GITHUB_OUTPUT
            echo "count=${MATCHED_COUNT}" >> $GITHUB_OUTPUT
            echo "matched_count=${MATCHED_COUNT}" >> $GITHUB_OUTPUT
            echo "unmatched_secondary=${UNMATCHED_SEC}" >> $GITHUB_OUTPUT
            echo "unmatched_primary=${UNMATCHED_PRI}" >> $GITHUB_OUTPUT
          fi

  # ============================================================================
  # ACTION-SPECIFIC JOB SEQUENCES (correct dependency ordering)
  # ============================================================================
  
  # STATUS: Check both workspaces (order doesn't matter)
  status-check:
    runs-on: ubuntu-latest
    needs: build-matrix
    if: always() && needs.build-matrix.result == 'success' && github.event.inputs.action == 'status' && fromJSON(needs.build-matrix.outputs.job_count) > 0
    environment: ${{ github.event.inputs.environment }}_secondary
    strategy:
      matrix: ${{ fromJson(needs.build-matrix.outputs.job_matrix) }}
      fail-fast: false
      max-parallel: 10

    steps:
      - name: Azure CLI Login (OIDC)
        uses: azure/login@v2
        with:
          client-id: ${{ vars.SP_CLIENT_ID }}
          tenant-id: ${{ vars.AZ_TENANT_ID }}
          audience: api://AzureADTokenExchange
          allow-no-subscriptions: true

      - name: Set up Databricks CLI
        uses: databricks/setup-cli@main

      - name: Execute on Secondary Workspace
        env:
          DATABRICKS_HOST: ${{ vars.DATABRICKS_HOST }}
          ARM_CLIENT_ID: ${{ vars.SP_CLIENT_ID }}
          ARM_TENANT_ID: ${{ vars.AZ_TENANT_ID }}
          DATABRICKS_AUTH_TYPE: azure-cli
        run: |
          ACTION="${{ github.event.inputs.action }}"
          DRY_RUN="${{ github.event.inputs.dry_run }}"
          
          # Get job IDs from matrix
          JOB_ID="${{ matrix.secondary_id }}"
          JOB_NAME="${{ matrix.name }}"
              
          echo ""
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          echo "Job Name    : ${JOB_NAME}"
          echo "Workspace   : Secondary (${{ github.event.inputs.environment }}_secondary)"
          echo "Job ID      : ${JOB_ID}"
          echo "Action      : ${ACTION}"
          if [ "$DRY_RUN" = "true" ]; then
            echo "Mode        : 🔍 DRY-RUN (preview only)"
          fi
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
              
          # Get job details
          JOB_JSON=$(databricks jobs get "$JOB_ID" --output json)
          CRON=$(echo "$JOB_JSON" | jq -r '.settings.schedule.quartz_cron_expression // empty')
          TZ=$(echo "$JOB_JSON" | jq -r '.settings.schedule.timezone_id // "UTC"')
          CURRENT_STATUS=$(echo "$JOB_JSON" | jq -r '.settings.schedule.pause_status // "NO_SCHEDULE"')
          
          echo "Current Status: ${CURRENT_STATUS}"
          if [ -n "$CRON" ]; then
            echo "Schedule: ${CRON} (${TZ})"
          else
            echo "Schedule: None (manual trigger only)"
          fi
          echo ""
         
          # Function to cancel a single run with retry logic
          cancel_run_with_retry() {
            local RUN_ID=$1
            local MAX_ATTEMPTS=3
            local DELAY=2
            
            for attempt in $(seq 1 $MAX_ATTEMPTS); do
              if databricks runs cancel --run-id "$RUN_ID" 2>/dev/null; then
                echo "     ✅ Cancelled run ${RUN_ID} (attempt ${attempt}/${MAX_ATTEMPTS})"
                return 0
              else
                if [ $attempt -lt $MAX_ATTEMPTS ]; then
                  echo "     ⚠️  Attempt ${attempt} failed, retrying in ${DELAY}s..."
                  sleep $DELAY
                  DELAY=$((DELAY * 2))  # Exponential backoff
                else
                  echo "     ❌ Failed to cancel run ${RUN_ID} after ${MAX_ATTEMPTS} attempts"
                  return 1
                fi
              fi
            done
          }
          
          # Function to cancel active runs before DR operations (secondary workspace)
          cancel_active_runs() {
            local JOB_ID=$1
            local DRY_RUN=$2
            local TIMEOUT_BEHAVIOR="block"
            
            echo "🔍 Checking for active runs..."
            ACTIVE_RUNS=$(databricks jobs list-runs --job-id "$JOB_ID" --active-only --output json 2>/dev/null || echo '{"runs":[]}')
            
            # Handle both response formats
            if echo "$ACTIVE_RUNS" | jq -e '.runs' > /dev/null 2>&1; then
              RUN_COUNT=$(echo "$ACTIVE_RUNS" | jq '.runs | length')
              RUN_IDS=$(echo "$ACTIVE_RUNS" | jq -r '.runs[].run_id')
            else
              RUN_COUNT=$(echo "$ACTIVE_RUNS" | jq '. | length')
              RUN_IDS=$(echo "$ACTIVE_RUNS" | jq -r '.[].run_id')
            fi
            
            if [ "$RUN_COUNT" -gt 0 ]; then
              echo "⚠️  Found ${RUN_COUNT} active run(s)"
              
              if [ "$DRY_RUN" = "true" ]; then
                echo "🔍 DRY-RUN: Would cancel the following runs:"
                echo "$RUN_IDS" | while read -r RUN_ID; do
                  echo "     - Run ID: ${RUN_ID}"
                done
              else
                echo "🚨 Cancelling active runs before DR operation..."
                
                # Track failed cancellations
                FAILED_CANCELLATIONS=()
                
                # Cancel each active run with retry logic
                while IFS= read -r RUN_ID; do
                  if ! cancel_run_with_retry "$RUN_ID"; then
                    FAILED_CANCELLATIONS+=("$RUN_ID")
                  fi
                done <<< "$RUN_IDS"
                
                # Check if any cancellations failed
                if [ ${#FAILED_CANCELLATIONS[@]} -gt 0 ]; then
                  echo ""
                  echo "❌ ERROR: Failed to cancel ${#FAILED_CANCELLATIONS[@]} run(s) after retries:"
                  printf '     - Run ID: %s\n' "${FAILED_CANCELLATIONS[@]}"
                  echo ""
                  echo "⛔ BLOCKING DR operation to prevent overlapping runs"
                  echo "   Manual intervention required to:"
                  echo "   1. Investigate why runs couldn't be cancelled"
                  echo "   2. Manually cancel or wait for completion"
                  echo "   3. Then retry DR operation"
                  exit 1
                fi
                
                # Wait for cancellations to complete (with timeout)
                echo "     Waiting for cancellations to complete..."
                TIMEOUT=120
                ELAPSED=0
                
                while [ $ELAPSED -lt $TIMEOUT ]; do
                  STILL_ACTIVE=$(databricks jobs list-runs --job-id "$JOB_ID" --active-only --output json 2>/dev/null || echo '{"runs":[]}')
                  if echo "$STILL_ACTIVE" | jq -e '.runs' > /dev/null 2>&1; then
                    STILL_ACTIVE_COUNT=$(echo "$STILL_ACTIVE" | jq '.runs | length')
                  else
                    STILL_ACTIVE_COUNT=$(echo "$STILL_ACTIVE" | jq '. | length')
                  fi
                  
                  if [ "$STILL_ACTIVE_COUNT" -eq 0 ]; then
                    echo "     ✅ All runs cancelled successfully"
                    break
                  fi
                  
                  sleep 5
                  ELAPSED=$((ELAPSED + 5))
                done
                
                if [ $ELAPSED -ge $TIMEOUT ]; then
                  echo ""
                  echo "⏱️  TIMEOUT: Run cancellation verification exceeded ${TIMEOUT}s"
                  echo "   ${STILL_ACTIVE_COUNT} run(s) still active"
                  echo ""
                  
                  if [ "$TIMEOUT_BEHAVIOR" = "block" ]; then
                    echo "⛔ BLOCKING DR operation (job cancellation timeout)"
                    echo "   This prevents overlapping runs and ensures data integrity"
                    echo ""
                    echo "   Manual options:"
                    echo "   1. Wait longer for runs to complete naturally"
                    echo "   2. Manually cancel runs via Databricks UI"
                    echo "   3. Force-kill cluster if runs are stuck"
                    echo "   4. Retry DR operation after resolution"
                    exit 1
                  fi
                fi
              fi
            else
              echo "✅ No active runs - safe to proceed"
            fi
            echo ""
          }
          
          if [ "$ACTION" = "status" ]; then
            echo "📊 Job Status:"
            databricks jobs get "$JOB_ID"
          
          elif [ "$ACTION" = "failover" ]; then
            # Cancel any active runs before failover
            cancel_active_runs "$JOB_ID" "$DRY_RUN"
            if [ "$DRY_RUN" = "true" ]; then
              echo "🔍 DRY-RUN: Would activate secondary job (step 2/2)"
              if [ -n "$CRON" ]; then
                echo "   → Would UNPAUSE schedule: ${CRON}"
                echo "   → Current: ${CURRENT_STATUS} → Target: UNPAUSED"
              else
                echo "   → No schedule configured (manual trigger only)"
              fi
            else
              echo "⚡ FAILOVER (2/2): Activating secondary job..."
              if [ -n "$CRON" ]; then
                databricks api post /api/2.1/jobs/update \
                  --json "{\"job_id\": $JOB_ID, \"new_settings\": {\"schedule\": {\"quartz_cron_expression\": \"$CRON\", \"timezone_id\": \"$TZ\", \"pause_status\": \"UNPAUSED\"}}}"
                echo "✅ Secondary job ACTIVE (schedule unpaused)"
              else
                echo "⚠️  Job has no schedule - skipping unpause"
              fi
            fi
          
          elif [ "$ACTION" = "failback" ]; then
            # Cancel any active runs before failback
            cancel_active_runs "$JOB_ID" "$DRY_RUN"
            
            if [ "$DRY_RUN" = "true" ]; then
              echo "🔍 DRY-RUN: Would pause secondary job (step 1/2)"
              if [ -n "$CRON" ]; then
                echo "   → Would PAUSE schedule: ${CRON}"
                echo "   → Current: ${CURRENT_STATUS} → Target: PAUSED"
              else
                echo "   → No schedule configured (manual trigger only)"
              fi
            else
              echo "🔄 FAILBACK (1/2): Pausing secondary job..."
              if [ -n "$CRON" ]; then
                databricks api post /api/2.1/jobs/update \
                  --json "{\"job_id\": $JOB_ID, \"new_settings\": {\"schedule\": {\"quartz_cron_expression\": \"$CRON\", \"timezone_id\": \"$TZ\", \"pause_status\": \"PAUSED\"}}}"
                echo "✅ Secondary job PAUSED"
              else
                echo "⚠️  Job has no schedule - skipping pause"
              fi
            fi
          fi

  # FAILOVER SEQUENCE: Primary FIRST (cancel/pause), then Secondary (activate)
  failover-primary:
    runs-on: ubuntu-latest
    needs: build-matrix
    if: always() && needs.build-matrix.result == 'success' && github.event.inputs.action == 'failover' && github.event.inputs.disaster_mode != 'true' && fromJSON(needs.build-matrix.outputs.job_count) > 0
    environment: ${{ github.event.inputs.environment }}_primary
    strategy:
      matrix: ${{ fromJson(needs.build-matrix.outputs.job_matrix) }}
      fail-fast: false
      max-parallel: 10

    steps:
      - name: Azure CLI Login (OIDC)
        uses: azure/login@v2
        with:
          client-id: ${{ vars.SP_CLIENT_ID }}
          tenant-id: ${{ vars.AZ_TENANT_ID }}
          audience: api://AzureADTokenExchange
          allow-no-subscriptions: true

      - name: Set up Databricks CLI
        uses: databricks/setup-cli@main

      - name: Execute on Primary Workspace
        env:
          DATABRICKS_HOST: ${{ vars.DATABRICKS_HOST }}
          ARM_CLIENT_ID: ${{ vars.SP_CLIENT_ID }}
          ARM_TENANT_ID: ${{ vars.AZ_TENANT_ID }}
          DATABRICKS_AUTH_TYPE: azure-cli
        run: |
          ACTION="${{ github.event.inputs.action }}"
          DRY_RUN="${{ github.event.inputs.dry_run }}"
          
          # Get job IDs from matrix
          JOB_ID="${{ matrix.primary_id }}"
          JOB_NAME="${{ matrix.name }}"
              
          echo ""
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          echo "Job Name    : ${JOB_NAME}"
          echo "Workspace   : Primary (${{ github.event.inputs.environment }}_primary)"
          echo "Job ID      : ${JOB_ID}"
          echo "Action      : ${ACTION}"
          if [ "$DRY_RUN" = "true" ]; then
            echo "Mode        : 🔍 DRY-RUN (preview only)"
          fi
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
              
          # Get job details
          JOB_JSON=$(databricks jobs get "$JOB_ID" --output json)
          CRON=$(echo "$JOB_JSON" | jq -r '.settings.schedule.quartz_cron_expression // empty')
          TZ=$(echo "$JOB_JSON" | jq -r '.settings.schedule.timezone_id // "UTC"')
          CURRENT_STATUS=$(echo "$JOB_JSON" | jq -r '.settings.schedule.pause_status // "NO_SCHEDULE"')
          
          echo "Current Status: ${CURRENT_STATUS}"
          if [ -n "$CRON" ]; then
            echo "Schedule: ${CRON} (${TZ})"
          else
            echo "Schedule: None (manual trigger only)"
          fi
          echo ""
          
          # Helper function to cancel a run with retries
          cancel_run_with_retry() {
            local RUN_ID=$1
            local MAX_ATTEMPTS=3
            local DELAY=2
            
            for attempt in $(seq 1 $MAX_ATTEMPTS); do
              if databricks runs cancel --run-id "$RUN_ID" 2>/dev/null; then
                echo "     ✅ Cancelled run ${RUN_ID} (attempt ${attempt}/${MAX_ATTEMPTS})"
                return 0
              else
                if [ $attempt -lt $MAX_ATTEMPTS ]; then
                  echo "     ⚠️  Attempt ${attempt} failed to cancel run ${RUN_ID}, retrying in ${DELAY}s..."
                  sleep $DELAY
                  DELAY=$((DELAY * 2))  # Exponential backoff
                else
                  echo "     ❌ ERROR: Failed to cancel run ${RUN_ID} after ${MAX_ATTEMPTS} attempts"
                  return 1
                fi
              fi
            done
          }
          
          # Function to cancel active runs before DR operations
          cancel_active_runs() {
            local JOB_ID=$1
            local DRY_RUN=$2
            local TIMEOUT_BEHAVIOR="block"
            
            echo "🔍 Checking for active runs..."
            ACTIVE_RUNS=$(databricks jobs list-runs --job-id "$JOB_ID" --active-only --output json 2>/dev/null || echo '{"runs":[]}')
            
            # Handle both response formats
            if echo "$ACTIVE_RUNS" | jq -e '.runs' > /dev/null 2>&1; then
              RUN_COUNT=$(echo "$ACTIVE_RUNS" | jq '.runs | length')
              RUN_IDS=$(echo "$ACTIVE_RUNS" | jq -r '.runs[].run_id')
            else
              RUN_COUNT=$(echo "$ACTIVE_RUNS" | jq '. | length')
              RUN_IDS=$(echo "$ACTIVE_RUNS" | jq -r '.[].run_id')
            fi
            
            if [ "$RUN_COUNT" -gt 0 ]; then
              echo "⚠️  Found ${RUN_COUNT} active run(s)"
              
              if [ "$DRY_RUN" = "true" ]; then
                echo "🔍 DRY-RUN: Would cancel the following runs:"
                echo "$RUN_IDS" | while read -r RUN_ID; do
                  echo "     - Run ID: ${RUN_ID}"
                done
              else
                echo "🚨 Cancelling active runs before DR operation..."
                
                # Cancel each active run with retry logic
                FAILED_CANCELLATIONS=()
                while IFS= read -r RUN_ID; do
                  if ! cancel_run_with_retry "$RUN_ID"; then
                    FAILED_CANCELLATIONS+=("$RUN_ID")
                  fi
                done <<< "$RUN_IDS"
                
                # If any cancellations failed after retries, block the DR operation
                if [ ${#FAILED_CANCELLATIONS[@]} -gt 0 ]; then
                  echo ""
                  echo "❌ ERROR: Failed to cancel ${#FAILED_CANCELLATIONS[@]} run(s) after ${MAX_ATTEMPTS} retry attempts:"
                  printf '     - Run ID: %s\n' "${FAILED_CANCELLATIONS[@]}"
                  echo ""
                  echo "⛔ BLOCKING DR operation to prevent overlapping runs"
                  echo "   Manual intervention required before proceeding:"
                  echo "   1. Check job status: databricks jobs get $JOB_ID"
                  echo "   2. Manual cancel: databricks runs cancel --run-id <RUN_ID>"
                  echo "   3. Force cluster termination if needed"
                  echo "   4. Re-run workflow after resolving"
                  exit 1
                fi
                
                # Wait for cancellations to complete (with timeout)
                echo "     Waiting for cancellations to complete..."
                TIMEOUT=120
                ELAPSED=0
                
                while [ $ELAPSED -lt $TIMEOUT ]; do
                  STILL_ACTIVE=$(databricks jobs list-runs --job-id "$JOB_ID" --active-only --output json 2>/dev/null || echo '{"runs":[]}')
                  if echo "$STILL_ACTIVE" | jq -e '.runs' > /dev/null 2>&1; then
                    STILL_ACTIVE_COUNT=$(echo "$STILL_ACTIVE" | jq '.runs | length')
                  else
                    STILL_ACTIVE_COUNT=$(echo "$STILL_ACTIVE" | jq '. | length')
                  fi
                  
                  if [ "$STILL_ACTIVE_COUNT" -eq 0 ]; then
                    echo "     ✅ All runs cancelled and confirmed stopped"
                    break
                  fi
                  
                  sleep 5
                  ELAPSED=$((ELAPSED + 5))
                done
                
                if [ $ELAPSED -ge $TIMEOUT ]; then
                  echo ""
                  echo "     ⚠️  WARNING: Timeout waiting for run cancellation confirmation (${TIMEOUT}s)"
                  if [ "$TIMEOUT_BEHAVIOR" = "block" ]; then
                    echo "     ⛔ BLOCKING DR operation (job cancellation timeout)"
                    echo "        This prevents overlapping runs and ensures data integrity"
                    echo "        Manual options:"
                    echo "        1. Wait longer for runs to finish cancelling"
                    echo "        2. Manually verify runs are cancelled: databricks jobs list-runs --job-id $JOB_ID --active-only"
                    echo "        3. Force cluster termination if needed"
                    echo "        4. Retry DR operation after resolution"
                    exit 1
                  fi
                fi
              fi
            else
              echo "✅ No active runs - safe to proceed"
            fi
            echo ""
          }
          
          if [ "$ACTION" = "status" ]; then
            echo "📊 Job Status:"
            databricks jobs get "$JOB_ID"
          
          elif [ "$ACTION" = "failover" ]; then
            # Cancel any active runs before failover
            cancel_active_runs "$JOB_ID" "$DRY_RUN"
            
            if [ "$DRY_RUN" = "true" ]; then
              echo "🔍 DRY-RUN: Would pause primary job (step 1/2)"
              if [ -n "$CRON" ]; then
                echo "   → Would PAUSE schedule: ${CRON}"
                echo "   → Current: ${CURRENT_STATUS} → Target: PAUSED"
              else
                echo "   → No schedule configured (manual trigger only)"
              fi
            else
              echo "⚡ FAILOVER (1/2): Pausing primary job..."
              if [ -n "$CRON" ]; then
                databricks api post /api/2.1/jobs/update \
                  --json "{\"job_id\": $JOB_ID, \"new_settings\": {\"schedule\": {\"quartz_cron_expression\": \"$CRON\", \"timezone_id\": \"$TZ\", \"pause_status\": \"PAUSED\"}}}"
                echo "✅ Primary job PAUSED"
              else
                echo "⚠️  Job has no schedule - skipping pause"
              fi
            fi
          
          elif [ "$ACTION" = "failback" ]; then
            # Cancel any active runs before failback
            cancel_active_runs "$JOB_ID" "$DRY_RUN"
            
            if [ "$DRY_RUN" = "true" ]; then
              echo "🔍 DRY-RUN: Would activate primary job (step 2/2)"
              if [ -n "$CRON" ]; then
                echo "   → Would UNPAUSE schedule: ${CRON}"
                echo "   → Current: ${CURRENT_STATUS} → Target: UNPAUSED"
              else
                echo "   → No schedule configured (manual trigger only)"
              fi
            else
              echo "✅ FAILBACK (2/2): Activating primary job..."
              if [ -n "$CRON" ]; then
                databricks api post /api/2.1/jobs/update \
                  --json "{\"job_id\": $JOB_ID, \"new_settings\": {\"schedule\": {\"quartz_cron_expression\": \"$CRON\", \"timezone_id\": \"$TZ\", \"pause_status\": \"UNPAUSED\"}}}"
                echo "✅ Primary job ACTIVE - Failback complete"
              else
                echo "⚠️  Job has no schedule - skipping unpause"
              fi
            fi
          fi

  failover-secondary:
    runs-on: ubuntu-latest
    needs: [build-matrix, failover-primary]
    if: always() && needs.build-matrix.result == 'success' && github.event.inputs.action == 'failover' && fromJSON(needs.build-matrix.outputs.job_count) > 0
    environment: ${{ github.event.inputs.environment }}_secondary
    strategy:
      matrix: ${{ fromJson(needs.build-matrix.outputs.job_matrix) }}
      fail-fast: false
      max-parallel: 10

    steps:
      - name: Azure CLI Login (OIDC)
        uses: azure/login@v2
        with:
          client-id: ${{ vars.SP_CLIENT_ID }}
          tenant-id: ${{ vars.AZ_TENANT_ID }}
          audience: api://AzureADTokenExchange
          allow-no-subscriptions: true

      - name: Set up Databricks CLI
        uses: databricks/setup-cli@main

      - name: Execute on Secondary Workspace
        env:
          DATABRICKS_HOST: ${{ vars.DATABRICKS_HOST }}
          ARM_CLIENT_ID: ${{ vars.SP_CLIENT_ID }}
          ARM_TENANT_ID: ${{ vars.AZ_TENANT_ID }}
          DATABRICKS_AUTH_TYPE: azure-cli
        run: |
          ACTION="${{ github.event.inputs.action }}"
          DRY_RUN="${{ github.event.inputs.dry_run }}"
          
          # Get job IDs from matrix
          JOB_ID="${{ matrix.secondary_id }}"
          JOB_NAME="${{ matrix.name }}"
              
          echo ""
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          echo "Job Name    : ${JOB_NAME}"
          echo "Workspace   : Secondary (${{ github.event.inputs.environment }}_secondary)"
          echo "Job ID      : ${JOB_ID}"
          echo "Action      : ${ACTION}"
          if [ "$DRY_RUN" = "true" ]; then
            echo "Mode        : 🔍 DRY-RUN (preview only)"
          fi
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
              
          # Get job details
          JOB_JSON=$(databricks jobs get "$JOB_ID" --output json)
          CRON=$(echo "$JOB_JSON" | jq -r '.settings.schedule.quartz_cron_expression // empty')
          TZ=$(echo "$JOB_JSON" | jq -r '.settings.schedule.timezone_id // "UTC"')
          CURRENT_STATUS=$(echo "$JOB_JSON" | jq -r '.settings.schedule.pause_status // "NO_SCHEDULE"')
          
          echo "Current Status: ${CURRENT_STATUS}"
          if [ -n "$CRON" ]; then
            echo "Schedule: ${CRON} (${TZ})"
          else
            echo "Schedule: None (manual trigger only)"
          fi
          echo ""
          
          # Cancel any active runs before failover (defensive check)
          echo "🔍 Checking for active runs..."
          ACTIVE_RUNS=$(databricks jobs list-runs --job-id "$JOB_ID" --active-only --output json 2>/dev/null || echo '{"runs":[]}')
          
          if echo "$ACTIVE_RUNS" | jq -e '.runs' > /dev/null 2>&1; then
            RUN_COUNT=$(echo "$ACTIVE_RUNS" | jq '.runs | length')
          else
            RUN_COUNT=$(echo "$ACTIVE_RUNS" | jq '. | length')
          fi
          
          if [ "$RUN_COUNT" -gt 0 ]; then
            echo "⚠️  Found ${RUN_COUNT} unexpected active run(s) - should be paused at this stage"
          else
            echo "✅ No active runs - safe to proceed"
          fi
          echo ""
          
          if [ "$DRY_RUN" = "true" ]; then
            echo "🔍 DRY-RUN: Would activate secondary job (step 2/2)"
            if [ -n "$CRON" ]; then
              echo "   → Would UNPAUSE schedule: ${CRON}"
              echo "   → Current: ${CURRENT_STATUS} → Target: UNPAUSED"
            else
              echo "   → No schedule configured (manual trigger only)"
            fi
          else
            echo "⚡ FAILOVER (2/2): Activating secondary job..."
            if [ -n "$CRON" ]; then
              databricks api post /api/2.1/jobs/update \
                --json "{\"job_id\": $JOB_ID, \"new_settings\": {\"schedule\": {\"quartz_cron_expression\": \"$CRON\", \"timezone_id\": \"$TZ\", \"pause_status\": \"UNPAUSED\"}}}"
              echo "✅ Secondary job ACTIVE - Failover complete"
            else
              echo "⚠️  Job has no schedule - skipping unpause"
            fi
          fi

  # FAILBACK SEQUENCE: Secondary FIRST (pause), then Primary (activate)
  failback-secondary:
    runs-on: ubuntu-latest
    needs: build-matrix
    if: always() && needs.build-matrix.result == 'success' && github.event.inputs.action == 'failback' && fromJSON(needs.build-matrix.outputs.job_count) > 0
    environment: ${{ github.event.inputs.environment }}_secondary
    strategy:
      matrix: ${{ fromJson(needs.build-matrix.outputs.job_matrix) }}
      fail-fast: false
      max-parallel: 10

    steps:
      - name: Azure CLI Login (OIDC)
        uses: azure/login@v2
        with:
          client-id: ${{ vars.SP_CLIENT_ID }}
          tenant-id: ${{ vars.AZ_TENANT_ID }}
          audience: api://AzureADTokenExchange
          allow-no-subscriptions: true

      - name: Set up Databricks CLI
        uses: databricks/setup-cli@main

      - name: Execute on Secondary Workspace
        env:
          DATABRICKS_HOST: ${{ vars.DATABRICKS_HOST }}
          ARM_CLIENT_ID: ${{ vars.SP_CLIENT_ID }}
          ARM_TENANT_ID: ${{ vars.AZ_TENANT_ID }}
          DATABRICKS_AUTH_TYPE: azure-cli
        run: |
          ACTION="${{ github.event.inputs.action }}"
          DRY_RUN="${{ github.event.inputs.dry_run }}"
          
          # Get job IDs from matrix
          JOB_ID="${{ matrix.secondary_id }}"
          JOB_NAME="${{ matrix.name }}"
              
          echo ""
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          echo "Job Name    : ${JOB_NAME}"
          echo "Workspace   : Secondary (${{ github.event.inputs.environment }}_secondary)"
          echo "Job ID      : ${JOB_ID}"
          echo "Action      : ${ACTION}"
          if [ "$DRY_RUN" = "true" ]; then
            echo "Mode        : 🔍 DRY-RUN (preview only)"
          fi
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
              
          # Get job details
          JOB_JSON=$(databricks jobs get "$JOB_ID" --output json)
          CRON=$(echo "$JOB_JSON" | jq -r '.settings.schedule.quartz_cron_expression // empty')
          TZ=$(echo "$JOB_JSON" | jq -r '.settings.schedule.timezone_id // "UTC"')
          CURRENT_STATUS=$(echo "$JOB_JSON" | jq -r '.settings.schedule.pause_status // "NO_SCHEDULE"')
          
          echo "Current Status: ${CURRENT_STATUS}"
          if [ -n "$CRON" ]; then
            echo "Schedule: ${CRON} (${TZ})"
          else
            echo "Schedule: None (manual trigger only)"
          fi
          echo ""
          
          # Helper function to cancel a run with retries
          cancel_run_with_retry() {
            local RUN_ID=$1
            local MAX_ATTEMPTS=3
            local DELAY=2
            
            for attempt in $(seq 1 $MAX_ATTEMPTS); do
              if databricks runs cancel --run-id "$RUN_ID" 2>/dev/null; then
                echo "     ✅ Cancelled run ${RUN_ID} (attempt ${attempt}/${MAX_ATTEMPTS})"
                return 0
              else
                if [ $attempt -lt $MAX_ATTEMPTS ]; then
                  echo "     ⚠️  Attempt ${attempt} failed to cancel run ${RUN_ID}, retrying in ${DELAY}s..."
                  sleep $DELAY
                  DELAY=$((DELAY * 2))
                else
                  echo "     ❌ ERROR: Failed to cancel run ${RUN_ID} after ${MAX_ATTEMPTS} attempts"
                  return 1
                fi
              fi
            done
          }
          
          # Cancel any active runs before failback
          echo "🔍 Checking for active runs..."
          ACTIVE_RUNS=$(databricks jobs list-runs --job-id "$JOB_ID" --active-only --output json 2>/dev/null || echo '{"runs":[]}')
          
          if echo "$ACTIVE_RUNS" | jq -e '.runs' > /dev/null 2>&1; then
            RUN_COUNT=$(echo "$ACTIVE_RUNS" | jq '.runs | length')
            RUN_IDS=$(echo "$ACTIVE_RUNS" | jq -r '.runs[].run_id')
          else
            RUN_COUNT=$(echo "$ACTIVE_RUNS" | jq '. | length')
            RUN_IDS=$(echo "$ACTIVE_RUNS" | jq -r '.[].run_id')
          fi
          
          if [ "$RUN_COUNT" -gt 0 ]; then
            echo "⚠️  Found ${RUN_COUNT} active run(s) - cancelling before failback"
            
            if [ "$DRY_RUN" != "true" ]; then
              FAILED_CANCELLATIONS=()
              while IFS= read -r RUN_ID; do
                if ! cancel_run_with_retry "$RUN_ID"; then
                  FAILED_CANCELLATIONS+=("$RUN_ID")
                fi
              done <<< "$RUN_IDS"
              
              if [ ${#FAILED_CANCELLATIONS[@]} -gt 0 ]; then
                echo "❌ ERROR: Failed to cancel ${#FAILED_CANCELLATIONS[@]} run(s)"
                echo "⛔ BLOCKING failback operation"
                exit 1
              fi
              
              # Wait for completion
              TIMEOUT=120
              ELAPSED=0
              while [ $ELAPSED -lt $TIMEOUT ]; do
                STILL_ACTIVE=$(databricks jobs list-runs --job-id "$JOB_ID" --active-only --output json 2>/dev/null || echo '{"runs":[]}')
                if echo "$STILL_ACTIVE" | jq -e '.runs' > /dev/null 2>&1; then
                  STILL_ACTIVE_COUNT=$(echo "$STILL_ACTIVE" | jq '.runs | length')
                else
                  STILL_ACTIVE_COUNT=$(echo "$STILL_ACTIVE" | jq '. | length')
                fi
                
                if [ "$STILL_ACTIVE_COUNT" -eq 0 ]; then
                  echo "✅ All runs cancelled"
                  break
                fi
                
                sleep 5
                ELAPSED=$((ELAPSED + 5))
              done
              
              if [ $ELAPSED -ge $TIMEOUT ]; then
                echo "⛔ BLOCKING failback (cancellation timeout)"
                exit 1
              fi
            fi
          else
            echo "✅ No active runs - safe to proceed"
          fi
          echo ""
          
          if [ "$DRY_RUN" = "true" ]; then
            echo "🔍 DRY-RUN: Would pause secondary job (step 1/2)"
            if [ -n "$CRON" ]; then
              echo "   → Would PAUSE schedule: ${CRON}"
              echo "   → Current: ${CURRENT_STATUS} → Target: PAUSED"
            else
              echo "   → No schedule configured (manual trigger only)"
            fi
          else
            echo "🔄 FAILBACK (1/2): Pausing secondary job..."
            if [ -n "$CRON" ]; then
              databricks api post /api/2.1/jobs/update \
                --json "{\"job_id\": $JOB_ID, \"new_settings\": {\"schedule\": {\"quartz_cron_expression\": \"$CRON\", \"timezone_id\": \"$TZ\", \"pause_status\": \"PAUSED\"}}}"
              echo "✅ Secondary job PAUSED"
            else
              echo "⚠️  Job has no schedule - skipping pause"
            fi
          fi

  failback-primary:
    runs-on: ubuntu-latest
    needs: [build-matrix, failback-secondary]
    if: always() && needs.build-matrix.result == 'success' && github.event.inputs.action == 'failback' && fromJSON(needs.build-matrix.outputs.job_count) > 0
    environment: ${{ github.event.inputs.environment }}_primary
    strategy:
      matrix: ${{ fromJson(needs.build-matrix.outputs.job_matrix) }}
      fail-fast: false
      max-parallel: 10

    steps:
      - name: Azure CLI Login (OIDC)
        uses: azure/login@v2
        with:
          client-id: ${{ vars.SP_CLIENT_ID }}
          tenant-id: ${{ vars.AZ_TENANT_ID }}
          audience: api://AzureADTokenExchange
          allow-no-subscriptions: true

      - name: Set up Databricks CLI
        uses: databricks/setup-cli@main

      - name: Execute on Primary Workspace
        env:
          DATABRICKS_HOST: ${{ vars.DATABRICKS_HOST }}
          ARM_CLIENT_ID: ${{ vars.SP_CLIENT_ID }}
          ARM_TENANT_ID: ${{ vars.AZ_TENANT_ID }}
          DATABRICKS_AUTH_TYPE: azure-cli
        run: |
          ACTION="${{ github.event.inputs.action }}"
          DRY_RUN="${{ github.event.inputs.dry_run }}"
          
          # Get job IDs from matrix
          JOB_ID="${{ matrix.primary_id }}"
          JOB_NAME="${{ matrix.name }}"
              
          echo ""
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          echo "Job Name    : ${JOB_NAME}"
          echo "Workspace   : Primary (${{ github.event.inputs.environment }}_primary)"
          echo "Job ID      : ${JOB_ID}"
          echo "Action      : ${ACTION}"
          if [ "$DRY_RUN" = "true" ]; then
            echo "Mode        : 🔍 DRY-RUN (preview only)"
          fi
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
              
          # Get job details
          JOB_JSON=$(databricks jobs get "$JOB_ID" --output json)
          CRON=$(echo "$JOB_JSON" | jq -r '.settings.schedule.quartz_cron_expression // empty')
          TZ=$(echo "$JOB_JSON" | jq -r '.settings.schedule.timezone_id // "UTC"')
          CURRENT_STATUS=$(echo "$JOB_JSON" | jq -r '.settings.schedule.pause_status // "NO_SCHEDULE"')
          
          echo "Current Status: ${CURRENT_STATUS}"
          if [ -n "$CRON" ]; then
            echo "Schedule: ${CRON} (${TZ})"
          else
            echo "Schedule: None (manual trigger only)"
          fi
          echo ""
          
          # Defensive check for active runs (should be none since was paused)
          echo "🔍 Checking for active runs..."
          ACTIVE_RUNS=$(databricks jobs list-runs --job-id "$JOB_ID" --active-only --output json 2>/dev/null || echo '{"runs":[]}')
          
          if echo "$ACTIVE_RUNS" | jq -e '.runs' > /dev/null 2>&1; then
            RUN_COUNT=$(echo "$ACTIVE_RUNS" | jq '.runs | length')
          else
            RUN_COUNT=$(echo "$ACTIVE_RUNS" | jq '. | length')
          fi
          
          if [ "$RUN_COUNT" -gt 0 ]; then
            echo "⚠️  Found ${RUN_COUNT} unexpected active run(s) - should be paused at this stage"
          else
            echo "✅ No active runs - safe to proceed"
          fi
          echo ""
          
          if [ "$DRY_RUN" = "true" ]; then
            echo "🔍 DRY-RUN: Would activate primary job (step 2/2)"
            if [ -n "$CRON" ]; then
              echo "   → Would UNPAUSE schedule: ${CRON}"
              echo "   → Current: ${CURRENT_STATUS} → Target: UNPAUSED"
            else
              echo "   → No schedule configured (manual trigger only)"
            fi
          else
            echo "✅ FAILBACK (2/2): Activating primary job..."
            if [ -n "$CRON" ]; then
              databricks api post /api/2.1/jobs/update \
                --json "{\"job_id\": $JOB_ID, \"new_settings\": {\"schedule\": {\"quartz_cron_expression\": \"$CRON\", \"timezone_id\": \"$TZ\", \"pause_status\": \"UNPAUSED\"}}}"
              echo "✅ Primary job ACTIVE - Failback complete"
            else
              echo "⚠️  Job has no schedule - skipping unpause"
            fi
          fi

  # Step 6: Summary
  summary:
    runs-on: ubuntu-latest
    needs: [validate-inputs, build-matrix, status-check, failover-primary, failover-secondary, failback-secondary, failback-primary]
    if: always()
    
    steps:
      - name: DR Switch Summary
        run: |
          MODE="${{ github.event.inputs.operation_mode }}"
          DRY_RUN="${{ github.event.inputs.dry_run }}"
          
          echo ""
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          if [ "$DRY_RUN" = "true" ]; then
            echo "🔍 DR SWITCH DRY-RUN SUMMARY (No Changes Made)"
          else
            echo "✅ DR SWITCH EXECUTION SUMMARY"
          fi
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          echo ""
          echo "📋 Configuration:"
          echo "   Operation   : ${MODE}"
          echo "   Action      : ${{ github.event.inputs.action }}"
          echo "   Environment : ${{ github.event.inputs.environment }}"
          echo "   Dry Run     : ${DRY_RUN}"
          echo "   Validation  : ${{ github.event.inputs.validate_pairing }}"
          echo "   Disaster Mode: ${{ github.event.inputs.disaster_mode }}"
          echo ""
          
          if [ "$MODE" = "all-in-scope" ]; then
            echo "🎯 Scope:"
            echo "   Scope Code     : ${{ github.event.inputs.scope_code }}"
            echo "   Discovery      : All jobs SP has access to"
            echo "   Matched Pairs  : ${{ needs.build-matrix.outputs.matched_count }}"
            echo "   Unmatched (Sec): ${{ needs.build-matrix.outputs.unmatched_secondary }}"
            echo "   Unmatched (Pri): ${{ needs.build-matrix.outputs.unmatched_primary }}"
            echo ""
          else
            echo "🎯 Selected Jobs:"
            echo "   Job Names   : ${{ github.event.inputs.job_names }}"
            echo "   Job Count   : ${{ needs.build-matrix.outputs.job_count }}"
            echo ""
          fi
          
          echo "👤 Execution Details:"
          echo "   Triggered by: ${{ github.actor }}"
          echo "   Timestamp   : $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
          echo ""
          
          echo "📊 Execution Results:"
          
          # Show which jobs were operated on
          JOB_MATRIX='${{ needs.build-matrix.outputs.job_matrix }}'
          JOB_COUNT=${{ needs.build-matrix.outputs.job_count }}
          
          if [ "$JOB_COUNT" -gt 0 ]; then
            echo ""
            echo "   📋 Jobs Operated On (${JOB_COUNT}):"
            echo "$JOB_MATRIX" | jq -r '.include[] | "      • \(.name)"'
            echo ""
            echo "   💡 Tip: Check individual job logs above for per-job status"
            echo "      (e.g., which jobs were paused/activated, which were skipped)"
          fi
          echo ""
          
          ACTION="${{ github.event.inputs.action }}"
          
          # Check results based on action (conditional jobs)
          if [ "$ACTION" = "status" ]; then
            # Status check job
            STATUS_RESULT="${{ needs.status-check.result }}"
            if [ "$STATUS_RESULT" = "success" ]; then
              echo "   ✅ Status check: SUCCESS"
            elif [ "$STATUS_RESULT" = "skipped" ]; then
              echo "   ⏭️  Status check: SKIPPED"
            else
              echo "   ❌ Status check: FAILED"
            fi
            
          elif [ "$ACTION" = "failover" ]; then
            # Failover sequence: primary → secondary
            FAILOVER_PRI_RESULT="${{ needs.failover-primary.result }}"
            FAILOVER_SEC_RESULT="${{ needs.failover-secondary.result }}"
            
            if [ "$FAILOVER_PRI_RESULT" = "success" ]; then
              echo "   ✅ Failover step 1/2 (Primary pause): SUCCESS"
            elif [ "$FAILOVER_PRI_RESULT" = "skipped" ]; then
              if [ "${{ github.event.inputs.disaster_mode }}" = "true" ]; then
                echo "   ⏭️  Failover step 1/2 (Primary pause): SKIPPED (disaster mode - primary region unreachable)"
              else
                echo "   ⏭️  Failover step 1/2 (Primary pause): SKIPPED"
              fi
            else
              echo "   ❌ Failover step 1/2 (Primary pause): FAILED"
            fi
            
            if [ "$FAILOVER_SEC_RESULT" = "success" ]; then
              echo "   ✅ Failover step 2/2 (Secondary activate): SUCCESS"
            elif [ "$FAILOVER_SEC_RESULT" = "skipped" ]; then
              echo "   ⏭️  Failover step 2/2 (Secondary activate): SKIPPED"
            else
              echo "   ❌ Failover step 2/2 (Secondary activate): FAILED"
            fi
            
          elif [ "$ACTION" = "failback" ]; then
            # Failback sequence: secondary → primary
            FAILBACK_SEC_RESULT="${{ needs.failback-secondary.result }}"
            FAILBACK_PRI_RESULT="${{ needs.failback-primary.result }}"
            
            if [ "$FAILBACK_SEC_RESULT" = "success" ]; then
              echo "   ✅ Failback step 1/2 (Secondary pause): SUCCESS"
            elif [ "$FAILBACK_SEC_RESULT" = "skipped" ]; then
              echo "   ⏭️  Failback step 1/2 (Secondary pause): SKIPPED"
            else
              echo "   ❌ Failback step 1/2 (Secondary pause): FAILED"
            fi
            
            if [ "$FAILBACK_PRI_RESULT" = "success" ]; then
              echo "   ✅ Failback step 2/2 (Primary activate): SUCCESS"
            elif [ "$FAILBACK_PRI_RESULT" = "skipped" ]; then
              echo "   ⏭️  Failback step 2/2 (Primary activate): SKIPPED"
            else
              echo "   ❌ Failback step 2/2 (Primary activate): FAILED"
            fi
          fi
          
          echo ""
          if [ "$DRY_RUN" = "true" ]; then
            echo "💡 Next Steps:"
            echo "   This was a preview run. No actual changes were made."
            echo "   Review the job details above, then run again with dry_run=false to execute."
          else
            echo "✅ DR switch operation completed."
            echo "   Check individual job logs above for detailed status."
          fi
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  # STEP FINAL: Record ServiceNow Change Request (post-deployment, prod failover/failback only)
  servicenow:
    needs: [tower_api, summary]
    if: |
      always() &&
      needs.tower_api.result == 'success' &&
      github.event.inputs.environment == 'prod' &&
      (github.event.inputs.action == 'failover' || github.event.inputs.action == 'failback') &&
      github.event.inputs.dry_run == 'false'
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
            Prod DR ${{ github.event.inputs.action }}: ${{ github.event.inputs.operation_mode }}
            Repository: ${{ github.repository }}
            Environment: ${{ github.event.inputs.environment }}
            Action: ${{ github.event.inputs.action }}
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
              <li><b>Workflow:</b> DR Switch (${{ github.event.inputs.action }})</li>
              <li><b>Environment:</b> ${{ github.event.inputs.environment }}</li>
              <li><b>Mode:</b> ${{ github.event.inputs.operation_mode }}</li>
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
              <li><b>Workflow:</b> DR Switch (${{ github.event.inputs.action }})</li>
              <li><b>Environment:</b> ${{ github.event.inputs.environment }}</li>
              <li><b>Mode:</b> ${{ github.event.inputs.operation_mode }}</li>
              <li><b>Pipeline Run:</b> <a href="https://github.com/${{ github.repository }}/actions/runs/${{ github.run_id }}">View Pipeline</a></li>
              <li><b>ServiceNow Ticket:</b> <a href="${{ steps.servicenow.outputs.change_request_url }}">${{ steps.servicenow.outputs.change_request_id }}</a></li>
            </ul>
          secretConfig: ${{ secrets.EMAILME_SECRET_CONFIG }}