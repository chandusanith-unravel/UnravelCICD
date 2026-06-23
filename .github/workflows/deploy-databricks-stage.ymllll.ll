name: deploy-databricks-stage

on:
  pull_request:
    types: [labeled]  # Only triggers when a developer adds a label
    branches: ["main"]
    paths:
      # Only trigger on deployment-relevant files (notebooks, pipelines, contracts)
      # Schema, test, and documentation changes do NOT trigger deployments
      # Supports both legacy full names and NorthStar abbreviations
      - 'bronze/*/notebooks/**'
      - 'bronze/*/pipelines/**'
      - 'bronze/*/contracts/**'
      - 'brz/*/notebooks/**'
      - 'brz/*/pipelines/**'
      - 'brz/*/contracts/**'
      - 'silver/notebooks/**'
      - 'silver/pipelines/**'
      - 'silver/contracts/**'
      - 'slv/notebooks/**'
      - 'slv/pipelines/**'
      - 'slv/contracts/**'
  workflow_dispatch:   # Allows manual trigger via GitHub Actions UI

permissions:
  contents: write
  actions: write
  pull-requests: write
  id-token: write    # Required for Azure OIDC authentication

# Ensure only one deploy-databricks-stage runs at a time per target branch.
# Uses base.ref for PRs so all PRs targeting main share one queue.
# Falls back to ref_name for workflow_dispatch / push events.
concurrency:
  group: deploy-databricks-stage-${{ github.event.pull_request.base.ref || github.ref_name }}
  cancel-in-progress: false

jobs:
  # STEP 1: Identify which specific bundles or common code changed
  detect-changes:
    # Condition: Run only if the 'deploy-stage' label was added (not removed) or if triggered manually
    if: |
      (github.event_name == 'pull_request' && github.event.action == 'labeled' && github.event.label.name == 'deploy-stage') ||
      (github.event_name == 'workflow_dispatch')
    
    runs-on: ubuntu-latest
    outputs:
      jobs_matrix: ${{ steps.detect.outputs.jobs }}
      has_changes: ${{ steps.detect.outputs.has_changes }}
      notebooks_changed: ${{ steps.detect.outputs.notebooks_changed }}
      notebook_only_changed: ${{ steps.detect.outputs.notebook_only_changed }}
      notebook_source_codes: ${{ steps.detect.outputs.notebook_source_codes }}
      bundle_and_notebook_source_codes: ${{ steps.detect.outputs.bundle_and_notebook_source_codes }}
      changed_zones: ${{ steps.detect.outputs.changed_zones }}  # Track which zones changed (bronze/silver)
    steps:
      - name: Checkout code
        uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - name: Detect changed DAB bundles
        id: detect
        run: |
          set +e  # Don't exit on error - we handle errors explicitly
          
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          echo "DETECTING CHANGED BUNDLES"
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          echo ""
          
          # For PRs (labeled), compare PR branch to main with explicit error handling
          git_fetch_output=$(git fetch origin main:refs/remotes/origin/main 2>&1)
          if [ $? -ne 0 ]; then
            echo "::error::Failed to fetch 'main' from 'origin'. Verify network connectivity, authentication, and that the branch exists."
            echo "$git_fetch_output"
            exit 1
          fi

          # Use two-dot syntax (..) for direct comparison: shows what PR adds to main
          # This matches prod workflow behavior and ensures consistent change detection
          git_diff_output=$(git diff --name-only origin/main..HEAD 2>&1)
          if [ $? -ne 0 ]; then
            echo "::error::Failed to compute git diff between 'origin/main' and 'HEAD'. See git output below for details."
            echo "$git_diff_output"
            exit 1
          fi
          changed_files="$git_diff_output"
          
          # Filter out markdown files and README files (documentation-only changes don't trigger deployments)
          changed_files=$(echo "$changed_files" | grep -vE '\.(md|MD)$' || true)
          changed_files=$(echo "$changed_files" | grep -viE '(readme|README)' || true)

          # Exit early if no bronze or silver directories exist (supports NorthStar abbreviations)
          if [ ! -d "bronze" ] && [ ! -d "brz" ] && [ ! -d "silver" ] && [ ! -d "slv" ]; then
            echo "jobs=[]" >> $GITHUB_OUTPUT
            echo "has_changes=false" >> $GITHUB_OUTPUT
            echo "notebooks_changed=false" >> $GITHUB_OUTPUT
            exit 0
          fi

          # Exit early if only markdown/README files changed
          if [ -z "$changed_files" ]; then
            echo "[INFO] Only documentation files changed - no bundle deployments needed"
            echo "jobs=[]" >> $GITHUB_OUTPUT
            echo "has_changes=false" >> $GITHUB_OUTPUT
            echo "notebooks_changed=false" >> $GITHUB_OUTPUT
            exit 0
          fi

          # Find all bundle directories (check both bronze and silver layers)
          # Supports both legacy names (bronze, silver) and NorthStar abbreviations (brz, slv)
          bundles_bronze=$(find bronze brz -type f -name 'databricks.yml' -path '*/pipelines/databricks/*' 2>/dev/null | xargs -I{} dirname {} || true)
          bundles_silver=$(find silver slv -type f -name 'databricks.yml' -path '*/pipelines/databricks/*' 2>/dev/null | xargs -I{} dirname {} || true)
          bundles=$(echo -e "${bundles_bronze}\n${bundles_silver}" | grep -v '^$' | sort -u)
          
          # Track which zones have changes (for smart filtering)
          changed_zones=""
          
          echo "Scanning bundles for changes..."
          echo ""
          jobs_list=""
          
          for bundle in $bundles; do
            # Extract layer, source_code, and contract_name from bundle path
            # Bronze/brz: bronze/{source_code}/pipelines/databricks/{contract_name}
            # Silver/slv: silver/pipelines/databricks/{contract_name}
            layer=$(echo "$bundle" | cut -d'/' -f1)
            if [ "$layer" = "bronze" ] || [ "$layer" = "brz" ]; then
              source_code=$(echo "$bundle" | cut -d'/' -f2)
              contract_name=$(basename "$bundle")
            else
              # Silver layer
              source_code=""
              contract_name=$(basename "$bundle")
            fi
            
            echo "Checking: $bundle"
            echo "   Layer: $layer, Source: $source_code, Contract: $contract_name"
            
            match_found=false
            match_reason=""
            
            # A) File changed inside the bundle folder (excluding *.md and README files)
            bundle_changes=$(echo "$changed_files" | grep -E "^$bundle/" || true)
            if [ -n "$bundle_changes" ]; then
              match_found=true
              match_reason="bundle folder change"
            fi
            
            # B) File changed in the contract folder specifically for this contract name
            #    (schema changes are excluded - they do NOT trigger deployments)
            #    Prevent substring matches: contract name must match at the start of the filename
            #    segment (after the last '/') and be followed by known suffixes (_contract, _contract_group)
            #    or separators (. or /). Plain underscore is NOT enough (avoids invtry matching invtry_seat).
            #    Example: flight-leg-seat-avail matches flight-leg-seat-avail_contract_group.json
            #             but NOT flight-leg-seat-avail-hist_contract_group.json
            if ([ "$layer" = "bronze" ] || [ "$layer" = "brz" ]) && [ -n "$source_code" ]; then
              contract_changes=$(echo "$changed_files" | grep -E "^(bronze|brz)/$source_code/contracts/.*/${contract_name}(_contract[_.]|[./])" || true)
              if [ -n "$contract_changes" ]; then
                match_found=true
                match_reason="contract folder change"
              fi
            elif [ "$layer" = "silver" ] || [ "$layer" = "slv" ]; then
              contract_changes=$(echo "$changed_files" | grep -E "^(silver|slv)/contracts/.*/${contract_name}(_contract[_.]|[./])" || true)
              if [ -n "$contract_changes" ]; then
                match_found=true
                match_reason="contract folder change"
              fi
            fi
            
            # C) Contract file changed in contracts/stage/producers
            #    Match only the exact contract file: {contract_name}_contract.json
            if ([ "$layer" = "bronze" ] || [ "$layer" = "brz" ]) && [ -n "$source_code" ]; then
              changed_contracts=$(echo "$changed_files" | grep -E "^(bronze|brz)/$source_code/contracts/stage/producers/" || true)
            elif [ "$layer" = "silver" ] || [ "$layer" = "slv" ]; then
              changed_contracts=$(echo "$changed_files" | grep -E "^(silver|slv)/contracts/stage/producers/" || true)
            fi
            if [ -n "$changed_contracts" ]; then
              # Extract just the filename and check for an exact match to {contract_name}_contract.json
              for contract_file in $changed_contracts; do
                filename=$(basename "$contract_file")
                expected_filename="${contract_name}_contract.json"
                if [ "$filename" = "$expected_filename" ]; then
                  match_found=true
                  match_reason="contract file change: $filename"
                  break
                fi
              done
            fi
            
            # D) REMOVED: Notebook changes no longer trigger bundle redeployment
            #    Notebooks deploy independently via deploy-notebooks-only job
            #    Bundles only redeploy when their specific config files change
            
            # E) REMOVED: Common files (bronze/common/*) no longer trigger ALL bundles
            #    Common files are now excluded from deployment triggers to prevent
            #    unnecessary redeployments when only documentation or shared variables change
            
            if [ "$match_found" = true ]; then
              echo "   [MATCH] $match_reason"
              jobs_list="$jobs_list\"$bundle\","
              
              # Track zone for this bundle (for smart filtering later)
              if [[ "$changed_zones" != *"$layer"* ]]; then
                changed_zones="$changed_zones $layer"
              fi
            else
              echo "   [SKIP] No match"
            fi
          done
          jobs="[${jobs_list%,}]"
          
          # Output changed zones (space-separated: "bronze silver" or just "bronze")
          changed_zones=$(echo "$changed_zones" | xargs)  # Trim whitespace
          echo "changed_zones=$changed_zones" >> $GITHUB_OUTPUT
          echo "Changed zones: $changed_zones"

          echo "jobs=$jobs" >> $GITHUB_OUTPUT
          echo "has_changes=$([ "$jobs" != "[]" ] && echo true || echo false)" >> $GITHUB_OUTPUT
          
          # Detect if notebook files changed (notebooks stored with .py extension in Git)
          # Zone-aware detection: track which zones have notebook changes
          # Patterns (subdomain repos have bronze and silver only, no gold):
          #   Bronze: bronze/{source_code}/notebooks/* (any depth)
          #   Silver: silver/notebooks/* (any depth, no source_code)
          notebooks_changed_bronze=$(echo "$changed_files" | grep -qE "^(bronze|brz)/[^/]+/notebooks/" && echo true || echo false)
          notebooks_changed_silver=$(echo "$changed_files" | grep -qE "^(silver|slv)/notebooks/" && echo true || echo false)
          notebooks_changed=false
          if [ "$notebooks_changed_bronze" = "true" ] || [ "$notebooks_changed_silver" = "true" ]; then
            notebooks_changed=true
          fi
          echo "notebooks_changed=$notebooks_changed" >> $GITHUB_OUTPUT
          
          # Extract unique source_codes/zones that have notebook changes (zone-aware, with zone prefix)
          notebook_source_codes="[]"
          bundle_and_notebook_source_codes="[]"
          notebook_only_changed=false
          
          if [ "$notebooks_changed" = "true" ]; then
            # Get unique identifiers from changed notebook files
            # Bronze: "bronze:source_code" (extract source_code from bronze/{source_code}/notebooks/*)
            # Silver: "silver" (no source_code, notebooks are shared)
            src_list=""
            
            # Handle bronze notebooks (extract source_code)
            bronze_entries=$(echo "$changed_files" | grep -E "^(bronze|brz)/[^/]+/notebooks/" | awk -F'/' '{print $1":"$2}' | sort -u)
            for entry in $bronze_entries; do
              src_list="$src_list\"$entry\","
            done
            
            # Handle silver/slv notebooks (no source_code extraction)
            # Detect actual directory name from changed files (silver or slv)
            silver_notebook_zone=$(echo "$changed_files" | grep -E "^(silver|slv)/notebooks/" | awk -F'/' '{print $1}' | head -n1)
            if [ -n "$silver_notebook_zone" ]; then
              src_list="$src_list\"$silver_notebook_zone\","
            fi
            
            notebook_source_codes="[${src_list%,}]"
            
            # Check if ONLY notebooks changed (no bundle, contract, or other bronze/silver changes)
            # Only consider files under bronze/brz/silver/slv to determine notebook-only deployment
            # (Changes outside these zones like .github/workflows/ don't affect deployment decision)
            # Supports both legacy names (bronze, silver) and NorthStar abbreviations (brz, slv)
            # Only consider deployment-relevant non-notebook changes (pipelines, contracts)
            # Schema, test, and documentation changes do NOT affect notebook-only detection
            zone_non_notebook_changes=$(echo "$changed_files" | grep -E "^(bronze|brz)/[^/]+/(pipelines|contracts)/|^(silver|slv)/(pipelines|contracts)/" || true)
            if [ -z "$zone_non_notebook_changes" ]; then
              notebook_only_changed=true
            else
              # Both bundles and notebooks changed - extract source_codes for notebook upload
              bundle_and_notebook_source_codes="$notebook_source_codes"
            fi
          fi
          
          echo "notebook_only_changed=$notebook_only_changed" >> $GITHUB_OUTPUT
          echo "notebook_source_codes=$notebook_source_codes" >> $GITHUB_OUTPUT
          echo "bundle_and_notebook_source_codes=$bundle_and_notebook_source_codes" >> $GITHUB_OUTPUT


  # STEP 2A: Deploy notebooks only (when only notebooks changed) - PRIMARY REGION
  # No matrix needed - notebooks are at source_code level, deploy once per source_code
  deploy-notebooks-only-primary:
    needs: detect-changes
    if: needs.detect-changes.outputs.notebook_only_changed == 'true'
    runs-on: ubuntu-latest
    environment: stage_primary
    
    # 2026 OIDC Authentication for Azure (passwordless)
    env:
      DATABRICKS_HOST: ${{ vars.DATABRICKS_HOST }}
      DATABRICKS_AUTH_TYPE: azure-cli

    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Azure CLI Login (OIDC)
        uses: azure/login@v2
        with:
          client-id: ${{ vars.SP_CLIENT_ID }}
          tenant-id: ${{ vars.AZ_TENANT_ID }}
          allow-no-subscriptions: true

      - name: Set up Databricks CLI
        uses: databricks/setup-cli@main

      - name: Install jq
        run: sudo apt-get update && sudo apt-get install -y jq

      - name: Install yq
        uses: mikefarah/yq@v4.40.5

      - name: Upload notebooks to workspace
        env:
          NOTEBOOK_SOURCE_CODES: ${{ needs.detect-changes.outputs.notebook_source_codes }}
        run: |
          echo "Uploading notebooks to workspace (notebook-only deployment)..."
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          echo ""
          
          # Parse affected source_codes from detect-changes output (passed via env to prevent injection)
          SOURCE_CODES="$NOTEBOOK_SOURCE_CODES"
          echo "Affected zones/source_codes: $SOURCE_CODES"
          echo ""
          
          # Process each entry (format: "bronze:source_code", "silver", or "gold")
          for entry in $(echo "$SOURCE_CODES" | jq -r '.[]'); do
            # Check if entry contains colon (bronze format)
            if [[ "$entry" == *":"* ]]; then
              zone=$(echo "$entry" | cut -d':' -f1)
              src_code=$(echo "$entry" | cut -d':' -f2)
            else
              # Silver or gold format (no source_code)
              zone="$entry"
              src_code=""
            fi
            
            echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            echo "Processing: Zone=$zone, Source=$src_code"
            echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            
            # Determine notebook directory based on zone (supports NorthStar abbreviations)
            if [ "$zone" = "bronze" ] || [ "$zone" = "brz" ]; then
              NOTEBOOK_DIR="${GITHUB_WORKSPACE}/${zone}/${src_code}/notebooks"
              # Find newest bundle for bronze/brz (most recently modified = most recently deployed)
              SAMPLE_BUNDLE=$(find "${GITHUB_WORKSPACE}/${zone}/${src_code}/pipelines/databricks" -name "databricks.yml" -printf '%T@ %p\n' 2>/dev/null | sort -rn | head -1 | awk '{print $2}' | xargs -r dirname || true)
            else
              # Silver or gold - notebooks at zone level
              NOTEBOOK_DIR="${GITHUB_WORKSPACE}/${zone}/notebooks"
              # Find newest bundle for silver/gold (most recently modified = most recently deployed)
              SAMPLE_BUNDLE=$(find "${GITHUB_WORKSPACE}/${zone}/pipelines/databricks" -name "databricks.yml" -printf '%T@ %p\n' 2>/dev/null | sort -rn | head -1 | awk '{print $2}' | xargs -r dirname || true)
            fi
            
            if [ -z "$SAMPLE_BUNDLE" ] || [ ! -d "$SAMPLE_BUNDLE" ]; then
              echo "[WARNING] No bundles found for zone=$zone (skipping)"
              continue
            fi
            
            cd "$SAMPLE_BUNDLE"
            
            # Extract subdomain_code from bundle variables
            SUBDOMAIN_CODE=$(yq eval '.targets.stage_primary.variables.subdomain_code' "${SAMPLE_BUNDLE}/includes/stage/bundle.env.variables.yml")
            ENVIRONMENT="stage"
            
            # Validate bundle is complete (catches incomplete bundles from --infra-update)
            if [ -z "$SUBDOMAIN_CODE" ] || [ "$SUBDOMAIN_CODE" = "null" ]; then
              echo "ERROR: Bundle incomplete (subdomain_code is null): ${SAMPLE_BUNDLE}"
              echo "   This usually happens when a bundle was deployed with --infra-update mode."
              echo "   Solution: Run full deployment (without --infra-update) for this bundle."
              exit 1
            fi
            
            # Validate that notebook directory exists before proceeding
            if [ ! -d "$NOTEBOOK_DIR" ]; then
              echo "ERROR: Notebook directory not found: ${NOTEBOOK_DIR}"
              echo "   Expected at: ${zone}/notebooks (silver) or bronze/${src_code}/notebooks"
              continue
            fi
            
            # Count notebooks recursively (notebooks stored without .py in repo)
            NOTEBOOK_COUNT=$(find "$NOTEBOOK_DIR" -type f ! -name '.*' 2>/dev/null | wc -l)
            
            # Construct target workspace path based on zone
            if [ "$zone" = "bronze" ] || [ "$zone" = "brz" ]; then
              TARGET_PATH="/Workspace/${SUBDOMAIN_CODE}/${ENVIRONMENT}/${zone}/${src_code}"
            else
              # Silver or gold - no source_code in path
              TARGET_PATH="/Workspace/${SUBDOMAIN_CODE}/${ENVIRONMENT}/${zone}"
            fi
            
            echo "Upload Details:"
            echo "  • Zone: ${zone}"
            [ -n "$src_code" ] && echo "  • Source Code: ${src_code}"
            echo "  • Subdomain: ${SUBDOMAIN_CODE}"
            echo "  • Target Path: ${TARGET_PATH}"
            echo "  • Notebook Count: ${NOTEBOOK_COUNT}"
            echo ""
            
            # Check clean_adb_folder flag (default: false for backward compatibility)
            CLEAN_ADB_FOLDER=$(yq eval '.targets.stage_primary.variables.clean_adb_folder // "false"' "${SAMPLE_BUNDLE}/includes/stage/bundle.env.variables.yml")
            if [ "$CLEAN_ADB_FOLDER" = "true" ]; then
              echo "[CLEAN] clean_adb_folder=true - Deleting workspace folder before upload..."
              echo "   Target: ${TARGET_PATH}"
              if databricks workspace delete "${TARGET_PATH}" --recursive 2>/dev/null; then
                echo "   [OK] Workspace folder deleted: ${TARGET_PATH}"
              else
                echo "   [INFO] Folder may not exist yet (safe to continue)"
              fi
            else
              echo "[INFO] clean_adb_folder=false - Keeping existing workspace contents (overwrite only)"
            fi
            
            # Create directory structure
            echo "Creating directory structure..."
            if ! databricks workspace mkdirs "${TARGET_PATH}" 2>/dev/null; then
              echo "⚠️  Warning: Could not create directory ${TARGET_PATH} (may already exist or permission issue)"
            fi
            
            # Upload notebooks recursively (preserving subdirectory structure for silver)
            # Repo: bronze/{source_code}/notebooks/* → Workspace: /Workspace/{subdomain}/{env}/bronze/{source_code}
            # Repo: silver/notebooks/* → Workspace: /Workspace/{subdomain}/{env}/silver
            echo "Uploading notebooks..."
            find "$NOTEBOOK_DIR" -type f ! -name '.*' | while read -r notebook; do
              notebook_name=$(basename "$notebook")
              
              # Calculate relative path from NOTEBOOK_DIR to preserve subdirectory structure
              rel_path="${notebook#$NOTEBOOK_DIR/}"
              rel_dir=$(dirname "$rel_path")
              
              # Skip README files
              if [[ "$notebook_name" == "README.md" ]]; then
                continue
              fi
              
              # For files in subdirectories, create the directory structure in workspace
              if [ "$rel_dir" != "." ]; then
                if ! databricks workspace mkdirs "${TARGET_PATH}/${rel_dir}" 2>/dev/null; then
                  echo "⚠️  Warning: Could not create directory ${TARGET_PATH}/${rel_dir} (may already exist or permission issue)"
                fi
                target_notebook="${TARGET_PATH}/${rel_path}"
                echo "  • ${rel_path}"
              else
                target_notebook="${TARGET_PATH}/${notebook_name}"
                echo "  • ${notebook_name}"
              fi
              
              databricks workspace import \
                "${target_notebook}" \
                --file "$notebook" \
                  --language PYTHON \
                  --overwrite
            done
            
            echo "Notebooks uploaded for source_code: $src_code"
            echo ""
            
            # Return to repo root for next iteration
            cd "$GITHUB_WORKSPACE"
          done
          
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          echo "All notebooks uploaded successfully!"
          echo "INFO: Bundle deployment SKIPPED (notebook-only change)"
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  # STEP 2B: Upload notebooks when bundles changed AND notebooks changed together - PRIMARY REGION
  # Uploads once per source_code (not once per bundle) to avoid redundancy
  deploy-notebooks-with-bundles-primary:
    needs: detect-changes
    if: needs.detect-changes.outputs.bundle_and_notebook_source_codes != '' && needs.detect-changes.outputs.bundle_and_notebook_source_codes != '[]'
    runs-on: ubuntu-latest
    strategy:
      matrix:
        source_code: ${{ fromJson(needs.detect-changes.outputs.bundle_and_notebook_source_codes || '[]') }}
      fail-fast: false
    
    environment: stage_primary
    
    # 2026 OIDC Authentication for Azure (passwordless)
    env:
      DATABRICKS_HOST: ${{ vars.DATABRICKS_HOST }}
      DATABRICKS_AUTH_TYPE: azure-cli
      SOURCE_CODE: ${{ matrix.source_code }}

    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Azure CLI Login (OIDC)
        uses: azure/login@v2
        with:
          client-id: ${{ vars.SP_CLIENT_ID }}
          tenant-id: ${{ vars.AZ_TENANT_ID }}
          allow-no-subscriptions: true

      - name: Set up Databricks CLI
        uses: databricks/setup-cli@main

      - name: Install yq
        uses: mikefarah/yq@v4.40.5

      - name: Upload notebooks to workspace
        run: |
          # Parse zone and source_code from matrix entry (format: "bronze:source_code", "silver", or "gold")
          if [[ "${SOURCE_CODE}" == *":"* ]]; then
            zone=$(echo "${SOURCE_CODE}" | cut -d':' -f1)
            src_code=$(echo "${SOURCE_CODE}" | cut -d':' -f2)
          else
            zone="${SOURCE_CODE}"
            src_code=""
          fi
          
          if [ -n "$src_code" ]; then
            echo "Uploading notebooks for zone=$zone, source_code=$src_code"
          else
            echo "Uploading notebooks for zone=$zone"
          fi
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          
          # Determine notebook directory based on zone
          if [ "$zone" = "bronze" ] || [ "$zone" = "brz" ]; then
            NOTEBOOK_DIR="${GITHUB_WORKSPACE}/${zone}/${src_code}/notebooks"
            # Find newest bundle for bronze/brz (most recently modified = most recently deployed)
            SAMPLE_BUNDLE=$(find "${GITHUB_WORKSPACE}/${zone}/${src_code}/pipelines/databricks" -name "databricks.yml" -printf '%T@ %p\n' 2>/dev/null | sort -rn | head -1 | awk '{print $2}' | xargs -r dirname || true)
          else
            # Silver or gold - notebooks at zone level
            NOTEBOOK_DIR="${GITHUB_WORKSPACE}/${zone}/notebooks"
            # Find newest bundle for silver/gold (most recently modified = most recently deployed)
            SAMPLE_BUNDLE=$(find "${GITHUB_WORKSPACE}/${zone}/pipelines/databricks" -name "databricks.yml" -printf '%T@ %p\n' 2>/dev/null | sort -rn | head -1 | awk '{print $2}' | xargs -r dirname || true)
          fi
          
          if [ -z "$SAMPLE_BUNDLE" ] || [ ! -d "$SAMPLE_BUNDLE" ]; then
            echo "WARNING: Bundle not found or invalid: ${SAMPLE_BUNDLE}"
            exit 0
          fi
          
          cd "$SAMPLE_BUNDLE"
          
          # Extract subdomain_code from bundle variables
          SUBDOMAIN_CODE=$(yq eval '.targets.stage_primary.variables.subdomain_code' "${SAMPLE_BUNDLE}/includes/stage/bundle.env.variables.yml")
          ENVIRONMENT="stage"
          
          # Validate bundle is complete (catches incomplete bundles from --infra-update)
          if [ -z "$SUBDOMAIN_CODE" ] || [ "$SUBDOMAIN_CODE" = "null" ]; then
            echo "ERROR: Bundle incomplete (subdomain_code is null): ${SAMPLE_BUNDLE}"
            echo "   This usually happens when a bundle was deployed with --infra-update mode."
            echo "   Solution: Run full deployment (without --infra-update) for this bundle."
            exit 1
          fi
          
          # Validate that notebook directory exists before proceeding
          if [ ! -d "$NOTEBOOK_DIR" ]; then
            echo "ERROR: Notebook directory not found: ${NOTEBOOK_DIR}"
            echo "   Expected at: ${zone}/notebooks (silver/gold) or bronze/${src_code}/notebooks"
            exit 1
          fi
          
          # Count notebooks recursively
          NOTEBOOK_COUNT=$(find "$NOTEBOOK_DIR" -type f -name '*.py' 2>/dev/null | wc -l)
          
          # Construct target workspace path based on zone
          if [ "$zone" = "bronze" ] || [ "$zone" = "brz" ]; then
            TARGET_PATH="/Workspace/${SUBDOMAIN_CODE}/${ENVIRONMENT}/${zone}/${src_code}"
          else
            # Silver or gold - no source_code in path
            TARGET_PATH="/Workspace/${SUBDOMAIN_CODE}/${ENVIRONMENT}/${zone}"
          fi
          
          echo "Upload Details:"
          echo "  • Zone: ${zone}"
          [ -n "$src_code" ] && echo "  • Source Code: ${src_code}"
          echo "  • Subdomain: ${SUBDOMAIN_CODE}"
          echo "  • Target Path: ${TARGET_PATH}"
          echo "  • Notebook Count: ${NOTEBOOK_COUNT}"
          echo ""
          
          # Check clean_adb_folder flag (default: false for backward compatibility)
          CLEAN_ADB_FOLDER=$(yq eval '.targets.stage_primary.variables.clean_adb_folder // "false"' "${SAMPLE_BUNDLE}/includes/stage/bundle.env.variables.yml")
          if [ "$CLEAN_ADB_FOLDER" = "true" ]; then
            echo "[CLEAN] clean_adb_folder=true - Deleting workspace folder before upload..."
            echo "   Target: ${TARGET_PATH}"
            if databricks workspace delete "${TARGET_PATH}" --recursive 2>/dev/null; then
              echo "   [OK] Workspace folder deleted: ${TARGET_PATH}"
            else
              echo "   [INFO] Folder may not exist yet (safe to continue)"
            fi
          else
            echo "[INFO] clean_adb_folder=false - Keeping existing workspace contents (overwrite only)"
          fi
          
          # Create directory structure
          echo "Creating directory structure..."
          if ! databricks workspace mkdirs "${TARGET_PATH}" 2>/dev/null; then
            echo "⚠️  Warning: Could not create directory ${TARGET_PATH} (may already exist or permission issue)"
          fi
          
          # Upload notebooks (Databricks automatically adds .py extension)
          # Supports subdirectory structures (e.g., silver_subdomain_curation/*)
          echo "Uploading notebooks..."
          while IFS= read -r -d '' notebook; do
            notebook_name=$(basename "$notebook")
            
            # Skip hidden files and documentation (README, markdown)
            if [[ "$notebook_name" =~ ^\. ]] || [[ "$notebook_name" == "README.md" ]] || [[ "$notebook_name" == "README" ]]; then
              continue
            fi
            
            # Calculate relative path from NOTEBOOK_DIR
            relative_path="${notebook#$NOTEBOOK_DIR/}"
            notebook_dir=$(dirname "$relative_path")
            
            # Create target path with subdirectory preservation
            if [ "$notebook_dir" = "." ]; then
              target_file="${TARGET_PATH}/${notebook_name}"
            else
              # Create subdirectory in workspace before uploading
              if ! databricks workspace mkdirs "${TARGET_PATH}/${notebook_dir}" 2>/dev/null; then
                echo "⚠️  Warning: Could not create directory ${TARGET_PATH}/${notebook_dir} (may already exist or permission issue)"
              fi
              target_file="${TARGET_PATH}/${notebook_dir}/${notebook_name}"
            fi
            
            echo "  • ${relative_path} → ${target_file}.py (auto-added by Databricks)"
            databricks workspace import \
              "$target_file" \
              --file "$notebook" \
              --language PYTHON \
              --overwrite
          done < <(find "$NOTEBOOK_DIR" -type f -print0)
          
          echo ""
          echo "Notebooks uploaded for: $entry"
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  # STEP 2C: Deploy identified bundles to the Stage environment (full deployment) - PRIMARY REGION
  deploy-jobs-primary:
    needs: detect-changes
    if: needs.detect-changes.outputs.has_changes == 'true' && needs.detect-changes.outputs.notebook_only_changed != 'true'
    runs-on: ubuntu-latest
    strategy:
      matrix:
        bundle_root: ${{ fromJson(needs.detect-changes.outputs.jobs_matrix || '[]') }}
      fail-fast: false
    
    environment: stage_primary
    
    # 2026 OIDC Authentication for Azure (passwordless)
    env:
      DATABRICKS_HOST: ${{ vars.DATABRICKS_HOST }}
      DATABRICKS_AUTH_TYPE: azure-cli

    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Azure CLI Login (OIDC)
        uses: azure/login@v2
        with:
          client-id: ${{ vars.SP_CLIENT_ID }}
          tenant-id: ${{ vars.AZ_TENANT_ID }}
          allow-no-subscriptions: true

      - name: Set up Databricks CLI
        uses: databricks/setup-cli@main

      - name: Install jq
        run: sudo apt-get update && sudo apt-get install -y jq

      - name: Install yq
        uses: mikefarah/yq@v4.40.5

      - name: Load contract JSON from stage.yml
        id: load_vars
        working-directory: ${{ matrix.bundle_root }}
        run: |
          echo ""
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          echo "LOADING CONTRACT CONFIGURATION"
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          
          # Get contract_name and check if contract_json is already embedded in bundle.env.variables.yml
          CONTRACT_NAME=$(yq eval -r '.targets.stage_primary.variables.contract_name // ""' includes/stage/bundle.env.variables.yml)
          SUBDOMAIN_CODE=$(yq eval -r '.targets.stage_primary.variables.subdomain_code // ""' includes/stage/bundle.env.variables.yml)
          SOURCE_CODE=$(yq eval -r '.targets.stage_primary.variables.source_code // ""' includes/stage/bundle.env.variables.yml)
          EMBEDDED_CONTRACT_JSON=$(yq eval -r '.targets.stage_primary.variables.contract_json // ""' includes/stage/bundle.env.variables.yml)
          
          echo "Contract Name: $CONTRACT_NAME"
          echo "Subdomain Code: $SUBDOMAIN_CODE"
          echo "Source Code: $SOURCE_CODE"
          echo ""
          
          # Validate bundle is complete before attempting deployment
          if [ -z "$CONTRACT_NAME" ]; then
            echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            echo "[ERROR] Bundle validation failed - contract_name is null or empty"
            echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            echo ""
            echo "This indicates a configuration error in bundle.env.variables.yml"
            echo ""
            echo "Required: contract_name must be defined and non-empty"
            echo "Check: $(pwd)/includes/stage/bundle.env.variables.yml"
            echo ""
            echo "Common causes:"
            echo "  • Missing contract_name field"
            echo "  • Typo in field name (e.g., 'contrat_name' instead of 'contract_name')"
            echo "  • Empty value in YAML configuration"
            echo ""
            exit 1
          fi
          
          if [ -z "$SUBDOMAIN_CODE" ]; then
            echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            echo "[ERROR] Bundle validation failed - subdomain_code is null or empty"
            echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            echo ""
            echo "This indicates a configuration error in bundle.env.variables.yml"
            echo ""
            echo "Required: subdomain_code must be defined and non-empty"
            echo "Check: $(pwd)/includes/stage/bundle.env.variables.yml"
            echo ""
            echo "Common causes:"
            echo "  • Missing subdomain_code field"
            echo "  • Typo in field name"
            echo "  • Empty value in YAML configuration"
            echo ""
            exit 1
          fi
          
          # Check if contract_json is already embedded (for silver/gold zones using job_params)
          if [ -n "$EMBEDDED_CONTRACT_JSON" ]; then
            echo "Contract JSON already embedded in bundle.env.variables.yml (silver/gold zone with job_params)"
            CONTRACT_JSON="$EMBEDDED_CONTRACT_JSON"
            CONTRACT_SIZE=$(echo "$CONTRACT_JSON" | wc -c)
            echo "Contract size: $CONTRACT_SIZE bytes"
          else
            # Bronze zone: Load contract JSON from contract file
            echo "Loading contract JSON from file (bronze zone with full contract)"
            CONTRACT_FILE="../../../contracts/stage/producers/${CONTRACT_NAME}_contract.json"
            
            if [ ! -f "$CONTRACT_FILE" ]; then
              echo "ERROR: Contract file not found: $CONTRACT_FILE"
              exit 1
            fi
            
            CONTRACT_JSON=$(cat "$CONTRACT_FILE" | jq -c .)
            CONTRACT_SIZE=$(echo "$CONTRACT_JSON" | wc -c)
            
            echo "Contract loaded: $CONTRACT_FILE"
            echo "Contract size: $CONTRACT_SIZE bytes"
          fi
          
          echo ""
          echo "contract_json<<EOF" >> $GITHUB_OUTPUT
          echo "$CONTRACT_JSON" >> $GITHUB_OUTPUT
          echo "EOF" >> $GITHUB_OUTPUT

      - name: Extract Variables for DAB (Dynamic)
        id: extract_vars
        working-directory: ${{ matrix.bundle_root }}
        run: |
          # Check if bundle.env.variables.yml exists
          if [ ! -f "includes/stage/bundle.env.variables.yml" ]; then
            echo "ERROR: includes/stage/bundle.env.variables.yml not found"
            exit 1
          fi
          
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          echo "DYNAMICALLY EXTRACTING ALL VARIABLES"
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          echo ""
          
          # Get ALL variable names from bundle.env.variables.yml
          VARIABLE_NAMES=$(yq eval '.targets.stage_primary.variables | keys | .[]' includes/stage/bundle.env.variables.yml)
          
          if [ -z "$VARIABLE_NAMES" ]; then
            echo "ERROR: No variables found in bundle.env.variables.yml"
            exit 1
          fi
          
          echo "Found variables:"
          echo "$VARIABLE_NAMES"
          echo ""
          
          # Required variables that must not be empty
          REQUIRED_VARS=("subdomain_code" "source_code" "contract_name" "service_principal_id" "zone")
          
          # Loop through and extract each variable dynamically
          while IFS= read -r var_name; do
            if [ -z "$var_name" ]; then
              continue
            fi
            
            # Extract variable value (use // "" for null/missing values)
            var_value=$(yq eval ".targets.stage_primary.variables.$var_name // \"\"" includes/stage/bundle.env.variables.yml)
            
            # Validate required variables are not empty
            for required_var in "${REQUIRED_VARS[@]}"; do
              if [ "$var_name" == "$required_var" ] && [ -z "$var_value" ]; then
                echo "ERROR: Required variable '$var_name' is empty"
                exit 1
              fi
            done
            
            # Export to GitHub outputs
            echo "${var_name}=${var_value}" >> $GITHUB_OUTPUT
            echo "  ✓ Extracted: ${var_name}"
          done <<< "$VARIABLE_NAMES"
          
          echo ""
          echo "All variables extracted successfully!"

      - name: Auto-bind existing job (migration from old target)
        working-directory: ${{ matrix.bundle_root }}
        continue-on-error: true
        env:
          BUNDLE_VAR_contract_json: ${{ steps.load_vars.outputs.contract_json }}
        run: |
          echo ""
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          echo "AUTO-BIND: Checking for existing jobs from old target"
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          echo ""
          
          # Set BUNDLE_VAR_* for bundle initialization (required for unbind/bind)
          VARIABLE_NAMES=$(yq eval '.targets.stage_primary.variables | keys | .[]' includes/stage/bundle.env.variables.yml)
          while IFS= read -r var_name; do
            [ -z "$var_name" ] && continue
            var_value=$(yq eval ".targets.stage_primary.variables.$var_name // \"\"" includes/stage/bundle.env.variables.yml)
            export "BUNDLE_VAR_${var_name}=${var_value}"
          done <<< "$VARIABLE_NAMES"
          
          # Extract job name pattern from bundle variables
          CONTRACT_NAME=$(yq eval '.targets.stage_primary.variables.contract_name' includes/stage/bundle.env.variables.yml)
          SUBDOMAIN_CODE=$(yq eval '.targets.stage_primary.variables.subdomain_code' includes/stage/bundle.env.variables.yml)
          SOURCE_CODE=$(yq eval '.targets.stage_primary.variables.source_code' includes/stage/bundle.env.variables.yml)
          ENVIRONMENT=$(yq eval '.targets.stage_primary.variables.environment' databricks.yml)
          BIGPANDA_SUFFIX=$(yq eval '.targets.stage_primary.variables.bigpanda_suffix' includes/stage/bundle.env.variables.yml)
          
          # Construct expected job name
          if [ "$SOURCE_CODE" = "null" ] || [ -z "$SOURCE_CODE" ]; then
            EXPECTED_JOB_NAME="[${ENVIRONMENT}]-${SUBDOMAIN_CODE}-${CONTRACT_NAME}${BIGPANDA_SUFFIX}"
          else
            EXPECTED_JOB_NAME="[${ENVIRONMENT}]-${SUBDOMAIN_CODE}-${SOURCE_CODE}-${CONTRACT_NAME}${BIGPANDA_SUFFIX}"
          fi
          
          echo "Searching for existing job: ${EXPECTED_JOB_NAME}"
          
          # List all jobs and find matching job ID
          JOB_ID=$(databricks jobs list --output json | jq -r ".[] | select(.settings.name == \"${EXPECTED_JOB_NAME}\") | .job_id" | head -n1)
          
          if [ -n "$JOB_ID" ]; then
            echo "[FOUND] Existing job ID: ${JOB_ID}"
            
            # Extract resource name from resources/*.yml files (DAB includes pattern)
            RESOURCE_NAME=""
            if [ -d "resources" ]; then
              for resource_file in resources/*.yml; do
                if [ -f "$resource_file" ]; then
                  RESOURCE_NAME=$(yq eval '.resources.jobs | keys | .[]' "$resource_file" 2>/dev/null | head -n1 || echo "")
                  [ -n "$RESOURCE_NAME" ] && break
                fi
              done
            fi
            
            # Fallback: Check main databricks.yml (for inline resources)
            if [ -z "$RESOURCE_NAME" ]; then
              RESOURCE_NAME=$(yq eval '.resources.jobs | keys | .[]' databricks.yml 2>/dev/null | head -n1 || echo "")
            fi
            
            if [ -n "$RESOURCE_NAME" ]; then
              echo "[BIND] Connecting job ${JOB_ID} to bundle resource: ${RESOURCE_NAME}"
              
              # Unbind first to clear any stale Terraform state, then re-bind
              echo "[UNBIND] Clearing existing Terraform state for ${RESOURCE_NAME}..."
              UNBIND_OUTPUT=$(databricks bundle deployment unbind "${RESOURCE_NAME}" --target stage_primary 2>&1) || true
              if [ -n "$UNBIND_OUTPUT" ]; then echo "$UNBIND_OUTPUT"; fi
              
              # Bind existing job to new target
              BIND_OUTPUT=$(databricks bundle deployment bind "${RESOURCE_NAME}" "${JOB_ID}" --target stage_primary --auto-approve 2>&1) && BIND_RC=0 || BIND_RC=$?
              if [ $BIND_RC -eq 0 ]; then
                echo "[OK] Job successfully bound to stage_primary target"
                echo "     Future deployments will update this job instead of creating duplicates"
              else
                echo "[WARNING] Bind failed - will proceed with deployment (may create duplicate)"
                echo "$BIND_OUTPUT"
              fi
            else
              echo "[WARNING] Could not determine bundle resource name"
            fi
          else
            echo "[INFO] No existing job found - this is a new deployment"
          fi
          
          echo ""

      - name: Validate Bundle (Dynamic Variables)
        working-directory: ${{ matrix.bundle_root }}
        env:
          BUNDLE_VAR_contract_json: ${{ steps.load_vars.outputs.contract_json }}
        run: |
          echo ""
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          echo "VALIDATING DATABRICKS BUNDLE (WITH DYNAMIC VARIABLES)"
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          echo ""
          
          # Dynamically export ALL variables from extract_vars step as BUNDLE_VAR_*
          echo "Setting up BUNDLE_VAR_ environment variables dynamically..."
          VARIABLE_NAMES=$(yq eval '.targets.stage_primary.variables | keys | .[]' includes/stage/bundle.env.variables.yml)
          
          while IFS= read -r var_name; do
            if [ -z "$var_name" ]; then
              continue
            fi
            
            # Get value from GitHub outputs (extract_vars step)
            var_value=$(yq eval ".targets.stage_primary.variables.$var_name // \"\"" includes/stage/bundle.env.variables.yml)
            export "BUNDLE_VAR_${var_name}=${var_value}"
            echo "  ✓ BUNDLE_VAR_${var_name} set"
          done <<< "$VARIABLE_NAMES"
          
          echo ""
          databricks bundle validate -t stage_primary
          
          echo ""
          echo "Bundle validation passed!"

      - name: Deploy Bundle (Dynamic Variables)
        working-directory: ${{ matrix.bundle_root }}
        env:
          BUNDLE_VAR_contract_json: ${{ steps.load_vars.outputs.contract_json }}
        run: |
          echo ""
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          echo "DEPLOYING TO STAGE ENVIRONMENT (WITH DYNAMIC VARIABLES)"
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          echo ""
          
          # Dynamically export ALL variables from extract_vars step as BUNDLE_VAR_*
          echo "Setting up BUNDLE_VAR_ environment variables dynamically..."
          VARIABLE_NAMES=$(yq eval '.targets.stage_primary.variables | keys | .[]' includes/stage/bundle.env.variables.yml)
          
          while IFS= read -r var_name; do
            if [ -z "$var_name" ]; then
              continue
            fi
            
            # Get value from GitHub outputs (extract_vars step)
            var_value=$(yq eval ".targets.stage_primary.variables.$var_name // \"\"" includes/stage/bundle.env.variables.yml)
            export "BUNDLE_VAR_${var_name}=${var_value}"
            echo "  ✓ BUNDLE_VAR_${var_name} set"
          done <<< "$VARIABLE_NAMES"
          
          echo ""
          echo "Contract JSON length: ${#BUNDLE_VAR_contract_json} characters"
          echo "Target: stage"
          echo ""
          
          # Deploy using environment target 'stage' with all variables from bundle.env.variables.yml
          # Auto-bind step above handles Terraform state migration (unbind+bind)
          databricks bundle deploy -t stage_primary
          
          echo ""
          echo "Deployment completed successfully!"

      - name: Print Deployment Summary
        working-directory: ${{ matrix.bundle_root }}
        run: |
          # Extract values from variables
          CONTRACT_NAME=$(yq eval '.targets.stage_primary.variables.contract_name' includes/stage/bundle.env.variables.yml)
          ENVIRONMENT=$(yq eval '.targets.stage_primary.variables.environment' databricks.yml)
          ZONE=$(yq eval '.targets.stage_primary.variables.zone' includes/stage/bundle.env.variables.yml)
          SUBDOMAIN_CODE=$(yq eval '.targets.stage_primary.variables.subdomain_code' includes/stage/bundle.env.variables.yml)
          SOURCE_CODE=$(yq eval '.targets.stage_primary.variables.source_code' includes/stage/bundle.env.variables.yml)
          BIGPANDA_SUFFIX=$(yq eval '.targets.stage_primary.variables.bigpanda_suffix' includes/stage/bundle.env.variables.yml)
          PAUSE_STATUS=$(yq eval '.targets.stage_primary.variables.pause_status' includes/stage/bundle.env.variables.yml)
          
          # Handle null SOURCE_CODE for silver/gold zones
          if [ "$SOURCE_CODE" = "null" ] || [ -z "$SOURCE_CODE" ]; then
            DEPLOYED_JOB_NAME="[${ENVIRONMENT}]-${SUBDOMAIN_CODE}-${CONTRACT_NAME}${BIGPANDA_SUFFIX}"
            NOTEBOOK_PATH="/Workspace/${SUBDOMAIN_CODE}/stage/${ZONE}/${CONTRACT_NAME}/ (transformation notebooks)"
          else
            DEPLOYED_JOB_NAME="[${ENVIRONMENT}]-${SUBDOMAIN_CODE}-${SOURCE_CODE}-${CONTRACT_NAME}${BIGPANDA_SUFFIX}"
            NOTEBOOK_PATH="/Workspace/${SUBDOMAIN_CODE}/stage/${ZONE}/${SOURCE_CODE}/contract_driven_kafka_ingestion_notebook"
          fi
          
          WORKSPACE_URL="${DATABRICKS_HOST}"
          
          echo ""
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          echo "[OK] DEPLOYMENT SUMMARY - STAGE (PRIMARY REGION - EAST)"
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          echo ""
          echo "Workspace:"
          echo "  • Target: stage_primary (Primary - East Region)"
          echo "  • Host: ${WORKSPACE_URL}"
          echo "  • Pause Status: ${PAUSE_STATUS}"
          echo ""
          echo "Job Details:"
          echo "  • Job Name: ${DEPLOYED_JOB_NAME}"
          echo "  • Contract: ${CONTRACT_NAME}"
          echo "  • Zone: ${ZONE}"
          [ "$SOURCE_CODE" != "null" ] && [ -n "$SOURCE_CODE" ] && echo "  • Source Code: ${SOURCE_CODE}"
          echo ""
          if [ "$SOURCE_CODE" = "null" ] || [ -z "$SOURCE_CODE" ]; then
            echo "Notebooks:"
            echo "  • Transformations: ${NOTEBOOK_PATH}"
            echo "  • Shared: /Workspace/${SUBDOMAIN_CODE}/stage/${ZONE}/common/common_functions"
          else
            echo "Notebook:"
            echo "  • Path: ${NOTEBOOK_PATH}"
          fi
          echo ""
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  # ============================================================================
  # DR (DISASTER RECOVERY) JOBS - WEST REGION
  # ============================================================================

  # STEP 3A: Deploy notebooks only (when only notebooks changed) - DR REGION
  # Waits for primary deployment to complete before deploying to DR workspace
  deploy-notebooks-only-secondary:
    needs: [detect-changes, deploy-notebooks-only-primary, deploy-notebooks-with-bundles-primary, deploy-jobs-primary]
    if: |
      !cancelled() &&
      needs.detect-changes.outputs.notebook_only_changed == 'true' &&
      needs.deploy-notebooks-only-primary.result != 'failure' &&
      needs.deploy-notebooks-with-bundles-primary.result != 'failure' &&
      needs.deploy-jobs-primary.result != 'failure'
    runs-on: ubuntu-latest
    environment: stage_secondary
    
    # 2026 OIDC Authentication for Azure (passwordless)
    env:
      DATABRICKS_HOST: ${{ vars.DATABRICKS_HOST }}
      DATABRICKS_AUTH_TYPE: azure-cli

    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Azure CLI Login (OIDC)
        uses: azure/login@v2
        with:
          client-id: ${{ vars.SP_CLIENT_ID }}
          tenant-id: ${{ vars.AZ_TENANT_ID }}
          allow-no-subscriptions: true

      - name: Set up Databricks CLI
        uses: databricks/setup-cli@main

      - name: Install jq
        run: sudo apt-get update && sudo apt-get install -y jq

      - name: Install yq
        uses: mikefarah/yq@v4.40.5

      - name: Upload notebooks to workspace (DR)
        env:
          NOTEBOOK_SOURCE_CODES: ${{ needs.detect-changes.outputs.notebook_source_codes }}
        run: |
          echo "Uploading notebooks to DR workspace (notebook-only deployment)..."
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          echo ""
          
          # Parse affected source_codes from detect-changes output (passed via env to prevent injection)
          SOURCE_CODES="$NOTEBOOK_SOURCE_CODES"
          echo "Affected zones/source_codes: $SOURCE_CODES"
          echo ""
          
          # Process each entry (format: "bronze:source_code", "silver", or "gold")
          for entry in $(echo "$SOURCE_CODES" | jq -r '.[]'); do
            # Check if entry contains colon (bronze format)
            if [[ "$entry" == *":"* ]]; then
              zone=$(echo "$entry" | cut -d':' -f1)
              src_code=$(echo "$entry" | cut -d':' -f2)
            else
              # Silver or gold format (no source_code)
              zone="$entry"
              src_code=""
            fi
            
            echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            echo "Processing: Zone=$zone, Source=$src_code (DR Region)"
            echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            
            # Determine notebook directory based on zone (supports NorthStar abbreviations)
            if [ "$zone" = "bronze" ] || [ "$zone" = "brz" ]; then
              NOTEBOOK_DIR="${GITHUB_WORKSPACE}/${zone}/${src_code}/notebooks"
              # Find newest bundle for bronze/brz (most recently modified = most recently deployed)
              SAMPLE_BUNDLE=$(find "${GITHUB_WORKSPACE}/${zone}/${src_code}/pipelines/databricks" -name "databricks.yml" -printf '%T@ %p\n' 2>/dev/null | sort -rn | head -1 | awk '{print $2}' | xargs -r dirname || true)
            else
              # Silver or gold - notebooks at zone level
              NOTEBOOK_DIR="${GITHUB_WORKSPACE}/${zone}/notebooks"
              # Find newest bundle for silver/gold (most recently modified = most recently deployed)
              SAMPLE_BUNDLE=$(find "${GITHUB_WORKSPACE}/${zone}/pipelines/databricks" -name "databricks.yml" -printf '%T@ %p\n' 2>/dev/null | sort -rn | head -1 | awk '{print $2}' | xargs -r dirname || true)
            fi
            
            if [ -z "$SAMPLE_BUNDLE" ] || [ ! -d "$SAMPLE_BUNDLE" ]; then
              echo "[WARNING] No bundles found for zone=$zone (skipping)"
              continue
            fi
            
            cd "$SAMPLE_BUNDLE"
            
            # Extract subdomain_code from bundle variables
            SUBDOMAIN_CODE=$(yq eval '.targets.stage_primary.variables.subdomain_code' "${SAMPLE_BUNDLE}/includes/stage/bundle.env.variables.yml")
            ENVIRONMENT="stage"
            
            # Validate bundle is complete (catches incomplete bundles from --infra-update)
            if [ -z "$SUBDOMAIN_CODE" ] || [ "$SUBDOMAIN_CODE" = "null" ]; then
              echo "ERROR: Bundle incomplete (subdomain_code is null): ${SAMPLE_BUNDLE}"
              echo "   This usually happens when a bundle was deployed with --infra-update mode."
              echo "   Solution: Run full deployment (without --infra-update) for this bundle."
              exit 1
            fi
            
            # Validate that notebook directory exists before proceeding
            if [ ! -d "$NOTEBOOK_DIR" ]; then
              echo "ERROR: Notebook directory not found: ${NOTEBOOK_DIR}"
              echo "   Expected at: ${zone}/notebooks (silver) or bronze/${src_code}/notebooks"
              continue
            fi
            
            # Count notebooks recursively (notebooks stored without .py in repo)
            NOTEBOOK_COUNT=$(find "$NOTEBOOK_DIR" -type f ! -name '.*' 2>/dev/null | wc -l)
            
            # Construct target workspace path based on zone
            if [ "$zone" = "bronze" ] || [ "$zone" = "brz" ]; then
              TARGET_PATH="/Workspace/${SUBDOMAIN_CODE}/${ENVIRONMENT}/${zone}/${src_code}"
            else
              # Silver or gold - no source_code in path
              TARGET_PATH="/Workspace/${SUBDOMAIN_CODE}/${ENVIRONMENT}/${zone}"
            fi
            
            echo "Upload Details (DR):"
            echo "  • Zone: ${zone}"
            [ -n "$src_code" ] && echo "  • Source Code: ${src_code}"
            echo "  • Subdomain: ${SUBDOMAIN_CODE}"
            echo "  • Target Path: ${TARGET_PATH}"
            echo "  • Notebook Count: ${NOTEBOOK_COUNT}"
            echo ""
            
            # Check clean_adb_folder flag (default: false for backward compatibility)
            CLEAN_ADB_FOLDER=$(yq eval '.targets.stage_primary.variables.clean_adb_folder // "false"' "${SAMPLE_BUNDLE}/includes/stage/bundle.env.variables.yml")
            if [ "$CLEAN_ADB_FOLDER" = "true" ]; then
              echo "[CLEAN] clean_adb_folder=true - Deleting DR workspace folder before upload..."
              echo "   Target: ${TARGET_PATH}"
              if databricks workspace delete "${TARGET_PATH}" --recursive 2>/dev/null; then
                echo "   [OK] DR Workspace folder deleted: ${TARGET_PATH}"
              else
                echo "   [INFO] Folder may not exist yet (safe to continue)"
              fi
            else
              echo "[INFO] clean_adb_folder=false - Keeping existing DR workspace contents (overwrite only)"
            fi
            
            # Create directory structure
            echo "Creating directory structure in DR workspace..."
            if ! databricks workspace mkdirs "${TARGET_PATH}" 2>/dev/null; then
              echo "⚠️  Warning: Could not create directory ${TARGET_PATH} (may already exist or permission issue)"
            fi
            
            # Upload notebooks recursively (preserving subdirectory structure for silver)
            # Repo: bronze/{source_code}/notebooks/* → Workspace: /Workspace/{subdomain}/{env}/bronze/{source_code}
            # Repo: silver/notebooks/* → Workspace: /Workspace/{subdomain}/{env}/silver
            echo "Uploading notebooks to DR..."
            find "$NOTEBOOK_DIR" -type f ! -name '.*' | while read -r notebook; do
              notebook_name=$(basename "$notebook")
              
              # Calculate relative path from NOTEBOOK_DIR to preserve subdirectory structure
              rel_path="${notebook#$NOTEBOOK_DIR/}"
              rel_dir=$(dirname "$rel_path")
              
              # Skip README files
              if [[ "$notebook_name" == "README.md" ]]; then
                continue
              fi
              
              # For files in subdirectories, create the directory structure in workspace
              if [ "$rel_dir" != "." ]; then
                if ! databricks workspace mkdirs "${TARGET_PATH}/${rel_dir}" 2>/dev/null; then
                  echo "⚠️  Warning: Could not create directory ${TARGET_PATH}/${rel_dir} (may already exist or permission issue)"
                fi
                target_notebook="${TARGET_PATH}/${rel_path}"
                echo "  • ${rel_path}"
              else
                target_notebook="${TARGET_PATH}/${notebook_name}"
                echo "  • ${notebook_name}"
              fi
              
              databricks workspace import \
                "${target_notebook}" \
                --file "$notebook" \
                --language PYTHON \
                --overwrite
            done
            
            echo "Notebooks uploaded to DR for source_code: $src_code"
            echo ""
            
            # Return to repo root for next iteration
            cd "$GITHUB_WORKSPACE"
          done
          
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          echo "All notebooks uploaded successfully to DR workspace!"
          echo "INFO: Bundle deployment SKIPPED (notebook-only change)"
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  # STEP 3B: Upload notebooks when bundles changed AND notebooks changed together - DR REGION
  # Waits for primary deployment to complete before deploying to DR workspace
  deploy-notebooks-with-bundles-secondary:
    needs: [detect-changes, deploy-notebooks-only-primary, deploy-notebooks-with-bundles-primary, deploy-jobs-primary]
    if: |
      !cancelled() &&
      needs.detect-changes.outputs.bundle_and_notebook_source_codes != '' &&
      needs.detect-changes.outputs.bundle_and_notebook_source_codes != '[]' &&
      needs.deploy-notebooks-only-primary.result != 'failure' &&
      needs.deploy-notebooks-with-bundles-primary.result != 'failure' &&
      needs.deploy-jobs-primary.result != 'failure'
    runs-on: ubuntu-latest
    strategy:
      matrix:
        source_code: ${{ fromJson(needs.detect-changes.outputs.bundle_and_notebook_source_codes || '[]') }}
      fail-fast: false
    
    environment: stage_secondary
    
    # 2026 OIDC Authentication for Azure (passwordless)
    env:
      DATABRICKS_HOST: ${{ vars.DATABRICKS_HOST }}
      DATABRICKS_AUTH_TYPE: azure-cli
      SOURCE_CODE: ${{ matrix.source_code }}

    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Azure CLI Login (OIDC)
        uses: azure/login@v2
        with:
          client-id: ${{ vars.SP_CLIENT_ID }}
          tenant-id: ${{ vars.AZ_TENANT_ID }}
          allow-no-subscriptions: true

      - name: Set up Databricks CLI
        uses: databricks/setup-cli@main

      - name: Install yq
        uses: mikefarah/yq@v4.40.5

      - name: Upload notebooks to workspace (DR)
        run: |
          # Parse zone and source_code from matrix entry
          # SOURCE_CODE can be:
          #   - Legacy format: "bronze:source_code"
          #   - Bundle path format: "bronze/source_code/pipelines/databricks/bundle-name"
          if [[ "${SOURCE_CODE}" == *"/pipelines/databricks/"* ]]; then
            # Full bundle path - use directly
            SAMPLE_BUNDLE="${GITHUB_WORKSPACE}/${SOURCE_CODE}"
            # Extract zone and src_code from bundle path
            zone=$(echo "${SOURCE_CODE}" | cut -d'/' -f1)
            # Only bronze/brz has source_code as 2nd path segment
            # Silver/gold paths: zone/pipelines/databricks/... (no source_code)
            if [ "$zone" = "bronze" ] || [ "$zone" = "brz" ]; then
              src_code=$(echo "${SOURCE_CODE}" | cut -d'/' -f2)
            else
              src_code=""
            fi
          elif [[ "${SOURCE_CODE}" == *":"* ]]; then
            # Legacy format: "bronze:source_code"
            zone=$(echo "${SOURCE_CODE}" | cut -d':' -f1)
            src_code=$(echo "${SOURCE_CODE}" | cut -d':' -f2)
            # For legacy format, we still need to find a bundle (shouldn't happen in practice)
            if [ "$zone" = "bronze" ] || [ "$zone" = "brz" ]; then
              SAMPLE_BUNDLE=$(find "${GITHUB_WORKSPACE}/${zone}/${src_code}/pipelines/databricks" -name "databricks.yml" -printf '%T@ %p\n' 2>/dev/null | sort -rn | head -1 | awk '{print $2}' | xargs -r dirname || true)
            else
              SAMPLE_BUNDLE=$(find "${GITHUB_WORKSPACE}/${zone}/pipelines/databricks" -name "databricks.yml" -printf '%T@ %p\n' 2>/dev/null | sort -rn | head -1 | awk '{print $2}' | xargs -r dirname || true)
            fi
          else
            # Plain zone format
            zone="${SOURCE_CODE}"
            src_code=""
            SAMPLE_BUNDLE=$(find "${GITHUB_WORKSPACE}/${zone}/pipelines/databricks" -name "databricks.yml" -printf '%T@ %p\n' 2>/dev/null | sort -rn | head -1 | awk '{print $2}' | xargs -r dirname || true)
          fi
          
          if [ -n "$src_code" ]; then
            echo "Uploading notebooks for zone=$zone, source_code=$src_code (DR Region)"
          else
            echo "Uploading notebooks for zone=$zone (DR Region)"
          fi
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          
          # Determine notebook directory based on zone
          if [ "$zone" = "bronze" ] || [ "$zone" = "brz" ]; then
            NOTEBOOK_DIR="${GITHUB_WORKSPACE}/${zone}/${src_code}/notebooks"
          else
            # Silver or gold - notebooks at zone level
            NOTEBOOK_DIR="${GITHUB_WORKSPACE}/${zone}/notebooks"
          fi
          
          # SAMPLE_BUNDLE was already set above from SOURCE_CODE
          if [ -z "$SAMPLE_BUNDLE" ] || [ ! -d "$SAMPLE_BUNDLE" ]; then
            echo "WARNING: Bundle not found or invalid: ${SAMPLE_BUNDLE} (skipping)"
            exit 0
          fi
          
          cd "$SAMPLE_BUNDLE"
          
          # Extract subdomain_code from bundle variables
          SUBDOMAIN_CODE=$(yq eval '.targets.stage_primary.variables.subdomain_code' "${SAMPLE_BUNDLE}/includes/stage/bundle.env.variables.yml")
          ENVIRONMENT="stage"
          
          # Validate bundle is complete (catches incomplete bundles from --infra-update)
          if [ -z "$SUBDOMAIN_CODE" ] || [ "$SUBDOMAIN_CODE" = "null" ]; then
            echo "ERROR: Bundle incomplete (subdomain_code is null): ${SAMPLE_BUNDLE}"
            echo "   This usually happens when a bundle was deployed with --infra-update mode."
            echo "   Solution: Run full deployment (without --infra-update) for this bundle."
            exit 1
          fi
          
          # Validate that notebook directory exists before proceeding
          if [ ! -d "$NOTEBOOK_DIR" ]; then
            echo "ERROR: Notebook directory not found: ${NOTEBOOK_DIR}"
            echo "   Expected at: ${zone}/notebooks (silver/gold) or bronze/${src_code}/notebooks"
            exit 1
          fi
          
          # Count notebooks recursively
          NOTEBOOK_COUNT=$(find "$NOTEBOOK_DIR" -type f -name '*.py' 2>/dev/null | wc -l)
          
          # Construct target workspace path based on zone
          if [ "$zone" = "bronze" ] || [ "$zone" = "brz" ]; then
            TARGET_PATH="/Workspace/${SUBDOMAIN_CODE}/${ENVIRONMENT}/${zone}/${src_code}"
          else
            # Silver or gold - no source_code in path
            TARGET_PATH="/Workspace/${SUBDOMAIN_CODE}/${ENVIRONMENT}/${zone}"
          fi
          
          echo "Upload Details (DR):"
          echo "  • Zone: ${zone}"
          [ -n "$src_code" ] && echo "  • Source Code: ${src_code}"
          echo "  • Subdomain: ${SUBDOMAIN_CODE}"
          echo "  • Target Path: ${TARGET_PATH}"
          echo "  • Notebook Count: ${NOTEBOOK_COUNT}"
          echo ""
          
          # Check clean_adb_folder flag (default: false for backward compatibility)
          CLEAN_ADB_FOLDER=$(yq eval '.targets.stage_primary.variables.clean_adb_folder // "false"' "${SAMPLE_BUNDLE}/includes/stage/bundle.env.variables.yml")
          if [ "$CLEAN_ADB_FOLDER" = "true" ]; then
            echo "[CLEAN] clean_adb_folder=true - Deleting DR workspace folder before upload..."
            echo "   Target: ${TARGET_PATH}"
            if databricks workspace delete "${TARGET_PATH}" --recursive 2>/dev/null; then
              echo "   [OK] DR Workspace folder deleted: ${TARGET_PATH}"
            else
              echo "   [INFO] Folder may not exist yet (safe to continue)"
            fi
          else
            echo "[INFO] clean_adb_folder=false - Keeping existing DR workspace contents (overwrite only)"
          fi
          
          # Create directory structure
          echo "Creating directory structure in DR workspace..."
          if ! databricks workspace mkdirs "${TARGET_PATH}" 2>/dev/null; then
            echo "⚠️  Warning: Could not create directory ${TARGET_PATH} (may already exist or permission issue)"
          fi
          
          # Upload notebooks (Databricks automatically adds .py extension)
          # Supports subdirectory structures (e.g., silver_subdomain_curation/*)
          echo "Uploading notebooks to DR..."
          while IFS= read -r -d '' notebook; do
            notebook_name=$(basename "$notebook")
            
            # Skip hidden files and documentation (README, markdown)
            if [[ "$notebook_name" =~ ^\. ]] || [[ "$notebook_name" == "README.md" ]] || [[ "$notebook_name" == "README" ]]; then
              continue
            fi
            
            # Calculate relative path from NOTEBOOK_DIR
            relative_path="${notebook#$NOTEBOOK_DIR/}"
            notebook_dir=$(dirname "$relative_path")
            
            # Create target path with subdirectory preservation
            if [ "$notebook_dir" = "." ]; then
              target_file="${TARGET_PATH}/${notebook_name}"
            else
              # Create subdirectory in workspace before uploading
              if ! databricks workspace mkdirs "${TARGET_PATH}/${notebook_dir}" 2>/dev/null; then
                echo "⚠️  Warning: Could not create directory ${TARGET_PATH}/${notebook_dir} (may already exist or permission issue)"
              fi
              target_file="${TARGET_PATH}/${notebook_dir}/${notebook_name}"
            fi
            
            echo "  • ${relative_path} → ${target_file}.py (auto-added by Databricks)"
            databricks workspace import \
              "$target_file" \
              --file "$notebook" \
              --language PYTHON \
              --overwrite
          done < <(find "$NOTEBOOK_DIR" -type f -print0)
          
          echo ""
          echo "Notebooks uploaded to DR for: $entry"
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  # STEP 3C: Deploy identified bundles to the Stage DR environment (full deployment) - DR REGION
  # Waits for primary deployment to complete before deploying to DR workspace
  deploy-jobs-secondary:
    needs: [detect-changes, deploy-notebooks-only-primary, deploy-notebooks-with-bundles-primary, deploy-jobs-primary]
    if: |
      !cancelled() &&
      needs.detect-changes.outputs.has_changes == 'true' &&
      needs.detect-changes.outputs.notebook_only_changed != 'true' &&
      needs.deploy-notebooks-only-primary.result != 'failure' &&
      needs.deploy-notebooks-with-bundles-primary.result != 'failure' &&
      needs.deploy-jobs-primary.result != 'failure'
    runs-on: ubuntu-latest
    strategy:
      matrix:
        bundle_root: ${{ fromJson(needs.detect-changes.outputs.jobs_matrix || '[]') }}
      fail-fast: false
    
    environment: stage_secondary
    
    # 2026 OIDC Authentication for Azure (passwordless)
    env:
      DATABRICKS_HOST: ${{ vars.DATABRICKS_HOST }}
      DATABRICKS_AUTH_TYPE: azure-cli

    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Azure CLI Login (OIDC)
        uses: azure/login@v2
        with:
          client-id: ${{ vars.SP_CLIENT_ID }}
          tenant-id: ${{ vars.AZ_TENANT_ID }}
          allow-no-subscriptions: true

      - name: Set up Databricks CLI
        uses: databricks/setup-cli@main

      - name: Install jq
        run: sudo apt-get update && sudo apt-get install -y jq

      - name: Install yq
        uses: mikefarah/yq@v4.40.5

      - name: Load contract JSON from stage_secondary.yml (DR)
        id: load_vars
        working-directory: ${{ matrix.bundle_root }}
        run: |
          echo ""
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          echo "LOADING CONTRACT CONFIGURATION (DR REGION)"
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          
          # Get contract_name and check if contract_json is already embedded in bundle.env.variables.yml
          # DR uses same config as primary, only DATABRICKS_HOST differs (from GitHub environment)
          CONTRACT_NAME=$(yq eval -r '.targets.stage_primary.variables.contract_name // ""' includes/stage/bundle.env.variables.yml)
          SUBDOMAIN_CODE=$(yq eval -r '.targets.stage_primary.variables.subdomain_code // ""' includes/stage/bundle.env.variables.yml)
          SOURCE_CODE=$(yq eval -r '.targets.stage_primary.variables.source_code // ""' includes/stage/bundle.env.variables.yml)
          EMBEDDED_CONTRACT_JSON=$(yq eval -r '.targets.stage_primary.variables.contract_json // ""' includes/stage/bundle.env.variables.yml)
          
          echo "Contract Name: $CONTRACT_NAME"
          echo "Subdomain Code: $SUBDOMAIN_CODE"
          echo "Source Code: $SOURCE_CODE"
          echo "Deployment Target: DR workspace"
          echo ""
          
          # Check if contract_json is already embedded (for silver/gold zones using job_params)
          if [ -n "$EMBEDDED_CONTRACT_JSON" ]; then
            echo "Contract JSON already embedded in bundle.env.variables.yml (silver/gold zone with job_params)"
            CONTRACT_JSON="$EMBEDDED_CONTRACT_JSON"
            CONTRACT_SIZE=$(echo "$CONTRACT_JSON" | wc -c)
            echo "Contract size: $CONTRACT_SIZE bytes"
          else
            # Bronze zone: Load contract JSON from contract file
            echo "Loading contract JSON from file (bronze zone with full contract)"
            CONTRACT_FILE="../../../contracts/stage/producers/${CONTRACT_NAME}_contract.json"
            
            if [ ! -f "$CONTRACT_FILE" ]; then
              echo "ERROR: Contract file not found: $CONTRACT_FILE"
              exit 1
            fi
            
            CONTRACT_JSON=$(cat "$CONTRACT_FILE" | jq -c .)
            CONTRACT_SIZE=$(echo "$CONTRACT_JSON" | wc -c)
            
            echo "Contract loaded: $CONTRACT_FILE"
            echo "Contract size: $CONTRACT_SIZE bytes"
          fi
          
          echo ""
          echo "contract_json<<EOF" >> $GITHUB_OUTPUT
          echo "$CONTRACT_JSON" >> $GITHUB_OUTPUT
          echo "EOF" >> $GITHUB_OUTPUT

      - name: Extract Variables for DAB (Dynamic) (DR)
        id: extract_vars
        working-directory: ${{ matrix.bundle_root }}
        run: |
          # Check if bundle.env.variables.yml exists
          if [ ! -f "includes/stage/bundle.env.variables.yml" ]; then
            echo "ERROR: includes/stage/bundle.env.variables.yml not found"
            exit 1
          fi
          
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          echo "DYNAMICALLY EXTRACTING ALL VARIABLES (DR REGION)"
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          echo ""
          
          # Get ALL variable names from bundle.env.variables.yml
          # DR uses same variables as primary (only DATABRICKS_HOST differs via GitHub environment)
          VARIABLE_NAMES=$(yq eval '.targets.stage_primary.variables | keys | .[]' includes/stage/bundle.env.variables.yml)
          
          if [ -z "$VARIABLE_NAMES" ]; then
            echo "ERROR: No variables found in bundle.env.variables.yml"
            exit 1
          fi
          
          echo "Found variables:"
          echo "$VARIABLE_NAMES"
          echo ""
          
          # Required variables that must not be empty
          REQUIRED_VARS=("subdomain_code" "source_code" "contract_name" "service_principal_id" "zone")
          
          # Loop through and extract each variable dynamically
          while IFS= read -r var_name; do
            if [ -z "$var_name" ]; then
              continue
            fi
            
            # Extract variable value (use // "" for null/missing values)
            var_value=$(yq eval ".targets.stage_primary.variables.$var_name // \"\"" includes/stage/bundle.env.variables.yml)
            
            # Validate required variables are not empty
            for required_var in "${REQUIRED_VARS[@]}"; do
              if [ "$var_name" == "$required_var" ] && [ -z "$var_value" ]; then
                echo "ERROR: Required variable '$var_name' is empty"
                exit 1
              fi
            done
            
            # Export to GitHub outputs
            echo "${var_name}=${var_value}" >> $GITHUB_OUTPUT
            echo "  ✓ Extracted: ${var_name}"
          done <<< "$VARIABLE_NAMES"
          
          echo ""
          echo "All variables extracted successfully (DR deployment)!"

      - name: Auto-bind existing job (migration from old target) (DR)
        working-directory: ${{ matrix.bundle_root }}
        continue-on-error: true
        env:
          BUNDLE_VAR_contract_json: ${{ steps.load_vars.outputs.contract_json }}
        run: |
          echo ""
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          echo "AUTO-BIND (DR): Checking for existing jobs from old target"
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          echo ""
          
          # Set BUNDLE_VAR_* for bundle initialization (required for unbind/bind)
          VARIABLE_NAMES=$(yq eval '.targets.stage_primary.variables | keys | .[]' includes/stage/bundle.env.variables.yml)
          while IFS= read -r var_name; do
            [ -z "$var_name" ] && continue
            var_value=$(yq eval ".targets.stage_primary.variables.$var_name // \"\"" includes/stage/bundle.env.variables.yml)
            export "BUNDLE_VAR_${var_name}=${var_value}"
          done <<< "$VARIABLE_NAMES"
          
          # Extract job name pattern from bundle variables
          CONTRACT_NAME=$(yq eval '.targets.stage_primary.variables.contract_name' includes/stage/bundle.env.variables.yml)
          SUBDOMAIN_CODE=$(yq eval '.targets.stage_primary.variables.subdomain_code' includes/stage/bundle.env.variables.yml)
          SOURCE_CODE=$(yq eval '.targets.stage_primary.variables.source_code' includes/stage/bundle.env.variables.yml)
          ENVIRONMENT=$(yq eval '.targets.stage_primary.variables.environment' databricks.yml)
          BIGPANDA_SUFFIX=$(yq eval '.targets.stage_primary.variables.bigpanda_suffix' includes/stage/bundle.env.variables.yml)
          
          # Construct expected job name
          if [ "$SOURCE_CODE" = "null" ] || [ -z "$SOURCE_CODE" ]; then
            EXPECTED_JOB_NAME="[${ENVIRONMENT}]-${SUBDOMAIN_CODE}-${CONTRACT_NAME}${BIGPANDA_SUFFIX}"
          else
            EXPECTED_JOB_NAME="[${ENVIRONMENT}]-${SUBDOMAIN_CODE}-${SOURCE_CODE}-${CONTRACT_NAME}${BIGPANDA_SUFFIX}"
          fi
          
          echo "Searching for existing job in DR workspace: ${EXPECTED_JOB_NAME}"
          
          # List all jobs and find matching job ID
          JOB_ID=$(databricks jobs list --output json | jq -r ".[] | select(.settings.name == \"${EXPECTED_JOB_NAME}\") | .job_id" | head -n1)
          
          if [ -n "$JOB_ID" ]; then
            echo "[FOUND] Existing job ID in DR: ${JOB_ID}"
            
            # Extract resource name from resources/*.yml files (DAB includes pattern)
            RESOURCE_NAME=""
            if [ -d "resources" ]; then
              for resource_file in resources/*.yml; do
                if [ -f "$resource_file" ]; then
                  RESOURCE_NAME=$(yq eval '.resources.jobs | keys | .[]' "$resource_file" 2>/dev/null | head -n1 || echo "")
                  [ -n "$RESOURCE_NAME" ] && break
                fi
              done
            fi
            
            # Fallback: Check main databricks.yml (for inline resources)
            if [ -z "$RESOURCE_NAME" ]; then
              RESOURCE_NAME=$(yq eval '.resources.jobs | keys | .[]' databricks.yml 2>/dev/null | head -n1 || echo "")
            fi
            
            if [ -n "$RESOURCE_NAME" ]; then
              echo "[BIND] Connecting job ${JOB_ID} to bundle resource: ${RESOURCE_NAME}"
              
              # Unbind first to clear any stale Terraform state, then re-bind
              echo "[UNBIND] Clearing existing Terraform state for ${RESOURCE_NAME}..."
              UNBIND_OUTPUT=$(databricks bundle deployment unbind "${RESOURCE_NAME}" --target stage_secondary 2>&1) || true
              if [ -n "$UNBIND_OUTPUT" ]; then echo "$UNBIND_OUTPUT"; fi
              
              # Bind existing job to new DR target
              BIND_OUTPUT=$(databricks bundle deployment bind "${RESOURCE_NAME}" "${JOB_ID}" --target stage_secondary --auto-approve 2>&1) && BIND_RC=0 || BIND_RC=$?
              if [ $BIND_RC -eq 0 ]; then
                echo "[OK] Job successfully bound to stage_secondary target"
                echo "     Future deployments will update this job instead of creating duplicates"
              else
                echo "[WARNING] Bind failed - will proceed with deployment (may create duplicate)"
                echo "$BIND_OUTPUT"
              fi
            else
              echo "[WARNING] Could not determine bundle resource name"
            fi
          else
            echo "[INFO] No existing job found in DR - this is a new deployment"
          fi
          
          echo ""

      - name: Validate Bundle (Dynamic Variables) (DR)
        working-directory: ${{ matrix.bundle_root }}
        env:
          BUNDLE_VAR_contract_json: ${{ steps.load_vars.outputs.contract_json }}
        run: |
          echo ""
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          echo "VALIDATING DATABRICKS BUNDLE (WITH DYNAMIC VARIABLES) (DR REGION)"
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          echo ""
          
          # Dynamically export ALL variables from extract_vars step as BUNDLE_VAR_*
          # DR uses same variables as primary (only DATABRICKS_HOST differs)
          # SKIP pause_status - databricks.yml target override (PAUSED) should take precedence
          echo "Setting up BUNDLE_VAR_ environment variables dynamically..."
          VARIABLE_NAMES=$(yq eval '.targets.stage_primary.variables | keys | .[]' includes/stage/bundle.env.variables.yml)
          
          while IFS= read -r var_name; do
            if [ -z "$var_name" ]; then
              continue
            fi
            
            # Skip pause_status for DR - let databricks.yml target-specific override apply
            if [ "$var_name" == "pause_status" ]; then
              echo "  ⏭️  Skipping pause_status (using databricks.yml target override: PAUSED)"
              continue
            fi
            
            # Get value from GitHub outputs (extract_vars step)
            var_value=$(yq eval ".targets.stage_primary.variables.$var_name // \"\"" includes/stage/bundle.env.variables.yml)
            export "BUNDLE_VAR_${var_name}=${var_value}"
            echo "  ✓ BUNDLE_VAR_${var_name} set"
          done <<< "$VARIABLE_NAMES"
          
          echo ""
          databricks bundle validate -t stage_secondary
          
          echo ""
          echo "Bundle validation passed (DR deployment)!"

      - name: Deploy Bundle (Dynamic Variables) (DR)
        working-directory: ${{ matrix.bundle_root }}
        env:
          BUNDLE_VAR_contract_json: ${{ steps.load_vars.outputs.contract_json }}
        run: |
          echo ""
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          echo "DEPLOYING TO STAGE DR ENVIRONMENT (WITH DYNAMIC VARIABLES)"
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          echo ""
          
          # Dynamically export ALL variables from extract_vars step as BUNDLE_VAR_*
          # DR uses same variables as primary (only DATABRICKS_HOST differs)
          # SKIP pause_status - databricks.yml target override (PAUSED) should take precedence
          echo "Setting up BUNDLE_VAR_ environment variables dynamically..."
          VARIABLE_NAMES=$(yq eval '.targets.stage_primary.variables | keys | .[]' includes/stage/bundle.env.variables.yml)
          
          while IFS= read -r var_name; do
            if [ -z "$var_name" ]; then
              continue
            fi
            
            # Skip pause_status for DR - let databricks.yml target-specific override apply
            if [ "$var_name" == "pause_status" ]; then
              echo "  ⏭️  Skipping pause_status (using databricks.yml target override: PAUSED)"
              continue
            fi
            
            # Get value from GitHub outputs (extract_vars step)
            var_value=$(yq eval ".targets.stage_primary.variables.$var_name // \"\"" includes/stage/bundle.env.variables.yml)
            export "BUNDLE_VAR_${var_name}=${var_value}"
            echo "  ✓ BUNDLE_VAR_${var_name} set"
          done <<< "$VARIABLE_NAMES"
          
          echo ""
          echo "Contract JSON length: ${#BUNDLE_VAR_contract_json} characters"
          echo "Target: stage_secondary (DR workspace with pause_status: PAUSED)"
          echo ""
          
          # Deploy using DR target 'stage_secondary' with pause_status: PAUSED from databricks.yml
          # Auto-bind step above handles Terraform state migration (unbind+bind)
          echo "NOTE: DR jobs deployed with pause_status: PAUSED (databricks.yml target-specific override)"
          echo "      Jobs start in PAUSED state - no race condition risk"
          databricks bundle deploy -t stage_secondary
          
          echo ""
          echo "Deployment completed successfully to DR workspace!"

      - name: Print Deployment Summary (DR)
        working-directory: ${{ matrix.bundle_root }}
        run: |
          # Extract values from variables (DR uses same variables as primary)
          CONTRACT_NAME=$(yq eval '.targets.stage_primary.variables.contract_name' includes/stage/bundle.env.variables.yml)
          ENVIRONMENT=$(yq eval '.targets.stage_primary.variables.environment' databricks.yml)
          ZONE=$(yq eval '.targets.stage_primary.variables.zone' includes/stage/bundle.env.variables.yml)
          SOURCE_CODE=$(yq eval '.targets.stage_primary.variables.source_code' includes/stage/bundle.env.variables.yml)
          SUBDOMAIN_CODE=$(yq eval '.targets.stage_primary.variables.subdomain_code' includes/stage/bundle.env.variables.yml)
          PAUSE_STATUS=$(yq eval '.targets.stage_secondary.variables.pause_status' databricks.yml)
          
          # Extract BigPanda suffix (optional - may be empty)
          BIGPANDA_SUFFIX=$(yq eval '.targets.stage_primary.variables.bigpanda_suffix' includes/stage/bundle.env.variables.yml)
          
          # Construct deployed job name matching deploy_contract.py format
          # Handle null SOURCE_CODE for silver/gold zones
          if [ "$SOURCE_CODE" = "null" ] || [ -z "$SOURCE_CODE" ]; then
            DEPLOYED_JOB_NAME="[${ENVIRONMENT}]-${SUBDOMAIN_CODE}-${CONTRACT_NAME}${BIGPANDA_SUFFIX}"
            NOTEBOOK_PATH="/Workspace/${SUBDOMAIN_CODE}/stage/${ZONE}/${CONTRACT_NAME}/ (transformation notebooks)"
          else
            DEPLOYED_JOB_NAME="[${ENVIRONMENT}]-${SUBDOMAIN_CODE}-${SOURCE_CODE}-${CONTRACT_NAME}${BIGPANDA_SUFFIX}"
            NOTEBOOK_PATH="/Workspace/${SUBDOMAIN_CODE}/stage/${ZONE}/${SOURCE_CODE}/contract_driven_kafka_ingestion_notebook"
          fi
          
          # Construct workspace URL with proper formatting (remove https:// prefix for display)
          WORKSPACE_URL="${DATABRICKS_HOST}"
          WORKSPACE_NAME=$(echo "$DATABRICKS_HOST" | awk -F'//' '{print $2}' | awk -F'.' '{print $1}')
          
          echo ""
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          echo "[OK] DEPLOYMENT SUMMARY - STAGE (DR REGION - FAILOVER/WEST)"
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          echo ""
          echo "Workspace:"
          echo "  • Target: stage_secondary (DR - West Region)" 
          echo "  • Host: ${WORKSPACE_URL}"
          echo "  • Pause Status: ${PAUSE_STATUS} (not running, ready for failover)"
          echo ""
          echo "Job Details:"
          echo "  • Job Name: ${DEPLOYED_JOB_NAME}"
          echo "  • Contract: ${CONTRACT_NAME}"
          echo "  • Zone: ${ZONE}"
          [ "$SOURCE_CODE" != "null" ] && [ -n "$SOURCE_CODE" ] && echo "  • Source Code: ${SOURCE_CODE}"
          echo ""
          if [ "$SOURCE_CODE" = "null" ] || [ -z "$SOURCE_CODE" ]; then
            echo "Notebooks:"
            echo "  • Transformations: ${NOTEBOOK_PATH}"
            echo "  • Shared: /Workspace/${SUBDOMAIN_CODE}/stage/${ZONE}/common/common_functions"
          else
            echo "Notebook:"
            echo "  • Path: ${NOTEBOOK_PATH}"
          fi
          echo ""
          echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"