#!/bin/bash

# ==============================================================================
#            Definitive CXL Benchmark Automation Script (v10)
#
# - Ensures logical, sorted output in the final CSV by defining a specific
#   execution order for memory policies, from CXL-heavy to DRAM-heavy.
#
# ==============================================================================

# ==============================================================================
#                              CONFIGURATIONS
# ==============================================================================
EXECUTABLE="./wordcount_df"
DATASET_PATH="/home/hzfu/tpp_codes/datasets/news.2024.en.shuffled.deduped"
NUM_RUNS=1
FIXED_THREADS_FOR_RATIO_TEST=20
THREAD_LIST_FOR_SCALING_TEST="4 8 16 32 64"
FIXED_RATIO_FOR_SCALING_TEST="Interleave5050"
CPU_NODE_BIND="0"
LOCAL_MEM_NODE=0
CXL_MEM_NODE=8
OUTPUT_CSV="wc_benchmark_definitive_results_v13_short_input.csv"
NUMACTL_CMD="numactl"

# ==============================================================================
#                          FUNCTION DEFINITIONS
# ==============================================================================

DEMOTION_PATH="/sys/kernel/mm/numa/demotion_enabled"
BALANCING_PATH="/proc/sys/kernel/numa_balancing"
ZONE_RECLAIM_PATH="/proc/sys/vm/zone_reclaim_mode"
WEIGHTED_INTERLEAVE_BASE_PATH="/sys/kernel/mm/mempolicy/weighted_interleave"

function get_vmstat_val() {
    local val=$(grep "$1" /proc/vmstat | awk '{print $2}'); echo "${val:-0}";
}

function set_scenario() {
    echo "Applying Scenario: demotion=$1, balancing=$2, reclaim=$3"
    if ! echo "$1" > "$DEMOTION_PATH" || ! echo "$2" > "$BALANCING_PATH" || ! echo "$3" > "$ZONE_RECLAIM_PATH"; then
        echo "ERROR: Failed to apply scenario settings." >&2; exit 1;
    fi; sleep 1;
}

function run_single_trial() {
    local scenario_name=$1 policy_name=$2 numactl_options=$3 threads=$4 run_id=$5 page_size=$6 policy_nodes=$7
    echo; echo "--> Trial: Scenario=[$scenario_name], Policy=[$policy_name], Threads=[$threads], Run=[$run_id]"
    if [[ "$policy_name" == Weighted-* ]]; then
        if [ ! -d "$WEIGHTED_INTERLEAVE_BASE_PATH" ]; then
            echo "    ERROR: Path for weighted interleave not found." >&2
            echo "$scenario_name,$policy_name,$CPU_NODE_BIND,\"$policy_nodes\",$threads,$run_id,ERROR,0,0.00,0,0.00" >> "$OUTPUT_CSV"; return;
        fi;
        local weights=$(echo "$policy_name" | cut -d'-' -f2); local weight_local=$(echo "$weights" | cut -d':' -f1); local weight_cxl=$(echo "$weights" | cut -d':' -f2)
        echo "    Setting weights for $policy_name -> Node $LOCAL_MEM_NODE: $weight_local, Node $CXL_MEM_NODE: $weight_cxl"
        echo "$weight_local" > "$WEIGHTED_INTERLEAVE_BASE_PATH/node$LOCAL_MEM_NODE"; echo "$weight_cxl" > "$WEIGHTED_INTERLEAVE_BASE_PATH/node$CXL_MEM_NODE"
    fi
    local promote_before=$(get_vmstat_val 'pgpromote_success'); local demote_kswapd_before=$(get_vmstat_val 'pgdemote_kswapd'); local demote_direct_before=$(get_vmstat_val 'pgdemote_direct'); local demote_before=$((demote_kswapd_before + demote_direct_before))
    echo "    Flushing page cache..."; sync; echo 3 > /proc/sys/vm/drop_caches; sleep 1
    local map_tasks=$threads; local reduce_tasks=$threads
    local full_command="$NUMACTL_CMD --cpunodebind=$CPU_NODE_BIND $numactl_options"
    echo "    Executing: stdbuf -o0 $full_command $EXECUTABLE $map_tasks $reduce_tasks standard file \"$DATASET_PATH\""
    local output_and_errors; output_and_errors=$(stdbuf -o0 $full_command $EXECUTABLE $map_tasks $reduce_tasks standard file "$DATASET_PATH" 2>&1)
    local promote_after=$(get_vmstat_val 'pgpromote_success'); local demote_kswapd_after=$(get_vmstat_val 'pgdemote_kswapd'); local demote_direct_after=$(get_vmstat_val 'pgdemote_direct'); local demote_after=$((demote_kswapd_after + demote_direct_after))
    local promotes_delta=$((promote_after - promote_before)); local demotes_delta=$((demote_after - demote_before))
    local pages_per_gb=$((1024 * 1024 * 1024 / page_size))
    local promotes_kb=$(echo "scale=2; ($promotes_delta * $page_size) / 1024" | bc)
    local demotes_gb="0.00"; if [ $pages_per_gb -gt 0 ]; then demotes_gb=$(echo "scale=2; $demotes_delta / $pages_per_gb" | bc); fi
    local execution_time_raw=$(echo "$output_and_errors" | grep '\[TOTAL TIME\]' | awk '{print $3}'); local execution_time="ERROR"
    if [ -z "$execution_time_raw" ]; then
        echo "    ERROR: Failed to capture execution time." >&2; echo "    Program Output:" >&2; echo "$output_and_errors" >&2
    else
        execution_time=$(printf "%.2f" "$execution_time_raw"); echo "    SUCCESS: Time=$execution_time s, Promotes=$promotes_delta, Demotes=$demotes_delta"
    fi
    echo "$scenario_name,$policy_name,$CPU_NODE_BIND,\"$policy_nodes\",$threads,$run_id,$execution_time,$promotes_delta,$promotes_kb,$demotes_delta,$demotes_gb" >> "$OUTPUT_CSV"
}

# ==============================================================================
#                           MAIN EXECUTION SCRIPT
# ==============================================================================

# 1. Initial System Checks
if [ "$(id -u)" -ne 0 ]; then echo "ERROR: Root privileges required." >&2; exit 1; fi
if [ ! -f "$EXECUTABLE" ]; then echo "ERROR: Executable not found: '$EXECUTABLE'." >&2; exit 1; fi
if ! command -v bc &> /dev/null; then echo "ERROR: 'bc' not installed." >&2; exit 1; fi
if ! $NUMACTL_CMD --help | grep -q "weighted-interleave"; then echo "WARNING: numactl lacks --weighted-interleave support."; fi

PAGE_SIZE=$(getconf PAGE_SIZE); echo "Detected System Page Size: $PAGE_SIZE Bytes"; echo

# 2. Backup Settings
echo "Backing up original system settings...";
ORIGINAL_DEMOTION=$(cat "$DEMOTION_PATH" 2>/dev/null || echo "N/A"); ORIGINAL_BALANCING=$(cat "$BALANCING_PATH" 2>/dev/null || echo "N/A"); ORIGINAL_RECLAIM=$(cat "$ZONE_RECLAIM_PATH" 2>/dev/null || echo "N/A");
echo "Backup complete."; echo

# 3. Prepare Results File
echo "Scenario,MemoryPolicy,CPU_Node,Mem_Policy_Node,Threads,RunID,ExecutionTime_s,Promotes,Promotes_KB,Demotes,Demotes_GB" > "$OUTPUT_CSV"

# 4. Define Scenarios and Policies
declare -A scenarios; scenarios["TPP_ON"]="true;2;1"; scenarios["TPP_OFF"]="false;0;0"
declare -A policies; declare -A policy_node_map

# --- *** ORDERED LIST OF POLICIES *** ---
# This indexed array controls the execution order.
declare -a policy_order=(
    "CXLOnly"
    "PreferredCXL"
    "Weighted-1:5"
    "Weighted-1:4"
    "Weighted-1:3"
    "Weighted-1:2"
    "Interleave5050"
    "Weighted-1:1"
    "Weighted-2:1"
    "Weighted-3:1"
    "Weighted-4:1"
    "Weighted-5:1"
    "LocalOnly"
)

# --- Policy Definitions (unordered is fine here) ---
policies["LocalOnly"]="--membind=$LOCAL_MEM_NODE"; policy_node_map["LocalOnly"]="$LOCAL_MEM_NODE"
policies["CXLOnly"]="--membind=$CXL_MEM_NODE"; policy_node_map["CXLOnly"]="$CXL_MEM_NODE"
policies["Interleave5050"]="--interleave=$LOCAL_MEM_NODE,$CXL_MEM_NODE"; policy_node_map["Interleave5050"]="$LOCAL_MEM_NODE,$CXL_MEM_NODE"
policies["PreferredCXL"]="--preferred=$CXL_MEM_NODE"; policy_node_map["PreferredCXL"]="$CXL_MEM_NODE (preferred)"
policies["Weighted-1:1"]="--weighted-interleave=$LOCAL_MEM_NODE,$CXL_MEM_NODE"; policy_node_map["Weighted-1:1"]="$LOCAL_MEM_NODE,$CXL_MEM_NODE"
policies["Weighted-1:2"]="--weighted-interleave=$LOCAL_MEM_NODE,$CXL_MEM_NODE"; policy_node_map["Weighted-1:2"]="$LOCAL_MEM_NODE,$CXL_MEM_NODE"
policies["Weighted-1:3"]="--weighted-interleave=$LOCAL_MEM_NODE,$CXL_MEM_NODE"; policy_node_map["Weighted-1:3"]="$LOCAL_MEM_NODE,$CXL_MEM_NODE"
policies["Weighted-1:4"]="--weighted-interleave=$LOCAL_MEM_NODE,$CXL_MEM_NODE"; policy_node_map["Weighted-1:4"]="$LOCAL_MEM_NODE,$CXL_MEM_NODE"
policies["Weighted-1:5"]="--weighted-interleave=$LOCAL_MEM_NODE,$CXL_MEM_NODE"; policy_node_map["Weighted-1:5"]="$LOCAL_MEM_NODE,$CXL_MEM_NODE"
policies["Weighted-2:1"]="--weighted-interleave=$LOCAL_MEM_NODE,$CXL_MEM_NODE"; policy_node_map["Weighted-2:1"]="$LOCAL_MEM_NODE,$CXL_MEM_NODE"
policies["Weighted-3:1"]="--weighted-interleave=$LOCAL_MEM_NODE,$CXL_MEM_NODE"; policy_node_map["Weighted-3:1"]="$LOCAL_MEM_NODE,$CXL_MEM_NODE"
policies["Weighted-4:1"]="--weighted-interleave=$LOCAL_MEM_NODE,$CXL_MEM_NODE"; policy_node_map["Weighted-4:1"]="$LOCAL_MEM_NODE,$CXL_MEM_NODE"
policies["Weighted-5:1"]="--weighted-interleave=$LOCAL_MEM_NODE,$CXL_MEM_NODE"; policy_node_map["Weighted-5:1"]="$LOCAL_MEM_NODE,$CXL_MEM_NODE"

# 5. Run Benchmark Phases
# PHASE 1
echo "========================================================================"
echo "== PHASE 1: Testing Memory Ratios with Fixed Threads ($FIXED_THREADS_FOR_RATIO_TEST)"
echo "========================================================================"
for scenario_name in "${!scenarios[@]}"; do
    IFS=';' read -r demotion bal reclaim <<< "${scenarios[$scenario_name]}"; set_scenario "$demotion" "$bal" "$reclaim"
    # MODIFIED: Iterate over the ordered array
    for policy_name in "${policy_order[@]}"; do
        numactl_options=${policies[$policy_name]}; policy_nodes=${policy_node_map[$policy_name]}
        for i in $(seq 1 $NUM_RUNS); do
            run_single_trial "$scenario_name" "$policy_name" "$numactl_options" "$FIXED_THREADS_FOR_RATIO_TEST" "$i" "$PAGE_SIZE" "$policy_nodes"
        done
    done
done

# PHASE 2
echo "========================================================================"
echo "== PHASE 2: Testing Thread Scaling with Fixed Memory Policy ($FIXED_RATIO_FOR_SCALING_TEST)"
echo "========================================================================"
fixed_policy_name=$FIXED_RATIO_FOR_SCALING_TEST; numactl_options=${policies[$fixed_policy_name]}; policy_nodes=${policy_node_map[$fixed_policy_name]}
for scenario_name in "${!scenarios[@]}"; do
    IFS=';' read -r demotion bal reclaim <<< "${scenarios[$scenario_name]}"; set_scenario "$demotion" "$bal" "$reclaim"
    for threads in $THREAD_LIST_FOR_SCALING_TEST; do
        for i in $(seq 1 $NUM_RUNS); do
            run_single_trial "$scenario_name" "$fixed_policy_name" "$numactl_options" "$threads" "$i" "$PAGE_SIZE" "$policy_nodes"
        done
    done
done

# 6. Cleanup
echo; echo "========================================================================"
echo "==  All tests complete. Restoring original system settings..."
if [ "$ORIGINAL_DEMOTION" != "N/A" ]; then echo "$ORIGINAL_DEMOTION" > "$DEMOTION_PATH"; fi
if [ "$ORIGINAL_BALANCING" != "N/A" ]; then echo "$ORIGINAL_BALANCING" > "$BALANCING_PATH"; fi
if [ "$ORIGINAL_RECLAIM" != "N/A" ]; then echo "$ORIGINAL_RECLAIM" > "$ZONE_RECLAIM_PATH"; fi
echo "==  System settings restored."; echo "==  Detailed results saved to $OUTPUT_CSV"
echo "========================================================================"