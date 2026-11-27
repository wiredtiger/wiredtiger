# Set the number of jobs and the budget for each job on the filesystem holding the run homes.
format_disk_budget()
{
    local path=$1 requested_jobs=$2 available_kib available_mib reserve_mib usable_mib
    local min_job_mib=4096

    available_kib=$(LC_ALL=C df -Pk -- "$path" | awk 'NR == 2 { print $4 }')
    if [[ ! $available_kib =~ ^[0-9]+$ ]]; then
        echo "Cannot determine available disk space for $path" >&2
        return 1
    fi

    available_mib=$((available_kib / 1024))
    reserve_mib=$((available_mib / 4))
    # Keep room for files outside the run home, including Evergreen artifacts.
    if ((reserve_mib < 4096)); then
        reserve_mib=4096
    fi
    usable_mib=$((available_mib - reserve_mib))
    if ((usable_mib < min_job_mib)); then
        echo "Insufficient disk space for test/format at $path: $available_mib MiB available," \
            "$reserve_mib MiB reserved, $min_job_mib MiB needed per job" >&2
        return 1
    fi

    format_disk_jobs=$((usable_mib / min_job_mib))
    if ((format_disk_jobs > requested_jobs)); then
        format_disk_jobs=$requested_jobs
    fi
    format_disk_budget_mib=$((usable_mib / format_disk_jobs))
    echo "format disk budget: $format_disk_budget_mib MiB per job, $format_disk_jobs/$requested_jobs" \
        "jobs, $reserve_mib MiB reserved on $path"
}
