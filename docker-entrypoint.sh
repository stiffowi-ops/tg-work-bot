#!/bin/sh
set -eu
export DATA_DIR="${DATA_DIR:-/app/data}"

if [ "$(id -u)" = 0 ]; then
    case "$DATA_DIR" in
        /app/data|/app/runtime)
            # Limit ownership changes to WorkHub on shared or restored volumes.
            for path in "$DATA_DIR" "$DATA_DIR/employees.json" "$DATA_DIR/data" "$DATA_DIR/data/projects"; do
                if [ -L "$path" ]; then
                    echo "storage setup: refusing symlink at $path" >&2
                    exit 1
                fi
            done
            mkdir -p "$DATA_DIR"
            # A provisioned volume can already be usable even if chown is restricted.
            for path in "$DATA_DIR" "$DATA_DIR/data" "$DATA_DIR/data/projects"; do
                [ -d "$path" ] || continue
                if ! su-exec workhub test -x "$path" || ! su-exec workhub test -w "$path"; then
                    chown workhub:workhub "$path"
                    chmod u+rwx "$path"
                fi
            done
            if [ -f "$DATA_DIR/employees.json" ] && ! su-exec workhub test -r "$DATA_DIR/employees.json"; then
                chown workhub:workhub "$DATA_DIR/employees.json"
                chmod u+r "$DATA_DIR/employees.json"
            fi
            # Restore private JSON files and uploads from volumes with an older owner.
            if [ -d "$DATA_DIR/data/projects" ]; then
                find "$DATA_DIR/data/projects" ! -type l ! -user workhub -exec chown -h workhub:workhub {} +
            fi
            ;;
        *) echo "storage setup: using externally provisioned DATA_DIR=$DATA_DIR" >&2 ;;
    esac
    exec su-exec workhub /usr/local/bin/workhub "$@"
fi
exec /usr/local/bin/workhub "$@"
