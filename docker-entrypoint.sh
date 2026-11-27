#!/bin/sh
set -eu

export DATA_DIR="${DATA_DIR:-/app/data}"

# Bothost mounts its persistent volume after the image is built.
# Prepare only the known storage directories; never follow symlinks.
if [ "$(id -u)" = 0 ]; then
    case "$DATA_DIR" in
        /app/data|/app/runtime)
            if [ -L "$DATA_DIR" ]; then
                echo "storage setup: refusing to change ownership of symlink $DATA_DIR" >&2
            elif mkdir -p "$DATA_DIR" && chown -Rh workhub:workhub "$DATA_DIR" && chmod u+rwx "$DATA_DIR"; then
                echo "storage setup: prepared $DATA_DIR for workhub"
            else
                echo "storage setup: cannot prepare $DATA_DIR; check mounted volume permissions" >&2
            fi
            ;;
        *) echo "storage setup: custom DATA_DIR; provision permissions externally" >&2 ;;
    esac
    exec su-exec workhub /usr/local/bin/workhub "$@"
fi

echo "storage setup: running as uid $(id -u); using existing permissions for $DATA_DIR"
exec /usr/local/bin/workhub "$@"
