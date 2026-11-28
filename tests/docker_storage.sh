#!/usr/bin/env bash
set -euo pipefail

# A new, disposable volume. Only the two test containers ever mount it.
image=${1:-workhub-verification}
volume=$(docker volume create)
container=''
cleanup() {
    if [[ -n "$container" ]]; then docker rm -f "$container" >/dev/null; fi
    docker volume rm "$volume" >/dev/null
}
trap cleanup EXIT

start() {
    container=$(docker run -d -p 127.0.0.1::3000 \
        --mount "type=volume,src=$volume,dst=/app/data" \
        --add-host api.telegram.org:127.0.0.1 \
        -e PORT=3000 -e BOT_TOKEN=container-test-only -e ADMIN_USER_IDS=42 "$image")
    base="http://$(docker port "$container" 3000/tcp)"
    for ((i=0; i<50; i++)); do
        if curl -fsS "$base/api/health" 2>/dev/null | jq -e '.ok' >/dev/null; then return; fi
        sleep 0.2
    done
    docker logs "$container" >&2
    return 1
}

# Use only the fixture token. Profile creation attempts an optional Telegram
# photo lookup, routed to loopback above so this test runs offline.
auth=$(python3 - <<'PY'
import hashlib, hmac, time, urllib.parse
params = {"auth_date": str(int(time.time())), "user": '{"id":42,"first_name":"Test"}'}
key = hmac.digest(b"WebAppData", b"container-test-only", hashlib.sha256)
data = "\n".join(f"{k}={v}" for k, v in sorted(params.items()))
params["hash"] = hmac.new(key, data.encode(), hashlib.sha256).hexdigest()
print("Authorization: tma " + urllib.parse.urlencode(params))
PY
)
start
# PID 1, the actual server, must run unprivileged.
docker exec "$container" awk '/^Uid:/{exit ($2==0 || $2!=$3)}' /proc/1/status
profile=$(curl -fsS -H "$auth" "$base/api/me" | jq '.name="Restart check" | .project="calendar"')
curl -fsS -X PUT -H "$auth" -H 'Content-Type: application/json' --data "$profile" "$base/api/me" >/dev/null
project=$(curl -fsS -H "$auth" "$base/api/content/calendar?edit=1" | jq '.title="Volume check"')
curl -fsS -X PUT -H "$auth" -H 'Content-Type: application/json' --data "$project" "$base/api/content/calendar" >/dev/null
block=$(printf 'persistent bytes' | curl -fsS -H "$auth" -F kind=document -F title=Persistence \
    -F published=true -F 'file=@-;filename=test.txt' "$base/api/content/calendar/blocks" | jq -er '.blocks[-1].id')
docker exec -u workhub "$container" test -r "/app/data/data/projects/calendar/files/$block"
docker stop "$container" >/dev/null
docker rm "$container" >/dev/null
container=''

start
curl -fsS -H "$auth" "$base/api/me" | jq -e '.name=="Restart check" and .project=="calendar"' >/dev/null
curl -fsS -H "$auth" "$base/api/content/calendar" | jq -e --arg id "$block" '.title=="Volume check" and any(.blocks[]; .id==$id)' >/dev/null
[[ "$(curl -fsS -H "$auth" "$base/api/content/calendar/file/$block")" == 'persistent bytes' ]]
docker stop "$container" >/dev/null
docker rm "$container" >/dev/null
container=''

# Running a real image with an unwritable store must fail before serving HTTP.
# The time limit also catches an accidental successful start.
set +e
output=$(timeout 12 docker run --rm --user workhub --mount "type=volume,src=$volume,dst=/app/data,readonly" "$image" 2>&1)
status=$?
set -e
if [[ $status == 0 || $status == 124 || "$output" != *"storage setup failed"* ]]; then
    echo "Unexpected read-only volume result ($status): $output" >&2
    exit 1
fi
echo "Container recreation preserved profile, project and file; read-only storage was rejected."
