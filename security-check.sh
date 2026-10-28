#!/usr/bin/env bash
set -u
pass=0; fail=0
check(){ if eval "$2" >/dev/null 2>&1; then printf '[PASS] %s\n' "$1"; pass=$((pass+1)); else printf '[FAIL] %s\n' "$1"; fail=$((fail+1)); fi; }
check 'container is running' 'docker inspect -f "{{.State.Running}}" index-bridge | grep -qx true'
check 'container user is non-root' 'test "$(docker inspect -f "{{.Config.User}}" index-bridge)" != "" && test "$(docker inspect -f "{{.Config.User}}" index-bridge)" != "0"'
check 'root filesystem is read-only' 'docker inspect -f "{{.HostConfig.ReadonlyRootfs}}" index-bridge | grep -qx true'
check 'no-new-privileges enabled' 'docker inspect -f "{{json .HostConfig.SecurityOpt}}" index-bridge | grep -q no-new-privileges'
check 'all Linux capabilities dropped' 'docker inspect -f "{{json .HostConfig.CapDrop}}" index-bridge | grep -q ALL'
check 'host port bound to localhost only' 'docker port index-bridge 8000/tcp | grep -q "127.0.0.1:8000"'
check 'outbox volume mounted' 'docker inspect -f "{{range .Mounts}}{{if eq .Destination \"/data\"}}{{.Type}}{{end}}{{end}}" index-bridge | grep -qx volume'
check '.env not world/group readable' 'test "$(stat -c %a .env)" = "600"'
printf '\n%d passed, %d failed\n' "$pass" "$fail"
exit "$fail"
