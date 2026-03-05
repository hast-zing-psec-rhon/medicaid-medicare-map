#!/usr/bin/env bash
set -euo pipefail

LABEL="com.mv.medicaid-medicare-map"
REPO_DIR="/Users/mv/code/Municipal Bonds/Medicaid - Medicare Map"
PLIST_SRC="$REPO_DIR/scripts/launchd/$LABEL.plist"
PLIST_DST="$HOME/Library/LaunchAgents/$LABEL.plist"
GUI_DOMAIN="gui/$(id -u)"

ensure_prereqs() {
  if [[ ! -f "$PLIST_SRC" ]]; then
    echo "Missing plist template: $PLIST_SRC" >&2
    exit 1
  fi
  if [[ ! -x "$REPO_DIR/.venv/bin/python" ]]; then
    echo "Missing virtualenv python at $REPO_DIR/.venv/bin/python" >&2
    echo "Run: python3 -m venv .venv && .venv/bin/pip install -r requirements.txt" >&2
    exit 1
  fi
  mkdir -p "$HOME/Library/LaunchAgents"
}

install_service() {
  ensure_prereqs
  cp "$PLIST_SRC" "$PLIST_DST"
  launchctl bootout "$GUI_DOMAIN/$LABEL" >/dev/null 2>&1 || true
  launchctl bootstrap "$GUI_DOMAIN" "$PLIST_DST"
  launchctl enable "$GUI_DOMAIN/$LABEL"
  launchctl kickstart -k "$GUI_DOMAIN/$LABEL"
  echo "Installed and started $LABEL"
}

start_service() {
  if [[ ! -f "$PLIST_DST" ]]; then
    echo "Service not installed yet. Installing now..."
    install_service
    return
  fi

  launchctl enable "$GUI_DOMAIN/$LABEL" >/dev/null 2>&1 || true

  if ! launchctl print "$GUI_DOMAIN/$LABEL" >/dev/null 2>&1; then
    launchctl bootstrap "$GUI_DOMAIN" "$PLIST_DST" >/dev/null 2>&1 || true
  fi

  if ! launchctl kickstart -k "$GUI_DOMAIN/$LABEL" >/dev/null 2>&1; then
    launchctl bootout "$GUI_DOMAIN/$LABEL" >/dev/null 2>&1 || true
    sleep 1
    launchctl bootstrap "$GUI_DOMAIN" "$PLIST_DST"
    launchctl kickstart -k "$GUI_DOMAIN/$LABEL"
  fi

  echo "Started $LABEL"
}

stop_service() {
  if launchctl print "$GUI_DOMAIN/$LABEL" >/dev/null 2>&1; then
    launchctl bootout "$GUI_DOMAIN/$LABEL" >/dev/null 2>&1 || true
    echo "Stopped $LABEL"
  else
    echo "$LABEL is not loaded"
  fi
}

status_service() {
  if launchctl print "$GUI_DOMAIN/$LABEL" >/tmp/${LABEL}.status 2>/dev/null; then
    echo "Service status for $LABEL"
    grep -E 'state = |pid = |last exit code = ' /tmp/${LABEL}.status || true
    rm -f /tmp/${LABEL}.status
  else
    echo "$LABEL is not loaded"
  fi

  if lsof -nP -iTCP:8080 -sTCP:LISTEN >/tmp/${LABEL}.port 2>/dev/null; then
    echo "Listening on 8080:"
    cat /tmp/${LABEL}.port
    rm -f /tmp/${LABEL}.port
  else
    echo "No process currently listening on 8080"
  fi

  code="000"
  for _ in {1..5}; do
    curl -s -o /tmp/${LABEL}.health -w "%{http_code}" http://127.0.0.1:8080/healthz > /tmp/${LABEL}.code || true
    code=$(cat /tmp/${LABEL}.code 2>/dev/null || echo "000")
    [[ "$code" == "200" ]] && break
    sleep 1
  done

  if [[ "$code" == "200" ]]; then
    echo "Health check: OK"
    cat /tmp/${LABEL}.health
    echo
  else
    echo "Health check: FAILED ($code)"
  fi
  rm -f /tmp/${LABEL}.code /tmp/${LABEL}.health
}

uninstall_service() {
  stop_service
  rm -f "$PLIST_DST"
  echo "Uninstalled $LABEL"
}

logs_service() {
  echo "--- stdout ---"
  tail -n 100 /tmp/medicaid_medicare_map.launchd.out.log 2>/dev/null || true
  echo "--- stderr ---"
  tail -n 100 /tmp/medicaid_medicare_map.launchd.err.log 2>/dev/null || true
}

case "${1:-}" in
  install)
    install_service
    ;;
  start)
    start_service
    ;;
  stop)
    stop_service
    ;;
  restart)
    stop_service
    sleep 1
    start_service
    ;;
  status)
    status_service
    ;;
  uninstall)
    uninstall_service
    ;;
  logs)
    logs_service
    ;;
  *)
    echo "Usage: $0 {install|start|stop|restart|status|uninstall|logs}" >&2
    exit 1
    ;;
esac
