#!/bin/bash
set -euo pipefail

LANDOS_DIR="$HOME/dev/landos"
BACKEND_DIR="$LANDOS_DIR/backend"
FRONTEND_DIR="$LANDOS_DIR/frontend"

# Prompt once for sudo and start MongoDB from the launching tab
sudo -v
sudo systemctl start mongod.service

# Wait until MongoDB is actually ready
echo "Waiting for mongod.service to be active..."
until systemctl is-active --quiet mongod.service; do
  sleep 0.5
done

echo "Waiting for MongoDB to accept connections..."
until mongosh --quiet --eval 'db.runCommand({ ping: 1 }).ok' >/dev/null 2>&1; do
  sleep 0.5
done

# Open the other tabs IN THIS SAME TERMINAL WINDOW
xfce4-terminal --tab --title='LandOS Backend' -x bash -lc "
  cd \"$BACKEND_DIR\" &&
  source .venv/bin/activate &&
  PYTHONPATH=.. .venv/bin/uvicorn backend:create_app --factory --host 0.0.0.0 --port 8000;
  exec bash
"

xfce4-terminal --tab --title='LandOS Frontend' -x bash -lc "
  cd \"$FRONTEND_DIR\" &&
  npm run dev;
  exec bash
"

xfce4-terminal --tab --title='Tests' -x bash -lc "
  set +e
  cd \"$BACKEND_DIR\"
  source .venv/bin/activate

  echo '=== Backend tests: PYTHONPATH=. ==='
  PYTHONPATH=. pytest tests -vs
  b1=\$?

  echo '=== Analytics tests ==='
  PYTHONPATH=. pytest services/analytics/tests -vs
  b2=\$?

  echo '=== Frontend unit tests ==='
  cd \"$FRONTEND_DIR\"
  npm run test:unit
  f1=\$?

  echo
  echo '=== Exit codes ==='
  echo \"backend:   \$b1\"
  echo \"analytics: \$b2\"
  echo \"frontend:  \$f1\"

  exec bash
"

# Turn the launching tab into the MongoDB tab
cd "$LANDOS_DIR"
printf '\033]0;%s\007' 'MongoDB'
exec mongosh
