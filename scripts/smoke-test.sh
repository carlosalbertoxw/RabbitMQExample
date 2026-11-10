#!/usr/bin/env bash
# Prueba de humo: ejecuta los 6 ejemplos contra un RabbitMQ en marcha y comprueba su salida.
#
# Uso (desde cualquier carpeta, con RabbitMQ levantado):
#   docker compose up -d --wait
#   bash scripts/smoke-test.sh
set -euo pipefail

cd "$(dirname "$0")/.."
LOGS="$(mktemp -d)"
PIDS=()

cleanup() {
  for pid in "${PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup EXIT

# Ruta del .dll compilado del proyecto que está en la carpeta indicada.
dll() {
  echo "$1/bin/Release/net10.0/$(basename "$(ls "$1"/*.csproj)" .csproj).dll"
}

# Ejecuta un proyecto en primer plano.
run() {
  local dir=$1; shift
  dotnet "$(dll "$dir")" "$@"
}

# Arranca un proyecto en segundo plano y guarda su salida en $LOGS/<nombre>.log.
# Se lanza "dotnet" directamente (no una función) para que $! sea su PID y las señales le lleguen a él.
start() {
  local name=$1 dir=$2; shift 2
  local path
  path=$(dll "$dir")
  dotnet "$path" "$@" > "$LOGS/$name.log" 2>&1 &
  LAST_PID=$!
  PIDS+=("$LAST_PID")
}

# Espera hasta que el texto aparezca en el log, o falla al vencer el plazo (segundos).
wait_for() {
  local name=$1 text=$2 timeout=${3:-30}
  for ((i = 0; i < timeout * 2; i++)); do
    if grep -qF -- "$text" "$LOGS/$name.log"; then
      echo "  ok  [$name] $text"
      return 0
    fi
    sleep 0.5
  done
  echo "FALLO [$name] no apareció \"$text\" en ${timeout} s. Salida:"
  cat "$LOGS/$name.log"
  exit 1
}

assert_absent() {
  local name=$1 text=$2
  if grep -qF -- "$text" "$LOGS/$name.log"; then
    echo "FALLO [$name] apareció \"$text\" y no debía. Salida:"
    cat "$LOGS/$name.log"
    exit 1
  fi
  echo "  ok  [$name] sin \"$text\""
}

echo "== Compilando"
dotnet build RabbitMQExample.slnx -c Release -nologo -v q

echo "== 01-HelloWorld"
start hello-consumer 01-HelloWorld/Consumer
wait_for hello-consumer "Esperando mensajes"
run 01-HelloWorld/Producer "hola-smoke-test" > "$LOGS/hello-producer.log"
wait_for hello-consumer "Recibido: \"hola-smoke-test\""

echo "== 02-WorkQueues"
start worker-a 02-WorkQueues/Consumer
start worker-b 02-WorkQueues/Consumer
wait_for worker-a "Esperando tareas"
wait_for worker-b "Esperando tareas"
run 02-WorkQueues/Producer 4 > "$LOGS/work-producer.log"
# Con prefetch 1, ambos workers reciben trabajo.
wait_for worker-a "Terminada" 30
wait_for worker-b "Terminada" 30

echo "== 03-PublishSubscribe"
start subscriber-a 03-PublishSubscribe/Consumer A
start subscriber-b 03-PublishSubscribe/Consumer B
wait_for subscriber-a "Suscrito"
wait_for subscriber-b "Suscrito"
run 03-PublishSubscribe/Producer "aviso-smoke-test" > "$LOGS/pubsub-producer.log"
wait_for subscriber-a "aviso-smoke-test"
wait_for subscriber-b "aviso-smoke-test"

echo "== 04-Topics"
start errors-only 04-Topics/Consumer "*.error"
wait_for errors-only "Escuchando"
run 04-Topics/Producer pagos.info "no-debe-llegar" > "$LOGS/topics-producer-1.log"
run 04-Topics/Producer pagos.error "si-debe-llegar" > "$LOGS/topics-producer-2.log"
wait_for errors-only "[pagos.error]: \"si-debe-llegar\""
assert_absent errors-only "no-debe-llegar"

echo "== 05-Rpc"
start rpc-server 05-Rpc/Consumer
wait_for rpc-server "Esperando peticiones"
run 05-Rpc/Producer 360 97 abc > "$LOGS/rpc-client.log"
wait_for rpc-client "360 = 2"
wait_for rpc-client "97 = 97"
wait_for rpc-client "Error: \"abc\""

echo "== 06-ReliableMessaging"
start reliable-consumer 06-ReliableMessaging/Consumer
RELIABLE_PID=$LAST_PID
wait_for reliable-consumer "Procesando pedidos"
run 06-ReliableMessaging/Producer --count 11 --interval 50 > "$LOGS/reliable-producer.log"
wait_for reliable-producer "el broker confirm"
wait_for reliable-consumer "Pedido #1 procesado"
wait_for reliable-consumer "debe ser mayor que cero" 30                # #7: inválido -> orders.dead
wait_for reliable-consumer "Pedido #5 procesado" 40                    # #5: 2 reintentos y éxito
wait_for reliable-consumer "se agotaron los 3 reintentos" 60           # #11: reintentos agotados -> orders.dead

# Apagado ordenado con SIGTERM, la señal que envía "docker stop"; Generic Host la trata igual que Ctrl+C.
# (SIGINT no sirve aquí: bash hace que los procesos lanzados en segundo plano desde un script la ignoren.)
# Git Bash en Windows no puede enviar señales a un proceso nativo, así que solo se comprueba en Linux y macOS.
if [[ "$(uname -s)" == Linux || "$(uname -s)" == Darwin ]]; then
  kill -TERM "$RELIABLE_PID"
  wait_for reliable-consumer "Apagado completo" 30
fi

echo "== Todos los ejemplos funcionan"
