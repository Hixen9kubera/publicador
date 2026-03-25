"""
scheduler.py — Ejecuta el publisher automáticamente cada día a las 13:00 (hora México).

Uso:
  python scheduler.py                   # inicia el scheduler en primer plano
  python scheduler.py --run-now         # ejecuta inmediatamente además de programar
  python scheduler.py --hora 15:30      # cambia la hora de ejecución diaria

El proceso debe permanecer activo en el servidor (usar screen, tmux, o un servicio systemd).

Requisitos:
  pip install apscheduler
"""

# Cargar .env en desarrollo local
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import argparse
import logging
import subprocess
import sys
import os
from datetime import datetime

try:
    from apscheduler.schedulers.blocking import BlockingScheduler
    from apscheduler.triggers.cron       import CronTrigger
except ImportError:
    print("[✗] Falta apscheduler. Instala con: pip install apscheduler")
    sys.exit(1)

# ── Configuración ─────────────────────────────────────────────────────────────

PUBLISHER_PATH = os.path.join(os.path.dirname(__file__), 'publisher.py')
PYTHON_BIN     = sys.executable          # mismo Python que corre este script
TIMEZONE       = 'America/Mexico_City'
DEFAULT_HORA   = '13:00'

# Comando que se ejecutará diariamente
CMD_ARGS = ['--todas-cuentas', '--ready', '--limit', '5']

# ── Logging ───────────────────────────────────────────────────────────────────

_handlers = [logging.StreamHandler(sys.stdout)]
_log_file = os.path.join(os.path.dirname(__file__), 'data', 'scheduler.log')
try:
    os.makedirs(os.path.dirname(_log_file), exist_ok=True)
    _handlers.append(logging.FileHandler(_log_file, encoding='utf-8'))
except Exception:
    pass  # En App Platform no hay filesystem, solo stdout

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=_handlers,
)
log = logging.getLogger('scheduler')


# ── Función que se ejecuta en cada disparo ────────────────────────────────────

def run_publisher():
    log.info("=" * 60)
    log.info("  DISPARO AUTOMÁTICO — iniciando publisher")
    log.info("=" * 60)

    cmd = [PYTHON_BIN, PUBLISHER_PATH] + CMD_ARGS
    log.info(f"  Comando: {' '.join(cmd)}")

    try:
        result = subprocess.run(
            cmd,
            cwd=os.path.dirname(__file__),
            capture_output=False,   # deja stdout/stderr pasar a la terminal/log
            timeout=3600,           # máximo 1 hora por corrida
        )
        if result.returncode == 0:
            log.info("  [✓] Publisher terminó correctamente")
        else:
            log.warning(f"  [!] Publisher terminó con código {result.returncode}")
    except subprocess.TimeoutExpired:
        log.error("  [✗] Timeout — el publisher tardó más de 1 hora")
    except Exception as e:
        log.error(f"  [✗] Error ejecutando publisher: {e}")

    log.info(f"  Próxima ejecución: a la siguiente hora en punto (hora México)")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Scheduler diario para publisher ML')
    parser.add_argument('--hora',     default=DEFAULT_HORA,
                        help='Hora de ejecución diaria en formato HH:MM (default: 13:00)')
    parser.add_argument('--run-now',  action='store_true',
                        help='Ejecutar inmediatamente al iniciar (además del cron)')
    args = parser.parse_args()

    # Asegurar que el directorio de logs existe
    os.makedirs(os.path.join(os.path.dirname(__file__), 'data'), exist_ok=True)

    hora, minuto = args.hora.split(':')

    log.info("=" * 60)
    log.info("  SCHEDULER KUBERA — WooCommerce → MercadoLibre")
    log.info("=" * 60)
    log.info(f"  Ejecución:         Cada hora en punto (todos los días, hora México)")
    log.info(f"  Comando:           publisher.py {' '.join(CMD_ARGS)}")
    log.info(f"  PID:               {os.getpid()}")
    log.info("=" * 60)

    scheduler = BlockingScheduler(timezone=TIMEZONE)
    # Cada hora, todos los días
    scheduler.add_job(
        run_publisher,
        trigger=CronTrigger(minute=0, timezone=TIMEZONE),
        id='publisher_cada_hora',
        name='Publisher WC->ML cada hora',
        misfire_grace_time=300,
        coalesce=True,
    )

    # Prueba única a las 14:31 hora México (2026-03-25)
    from datetime import datetime as _dt
    from zoneinfo import ZoneInfo as _ZI
    from apscheduler.triggers.date import DateTrigger as _DT
    _prueba = _dt(2026, 3, 25, 14, 31, 0, tzinfo=_ZI(TIMEZONE))
    scheduler.add_job(
        run_publisher,
        trigger=_DT(run_date=_prueba, timezone=TIMEZONE),
        id='prueba_1431',
        name='Prueba única 14:31',
    )
    log.info("  Prueba única programada: 2026-03-25 14:31 hora Mexico")

    if args.run_now:
        log.info("  --run-now: ejecutando inmediatamente...")
        run_publisher()

    log.info(f"\n  Scheduler activo. Disparos: cada hora en punto")
    log.info("  Ctrl+C para detener.\n")

    try:
        scheduler.start()
    except KeyboardInterrupt:
        log.info("  Scheduler detenido manualmente.")


if __name__ == '__main__':
    main()
