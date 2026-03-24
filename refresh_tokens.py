#!/usr/bin/env python3
"""
refresh_tokens.py — Refresca los tokens de ML para todas las cuentas configuradas

Uso:
  python refresh_tokens.py
  python refresh_tokens.py --cuenta SANCORFASHION
"""
import argparse
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

import ml_api
from config import ML_CUENTAS

def main():
    parser = argparse.ArgumentParser(description='Refresca tokens OAuth de MercadoLibre')
    parser.add_argument('--cuenta', default=None,
                        help=f'Cuenta específica (default: todas). Opciones: {ML_CUENTAS}')
    args = parser.parse_args()

    cuentas = [args.cuenta] if args.cuenta else ML_CUENTAS

    print("=" * 50)
    print("  REFRESH TOKENS — MercadoLibre")
    print("=" * 50)

    ok = 0
    for cuenta in cuentas:
        print(f"\n  Refrescando {cuenta}...")
        try:
            ml_api.refresh_token(cuenta)
            ok += 1
        except Exception as e:
            print(f"  [✗] Error en {cuenta}: {e}")

    print(f"\n  Resultado: {ok}/{len(cuentas)} tokens refrescados")
    print("=" * 50)

if __name__ == '__main__':
    main()
