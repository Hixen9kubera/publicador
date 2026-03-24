#!/usr/bin/env python3
"""
publisher.py — Publicación masiva de productos WooCommerce en MercadoLibre

Flujo por producto:
  1. Leer producto de WooCommerce (meta_data incluye ml_category_id y ml_attr_*)
  2. Obtener atributos requeridos de la categoría ML
  3. Mapear atributos WC → ML
  4. Crear item en ML con status 'paused'
  5. Agregar descripción
  6. Guardar progreso
  7. Esperar 10 segundos antes del siguiente

Uso:
  python publisher.py --cuenta SANCORFASHION
  python publisher.py --cuenta BEKURA --tag 1799
  python publisher.py --cuenta SANCORFASHION --id-min 24924 --id-max 24973
  python publisher.py --cuenta BEKURA --sku VAR-0037-EST-40X30
  python publisher.py --cuenta SANCORFASHION --dry-run
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime

# Cargar .env en desarrollo local (ignorado en producción si no existe)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Forzar UTF-8 en stdout/stderr (evita UnicodeEncodeError en Windows)
if sys.stdout.encoding != 'utf-8':
    sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf-8', buffering=1)
    sys.stderr = open(sys.stderr.fileno(), mode='w', encoding='utf-8', buffering=1)

# Asegurar imports locales
sys.path.insert(0, os.path.dirname(__file__))

import ml_api
import wc_api
from wc_api import update_product_status
from attribute_mapper import build_attributes, build_secondary_attributes
import db
from config import (
    DELAY_ENTRE_PRODUCTOS, MAX_IMAGENES,
    DEFAULT_CURRENCY, DEFAULT_LISTING_TYPE, DEFAULT_CONDITION,
    DEFAULT_BUYING_MODE, DEFAULT_QUANTITY, DEFAULT_BRAND, FREE_SHIPPING_MIN,
    DATA_DIR, PROGRESS_FILE, ML_CUENTAS,
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD,
)

# Inicializar BD si hay credenciales configuradas
if DB_HOST and DB_NAME and DB_USER:
    db.set_credentials(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD)
    try:
        db.create_tables()
    except Exception as _e:
        print(f"  [db] Advertencia — no se pudo conectar a la BD: {_e}")


# ══════════════════════════════════════════════════════════════════════════════
# PROGRESO
# ══════════════════════════════════════════════════════════════════════════════

_USE_DB_PROGRESS = bool(os.environ.get("USE_DB_PROGRESS", ""))


def load_progress() -> dict:
    if _USE_DB_PROGRESS:
        return db.load_progress_db()
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_progress(progress: dict, prog_key: str = None, entry: dict = None) -> None:
    if _USE_DB_PROGRESS:
        if prog_key and entry:
            db.save_progress_db(prog_key, entry)
        return
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
        json.dump(progress, f, ensure_ascii=False, indent=2)


_run_backlog_file: str = ''  # se asigna en main() al inicio de cada corrida


def save_backlog(sku: str, entry: dict) -> None:
    """Agrega el registro del SKU al archivo de backlog de la corrida actual."""
    if not _run_backlog_file:
        return
    os.makedirs(os.path.dirname(_run_backlog_file), exist_ok=True)

    data = {}
    if os.path.exists(_run_backlog_file):
        try:
            with open(_run_backlog_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception:
            data = {}

    if sku not in data:
        data[sku] = []
    data[sku].append(entry)

    with open(_run_backlog_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    # También guardar en BD si está configurada
    db.save_backlog_db(sku, entry)
    print(f"  [db] Registro guardado en ml_backlog ({sku})")


# ══════════════════════════════════════════════════════════════════════════════
# CACHÉ DE ATRIBUTOS ML (para no llamar la API por cada producto)
# ══════════════════════════════════════════════════════════════════════════════

_attr_cache: dict[str, list] = {}

def get_category_attrs_cached(category_id: str, token: str) -> list:
    if category_id not in _attr_cache:
        attrs = ml_api.get_category_attributes(category_id, token)
        _attr_cache[category_id] = attrs
    return _attr_cache[category_id]


# ══════════════════════════════════════════════════════════════════════════════
# CONSTRUCCIÓN DEL PAYLOAD ML
# ══════════════════════════════════════════════════════════════════════════════

def build_payload(prod: dict, token: str, dry_run: bool = False) -> dict | None:
    """
    Construye el payload para POST /items de ML.

    Reglas obligatorias:
    - SIN description (se sube en paso separado)
    - SIN pictures (se suben en paso separado)
    - listing_type_id: gold_special
    - shipping.mode: me2
    - free_shipping: True si precio > $149 MXN
    - BRAND: siempre Ferrahome
    - CONDITION: siempre Nuevo
    - SELLER_SKU: SKU del producto
    - sale_terms: garantía del vendedor 30 días
    """
    category_id = prod['ml_category_id']
    if not category_id:
        print(f"  [!] Sin ml_category_id — saltando {prod['sku']}")
        return None

    if not prod['title']:
        print(f"  [!] Sin título — saltando {prod['sku']}")
        return None

    if prod['price'] <= 0:
        print(f"  [!] Precio inválido ({prod['price']}) — saltando {prod['sku']}")
        return None

    # Stock
    stock = int(prod['stock']) if prod['stock'] else DEFAULT_QUANTITY

    # Detectar si la categoría requiere catálogo (tiene catalog_domain)
    cat_info = ml_api.get_category_info(category_id, token)
    is_catalog_category = bool(cat_info.get('settings', {}).get('catalog_domain'))
    if is_catalog_category:
        print(f"  [cat] Categoría con catalog_domain — usando family_name, omitiendo title")

    # Atributos de categoría + atributos del producto
    ml_category_attrs = get_category_attrs_cached(category_id, token)
    extra_attrs = build_attributes(prod['ml_attrs'], ml_category_attrs, prod.get('wc_attrs', {}))

    # Atributos obligatorios fijos
    fixed_ids = {a['id'] for a in extra_attrs}
    attributes = []

    if 'BRAND' not in fixed_ids:
        attributes.append({'id': 'BRAND', 'value_name': DEFAULT_BRAND})
    # CONDITION va en top-level (condition: new), no en attributes

    attributes.append({'id': 'SELLER_SKU', 'value_name': prod['sku']})
    attributes.extend(extra_attrs)

    def _attr_ids():
        return {a['id'] for a in attributes}

    # MODEL: requerido en muchas categorías — usar el valor de ml_attrs si existe, si no el título
    if 'MODEL' not in _attr_ids():
        model_val = prod['ml_attrs'].get('model') or prod['ml_attrs'].get('modelo') or prod['title'][:60]
        attributes.append({'id': 'MODEL', 'value_name': model_val})

    # PART_NUMBER: requerido en algunas categorías — usar SKU como fallback
    if 'PART_NUMBER' not in _attr_ids():
        attributes.append({'id': 'PART_NUMBER', 'value_name': prod['sku']})

    # GTIN: solo incluir si el producto tiene uno real en sus atributos.
    # Si la categoría requiere GTIN (missing_conditional_required), se reintenta
    # en publish_product con placeholder "0000000000000".
    if 'GTIN' not in _attr_ids():
        gtin_val = (prod['ml_attrs'].get('gtin') or prod['ml_attrs'].get('ean')
                    or prod['ml_attrs'].get('upc') or prod['meta'].get('_gtin'))
        if gtin_val:
            attributes.append({'id': 'GTIN', 'value_name': str(gtin_val)})
    if 'EMPTY_GTIN_REASON' not in _attr_ids():
        attributes.append({'id': 'EMPTY_GTIN_REASON', 'value_id': '17055161', 'value_name': 'Otra razón'})

    # Dimensiones de paquete — ML requiere enteros con unidad: "33 cm" / "600 g"
    # WC guarda peso en kg → convertir a gramos; dimensiones en cm → entero
    # Validación: omitir si alguna dimensión es 0 o inconsistente con el peso
    _w = float(prod['weight']) if prod.get('weight') else 0
    _l = float(prod['length']) if prod.get('length') else 0
    _wi = float(prod['width'])  if prod.get('width')  else 0
    _h = float(prod['height']) if prod.get('height') else 0
    _dims_ok = _l > 0 and _wi > 0 and _h > 0
    # Densidad mínima: 0.001 g/cm³ (objetos muy livianos) y máxima 30 g/cm³ (metal sólido)
    if _dims_ok and _w > 0:
        _vol = _l * _wi * _h
        _density = (_w * 1000) / _vol
        _dims_ok = 0.001 <= _density <= 30
        if not _dims_ok:
            print(f"  [!] Dims paquete omitidas (densidad {_density:.2f} g/cm³ fuera de rango): {_l}x{_wi}x{_h} cm / {_w} kg")
    if _w > 0:
        attributes.append({'id': 'SELLER_PACKAGE_WEIGHT', 'value_name': f"{int(round(_w * 1000))} g"})
    if _dims_ok:
        attributes.append({'id': 'SELLER_PACKAGE_LENGTH', 'value_name': f"{int(round(_l))} cm"})
        attributes.append({'id': 'SELLER_PACKAGE_WIDTH',  'value_name': f"{int(round(_wi))} cm"})
        attributes.append({'id': 'SELLER_PACKAGE_HEIGHT', 'value_name': f"{int(round(_h))} cm"})

    # Características secundarias — atributos opcionales de la categoría
    existing_ids = {a['id'] for a in attributes}
    secondary = build_secondary_attributes(prod, ml_category_attrs, existing_ids)
    if secondary:
        print(f"  Características secundarias: {len(secondary)} atributo(s) encontrado(s)")
    attributes.extend(secondary)

    # Shipping — free_shipping si precio > $149
    free_shipping = prod['price'] > FREE_SHIPPING_MIN

    # Pre-subir imágenes a ML para obtener picture_ids (más rápido que URLs externas)
    raw_images = prod['images'][:MAX_IMAGENES]
    picture_ids = []
    if raw_images and not dry_run:
        print(f"  Pre-subiendo {len(raw_images)} imágenes a ML...")
        for i, url in enumerate(raw_images, 1):
            pid = ml_api.preupload_picture(url, token)
            if pid:
                picture_ids.append({'id': pid})
                print(f"  [✓] Imagen {i}/{len(raw_images)} → {pid}")
            else:
                picture_ids.append({'source': url})
                print(f"  [!] Imagen {i}/{len(raw_images)} falló pre-upload, usando URL")
    elif raw_images:
        picture_ids = [{'source': url} for url in raw_images]

    payload = {
        'category_id':        category_id,
        'price':              prod['price'],
        'currency_id':        DEFAULT_CURRENCY,
        'available_quantity': stock,
        'buying_mode':        DEFAULT_BUYING_MODE,
        'listing_type_id':    DEFAULT_LISTING_TYPE,
        'condition':          DEFAULT_CONDITION,
        'status':             'paused',
        'pictures':           picture_ids,
        'attributes':         attributes,
        'sale_terms': [
            {'id': 'WARRANTY_TYPE', 'value_name': 'Garantía del vendedor'},
            {'id': 'WARRANTY_TIME', 'value_name': '30 días'},
        ],
        'shipping': {
            'mode':           'me2',
            'local_pick_up':  False,
            'free_shipping':  free_shipping,
        },
    }

    # Categorías con catalog_domain: title NO permitido, usar family_name con el título del producto
    # Categorías normales: title requerido
    if is_catalog_category:
        payload['family_name'] = prod['title'][:60]
    else:
        payload['title'] = prod['title']

    return payload


# ══════════════════════════════════════════════════════════════════════════════
# PUBLICAR UN PRODUCTO
# ══════════════════════════════════════════════════════════════════════════════

def publish_product(prod: dict, token: str, dry_run: bool = False, cuenta: str = '') -> dict:
    """
    Publica un producto en ML.
    Retorna dict con resultado: {success, ml_item_id, error}
    """
    sku = prod['sku']
    backlog_key = f"{cuenta}:{sku}" if cuenta else sku
    print(f"\n{'─'*60}")
    print(f"  Cuenta:   {cuenta or '—'}")
    print(f"  SKU:      {sku}")
    print(f"  Título:   {prod['title'][:70]}")
    print(f"  Precio:   ${prod['price']:,.2f} MXN")
    print(f"  Cat ML:   {prod['ml_category_id']} ({prod['ml_category_name']})")
    print(f"  Imágenes: {len(prod['images'])}")
    print(f"  Attrs WC: {prod['ml_attrs']}")

    # Construir payload
    payload = build_payload(prod, token, dry_run=dry_run)
    if payload is None:
        return {'success': False, 'sku': sku, 'error': 'datos_insuficientes'}

    if dry_run:
        print(f"  [DRY RUN] Payload construido OK — no se envía a ML")
        print(f"  Atributos mapeados: {len(payload.get('attributes', []))}")
        result = {'success': True, 'sku': sku, 'ml_item_id': 'DRY_RUN', 'dry_run': True}
        save_backlog(backlog_key, {
            'timestamp':   datetime.now().isoformat(),
            'cuenta':      cuenta,
            'dry_run':     True,
            'wc_id':       prod['wc_id'],
            'payload':     payload,
            'result':      result,
        })
        return result

    timestamp = datetime.now().isoformat()

    # 1. Crear item
    print(f"  Creando item en ML...")
    response, status_code = ml_api.create_item(payload, token)

    # Retry si ML devuelve error 5xx (timeout interno, sobrecarga, etc.)
    if status_code >= 500:
        print(f"  [!] Error {status_code} de ML — reintentando en 15s...")
        time.sleep(15)
        response, status_code = ml_api.create_item(payload, token)

    # Retry 1: si ML exige GTIN → buscar en catálogo ML, luego UPC Item DB, luego placeholder
    if status_code == 400 and any(
        c.get('code') == 'item.attribute.missing_conditional_required'
        and 'GTIN' in c.get('message', '')
        for c in response.get('cause', [])
    ):
        gtin_found = None

        # Opción 1: buscar en catálogo ML por título+categoría
        print(f"  [!] GTIN requerido — buscando en catálogo ML...")
        gtin_found = ml_api.search_gtin_in_catalog(prod['ml_category_id'], prod['title'], token)
        if gtin_found:
            print(f"  [gtin] Encontrado en catálogo ML: {gtin_found}")

        # Opción 2: buscar en UPC Item DB por marca+modelo
        if not gtin_found:
            brand = prod['ml_attrs'].get('BRAND', '') or prod['meta'].get('marca', '')
            model = prod['ml_attrs'].get('MODEL', '') or prod['meta'].get('modelo', '')
            if brand or model:
                print(f"  [!] No encontrado en ML — buscando en UPC Item DB ({brand} {model})...")
                gtin_found = ml_api.search_gtin_upc(brand, model)
                if gtin_found:
                    print(f"  [gtin] Encontrado en UPC Item DB: {gtin_found}")

        # Opción 3: placeholder
        if not gtin_found:
            print(f"  [!] No encontrado — usando placeholder GTIN...")
            gtin_found = '0000000000000'

        # Si encontramos GTIN real, guardarlo en WC para futuros runs
        if gtin_found != '0000000000000':
            from wc_api import save_gtin_to_wc
            if save_gtin_to_wc(prod['wc_id'], gtin_found):
                print(f"  [gtin] Guardado en WooCommerce (_barcode)")

        payload['attributes'] = [a for a in payload['attributes'] if a.get('id') != 'GTIN']
        payload['attributes'].append({'id': 'GTIN', 'value_name': gtin_found})
        response, status_code = ml_api.create_item(payload, token)

    # Retry 2: si el placeholder fue rechazado por formato inválido → quitar todo GTIN
    if status_code == 400 and any(
        'product_identifier.invalid_format' in c.get('code', '')
        for c in response.get('cause', [])
    ):
        print(f"  [!] Placeholder GTIN rechazado — reintentando sin GTIN...")
        payload['attributes'] = [a for a in payload['attributes']
                                  if a.get('id') not in ('GTIN', 'EMPTY_GTIN_REASON')]
        response, status_code = ml_api.create_item(payload, token)

    # Retry 3: GTIN sigue requerido después del placeholder → publicar sin GTIN
    if status_code == 400 and any(
        c.get('code') == 'item.attribute.missing_conditional_required'
        and 'GTIN' in c.get('message', '')
        for c in response.get('cause', [])
    ):
        print(f"  [!] GTIN sigue requerido — reintentando sin GTIN...")
        payload['attributes'] = [a for a in payload['attributes']
                                  if a.get('id') not in ('GTIN', 'EMPTY_GTIN_REASON')]
        response, status_code = ml_api.create_item(payload, token)

    if status_code != 201:
        error_msg = response.get('message', str(response))[:200]
        # Detectar si el error es específicamente por GTIN inválido
        is_gtin_error = any(
            'product_identifier.invalid_format' in c.get('code', '') or
            ('GTIN' in c.get('message', '') and 'invalid' in c.get('message', '').lower())
            for c in response.get('cause', [])
        )
        if is_gtin_error:
            print(f"  [✗] Error GTIN — la cuenta {cuenta} requiere código de barras real para {sku}")
        else:
            print(f"  [✗] Error {status_code}: {error_msg}")
        print(f"  Detalle: {response.get('error', '')} | Causes: {response.get('cause', [])}")
        error_label = f"GTIN_INVALIDO: cuenta {cuenta} requiere código de barras real" if is_gtin_error else f"HTTP {status_code}: {error_msg}"
        result = {'success': False, 'sku': sku, 'error': error_label, 'gtin_error': is_gtin_error}
        save_backlog(backlog_key, {
            'timestamp':    timestamp,
            'cuenta':       cuenta,
            'wc_id':        prod['wc_id'],
            'payload':      payload,
            'ml_response':  response,
            'ml_status':    status_code,
            'result':       result,
        })
        return result

    ml_item_id = response.get('id', '')
    print(f"  [✓] Item creado: {ml_item_id}")

    # 2. Pausar explícitamente (ML ignora status:paused en categorías de catálogo)
    pause_status = ml_api.pause_item(ml_item_id, token)
    if pause_status == 200:
        print(f"  [✓] Publicación pausada")
    elif pause_status == -1:
        print(f"  [!] Timeout al pausar — pausar manualmente desde ML")
    else:
        print(f"  [!] No se pudo pausar (HTTP {pause_status}) — quedó activa")

    # 3. Agregar descripción
    desc_status = None
    if prod['description']:
        print(f"  Subiendo descripción ({len(prod['description'])} chars)...")
        desc_status = ml_api.update_description(ml_item_id, prod['description'], token)
        if desc_status in (200, 201):
            print(f"  [✓] Descripción actualizada")
        else:
            print(f"  [!] Descripción falló (HTTP {desc_status}) — item creado de todas formas")

    ml_url = f"https://articulo.mercadolibre.com.mx/{ml_item_id.replace('MLM', 'MLM-')}"
    result = {
        'success':      True,
        'sku':          sku,
        'wc_id':        prod['wc_id'],
        'ml_item_id':   ml_item_id,
        'ml_url':       ml_url,
        'published_at': timestamp,
    }

    save_backlog(backlog_key, {
        'timestamp':        timestamp,
        'cuenta':           cuenta,
        'wc_id':            prod['wc_id'],
        'title':            prod['title'],
        'price':            prod['price'],
        'category_id':      prod['ml_category_id'],
        'payload':          payload,
        'ml_response':      response,
        'ml_status':        status_code,
        'pics_preuploaded': len([p for p in payload.get('pictures', []) if 'id' in p]),
        'desc_status':      desc_status,
        'ml_item_id':       ml_item_id,
        'ml_url':           ml_url,
        'result':           result,
    })

    return result


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description='Publicación masiva WooCommerce → MercadoLibre'
    )
    parser.add_argument('--cuenta',   default=None,
                        help='Cuenta ML: SANCORFASHION o BEKURA')
    parser.add_argument('--todas-cuentas', action='store_true',
                        help='Publicar en todas las cuentas ML configuradas')
    parser.add_argument('--tag',      type=int, default=None,
                        help='ID de tag WooCommerce (ej: 1799 para tag 41, 1800 para tag 52)')
    parser.add_argument('--id-min',   type=int, default=None,
                        help='ID mínimo de producto WC (para dividir carga entre workers)')
    parser.add_argument('--id-max',   type=int, default=None,
                        help='ID máximo de producto WC')
    parser.add_argument('--sku',      default=None,
                        help='Procesar solo este SKU')
    parser.add_argument('--status',   default='pending',
                        help='Status WooCommerce a procesar (default: pending)')
    parser.add_argument('--ready',    action='store_true',
                        help='Procesar todos los productos con status "ready" (atajo para --status ready)')
    parser.add_argument('--dry-run',  action='store_true',
                        help='Simular sin crear nada en ML')
    parser.add_argument('--delay',    type=int, default=DELAY_ENTRE_PRODUCTOS,
                        help=f'Segundos entre publicaciones (default: {DELAY_ENTRE_PRODUCTOS})')
    parser.add_argument('--limit',    type=int, default=None,
                        help='Procesar solo los primeros N productos (útil para pruebas)')
    parser.add_argument('--solo-imagenes', metavar='SKU',
                        help='Solo subir imágenes de un SKU ya publicado (busca ml_item_id en backlog)')
    parser.add_argument('--sync-wc-status', action='store_true',
                        help='Actualizar a "publish" en WC todos los SKUs exitosos en progress.json')
    args = parser.parse_args()

    # ── Modo --solo-imagenes ──────────────────────────────────────────────────
    if args.solo_imagenes:
        sku = args.solo_imagenes
        backlog_path = os.path.join(DATA_DIR, 'backlog', f"{sku}.json")
        if not os.path.exists(backlog_path):
            print(f"[✗] No hay backlog para {sku} en {backlog_path}")
            sys.exit(1)
        history = json.load(open(backlog_path, encoding='utf-8'))
        # Buscar la última entrada exitosa con ml_item_id
        ml_item_id = None
        images = []
        for entry in reversed(history):
            if entry.get('ml_item_id') and entry['ml_item_id'] != 'DRY_RUN':
                ml_item_id = entry['ml_item_id']
                images = entry.get('payload', {}).get('pictures', [])
                images = [p['source'] for p in images if 'source' in p]
                break
        if not ml_item_id:
            print(f"[✗] No se encontró ml_item_id en el backlog de {sku}")
            sys.exit(1)
        token = ml_api.get_token(args.cuenta)
        print(f"[✓] Token ML: {args.cuenta}")
        print(f"Subiendo {len(images)} imágenes a {ml_item_id} (1 por 1 con delay)...")
        accumulated = []
        for idx, img_url in enumerate(images, start=1):
            accumulated.append(img_url)
            _, status = ml_api.upload_pictures(ml_item_id, accumulated, token)
            if status == 200:
                print(f"  [✓] Imagen {idx}/{len(images)} subida OK")
            else:
                print(f"  [!] Imagen {idx}/{len(images)} falló (HTTP {status})")
            if idx < len(images):
                time.sleep(10)
        sys.exit(0)

    # ── Modo --sync-wc-status ─────────────────────────────────────────────────
    if args.sync_wc_status:
        progress = load_progress()
        exitosos = {k: v for k, v in progress.items() if v.get('success') and not v.get('dry_run')}
        print(f"  {len(exitosos)} entradas exitosas en progress.json")
        ok = err = 0
        seen_wc_ids = set()
        for key, entry in exitosos.items():
            wc_id = entry.get('wc_id')
            if not wc_id or wc_id in seen_wc_ids:
                continue
            seen_wc_ids.add(wc_id)
            sku = entry.get('sku', key)
            if update_product_status(wc_id, 'publish'):
                print(f"  [✓] {sku} (WC {wc_id}) → publish")
                ok += 1
            else:
                print(f"  [✗] {sku} (WC {wc_id}) — falló")
                err += 1
        print(f"\n  Actualizados: {ok} | Fallidos: {err}")
        sys.exit(0)

    # Inicializar archivo de backlog para esta corrida
    global _run_backlog_file
    run_ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    _run_backlog_file = os.path.join(DATA_DIR, 'backlog', f"backlog_progress_{run_ts}.json")

    print("=" * 60)
    print("  PUBLISHER — WooCommerce → MercadoLibre")
    print("=" * 60)
    print(f"  Cuenta:   {args.cuenta}")
    print(f"  Tag:      {args.tag or 'todos'}")
    print(f"  ID range: {args.id_min or '—'} → {args.id_max or '—'}")
    print(f"  SKU:      {args.sku or 'todos'}")
    print(f"  Status WC:{args.status}")
    print(f"  Delay:    {args.delay}s")
    print(f"  Dry run:  {'SÍ' if args.dry_run else 'NO'}")
    print("=" * 60)

    # --ready es atajo para --status ready
    if args.ready:
        args.status = 'ready'

    # Resolver cuentas a usar
    if args.todas_cuentas:
        cuentas = ML_CUENTAS
    elif args.cuenta:
        cuentas = [args.cuenta]
    else:
        print("  [✗] Debes indicar --cuenta o --todas-cuentas")
        sys.exit(1)

    # Cargar productos de WooCommerce (una sola vez)
    print(f"\n  Cargando productos de WooCommerce...")
    products_raw = wc_api.get_products(
        status=args.status,
        tag_id=args.tag,
        id_min=args.id_min,
        id_max=args.id_max,
    )

    if not products_raw:
        print("  No se encontraron productos. Verifica los filtros.")
        sys.exit(0)

    products = [wc_api.parse_product(p) for p in products_raw]
    if args.sku:
        products = [p for p in products if p['sku'] == args.sku]
        if not products:
            print(f"  SKU '{args.sku}' no encontrado.")
            sys.exit(0)

    if args.limit:
        products = products[:args.limit]
        print(f"  Limitando a {args.limit} producto(s)")

    print(f"  [✓] {len(products)} productos × {len(cuentas)} cuenta(s) = {len(products)*len(cuentas)} publicaciones\n")

    # Loop por cuenta
    stats = {'exitosos': 0, 'saltados': 0, 'fallidos': 0}
    publicados_resumen = []  # [{sku, titulo, tags, ml_item_id, cuenta}]
    errores_resumen    = []  # [{sku, cuenta, error}]
    # Rastrear qué cuentas publicaron exitosamente cada SKU en esta corrida
    exitosos_por_sku: dict = {}   # sku → set de cuentas exitosas
    wc_id_por_sku: dict   = {}   # sku → wc_id (para update final)
    for cuenta in cuentas:
        print(f"\n{'='*60}")
        print(f"  CUENTA: {cuenta}")
        print(f"{'='*60}")

        try:
            token = ml_api.get_token(cuenta)
            print(f"  [✓] Token ML cargado para {cuenta}")
        except Exception as e:
            print(f"  [✗] Error cargando token para {cuenta}: {e} — saltando cuenta")
            continue

        progress = load_progress()
        ya_publicados = {k for k, v in progress.items()
                         if v.get('success') and v.get('cuenta') == cuenta
                         and not v.get('dry_run')}
        print(f"  Progreso previo: {len(ya_publicados)} ya publicados en {cuenta}")

        for idx, prod in enumerate(products, 1):
            sku = prod['sku']
            prog_key = f"{cuenta}:{sku}"

            if prog_key in ya_publicados or sku in ya_publicados:
                print(f"\n  [{idx}/{len(products)}] {sku} — ya publicado en {cuenta}, saltando")
                stats['saltados'] += 1
                # Registrar wc_id para verificar al final si todas las cuentas ya publicaron
                wc_id_por_sku[sku] = prod['wc_id']
                exitosos_por_sku.setdefault(sku, set()).add(cuenta)
                continue

            print(f"\n  [{idx}/{len(products)}]", end='')

            result = publish_product(prod, token, dry_run=args.dry_run, cuenta=cuenta)
            result['cuenta'] = cuenta

            progress[prog_key] = result
            save_progress(progress, prog_key=prog_key, entry=result)

            if result['success']:
                stats['exitosos'] += 1
                if not result.get('dry_run'):
                    print(f"  URL: {result.get('ml_url', '')}")
                    publicados_resumen.append({
                        'sku':        sku,
                        'titulo':     prod['title'][:60],
                        'tags':       prod.get('tags', []),
                        'ml_item_id': result.get('ml_item_id', ''),
                        'cuenta':     cuenta,
                    })
                    # Registrar éxito por SKU para actualizar WC al final
                    exitosos_por_sku.setdefault(sku, set()).add(cuenta)
                    wc_id_por_sku[sku] = prod['wc_id']
            else:
                stats['fallidos'] += 1
                errores_resumen.append({
                    'sku':    sku,
                    'cuenta': cuenta,
                    'error':  result.get('error', 'desconocido'),
                })

            if idx < len(products) and not args.dry_run:
                print(f"\n  Esperando {args.delay}s antes del siguiente...")
                time.sleep(args.delay)

    # Actualizar WC a 'publish' si TODAS las cuentas publicaron ese SKU
    # Combina éxitos del run actual + historial en BD (para runs previos)
    cuentas_set = set(cuentas)
    skus_a_verificar = set(exitosos_por_sku.keys()) | set(wc_id_por_sku.keys())
    for sku in skus_a_verificar:
        if args.dry_run:
            continue
        # Éxitos del run actual
        cuentas_ok = exitosos_por_sku.get(sku, set()).copy()
        # Sumar éxitos de runs anteriores desde la BD
        for c in cuentas_set - cuentas_ok:
            if db.is_published(c, sku):
                cuentas_ok.add(c)
        if cuentas_ok >= cuentas_set:
            wc_ok = update_product_status(wc_id_por_sku[sku], 'publish')
            if wc_ok:
                print(f"  [✓] {sku} → WooCommerce 'publish' (todas las cuentas OK)")
            else:
                print(f"  [!] {sku} → no se pudo actualizar WC a 'publish'")
        else:
            faltantes = cuentas_set - cuentas_ok
            print(f"  [~] {sku} → WC sigue en 'ready' (pendiente en: {', '.join(faltantes)})")

    # Resumen final
    print(f"\n{'='*60}")
    print(f"  RESUMEN")
    print(f"{'='*60}")
    print(f"  Total:     {len(products)}")
    print(f"  Exitosos:  {stats['exitosos']}")
    print(f"  Saltados:  {stats['saltados']}")
    print(f"  Fallidos:  {stats['fallidos']}")
    print(f"  Progreso:  {PROGRESS_FILE}")
    if publicados_resumen:
        print(f"\n  PUBLICADOS EN ESTA CORRIDA:")
        print(f"  {'SKU':<20} {'Tags':<30} {'ML Item':<15} {'Cuenta'}")
        print(f"  {'-'*20} {'-'*30} {'-'*15} {'-'*15}")
        for p in publicados_resumen:
            tags_str = ', '.join(p['tags']) if p['tags'] else '(sin tag)'
            print(f"  {p['sku']:<20} {tags_str:<30} {p['ml_item_id']:<15} {p['cuenta']}")
    if errores_resumen:
        print(f"\n  ERRORES EN ESTA CORRIDA (se reintentarán en la siguiente):")
        print(f"  {'Cuenta':<20} {'SKU':<20} {'Error'}")
        print(f"  {'-'*20} {'-'*20} {'-'*30}")
        for e in errores_resumen:
            print(f"  {e['cuenta']:<20} {e['sku']:<20} {str(e['error'])[:60]}")
    print(f"{'='*60}\n")


if __name__ == '__main__':
    main()