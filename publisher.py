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
import image_editor
import wc_media
from config import (
    DELAY_ENTRE_PRODUCTOS, MAX_IMAGENES,
    DEFAULT_CURRENCY, DEFAULT_LISTING_TYPE, DEFAULT_CONDITION,
    DEFAULT_BUYING_MODE, DEFAULT_QUANTITY, DEFAULT_BRAND, FREE_SHIPPING_MIN,
    DATA_DIR, PROGRESS_FILE, ML_CUENTAS,
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD,
)

# Inicializar BD — REQUERIDA: si no conecta, abortar (no procesar nada sin BD)
if not (DB_HOST and DB_NAME and DB_USER):
    print("  [db] ERROR — Credenciales de BD no configuradas (DB_HOST/DB_NAME/DB_USER).")
    print("  [db] El publisher requiere conexión a BD para ejecutarse. Abortando.")
    sys.exit(1)

db.set_credentials(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD)
if db.ensure_connection(max_retries=5, base_delay=5):
    print(f"  [db] BD conectada exitosamente")
    try:
        db.create_tables()
    except Exception as _e:
        print(f"  [db] Advertencia — create_tables falló: {_e}")
else:
    print(f"  [db] ERROR — No se pudo conectar a la BD después de varios intentos.")
    print(f"  [db] Abortando sin procesar productos. Verifica la conexión y vuelve a ejecutar.")
    sys.exit(1)


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
_sale_terms_cache: dict[str, list] = {}

def get_category_attrs_cached(category_id: str, token: str) -> list:
    if category_id not in _attr_cache:
        attrs = ml_api.get_category_attributes(category_id, token)
        _attr_cache[category_id] = attrs
    return _attr_cache[category_id]


def get_sale_terms_cached(category_id: str, token: str) -> list:
    if category_id not in _sale_terms_cache:
        terms = ml_api.get_category_sale_terms(category_id, token)
        _sale_terms_cache[category_id] = terms
    return _sale_terms_cache[category_id]


def build_sale_terms(category_id: str, token: str) -> list:
    """
    Construye la lista de sale_terms usando value_id del API de ML.
    Fallback a IDs conocidos si el API no responde.
    """
    # Fallbacks conocidos para MLM (México)
    WARRANTY_TYPE_SELLER = '6150835'   # "Garantía del vendedor"
    WARRANTY_TIME_30D    = '180 días'  # value_name para WARRANTY_TIME (acepta texto libre)

    terms = get_sale_terms_cached(category_id, token)
    sale_terms = []

    # WARRANTY_TYPE — requiere value_id obligatorio
    wt = next((t for t in terms if t.get('id') == 'WARRANTY_TYPE'), None)
    if wt:
        # Buscar el value_id de "Garantía del vendedor"
        seller_val = None
        for v in wt.get('values', []):
            vname = (v.get('name') or '').lower()
            if 'vendedor' in vname or 'seller' in vname:
                seller_val = v.get('id')
                break
        # Si no encontramos "vendedor", usar el primer valor disponible
        if not seller_val and wt.get('values'):
            seller_val = wt['values'][0].get('id')
        sale_terms.append({'id': 'WARRANTY_TYPE', 'value_id': seller_val or WARRANTY_TYPE_SELLER})
    else:
        sale_terms.append({'id': 'WARRANTY_TYPE', 'value_id': WARRANTY_TYPE_SELLER})

    # WARRANTY_TIME — puede ser value_name (texto libre) o value_id según la categoría
    wtime = next((t for t in terms if t.get('id') == 'WARRANTY_TIME'), None)
    if wtime and wtime.get('values'):
        # Buscar "30 días" o similar en los valores permitidos
        time_val = None
        for v in wtime.get('values', []):
            vname = (v.get('name') or '').lower()
            if '30' in vname or '180' in vname:
                time_val = v.get('id')
                break
        if time_val:
            sale_terms.append({'id': 'WARRANTY_TIME', 'value_id': time_val})
        else:
            sale_terms.append({'id': 'WARRANTY_TIME', 'value_name': WARRANTY_TIME_30D})
    else:
        sale_terms.append({'id': 'WARRANTY_TIME', 'value_name': WARRANTY_TIME_30D})

    return sale_terms


# ══════════════════════════════════════════════════════════════════════════════
# PREPROCESS IMÁGENES — Edición IA + upload a WP Media (una sola vez por SKU)
# ══════════════════════════════════════════════════════════════════════════════

def preprocess_product_images(prod: dict) -> dict:
    """
    Por cada imagen del producto:
      - Si tiene los 3 flags en False → usa la URL original
      - Si tiene algún flag True → descarga, edita con Gemini, sube a WP Media UNA vez
      - Si ya fue editada en una corrida previa (ml_image_edit_backlog) → reusa

    Guarda fila por imagen en ml_image_edit_backlog (edited | skip_no_flags | error).
    NUNCA guarda bytes en disco local.

    Retorna:
      {
        'urls_for_ml': [str, ...],       # URLs a pre-subir a ML (nuevas o originales)
        'id_map':      {old_wc_id: new_wc_id},  # sólo imágenes editadas
        'gallery':     {...},            # commercekit_image_gallery (sin modificar)
        'all_skip':    bool              # True si NINGUNA imagen necesitó edición
      }
    """
    from datetime import datetime as _dt

    sku          = prod['sku']
    wc_id        = prod['wc_id']
    edit_flags   = prod.get('edit_flags', {}) or {}
    images_det   = prod.get('images_detail', []) or []
    gallery      = prod.get('commercekit_gallery', {}) or {}

    result = {
        'urls_for_ml': [],
        'id_map':      {},
        'gallery':     gallery,
        'all_skip':    True,
        'errors':      [],       # detalles de imágenes con error IA
        'has_errors':  False,    # si True → NO publicar en ML
    }

    if not images_det:
        return result

    prior_cache = db.load_edit_cache(wc_id) if wc_id else {}
    n = len(images_det)

    print(f"\n  ── Preprocess imágenes IA ({n} imágenes) — SKU {sku} ──")

    for i, img in enumerate(images_det, 1):
        wc_img_id = img.get('id')
        src_url   = img.get('src') or ''
        if not src_url:
            continue

        flags = edit_flags.get(wc_img_id) or {}
        flags_active = bool(
            flags.get('quitar_fondo') or
            flags.get('traducir_texto') or
            flags.get('cambiar_modelo')
        )

        # ── Sin flags activos → usar URL original ──────────────────────────
        if not flags_active:
            print(f"  [IMG {i}/{n}] wc_img={wc_img_id} | sin flags → sube original tal cual")
            result['urls_for_ml'].append(src_url)
            db.save_image_edit_backlog({
                'run_key':     sku,
                'sku':         sku,
                'wc_id':       wc_id,
                'wc_image_id': wc_img_id or 0,
                'src_url':     src_url,
                'flag_quitar_fondo':   False,
                'flag_traducir_texto': False,
                'flag_cambiar_modelo': False,
                'action':      'skip_no_flags',
            })
            continue

        # ── Ya editada previamente → reusar (sin re-gastar Gemini) ─────────
        if wc_img_id in prior_cache:
            cached = prior_cache[wc_img_id]
            print(f"  [IMG {i}/{n}] wc_img={wc_img_id} | flags: {image_editor.format_flags_line(flags)}")
            print(f"    └─ ✨ ya editada (wp_media_id={cached['wp_media_id_new']}) → reusando de BD")
            result['urls_for_ml'].append(cached['wp_url_new'])
            result['id_map'][wc_img_id] = cached['wp_media_id_new']
            result['all_skip'] = False
            continue

        # ── Editar con Gemini + subir a WP Media ───────────────────────────
        print(f"  [IMG {i}/{n}] wc_img={wc_img_id} | flags: {image_editor.format_flags_line(flags)}")
        edited_bytes, info = image_editor.process_image(src_url, flags)
        info['run_key']     = sku
        info['sku']         = sku
        info['wc_id']       = wc_id
        info['wc_image_id'] = wc_img_id or 0

        if info.get('person_desc'):
            print(f"    └─ describe_person: '{info['person_desc']}'")
        if info.get('prompt_used'):
            p = info['prompt_used']
            print(f"    └─ prompt: {p[:180]}{'...' if len(p) > 180 else ''}")

        if edited_bytes is None:
            err = info.get('gemini_error') or 'unknown'
            print(f"    └─ ✗ GEMINI FAIL tras 3 reintentos: {err}")
            print(f"    └─ ⛔ producto NO se publicará hasta que Gemini funcione")
            bl_id = db.save_image_edit_backlog(info)
            print(f"    └─ backlog row: ml_image_edit_backlog.id={bl_id} (action=error)")
            result['errors'].append({
                'wc_image_id': wc_img_id,
                'flags':       image_editor.format_flags_line(flags),
                'error':       err[:200],
                'backlog_id':  bl_id,
            })
            result['has_errors'] = True
            result['urls_for_ml'].append(src_url)  # placeholder; no se usará porque no publicamos
            continue

        kb_in  = (info.get('bytes_in')  or 0) // 1024
        kb_out = (info.get('bytes_out') or 0) // 1024
        print(f"    └─ Gemini {info.get('gemini_model')} → OK ({kb_in}KB → {kb_out}KB)")

        filename = f"{sku}_img{wc_img_id}_{int(_dt.now().timestamp())}.jpg"
        wp = wc_media.upload_edited_image(edited_bytes, filename)
        if wp is None:
            err = 'wp_upload_failed'
            info['gemini_error'] = (info.get('gemini_error') or '') + f' | {err}'
            info['action'] = 'error'
            print(f"    └─ ✗ WP Media upload falló")
            print(f"    └─ ⛔ producto NO se publicará (imagen editada pero no almacenada en WP)")
            bl_id = db.save_image_edit_backlog(info)
            print(f"    └─ backlog row: ml_image_edit_backlog.id={bl_id} (action=error)")
            result['errors'].append({
                'wc_image_id': wc_img_id,
                'flags':       image_editor.format_flags_line(flags),
                'error':       err,
                'backlog_id':  bl_id,
            })
            result['has_errors'] = True
            result['urls_for_ml'].append(src_url)  # placeholder; no se usará
            continue

        info['wp_media_id_new'] = wp['id']
        info['wp_url_new']      = wp['url']
        bl_id = db.save_image_edit_backlog(info)
        print(f"    └─ WP Media: id={wp['id']} url={wp['url']}")
        print(f"    └─ backlog row: ml_image_edit_backlog.id={bl_id}")

        result['urls_for_ml'].append(wp['url'])
        result['id_map'][wc_img_id] = wp['id']
        result['all_skip'] = False

    n_edit = len(result['id_map'])
    n_err  = len(result['errors'])
    if result['has_errors']:
        print(f"  Preprocess: {n_err} imagen(es) con ERROR — SKU {sku} NO se publicará hasta corregir")
    elif result['all_skip']:
        print(f"  Preprocess: 0 editadas / {n} originales → se suben todas tal cual a ML")
    else:
        print(f"  Preprocess: {n_edit} editada(s) + {n - n_edit} original(es) = {n} total a ML")

    return result


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

    # MANUFACTURER: requerido en algunas categorías — usar valor de BRAND como fallback
    if 'MANUFACTURER' not in _attr_ids():
        cat_has_manufacturer = any(a.get('id') == 'MANUFACTURER' for a in ml_category_attrs)
        if cat_has_manufacturer:
            brand_val = next((a.get('value_name', '') for a in attributes if a.get('id') == 'BRAND'), DEFAULT_BRAND) or DEFAULT_BRAND
            attributes.append({'id': 'MANUFACTURER', 'value_name': brand_val})

    # GTIN: incluir si el producto tiene uno en _barcode (campo manual WC), ml_attrs o _gtin.
    # Si la categoría requiere GTIN (missing_conditional_required), se reintenta
    # en publish_product con _barcode manual → catálogo ML → UPC Item DB → placeholder.
    if 'GTIN' not in _attr_ids():
        gtin_val = (prod['meta'].get('_barcode') or prod['ml_attrs'].get('gtin')
                    or prod['ml_attrs'].get('ean') or prod['ml_attrs'].get('upc')
                    or prod['meta'].get('_gtin'))
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
    # ML requiere las 4 dimensiones de paquete juntas o ninguna
    if _dims_ok and _w > 0:
        attributes.append({'id': 'SELLER_PACKAGE_WEIGHT', 'value_name': f"{int(round(_w * 1000))} g"})
        attributes.append({'id': 'SELLER_PACKAGE_LENGTH', 'value_name': f"{int(round(_l))} cm"})
        attributes.append({'id': 'SELLER_PACKAGE_WIDTH',  'value_name': f"{int(round(_wi))} cm"})
        attributes.append({'id': 'SELLER_PACKAGE_HEIGHT', 'value_name': f"{int(round(_h))} cm"})

    # DEPTH: requerido en algunas categorías — usar prod['length'] como fallback si no se mapeó
    if 'DEPTH' not in _attr_ids() and _l > 0:
        cat_has_depth = any(a.get('id') == 'DEPTH' for a in ml_category_attrs)
        if cat_has_depth:
            attributes.append({'id': 'DEPTH', 'value_name': f"{_l} cm"})

    # Características secundarias — atributos opcionales de la categoría
    existing_ids = {a['id'] for a in attributes}
    secondary = build_secondary_attributes(prod, ml_category_attrs, existing_ids)
    if secondary:
        print(f"  Características secundarias: {len(secondary)} atributo(s) encontrado(s)")
    attributes.extend(secondary)

    # Shipping — free_shipping si precio > $149
    free_shipping = prod['price'] > FREE_SHIPPING_MIN

    # Pre-subir imágenes a ML para obtener picture_ids (más rápido que URLs externas)
    # Si hubo preprocess IA, usa las URLs nuevas (WP Media); si no, las originales.
    raw_images = (prod.get('images_for_ml') or prod['images'])[:MAX_IMAGENES]
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
        'sale_terms': build_sale_terms(category_id, token),
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

    # Retry si token expiró durante la ejecución (401) → refrescar y reintentar
    if status_code == 401:
        print(f"  [!] Token expirado (401) — refrescando token de {cuenta}...")
        try:
            token = ml_api.refresh_token(cuenta)
            print(f"  [✓] Token refrescado — reintentando create_item...")
            response, status_code = ml_api.create_item(payload, token)
        except Exception as e:
            print(f"  [✗] No se pudo refrescar token: {e}")

    # Retry si ML devuelve error 5xx (timeout interno, sobrecarga, etc.)
    if status_code >= 500:
        print(f"  [!] Error {status_code} de ML — reintentando en 15s...")
        time.sleep(15)
        response, status_code = ml_api.create_item(payload, token)

    # Retry 1: si ML exige GTIN → _barcode WC → catálogo ML → UPC Item DB → placeholder
    # Si el placeholder también falla, el error queda en backlog para revisión manual.
    if status_code == 400 and any(
        c.get('code') == 'item.attribute.missing_conditional_required'
        and 'GTIN' in c.get('message', '')
        for c in response.get('cause', [])
    ):
        gtin_found = None

        # Opción 0: _barcode ingresado manualmente en WooCommerce
        gtin_wc = (prod['meta'].get('_barcode') or prod['meta'].get('_gtin') or '').strip()
        if gtin_wc:
            gtin_found = gtin_wc
            print(f"  [gtin] Usando _barcode de WooCommerce: {gtin_found}")

        # Opción 1: buscar en catálogo ML por título+categoría
        if not gtin_found:
            print(f"  [!] GTIN requerido — buscando en catálogo ML...")
            gtin_found = ml_api.search_gtin_in_catalog(prod['ml_category_id'], prod['title'], token)
            if gtin_found:
                print(f"  [gtin] Encontrado en catálogo ML: {gtin_found}")

        # Opción 2: buscar en UPC Item DB por título genérico (sin marca propia)
        if not gtin_found:
            model = prod['ml_attrs'].get('MODEL', '') or prod['meta'].get('modelo', '')
            query = model if model else prod['title']
            print(f"  [!] No encontrado en ML — buscando en UPC Item DB ({query[:60]})...")
            gtin_found = ml_api.search_gtin_upc('', query)
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

    # Retry: SALE_FORMAT=Unidad requiere UNITS_PER_PACK → agregar con valor 1
    if status_code == 400 and any(
        c.get('code') == 'item.attribute.invalid_sale_units'
        for c in response.get('cause', [])
    ):
        print(f"  [!] UNITS_PER_PACK requerido — reintentando con valor 1...")
        payload['attributes'] = [a for a in payload['attributes'] if a.get('id') != 'UNITS_PER_PACK']
        payload['attributes'].append({'id': 'UNITS_PER_PACK', 'value_name': '1'})
        response, status_code = ml_api.create_item(payload, token)

    # Retry: imágenes demasiado pequeñas (<500px) → re-preupload con escalado
    # ml_api.preupload_picture ya escala imágenes pequeñas con Pillow. Forzar
    # pre-upload de las que quedaron como {'source': url}.
    if status_code == 400 and any(
        'item.pictures.invalid_size' in c.get('code', '')
        for c in response.get('cause', [])
    ):
        print(f"  [!] Imágenes rechazadas por tamaño — re-subiendo con escalado automático...")
        new_pictures = []
        for pic in payload.get('pictures', []):
            if 'id' in pic:
                new_pictures.append(pic)  # ya pre-subida, conservar
            elif 'source' in pic:
                pid = ml_api.preupload_picture(pic['source'], token)
                if pid:
                    new_pictures.append({'id': pid})
                    print(f"    [✓] Re-subida con escalado → {pid}")
                # Si falla, omitir esta imagen
        if new_pictures:
            payload['pictures'] = new_pictures
            response, status_code = ml_api.create_item(payload, token)

    # Retry: título no concuerda con el atributo GENDER (ej: ROP-0197 con GENDER mal asignado).
    # Quitar GENDER del payload — ML lo infiere del título o queda como omitido.
    if status_code == 400 and any(
        c.get('code') == 'invalid.title.gender'
        for c in response.get('cause', [])
    ):
        _removed = [a for a in payload['attributes'] if a.get('id') in ('GENDER', 'GENDER_NAME')]
        if _removed:
            print(f"  [!] Title/gender mismatch — quitando atributo GENDER y reintentando")
            payload['attributes'] = [a for a in payload['attributes']
                                     if a.get('id') not in ('GENDER', 'GENDER_NAME')]
            response, status_code = ml_api.create_item(payload, token)

    # Retry: SIZE_GRID_ID inválido o faltante (categorías de ropa / calzado con fashion_grid).
    # Sin acceso a una tabla de tallas real, lo mejor que podemos hacer es quitar SIZE_GRID_ID
    # del payload para evitar el valor placeholder. Si ML también exige el grid, el item
    # quedará como error en backlog (requiere que el seller configure una guía de tallas
    # en su cuenta ML).
    if status_code == 400 and any(
        c.get('code') in ('invalid.fashion_grid.grid_id.values',
                          'missing.fashion_grid.grid_id.values')
        for c in response.get('cause', [])
    ):
        removed = [a for a in payload['attributes'] if a.get('id') == 'SIZE_GRID_ID']
        if removed:
            print(f"  [!] SIZE_GRID_ID inválido — quitando y reintentando (valor previo: {removed[0].get('value_name')})")
            payload['attributes'] = [a for a in payload['attributes'] if a.get('id') != 'SIZE_GRID_ID']
            response, status_code = ml_api.create_item(payload, token)

    # Retry: atributo de tipo picture con value_name inválido (ej: ENERGY_EFFICIENCY_LABEL='A')
    # → quitar el atributo y reintentar. ML los valida contra un picture_id real.
    if status_code == 400:
        _bad_picture_attrs = set()
        for c in response.get('cause', []):
            if c.get('code') == 'item.attribute.value_name.invalid' and 'type picture' in c.get('message', ''):
                import re as _re
                m = _re.search(r'Attribute (\w+)', c.get('message', ''))
                if m:
                    _bad_picture_attrs.add(m.group(1))
        if _bad_picture_attrs:
            print(f"  [!] Atributos tipo picture con valor inválido — quitando: {_bad_picture_attrs}")
            payload['attributes'] = [a for a in payload['attributes'] if a.get('id') not in _bad_picture_attrs]
            response, status_code = ml_api.create_item(payload, token)

    # Retry: dimensiones de paquete inválidas → quitar todas y reintentar
    if status_code == 400 and any(
        'invalid.seller.package.dimensions' in c.get('code', '')
        for c in response.get('cause', [])
    ):
        _pkg_ids = {'SELLER_PACKAGE_WEIGHT', 'SELLER_PACKAGE_LENGTH', 'SELLER_PACKAGE_WIDTH', 'SELLER_PACKAGE_HEIGHT'}
        print(f"  [!] Dims paquete rechazadas por ML — reintentando sin dimensiones de paquete...")
        payload['attributes'] = [a for a in payload['attributes'] if a.get('id') not in _pkg_ids]
        response, status_code = ml_api.create_item(payload, token)

    # Retry: dimensiones de paquete FALTANTES (requeridas por la categoría pero no enviadas)
    # → agregar valores por defecto razonables y reintentar
    if status_code == 400 and any(
        'missing.seller.package.dimensions' in c.get('code', '')
        for c in response.get('cause', [])
    ):
        _pkg_ids = {'SELLER_PACKAGE_WEIGHT', 'SELLER_PACKAGE_LENGTH', 'SELLER_PACKAGE_WIDTH', 'SELLER_PACKAGE_HEIGHT'}
        # Quitar dims parciales si hubiera alguna
        payload['attributes'] = [a for a in payload['attributes'] if a.get('id') not in _pkg_ids]
        # Valores por defecto conservadores: 1 kg, 30x20x15 cm (caja mediana)
        payload['attributes'].append({'id': 'SELLER_PACKAGE_WEIGHT', 'value_name': '1000 g'})
        payload['attributes'].append({'id': 'SELLER_PACKAGE_LENGTH', 'value_name': '30 cm'})
        payload['attributes'].append({'id': 'SELLER_PACKAGE_WIDTH',  'value_name': '20 cm'})
        payload['attributes'].append({'id': 'SELLER_PACKAGE_HEIGHT', 'value_name': '15 cm'})
        print(f"  [!] Dims paquete requeridas pero no disponibles — reintentando con defaults (1kg, 30x20x15cm)...")
        response, status_code = ml_api.create_item(payload, token)

    # Retry: sale_term WARRANTY_TYPE inválido → obtener value_id correcto del error y reintentar
    if status_code == 400 and any(
        c.get('code') in ('sale_term.invalid_value_id', 'sale_term.value_id_required')
        for c in response.get('cause', [])
    ):
        print(f"  [!] sale_terms inválidos — corrigiendo value_id y reintentando...")
        # Extraer value_id permitido del mensaje de error si está disponible
        for cause in response.get('cause', []):
            msg = cause.get('message', '')
            if 'WARRANTY_TYPE' in msg and 'Allowed values are' in msg:
                import re
                match = re.search(r'\[(\d+)\]', msg)
                if match:
                    correct_id = match.group(1)
                    print(f"  [!] Usando WARRANTY_TYPE value_id={correct_id} del error de ML")
                    for st in payload['sale_terms']:
                        if st['id'] == 'WARRANTY_TYPE':
                            st.pop('value_name', None)
                            st['value_id'] = correct_id
                            break
        # Asegurar que todos los sale_terms tengan value_id en lugar de value_name para WARRANTY_TYPE
        for st in payload['sale_terms']:
            if st['id'] == 'WARRANTY_TYPE' and 'value_id' not in st:
                st.pop('value_name', None)
                st['value_id'] = '6150835'
        response, status_code = ml_api.create_item(payload, token)

    # Retry: GTIN con formato inválido (placeholder rechazado) → quitar GTIN, dejar solo EMPTY_GTIN_REASON
    # Si la categoría requiere GTIN obligatorio, el retry sin GTIN también fallará —
    # en ese caso restauramos y dejamos que se marque como gtin_error.
    _gtin_placeholder_rejected = False
    if status_code == 400 and any(
        'product_identifier.invalid_format' in c.get('code', '')
        for c in response.get('cause', [])
    ):
        _gtin_placeholder_rejected = True
        # Guardar GTIN actual por si hay que restaurarlo
        _saved_gtin = next((a for a in payload['attributes'] if a.get('id') == 'GTIN'), None)
        print(f"  [!] GTIN rechazado por formato inválido — reintentando sin GTIN (solo EMPTY_GTIN_REASON)...")
        payload['attributes'] = [a for a in payload['attributes'] if a.get('id') != 'GTIN']
        # Asegurar que EMPTY_GTIN_REASON esté presente
        if not any(a.get('id') == 'EMPTY_GTIN_REASON' for a in payload['attributes']):
            payload['attributes'].append({'id': 'EMPTY_GTIN_REASON', 'value_id': '17055161', 'value_name': 'Otra razón'})
        response, status_code = ml_api.create_item(payload, token)
        # Si la categoría requiere GTIN obligatorio, restaurar el GTIN para el backlog
        if status_code == 400 and any(
            c.get('code') == 'item.attribute.missing_conditional_required'
            and 'GTIN' in c.get('message', '')
            for c in response.get('cause', [])
        ):
            print(f"  [!] Categoría requiere GTIN obligatorio — se necesita código de barras real")
            if _saved_gtin:
                payload['attributes'].append(_saved_gtin)

    if status_code != 201:
        error_msg = response.get('message', str(response))[:200]
        # Detectar si el error es específicamente por GTIN inválido/requerido
        is_gtin_error = _gtin_placeholder_rejected or any(
            'product_identifier.invalid_format' in c.get('code', '') or
            (c.get('code') == 'item.attribute.missing_conditional_required'
             and 'GTIN' in c.get('message', '')) or
            ('GTIN' in c.get('message', '') and 'invalid' in c.get('message', '').lower())
            for c in response.get('cause', [])
        )
        # Detectar errores que requieren configuración manual en la cuenta ML del seller
        # (no-recuperables desde el código — marcar para no reintentar en corridas futuras)
        needs_manual = any(
            c.get('code') in ('missing.fashion_grid.grid_id.values',
                              'invalid.fashion_grid.grid_id.values',
                              'shipping.lost_me1_by_user',
                              'invalid.title.gender',
                              'item.pictures.invalid_size')
            for c in response.get('cause', [])
        )
        manual_reasons = []
        for c in response.get('cause', []):
            code = c.get('code', '')
            if code in ('missing.fashion_grid.grid_id.values', 'invalid.fashion_grid.grid_id.values'):
                manual_reasons.append('GRID_REQUERIDO (configurar guía de tallas en ML)')
            elif code == 'shipping.lost_me1_by_user':
                manual_reasons.append('ME1_INACTIVO (activar Mercado Envíos 1 en dashboard ML)')
            elif code == 'invalid.title.gender':
                manual_reasons.append('TITLE_GENDER_MISMATCH (revisar título y atributo GENDER del producto en WC)')
            elif code == 'item.pictures.invalid_size':
                manual_reasons.append('IMAGES_TOO_SMALL (subir imágenes ≥500x250 px al producto en WC)')
        if is_gtin_error:
            print(f"  [✗] Error GTIN — la cuenta {cuenta} requiere código de barras real para {sku}")
        elif needs_manual:
            print(f"  [✗] Requiere configuración manual en cuenta {cuenta}: {', '.join(manual_reasons)}")
        else:
            print(f"  [✗] Error {status_code}: {error_msg}")
        print(f"  Detalle: {response.get('error', '')} | Causes: {response.get('cause', [])}")
        if is_gtin_error:
            error_label = f"GTIN_INVALIDO: cuenta {cuenta} requiere código de barras real"
        elif needs_manual:
            error_label = f"NEEDS_MANUAL_CONFIG: {' | '.join(manual_reasons)}"
        else:
            error_label = f"HTTP {status_code}: {error_msg}"
        result = {'success': False, 'sku': sku, 'error': error_label,
                  'gtin_error': is_gtin_error, 'needs_manual_config': needs_manual}
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

    # NOTA: el slice por --limit se aplica MÁS ABAJO, dentro del loop por cuenta,
    # DESPUÉS de descartar SKUs ya publicados y los marcados como NEEDS_MANUAL_CONFIG.
    # Así el límite cuenta solo "intentos reales" y los saltos por error manual no
    # consumen el cupo (ej: si limit=8 y 3 SKUs son NEEDS_MANUAL, se procesan 8 nuevos
    # en lugar de quedarse con solo 5).
    if args.limit:
        print(f"  Límite por cuenta: {args.limit} producto(s) publicables (NEEDS_MANUAL no consume cupo)")

    # Índice por SKU para acceso rápido en el sync post-publicación
    prod_by_sku: dict = {p['sku']: p for p in products}

    print(f"  [✓] {len(products)} productos en pool × {len(cuentas)} cuenta(s)\n")

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
        # SKUs con error no-recuperable (requieren config manual en ML): saltarlos
        # para no seguir contaminando el backlog con reintentos que siempre fallan.
        needs_manual = {k for k, v in progress.items()
                        if v.get('cuenta') == cuenta
                        and not v.get('success')
                        and (v.get('error') or '').startswith('NEEDS_MANUAL_CONFIG')}
        print(f"  Progreso previo: {len(ya_publicados)} ya publicados en {cuenta}")
        if needs_manual:
            print(f"  En pool {len(needs_manual)} SKU(s) marcados NEEDS_MANUAL — no consumen cupo del límite")

        # Filtrar productos publicables EN ESTA CUENTA: descartar ya publicados
        # y los marcados como NEEDS_MANUAL_CONFIG. Aplicar --limit aquí (por cuenta)
        # para que el cupo cuente solo intentos reales.
        def _es_publicable(p):
            pk = f"{cuenta}:{p['sku']}"
            if pk in needs_manual:
                return False
            if pk in ya_publicados or p['sku'] in ya_publicados:
                return False
            return True

        products_publicables = [p for p in products if _es_publicable(p)]
        descartados = len(products) - len(products_publicables)
        if args.limit:
            products_iter = products_publicables[:args.limit]
        else:
            products_iter = products_publicables
        print(f"  Pool {len(products)} → publicables {len(products_publicables)} (descartados {descartados}: ya pub + needs_manual) → procesando {len(products_iter)}")

        for idx, prod in enumerate(products_iter, 1):
            sku = prod['sku']
            prog_key = f"{cuenta}:{sku}"

            # Verificar conexión a BD antes de cada producto. Si se cae, abortar.
            if not db.ensure_connection(max_retries=5, base_delay=5):
                print(f"\n  [db] ERROR — Se perdió la conexión a la BD a mitad del proceso.")
                print(f"  [db] Abortando para no continuar sin registrar progreso. Restaura la BD y vuelve a ejecutar.")
                sys.exit(1)

            # Safety: estos dos branches no deberían dispararse porque pre-filtré
            # en products_iter, pero los dejo por defensa por si carga progreso vieja.
            if prog_key in needs_manual:
                print(f"\n  [{idx}/{len(products_iter)}] {sku} — requiere config manual en ML, saltando")
                stats['saltados'] += 1
                wc_id_por_sku[sku] = prod['wc_id']
                continue

            if prog_key in ya_publicados or sku in ya_publicados:
                print(f"\n  [{idx}/{len(products_iter)}] {sku} — ya publicado en {cuenta}, saltando")
                stats['saltados'] += 1
                wc_id_por_sku[sku] = prod['wc_id']
                exitosos_por_sku.setdefault(sku, set()).add(cuenta)
                continue

            print(f"\n  [{idx}/{len(products_iter)}]", end='')

            # Preprocess IA de imágenes (lazy — solo la primera cuenta lo ejecuta,
            # las siguientes reusan prod['images_for_ml']). Skip en dry_run.
            if 'images_for_ml' not in prod and not args.dry_run:
                try:
                    preproc = preprocess_product_images(prod)
                    prod['_preprocess']   = preproc
                    prod['images_for_ml'] = preproc['urls_for_ml']
                except Exception as e:
                    # Error crítico de preprocess (Gemini API key faltante, import error, etc.).
                    # Si el producto tenía flags activos, bloquear publicación. Si no tenía, seguir.
                    has_active_flags = any(
                        bool(f.get('quitar_fondo') or f.get('traducir_texto') or f.get('cambiar_modelo'))
                        for f in (prod.get('edit_flags') or {}).values()
                    )
                    print(f"  [preprocess] EXCEPCIÓN en {sku}: {e}")
                    if has_active_flags:
                        print(f"  [preprocess] ⛔ {sku} tiene flags IA activos — NO se publicará")
                        prod['_preprocess'] = {
                            'id_map': {}, 'gallery': prod.get('commercekit_gallery') or {},
                            'all_skip': False, 'has_errors': True,
                            'errors': [{'wc_image_id': None, 'flags': 'N/A',
                                        'error': f'preprocess_exception: {e}'[:200]}],
                        }
                    else:
                        print(f"  [preprocess] Sin flags activos → continuando con imágenes originales")
                        prod['_preprocess'] = {
                            'id_map': {}, 'gallery': prod.get('commercekit_gallery') or {},
                            'all_skip': True, 'has_errors': False, 'errors': [],
                        }
                    prod['images_for_ml'] = prod['images']

            # ── Gate: si preprocess tiene errores → NO publicar, registrar en BD ──
            preproc_errors = prod.get('_preprocess', {}).get('has_errors')
            if preproc_errors and not args.dry_run:
                errs = prod['_preprocess'].get('errors') or []
                first_err = (errs[0]['error'] if errs else 'unknown')[:120]
                n_err = len(errs)
                error_label = f"GEMINI_ERROR: {n_err} imagen(es) fallaron — {first_err}"
                print(f"  [✗] {sku} — PUBLICACIÓN BLOQUEADA: {error_label}")
                for e in errs:
                    print(f"    - wc_img={e.get('wc_image_id')} flags={e.get('flags')} "
                          f"backlog_id={e.get('backlog_id')} → {e.get('error')}")

                fail_result = {
                    'success':      False,
                    'sku':          sku,
                    'wc_id':        prod['wc_id'],
                    'error':        error_label,
                    'gemini_error': True,
                    'cuenta':       cuenta,
                }
                progress[prog_key] = fail_result
                save_progress(progress, prog_key=prog_key, entry=fail_result)
                save_backlog(prog_key, {
                    'timestamp': datetime.now().isoformat(),
                    'cuenta':    cuenta,
                    'wc_id':     prod['wc_id'],
                    'result':    fail_result,
                })
                stats['fallidos'] += 1
                errores_resumen.append({
                    'sku':    sku,
                    'cuenta': cuenta,
                    'error':  error_label,
                })
                continue

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

            if idx < len(products_iter) and not args.dry_run:
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
            # Antes de marcar 'publish' en WC, si hubo ediciones IA, sincronizar
            # los nuevos wp_media_id en los 3 lugares: images[] del padre,
            # commercekit_image_gallery (CSVs por variante), y image.id de cada
            # variación hija. Sólo se ejecuta cuando id_map no está vacío.
            prod_ref = prod_by_sku.get(sku, {})
            preproc  = prod_ref.get('_preprocess') or {}
            id_map   = preproc.get('id_map') or {}
            if id_map:
                print(f"  [wc_media] {sku}: sincronizando {len(id_map)} imagen(es) editada(s) en WC...")
                sync_res = wc_media.sync_edited_images(
                    wc_id_por_sku[sku], id_map, preproc.get('gallery')
                )
                print(f"  [wc_media] {sku}: parent={sync_res['parent_ok']} "
                      f"gallery={sync_res['gallery_updated']} "
                      f"variations={sync_res['variations_ok']}/"
                      f"{sync_res['variations_ok']+sync_res['variations_fail']}")

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