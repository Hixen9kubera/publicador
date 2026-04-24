"""
wc_media.py — Subida de imágenes editadas a WP Media + sincronización del
producto WooCommerce (galería padre, commercekit gallery por variante, e
image.id en cada variación hija).

Reglas clave:
  - Una imagen editada se sube UNA sola vez a WP Media (wp.uploadFile XML-RPC).
  - El nuevo ID resultante se reutiliza en los 3 lugares:
      1. product.images[]               (galería del padre)
      2. meta.commercekit_image_gallery (CSV por variante; string replace por ID)
      3. /products/{parent}/variations/{var_id}  (campo image.id individual)

Se ejecuta SIEMPRE después de que AMBAS cuentas ML publicaron con éxito —
respeta el flujo existente donde el PUT a status='publish' ocurre al final.
"""

import io
import xmlrpc.client
from typing import Optional

import requests

from config import WC_URL, WC_KEY, WC_SECRET, WC_WP_USER, WC_WP_PASS

_AUTH      = (WC_KEY, WC_SECRET)
_BASE_WC   = f"{WC_URL}/wp-json/wc/v3"
_XMLRPC_URL = f"{WC_URL}/xmlrpc.php"


# ══════════════════════════════════════════════════════════════════════════════
# UPLOAD BYTES → WP MEDIA (XML-RPC, funciona sin Application Password)
# ══════════════════════════════════════════════════════════════════════════════
def upload_edited_image(
    image_bytes: bytes,
    filename: str,
    mime: str = "image/jpeg",
) -> Optional[dict]:
    """
    Sube bytes a la media library de WordPress vía XML-RPC (wp.uploadFile).
    Retorna {'id': int, 'url': str} o None si falla.

    NOTA: No guarda en disco local, pasa los bytes directamente.
    """
    try:
        proxy  = xmlrpc.client.ServerProxy(_XMLRPC_URL, allow_none=True)
        result = proxy.wp.uploadFile(
            1, WC_WP_USER, WC_WP_PASS,
            {
                'name':      filename,
                'type':      mime,
                'bits':      xmlrpc.client.Binary(image_bytes),
                'overwrite': False,
            }
        )
        return {'id': int(result['id']), 'url': result['url']}
    except Exception as e:
        print(f"  [wc_media] Error subiendo '{filename}' a WP Media: {e}")
        return None


# ══════════════════════════════════════════════════════════════════════════════
# SYNC DE IDs EN WC (3 lugares)
# ══════════════════════════════════════════════════════════════════════════════
def _replace_ids_in_gallery_csv(gallery: dict, id_map: dict) -> dict:
    """
    Reemplaza old_id → new_id en cada CSV del meta commercekit_image_gallery.
    gallery: {'negro_cgkit_43': '46312,46316,...', ...}
    id_map:  {46312: 99001, 46316: 99002, ...}
    """
    if not gallery or not id_map:
        return gallery
    out = {}
    for key, csv_str in gallery.items():
        ids = []
        for x in str(csv_str).split(','):
            x = x.strip()
            if not x.isdigit():
                continue
            old = int(x)
            ids.append(str(id_map.get(old, old)))
        out[key] = ','.join(ids)
    return out


def _get_product(wc_id: int) -> Optional[dict]:
    r = requests.get(f"{_BASE_WC}/products/{wc_id}", auth=_AUTH, timeout=30)
    if r.status_code == 200:
        return r.json()
    print(f"  [wc_media] GET product {wc_id} → HTTP {r.status_code}")
    return None


def _put_product(wc_id: int, body: dict) -> bool:
    r = requests.put(f"{_BASE_WC}/products/{wc_id}", json=body, auth=_AUTH, timeout=30)
    if r.status_code == 200:
        return True
    print(f"  [wc_media] PUT product {wc_id} → HTTP {r.status_code} — {r.text[:200]}")
    return False


def _list_variations(parent_id: int) -> list:
    """GET /products/{parent}/variations?per_page=100 (paginando si fuera necesario)."""
    out = []
    page = 1
    while True:
        r = requests.get(
            f"{_BASE_WC}/products/{parent_id}/variations",
            auth=_AUTH,
            params={'per_page': 100, 'page': page},
            timeout=30,
        )
        if r.status_code != 200:
            print(f"  [wc_media] GET variations {parent_id} → HTTP {r.status_code}")
            break
        batch = r.json()
        if not batch:
            break
        out.extend(batch)
        if len(batch) < 100:
            break
        page += 1
    return out


def _put_variation_image(parent_id: int, var_id: int, new_image_id: int) -> bool:
    r = requests.put(
        f"{_BASE_WC}/products/{parent_id}/variations/{var_id}",
        json={'image': {'id': new_image_id}},
        auth=_AUTH,
        timeout=30,
    )
    if r.status_code == 200:
        return True
    print(f"  [wc_media] PUT variation {parent_id}/{var_id} → HTTP {r.status_code} — {r.text[:200]}")
    return False


def sync_edited_images(
    wc_id:      int,
    id_map:     dict,
    gallery:    Optional[dict] = None,
) -> dict:
    """
    Reemplaza old_id → new_id en los 3 lugares del producto WC:
      1. product.images[] (padre)
      2. meta_data.commercekit_image_gallery (CSVs por variante)
      3. variation.image.id (cada variación hija)

    id_map: {wc_image_id_old: wc_image_id_new}
    gallery: dict del meta commercekit_image_gallery actual (del producto ya cargado).
             Si es None, se lee del producto desde WC.

    Retorna dict con contadores:
      {'parent_ok': bool, 'gallery_updated': bool, 'variations_ok': int, 'variations_fail': int}
    """
    result = {'parent_ok': False, 'gallery_updated': False, 'variations_ok': 0, 'variations_fail': 0}
    if not id_map:
        print(f"  [wc_media] Sin id_map — nada que sincronizar")
        return result

    # ── 1. Leer producto padre ──────────────────────────────────────────────
    prod = _get_product(wc_id)
    if prod is None:
        return result

    # ── 2. Reemplazar en images[] ───────────────────────────────────────────
    new_images = []
    for img in prod.get('images', []):
        old = img.get('id')
        if old in id_map:
            new_images.append({'id': id_map[old]})
        else:
            new_images.append({'id': old})

    # ── 3. Reemplazar en commercekit_image_gallery ──────────────────────────
    if gallery is None:
        for m in prod.get('meta_data', []):
            if m.get('key') == 'commercekit_image_gallery':
                gallery = m.get('value') or {}
                break
    gallery = gallery or {}
    new_gallery = _replace_ids_in_gallery_csv(gallery, id_map)
    gallery_changed = new_gallery != gallery

    # ── 4. PUT padre: images[] + meta commercekit_image_gallery ─────────────
    body = {'images': new_images}
    if gallery_changed:
        body['meta_data'] = [
            {'key': 'commercekit_image_gallery', 'value': new_gallery}
        ]
    result['parent_ok']       = _put_product(wc_id, body)
    result['gallery_updated'] = gallery_changed and result['parent_ok']
    if result['parent_ok']:
        n_swap = sum(1 for i in prod.get('images', []) if i.get('id') in id_map)
        print(f"  [wc_media] Padre {wc_id}: images[] actualizado ({n_swap} ID(s) reemplazados), "
              f"commercekit_gallery={'sí' if gallery_changed else 'sin cambios'}")

    # ── 5. Actualizar variation.image.id en cada variación hija ─────────────
    variations = _list_variations(wc_id)
    if variations:
        print(f"  [wc_media] Variaciones encontradas: {len(variations)}")
    for var in variations:
        var_img = (var.get('image') or {}).get('id')
        if var_img in id_map:
            new_id = id_map[var_img]
            if _put_variation_image(wc_id, var['id'], new_id):
                result['variations_ok'] += 1
                print(f"    [wc_media] var {var['id']}: image.id {var_img} → {new_id} ✓")
            else:
                result['variations_fail'] += 1

    return result
