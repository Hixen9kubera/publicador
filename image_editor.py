"""
image_editor.py — Edición dinámica de imágenes WC → ML con Gemini según flags Kubera.

Lee meta_data['_kubera_editar_imagenes'] de WooCommerce (lista de dicts por imagen):
  {
    "imagen_id":      <int>,
    "quitar_fondo":   <bool>,
    "traducir_texto": <bool>,
    "cambiar_modelo": <bool>
  }

8 combinaciones posibles → compone prompt dinámico concatenando cláusulas activas:
  (0,0,0) → skip (no toca la imagen, se sube original a ML)
  (1,0,0) → quitar fondo
  (0,1,0) → traducir texto + quitar logos
  (0,0,1) → reemplazar persona (requiere describe_person extra)
  (1,1,0) → fondo + texto + center + studio light
  (1,0,1) → fondo + reemplazar persona
  (0,1,1) → texto + reemplazar persona (fondo intacto)
  (1,1,1) → fondo + texto + reemplazar persona

describe_person() solo se llama cuando cambiar_modelo=True (ahorra 1 Gemini call).
Todo in-memory: bytes in → Gemini → bytes out (sin archivos temporales).
"""

import logging
import time
from typing import Optional

import requests

from config import GEMINI_API_KEY, GEMINI_MODEL

# ── google-genai SDK ───────────────────────────────────────────────────────────
try:
    from google import genai
    from google.genai import types as genai_types
    GENAI_OK = True
except ImportError:
    GENAI_OK = False

log = logging.getLogger('image_editor')


# ══════════════════════════════════════════════════════════════════════════════
# CLIENTE GEMINI (singleton)
# ══════════════════════════════════════════════════════════════════════════════
_client: Optional["genai.Client"] = None

def _get_client() -> Optional["genai.Client"]:
    global _client
    if not GENAI_OK:
        return None
    if _client is None:
        if not GEMINI_API_KEY:
            raise RuntimeError("GEMINI_API_KEY no configurada en el entorno")
        _client = genai.Client(api_key=GEMINI_API_KEY)
    return _client


# ══════════════════════════════════════════════════════════════════════════════
# PROMPT DESCRIBE_PERSON (solo cuando cambiar_modelo=True)
# ══════════════════════════════════════════════════════════════════════════════
PROMPT_DESCRIBE_PERSON = (
    "Look at this image carefully. If there is a visible human person, model, or someone wearing clothes:\n"
    "Respond with ONE short English phrase describing them. Use this format:\n"
    "  [gender+age_group] approximately [age], [build], wearing [clothing description]\n"
    "\n"
    "age_group options: baby, child, teen, adult, elderly\n"
    "gender options: boy, girl, man, woman\n"
    "build options: slim, average, athletic, overweight\n"
    "\n"
    "Examples:\n"
    "  'girl approximately 6 years old, slim, wearing pink dress'\n"
    "  'woman approximately 30 years old, average, wearing sportswear'\n"
    "  'boy approximately 10 years old, slim, wearing school uniform'\n"
    "  'man approximately 45 years old, athletic, wearing business suit'\n"
    "\n"
    "If there is NO visible person, respond only with: NO_PERSON\n"
    "Respond with the single phrase or NO_PERSON only, nothing else."
)


def _replacement_for(person_desc: str) -> str:
    """
    Devuelve la cláusula de 'persona de reemplazo' (siempre latina) según el
    grupo demográfico detectado en person_desc.
    """
    d = (person_desc or "").lower()
    if any(w in d for w in ("baby", "infant", "toddler")):
        return "an attractive Latin baby of the same age and gender"
    if ("teen" not in d) and (("child" in d) or ("year old" in d)):
        if "girl" in d: return "an attractive Latin girl of similar age"
        if "boy"  in d: return "an attractive Latin boy of similar age"
        return "an attractive Latin child of similar age and gender"
    if "teen" in d:
        if "girl" in d: return "an attractive Latin teenage girl of similar age"
        if "boy"  in d: return "an attractive Latin teenage boy of similar age"
        return "an attractive Latin teenager of similar age and gender"
    if "elderly" in d or "old man" in d or "old woman" in d:
        if any(w in d for w in ("woman", "lady")):
            return "an attractive Latin elderly woman"
        return "an attractive Latin elderly person"
    if any(w in d for w in ("woman", "girl")):
        return "an attractive Latin woman of similar age"
    if any(w in d for w in ("man", "boy")):
        return "an attractive Latin man of similar age"
    return "an attractive Latin person of the same demographic"


# ══════════════════════════════════════════════════════════════════════════════
# COMPOSICIÓN DE PROMPT SEGÚN FLAGS (8 casos)
# ══════════════════════════════════════════════════════════════════════════════
def compose_prompt(
    quitar_fondo:   bool,
    traducir_texto: bool,
    cambiar_modelo: bool,
    person_desc:    Optional[str] = None,
) -> Optional[str]:
    """
    Compone prompt Gemini concatenando cláusulas activas.
    Retorna None si los 3 flags son False (skip, no se edita la imagen).
    """
    qf, tt, cm = bool(quitar_fondo), bool(traducir_texto), bool(cambiar_modelo)
    if not (qf or tt or cm):
        return None

    desc = person_desc or "the person"
    replacement = _replacement_for(person_desc or "") if cm else ""

    # Matriz literal de los 8 casos del PRD (tabla del usuario)
    if qf and tt and cm:
        return (
            f"Remove the background and replace it with pure white, "
            f"translate all text to Spanish, remove logos, blue borders and blue bottom banners, "
            f"and replace the person ({desc}) with {replacement}, same pose and outfit. "
            f"Keep the product unchanged. Return the edited image."
        )
    if qf and tt:
        return (
            "Remove the background and replace it with pure white, "
            "translate all text to Spanish, remove logos, blue borders and blue bottom banners, "
            "center the product and apply studio lighting. Return the edited image."
        )
    if qf and cm:
        return (
            f"Remove the background and replace it with pure white and "
            f"replace the person ({desc}) with {replacement}, same pose and outfit. "
            f"Keep the original text and logos unchanged. Return the edited image."
        )
    if qf:
        return (
            "Remove the background and replace it with pure white only. "
            "Keep everything else unchanged (original text, logos and any person). "
            "Return the edited image."
        )
    if tt and cm:
        return (
            f"Translate all text to Spanish, remove logos, blue borders and blue bottom banners, "
            f"and replace the person ({desc}) with {replacement}, same pose and outfit. "
            f"Keep the background and product unchanged. Return the edited image."
        )
    if tt:
        return (
            "Translate all text to Spanish, remove logos, blue borders and blue bottom banners. "
            "Keep the product, background and any person unchanged. Return the edited image."
        )
    if cm:
        return (
            f"Replace the person ({desc}) with {replacement}, same pose and outfit. "
            f"Keep the background, text, logos and product unchanged. Return the edited image."
        )
    return None


# ══════════════════════════════════════════════════════════════════════════════
# GEMINI IN-MEMORY
# ══════════════════════════════════════════════════════════════════════════════
def describe_person_bytes(image_bytes: bytes, mime_type: str = "image/jpeg") -> Optional[str]:
    """
    Llama Gemini con PROMPT_DESCRIBE_PERSON y retorna la descripción
    ('woman approximately 30 years old, ...') o None si no hay persona / falla.
    """
    client = _get_client()
    if client is None:
        return None
    try:
        img_part = genai_types.Part.from_bytes(data=image_bytes, mime_type=mime_type)
        cfg = genai_types.GenerateContentConfig(response_modalities=["TEXT"])
        resp = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=[PROMPT_DESCRIBE_PERSON, img_part],
            config=cfg,
        )
        if resp.candidates and resp.candidates[0].content:
            for part in (resp.candidates[0].content.parts or []):
                txt = getattr(part, "text", None)
                if txt:
                    t = txt.strip()
                    if t.upper().startswith("NO_PERSON") or not t:
                        return None
                    return t
    except Exception as e:
        log.warning(f"describe_person_bytes error: {e}")
    return None


def gemini_edit_bytes(
    image_bytes: bytes,
    prompt: str,
    mime_type: str = "image/jpeg",
    retries: int = 3,
) -> tuple[Optional[bytes], Optional[str]]:
    """
    Envía (prompt + imagen) a Gemini y devuelve (bytes_editados, error).
    Si Gemini devuelve imagen → (bytes, None).
    Si falla → (None, mensaje_error).
    """
    client = _get_client()
    if client is None:
        return None, "google-genai no disponible o GEMINI_API_KEY no configurada"

    img_part = genai_types.Part.from_bytes(data=image_bytes, mime_type=mime_type)
    cfg = genai_types.GenerateContentConfig(response_modalities=["TEXT", "IMAGE"])
    waits = [15, 60]
    last_err = "no_image_returned"

    for attempt in range(retries):
        if attempt > 0:
            w = waits[min(attempt - 1, len(waits) - 1)]
            log.info(f"    Gemini reintento {attempt + 1}/{retries} en {w}s...")
            time.sleep(w)
        try:
            resp = client.models.generate_content(
                model=GEMINI_MODEL,
                contents=[prompt, img_part],
                config=cfg,
            )
            if resp.candidates and resp.candidates[0].content:
                for part in (resp.candidates[0].content.parts or []):
                    inline = getattr(part, "inline_data", None)
                    if inline and getattr(inline, "data", None):
                        return inline.data, None
            last_err = "sin imagen en la respuesta"
        except Exception as e:
            last_err = str(e)[:200]
            log.warning(f"    Gemini intento {attempt + 1} falló: {last_err}")

    return None, last_err


# ══════════════════════════════════════════════════════════════════════════════
# ORQUESTADOR POR IMAGEN
# ══════════════════════════════════════════════════════════════════════════════
def process_image(src_url: str, flags: dict) -> tuple[Optional[bytes], dict]:
    """
    Procesa una imagen según sus flags.

    Retorna (bytes_editados | None, info_backlog)
      - bytes_editados: bytes de la imagen editada. None si se debe usar la original
        (porque skip_no_flags o error → el caller decide subir original a ML).
      - info_backlog: dict con todos los campos para insertar en ml_image_edit_backlog
        (action, prompt_used, person_desc, gemini_error, bytes_in/out, etc.).
    """
    qf = bool(flags.get('quitar_fondo'))
    tt = bool(flags.get('traducir_texto'))
    cm = bool(flags.get('cambiar_modelo'))

    info = {
        'src_url':             src_url,
        'flag_quitar_fondo':   qf,
        'flag_traducir_texto': tt,
        'flag_cambiar_modelo': cm,
        'action':              'skip_no_flags',
        'person_desc':         None,
        'prompt_used':         None,
        'gemini_model':        None,
        'gemini_success':      False,
        'gemini_error':        None,
        'bytes_in':            None,
        'bytes_out':            None,
    }

    if not (qf or tt or cm):
        return None, info

    # 1. Descargar imagen original
    try:
        r = requests.get(src_url, timeout=30)
        r.raise_for_status()
        img_bytes = r.content
    except Exception as e:
        info['action'] = 'error'
        info['gemini_error'] = f'download_error: {e}'
        return None, info
    info['bytes_in'] = len(img_bytes)

    # 2. describe_person (solo si cambiar_modelo)
    person_desc = None
    if cm:
        person_desc = describe_person_bytes(img_bytes)
        info['person_desc'] = person_desc

    # 3. Componer prompt y llamar Gemini
    prompt = compose_prompt(qf, tt, cm, person_desc=person_desc)
    info['prompt_used'] = prompt
    info['gemini_model'] = GEMINI_MODEL

    edited, err = gemini_edit_bytes(img_bytes, prompt)
    if edited is None:
        info['action'] = 'error'
        info['gemini_error'] = err
        return None, info

    info['action'] = 'edited'
    info['gemini_success'] = True
    info['bytes_out'] = len(edited)
    return edited, info


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS PARA LOGGING
# ══════════════════════════════════════════════════════════════════════════════
def format_flags_line(flags: dict) -> str:
    """'fondo=✓ texto=✗ modelo=✓' para logs."""
    def mark(k): return "✓" if flags.get(k) else "✗"
    return (
        f"fondo={mark('quitar_fondo')} "
        f"texto={mark('traducir_texto')} "
        f"modelo={mark('cambiar_modelo')}"
    )
