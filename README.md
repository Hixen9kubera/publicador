# Publicaciones ML - Publisher

Sistema de publicacion masiva de productos WooCommerce en MercadoLibre (Mexico - MLM).

## Flujo de publicacion

1. Lee productos de WooCommerce (meta_data incluye `ml_category_id` y `ml_attr_*`)
2. Obtiene atributos requeridos de la categoria ML
3. Mapea atributos WC -> ML
4. Pre-sube imagenes a ML
5. Crea item en ML con status `paused`
6. Agrega descripcion
7. Guarda progreso en BD

## Uso

```bash
python publisher.py --cuenta SANCORFASHION
python publisher.py --cuenta BEKURA --tag 1799
python publisher.py --cuenta SANCORFASHION --id-min 24924 --id-max 24973
python publisher.py --cuenta BEKURA --sku VAR-0037-EST-40X30
python publisher.py --cuenta SANCORFASHION --dry-run
```

## Archivos principales

| Archivo | Descripcion |
|---------|-------------|
| `publisher.py` | Orquestador principal: construye payloads, retries, publica |
| `ml_api.py` | Wrapper REST para la API de MercadoLibre |
| `wc_api.py` | Wrapper REST para la API de WooCommerce |
| `attribute_mapper.py` | Mapeo de atributos WC -> ML por categoria |
| `config.py` | Configuracion, credenciales (env vars), constantes |
| `db.py` | Persistencia MySQL/MariaDB (progreso, backlog, tokens) |
| `scheduler.py` | Gestion de tareas programadas |
| `fetch_ml_attrs.py` | Herramienta para descubrir atributos de categorias ML |

## Configuracion

Copia `.env.example` a `.env` y configura las variables de entorno necesarias (WooCommerce, MercadoLibre, BD).

---

## Changelog

### 2026-04-08 - Fix WARRANTY_TYPE sale_terms (value_id)

**Problema:** Error 400 al crear items en ML:
- `sale_term.invalid_value_id`: WARRANTY_TYPE tenia valor null
- `sale_term.value_id_required`: ML requiere value_id, no value_name para sale_terms

**Cambios realizados:**

- **ml_api.py**: Nueva funcion `get_category_sale_terms()` que consulta `/categories/{id}/sale_terms` para obtener los IDs validos de warranty por categoria.
- **publisher.py**:
  - Nueva funcion `build_sale_terms()` que construye sale_terms dinamicamente usando `value_id` del API en lugar de `value_name` hardcodeado.
  - Cache de sale_terms por categoria (`get_sale_terms_cached()`) para evitar llamadas repetidas al API.
  - Nuevo bloque de retry: si ML devuelve error `sale_term.invalid_value_id` o `sale_term.value_id_required`, extrae el `value_id` correcto del mensaje de error y reintenta automaticamente.
  - Fallback hardcodeado a `6150835` (Garantia del vendedor) si el API no responde.
