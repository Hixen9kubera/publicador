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

### 2026-04-28 - Subir cadencia a 8/hora y manejar invalid.title.gender

**Cambios:**

- **scheduler.py**: `CMD_ARGS` ahora pasa `--limit 8` (antes `5`). El cron sigue corriendo cada hora, pero procesa hasta 8 productos por cuenta por corrida (16 totales/hora).
- **publisher.py — nuevo retry para `invalid.title.gender`**: cuando ML rechaza el item porque el título no concuerda con el atributo `GENDER` (ej: ROP-0197 con GENDER mal asignado), el publisher quita el atributo `GENDER`/`GENDER_NAME` del payload y reintenta — ML lo infiere del título. Si el retry vuelve a fallar, el item se marca con `NEEDS_MANUAL_CONFIG: TITLE_GENDER_MISMATCH` y se salta en corridas futuras.
- **publisher.py — `item.pictures.invalid_size` como NEEDS_MANUAL_CONFIG**: el retry de re-preupload con escalado Pillow ya existía; ahora si las imágenes siguen siendo demasiado pequeñas tras el escalado (URLs irrecuperables o resolución muy baja), el item se marca con `NEEDS_MANUAL_CONFIG: IMAGES_TOO_SMALL` para que el seller suba imágenes ≥500×250 px en WooCommerce.

**Estado de errores BD al 2026-04-28:**

- `ml_progress`: 389 OK / 13 errores por cuenta. Los 13 errores ya están todos clasificados como `NEEDS_MANUAL_CONFIG` (10 GRID_REQUERIDO + 1 GTIN_REAL_REQUERIDO + 2 ROP-0197 que tras este deploy quedarán como TITLE_GENDER_MISMATCH).
- `ml_backlog` últimos 2 días: 28 × `invalid.title.gender` (ROP-0197) + 6 × `item.pictures.invalid_size` (ROP-0197-NEG-XL) — ambos cubiertos por este deploy.

### 2026-04-21 - Saltar SKUs con errores no-recuperables (NEEDS_MANUAL_CONFIG)

**Problema:** Después del deploy que skipea `SIZE_GRID_ID` con valor placeholder, 4 SKUs de ropa/calzado (ROP-0456, ROP-0240, ROP-0465, CALZ-0200-TRANS-43) seguían fallando con `missing.fashion_grid.grid_id.values` porque la categoría de ML **exige** una guía de tallas. Estos errores NO son bugs del publisher — requieren que el seller configure una guía de tallas en su cuenta ML. Sin embargo, el cron los reintentaba cada hora contaminando el backlog con errores repetitivos (6 errores × hora × 2 cuentas).

**Cambios en publisher.py:**

- Detectar errores no-recuperables (`missing.fashion_grid.grid_id.values`, `invalid.fashion_grid.grid_id.values`, `shipping.lost_me1_by_user`) y marcar el `error_label` en `ml_progress` con prefijo `NEEDS_MANUAL_CONFIG:` incluyendo la acción que el seller debe tomar.
- En el main loop: cargar en un set aparte los SKUs con `error` que empiece por `NEEDS_MANUAL_CONFIG` y saltarlos antes de reintentar. Se muestra un contador al inicio de cada cuenta para que el usuario sepa cuántos SKUs están pendientes de config manual.
- Los 4 SKUs de grid existentes fueron marcados retroactivamente en la BD para que tomen efecto desde la próxima corrida.

### 2026-04-21 - Abort-on-DB-failure (fail-hard si no hay conexión a BD)

**Problema:** Cuando la conexión a MySQL fallaba al iniciar (timeout, red caída, credenciales), el publisher continuaba procesando productos sin registrar nada en BD. El progreso se perdía silenciosamente — solo aparecía una advertencia `[db] Advertencia — no se pudo guardar progress en BD` entre productos, pero la corrida seguía adelante.

**Cambios en publisher.py:**

- Si `DB_HOST`/`DB_NAME`/`DB_USER` no están configurados, el publisher aborta con `sys.exit(1)` en lugar de continuar.
- Si `db.ensure_connection()` falla tras los 5 reintentos (backoff 5s, 10s, 20s, 40s, 80s), el publisher aborta con `sys.exit(1)` en lugar de imprimir una advertencia y seguir.
- Nuevo health-check antes de cada producto en el loop principal: si `ensure_connection()` falla a mitad de corrida (conexión caída por timeout/red), el publisher aborta inmediatamente sin procesar el siguiente producto. Esto garantiza que no se creen items en ML sin su correspondiente registro en `ml_progress`/`ml_backlog`.

### 2026-04-21 - Fix SIZE_GRID_ID, ENERGY_EFFICIENCY_LABEL y SHEETS_NUMBER

**Problema 1:** Error 400 `invalid.fashion_grid.grid_id.values` / `missing.fashion_grid.grid_id.values` — categorias de ropa/calzado (MLM112156 Vestidos, MLM112197 Ropa, MLM192717 Calzado) rechazaban el item porque el atributo `SIZE_GRID_ID` se estaba enviando con el valor literal placeholder `"grid_id"` heredado de los atributos WC. Afecto a ROP-0456, ROP-0240, CALZ-0200-TRANS-43, ROP-0465.

**Problema 2:** Error 400 `item.attribute.value_name.invalid` — el atributo `ENERGY_EFFICIENCY_LABEL` (value_type=picture) en MLM1584 se enviaba con `value_name='A'` en lugar de un picture_id valido. Afecto a ACC-0091-AST-PLA.

**Problema 3:** Error 400 `item.attribute.invalid` — `SHEETS_NUMBER` (value_type=number) se enviaba con `value_name='N/A'` en ORG-0773-MUL, pero ML requiere un numero.

**Cambios realizados:**

- **attribute_mapper.py**: Nuevo set `_SKIP_VALUE_TYPES = {grid_id, grid_row_id, picture}` que omite estos atributos en `build_attributes()` y `build_secondary_attributes()`. Los tipos `grid_id`/`grid_row_id` requieren una guia de tallas real (no se pueden inferir); `picture` requiere un picture_id, no texto.
- **attribute_mapper.py**: Nueva funcion `_validate_value()` que valida el value_name segun `value_type` antes de agregarlo. Para `value_type=number`, rechaza strings no-numericos como "N/A" y omite el atributo.
- **publisher.py**: Nuevo retry para `invalid.fashion_grid.grid_id.values` y `missing.fashion_grid.grid_id.values` — quita `SIZE_GRID_ID` del payload y reintenta. Si la categoria realmente exige el grid, el error queda en backlog (requiere que el seller configure una guia de tallas en su cuenta ML).
- **publisher.py**: Nuevo retry para atributos de tipo `picture` con valor invalido (`item.attribute.value_name.invalid` con mensaje "type picture") — detecta el atributo ofensor del mensaje de error y lo quita del payload.

**Nota:** Errores `shipping.lost_me1_by_user` observados en el backlog **no son bugs del publisher** — requieren que el usuario active el modo "Mercado Envios 1" en el dashboard de la cuenta ML (BEKURA y SANCORFASHION).

### 2026-04-09 - Retry dims faltantes y GTIN placeholder rechazado

**Problema 1:** Error 400 `item.attribute.missing.seller.package.dimensions` — productos sin peso/dimensiones en WooCommerce (ej: TEC-0472-HBM, TEC-1353-MET). ML las exige pero el publisher no las enviaba. El retry existente solo manejaba dims *invalidas*, no *faltantes*.

**Problema 2:** Error 400 `product_identifier.invalid_format` — el placeholder GTIN `0000000000000` es rechazado por algunas cuentas (ej: BEKURA/TEC-1254). El error se registraba pero no se reintentaba.

**Cambios realizados en publisher.py:**

- Nuevo retry para `missing.seller.package.dimensions`: cuando ML exige dimensiones y el producto no las tiene, agrega valores por defecto conservadores (1 kg, 30x20x15 cm) y reintenta. Resolvio TEC-1352-NEG, TEC-0472-HBM, TEC-1353-MET.
- Nuevo retry para `product_identifier.invalid_format`: quita el atributo GTIN del payload y deja solo `EMPTY_GTIN_REASON` ("Otra razon"), luego reintenta. Si la categoria requiere GTIN obligatorio (ej: MLM190081), restaura el GTIN y marca como `gtin_error` para revision manual.
- Mejorada la deteccion de `gtin_error`: ahora tambien detecta `missing_conditional_required` para GTIN (antes solo detectaba `invalid_format`).
- Nuevo retry para HTTP 401 (token expirado) en `create_item`: si el token expira durante la ejecucion (ej: entre pre-upload de imagenes y creacion del item), refresca automaticamente y reintenta.

### 2026-04-13 - Fix imagenes pequenas rechazadas por ML (item.pictures.invalid_size)

**Problema:** Error 400 `item.pictures.invalid_size` — ML rechaza imagenes con menos de 500px en el lado largo o 250px en el lado corto. Afectaba multiples SKUs (TEC-1409-MET-AVEO-1.5L, OFI-0107-NEG, TEC-1258-2M-NEG, TEC-0834-NEG, VEH-0027, MASC-0051-NEG-VER).

**Cambios realizados:**

- **ml_api.py**: Nueva funcion `_ensure_min_size()` que usa Pillow para escalar imagenes pequenas a minimo 500x250 px antes de pre-subirlas a ML. Se aplica automaticamente en `preupload_picture()`.
- **publisher.py**: Nuevo retry cuando ML devuelve `item.pictures.invalid_size` — fuerza el re-preupload de las imagenes que quedaron como fallback URL (`{'source': url}`), aplicando el escalado y reintentando `create_item`.

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

### 2026-04-08 - Retry conexion BD con backoff exponencial

**Problema:** La conexion a MySQL se intentaba una sola vez al iniciar el publisher. Si fallaba (timeout, red), toda la corrida continuaba sin BD.

**Cambios realizados:**

- **db.py**: Nueva funcion `ensure_connection(max_retries=5, base_delay=5)` que reintenta la conexion con backoff exponencial (5s, 10s, 20s, 40s, 80s).
- **publisher.py**: Usa `ensure_connection()` al iniciar en lugar del intento unico anterior.
