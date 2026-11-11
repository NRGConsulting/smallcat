# Plan de implementación — #10: Delta merge/upsert y update

> **Estado: implementado.** Mergeado en `main` (`d154a12`, a partir del commit `5cbb012`); sin publicar en `fork` ni cerrado en GitHub. El documento se conserva como referencia de diseño. Lo que difiere de lo previsto abajo:
>
> * `DeltaTable.is_deltatable` no existe en `deltalake` 0.15, así que la tabla se detecta capturando `TableNotFoundError` (§4, trampa 3).
> * `merge` no admite `schema_mode`, y las columnas del origen deben existir ya en la tabla.
> * La cota `deltalake>=0.15` **no era válida**: con 0.15.0 fallan los tests que leen con Polars y con 0.25.5 fallan 14. El mínimo real es 1.0 y está corregido (commit `2235641`, `Refs #21`). Donde el texto de abajo dice "cota actual 0.15", ya no es así.
> * El esquema JSON del catálogo no se había regenerado y se actualizó aparte (commit `8af74ff`).

Documento de traspaso para el agente que implementó el issue: recoge el diseño acordado con el usuario, el estado del código verificado a 2026-11-10 y los pasos que se siguieron.

- Issue: [#10](https://github.com/NRGConsulting/smallcat/issues/10) en `NRGConsulting/smallcat` (remoto `fork`, **no** `origin`).
- Issue relacionado, fuera de alcance: [#32](https://github.com/NRGConsulting/smallcat/issues/32) (DELETE por predicado y cláusula `when_not_matched_by_source_delete`; registrado como SC-036). [#12](https://github.com/NRGConsulting/smallcat/issues/12) (`optimize`/`vacuum`).
- Reglas del repositorio: `AGENTS.md` (raíz de `smallcat/`).

## 1. Reglas que debes cumplir

1. **Antes de tocar nada, muestra el plan al usuario y espera su confirmación explícita** (`AGENTS.md` del espacio de trabajo). Este documento es el plan; la confirmación hay que pedirla igualmente.
2. Commits **sin** `Co-Authored-By` ni atribución a ningún agente. Referencia el issue: `Closes #10` si queda resuelto por completo, `Refs #10` si es parcial.
3. Sin `uv` en workflows nuevos.
4. **No hagas `git push` a `fork` ni cierres el issue en GitHub salvo petición expresa del usuario.** Actualiza `docs/issues.md` al resolver.
5. Reproduce o comprueba antes de afirmar. Un diagnóstico erróneo anterior casi se mergea (SC-012).

## 2. Alcance

**Dentro**

| Capa | Resultado |
|---|---|
| Dataset | `WriteMode.MERGE`; `save_arrow_table` hace merge; método `update(path, predicate, values)` |
| Catálogo | `save_options: {mode: merge, merge_keys: [...], update_columns: [...]}` funciona con `catalog.save_polars` sin cambiar su API |
| Modelos (ORM) | `ModelWriteMode.MERGE`, `Meta.merge_keys` (opcional), `save(df, mode="merge", merge_keys=[...])` |
| Documentación | ver §6 |

**Fuera** (no lo implementes): `DELETE`, `when_not_matched_by_source_delete` (#32); `optimize`/`vacuum` (#12); `ignore`/`append`/`overwrite` sin cambios; duplicados por reintentos con `append` (#5); exponer `update` en catálogo u ORM; deduplicación automática del origen.

## 3. Diseño acordado

### Comportamiento

- Modo nombrado **`merge`** (no `upsert`) en todas las capas.
- Solo `delta_table`. Cualquier otro formato lo rechaza **antes de escribir**.
- `merge_keys` identifica la fila (el `ON` del merge). Las claves nunca se actualizan.
- `update_columns` (opcional): columnas que se sobrescriben en las filas que coinciden. Por defecto, todas las del DataFrame salvo las claves. Las filas nuevas se insertan completas.
- El merge hace `when_matched_update` + `when_not_matched_insert`. **No borra nada.**
- Si la tabla no existe, el primer `merge` la crea con `write_deltalake` (porque `DeltaTable.merge` exige tabla existente).
- Claves duplicadas o nulas en el origen: error claro previo a escribir (Delta falla de forma ambigua). No hay dedupe automático.
- Databricks: `NotImplementedError` con `DATABRICKS_UNSUPPORTED_MSG`, como el resto del módulo.

### Modelos

```python
class Customer(Model):
    id: int
    name: str
    email: str | None

    class Meta:
        location = "silver/customers"
        file_format = ModelFileFormat.DELTA_TABLE
        merge_keys = ["id"]            # opcional

Customer.objects.save(df, mode="merge")                        # usa Meta.merge_keys
Customer.objects.save(df, mode="merge", merge_keys=["email"])  # sustituye a las de Meta
```

- **Precedencia:** las claves de la llamada mandan sobre `Meta`. Si no hay ninguna, error antes de escribir.
- **Validación al registrar** (`registry.register`, no al definir la clase): cada clave de `Meta.merge_keys` debe ser un campo del modelo. Si no, `ModelRegistrationError` nombrando las claves que faltan y los campos disponibles. También se rechazan lista vacía, claves repetidas y claves nullable (`X | None`), porque no identifican una fila. Se revalida con `replace=True`.
- **Validación en la llamada:** las claves de `save(..., merge_keys=...)` deben ser campos del modelo y estar en el DataFrame, sin nulos ni duplicados. Pasar `merge_keys` con un modo distinto de `merge` es un error (no se ignora).
- `allowed_write_modes` incluye `merge` por defecto (hoy el defecto es "todos"). Un modelo sin claves falla al guardar, no al registrar. Se puede restringir al registrar como los demás modos.
- `update_columns` va en `save_options`/`Meta.save_options`, no en la llamada a `save`.

### Seguridad

- Identificadores (claves, columnas) validados contra el esquema de la tabla/DataFrame y entrecomillados. El predicado del merge se construye **solo** a partir de claves ya validadas.
- `update`: el predicado pasa por el validador de `where` (`BaseDataset._validated_where`, parseo con DuckDB, sin subconsultas). Los valores van como literales escapados (`sql_string_literal`, en `base_dataset.py`), nunca como texto libre. Exigir predicado no vacío.

## 4. Estado verificado del código

Entorno: `deltalake` 1.6.6 (la cota era `deltalake>=0.15` en `pyproject.toml`; ahora es `>=1.0`), Python 3.13.

| Pieza | Ubicación | Notas |
|---|---|---|
| `WriteMode`, `SchemaMode`, `DeltaTableSaveOptions`, `save_arrow_table` | `src/smallcat/datasets/delta_table_dataset.py` | `save_arrow_table` llama a `write_deltalake(**save_options_dict())`; `_WRITE_DELTALAKE_HAS_ENGINE` decide si se pasa `engine` |
| `save_options_dict()` | `src/smallcat/datasets/base_dataset.py` (~L346) | Usa `model_dump(exclude_unset=True)` |
| `save_polars` | `base_dataset.py` (~L308) | `df.to_arrow()` → `save_arrow_table`. No conoce el modo |
| `DeltaTableEntry` | `src/smallcat/catalog.py` (~L314) | `save_options: DeltaTableSaveOptions \| None` |
| `ModelWriteMode` | `src/smallcat/models/enums.py` | Hoy `APPEND`, `OVERWRITE` |
| `ModelMeta`, `META_ATTRIBUTES`, `unsupported_reason`, `entry_dict`/`build_entry` | `src/smallcat/models/meta.py` | `ModelMeta` es `frozen`, `extra="forbid"` |
| `register`, `entry_for` | `src/smallcat/models/registry.py` | `entry_for` solo fija `mode` en `save_options` si el formato es Delta |
| `ModelManager.save` | `src/smallcat/models/query.py` (~L303) | Valida modo permitido → `unsupported_reason` → `validate_frame` → `entry_for(...).save_polars` |
| `FieldSpec.nullable`, `ModelSchema` | `src/smallcat/models/schema.py` | Para detectar claves nullable |
| Errores | `src/smallcat/models/errors.py` | `ModelRegistrationError`, `ModelWriteNotAllowedError`, `ModelUnsupportedWriteModeError`, `ModelDefinitionError` |

API de `deltalake` 1.6.6 (verificada):

```python
DeltaTable.merge(source, predicate, source_alias=None, target_alias=None,
                 merge_schema=False, error_on_type_mismatch=True, ...) -> TableMerger
TableMerger.when_matched_update(updates: dict[str, str], predicate=None)
TableMerger.when_not_matched_insert(updates: dict[str, str], predicate=None)
TableMerger.execute() -> dict[str, Any]          # métricas del merge
DeltaTable.update(updates=None, new_values=None, predicate=None, ...) -> dict
```

En `updates`, las expresiones son SQL sobre los alias: p. ej. `{"name": 'source."name"'}`. Usa `source_alias="source"` y `target_alias="target"`; predicado `target."id" = source."id"`.

### Trampas conocidas

1. **`save_options_dict()` se vuelca como kwargs a `write_deltalake`.** Si añades `merge_keys`/`update_columns` a `DeltaTableSaveOptions`, **debes retirarlos** antes de llamar a `write_deltalake` (en la rama de creación de tabla y en el resto de modos), o fallará con un argumento inesperado.
2. `DeltaTableSaveOptions` tiene `extra="forbid"`. Los campos nuevos deben declararse ahí.
3. `DeltaTable.merge` requiere tabla existente: comprueba existencia (p. ej. `DeltaTable.is_deltatable(uri, storage_options=...)`) antes de decidir entre crear y mergear. Verifica ese método en 1.6.6 y en la cota mínima que elijas.
4. **La API de merge cambió entre 0.x y 1.x.** Comprueba desde qué versión existen `merge` + `when_matched_update` + `when_not_matched_insert` + `execute` con esta firma y sube la cota en `pyproject.toml` si hace falta (relacionado con SC-024). Si subes la cota, `requirements*.txt` y `pdm.lock` están sin trackear / generados con `uv`: no los toques sin avisar.
5. `entry_for` solo inyecta `mode` para Delta. Hay que extenderlo para inyectar también `merge_keys` (de la llamada o de `Meta`) en `save_options`.
6. `ModelMeta` valida `save_options` contra el esquema de la entrada (`DeltaTableSaveOptions`). `Meta.merge_keys` es un atributo **propio de `Meta`**, distinto de `save_options`.

## 5. Plan de implementación

Trabaja en un worktree, sin tocar el árbol principal.

0. **Preparación**
   - Rama `feat/SC-delta-merge` en `git worktree` (`../smallcat-feat-SC-delta-merge`).
   - El árbol principal tiene cambios del usuario (`pyproject.toml` staged, `requirements.txt` modificado, `AGENTS.md` modificado, `docs/issues.md` y `pdm.lock` sin trackear): **no los commitees ni los toques**. `docs/issues.md` se edita en el árbol principal, no en el worktree.
   - Para mergear en `main` hay que apartar esos ficheros con `git stash push -- <ficheros>` y restaurar con `pop --index`, tras guardar un backup del diff.
   - Tests desde el worktree: `python` no está en el PATH de bash y el venv editable apunta al `src` del árbol principal. Usa `/e/Repositorios/NRG_GH/.dev/smallcat/.venv/Scripts/python.exe` con `PYTHONPATH=src` desde el worktree, o probarás el código equivocado.
   - Marca #10 como "en curso" en `docs/issues.md` con sus tareas.
1. **Compatibilidad de `deltalake`**: comprobar API y cota mínima (trampa 4).
2. **Dataset** (`delta_table_dataset.py`)
   - `WriteMode.MERGE`.
   - `DeltaTableSaveOptions`: `merge_keys: list[str] | None`, `update_columns: list[str] | None`.
   - Validador Pydantic: `merge_keys` obligatorio y no vacío con `mode == merge`; prohibido con otros modos; sin repetidos; `update_columns` solo con `merge` y sin solapar con las claves.
   - Rama de merge en `save_arrow_table`: validar claves/columnas contra el esquema del origen y de la tabla; comprobar nulos y duplicados en el origen; crear la tabla si no existe; si existe, `DeltaTable.merge(...).when_matched_update(...).when_not_matched_insert(...).execute()`. Retirar las opciones de merge de los kwargs de `write_deltalake`.
   - Rechazo de Databricks.
   - Método `update(path, predicate, values)` con predicado validado y valores como literales escapados; devuelve las métricas.
3. **Modelos**
   - `enums.py`: `ModelWriteMode.MERGE = "merge"`.
   - `meta.py`: `merge_keys` en `META_ATTRIBUTES` y `ModelMeta` (tupla o lista inmutable, normalizada); `unsupported_reason` rechaza `merge` fuera de Delta.
   - `registry.py`: validar `Meta.merge_keys` en `register` (campos existentes, no vacío, sin repetidos, no nullable). `entry_for(model, write_mode, merge_keys=None)` inyecta `merge_keys` en `save_options`.
   - `query.py`: `save(data, *, mode, merge_keys=None)`; precedencia llamada > `Meta`; validación de claves; error si `merge_keys` con otro modo; error si no hay claves.
4. **Tests**
   - `tests/test_delta_merge.py`, local con `tmp_path` y el fixture `local_conn` de `tests/conftest.py`: insert de filas nuevas, update de existentes, merge sin cambios, tabla inexistente, `update_columns`, claves inexistentes, claves nulas o duplicadas en el origen, inyección SQL en claves/columnas/predicado, `update` con predicado, Databricks, opciones inválidas (merge sin claves, claves con otro modo).
   - Modelos (en `tests/test_models.py` o fichero nuevo): registro con claves inválidas, precedencia llamada > `Meta`, `merge_keys` con otro modo, formato no Delta, modelo sin claves, `allowed_write_modes` sin `merge`.
   - Catálogo: `save_polars` con `mode: merge` desde YAML.
5. **Documentación**: ver §6.
6. **Verificación**
   - Suite completa. **Línea base en la máquina Windows del usuario: 14 fallos conocidos y ajenos** (10 por rutas `file://C:\...` — SC-022; 3 por no poder descargar la extensión `excel` de DuckDB; 1 por Airflow en Windows). No debe aparecer ninguno nuevo.
   - `ruff` y `mypy`.
   - `python -m mkdocs build` (el sitio usa `strict: true`: cualquier enlace roto o referencia a un fichero inexistente falla el build).
7. **Cierre**
   - Commit con `Closes #10` (o `Refs #10`), sin `Co-Authored-By`.
   - Merge `--no-ff` a `main`.
   - Actualizar `docs/issues.md` (estado, tareas, cambios).
   - Push a `fork` y cierre en GitHub solo si el usuario lo pide.

## 6. Documentación a actualizar

| Fichero | Qué |
|---|---|
| `docs/catalog-reference.md` | `mode: merge`, `merge_keys`, `update_columns` en `save_options` de Delta; ejemplo YAML; reglas (claves obligatorias, tabla inexistente, claves duplicadas); nota de que `merge` no borra filas (#32) |
| `docs/models.md` | Tabla de `ModelWriteMode` con `MERGE`; **eliminar** el párrafo "There is no `upsert`"; documentar `Meta.merge_keys`, `save(..., mode="merge", merge_keys=[...])`, la precedencia y la tabla de errores (incl. `ModelRegistrationError` por claves inexistentes) |
| `README.md` | Quitar "There is no upsert" (~L165); añadir un ejemplo corto |
| `docs/examples/models.md`, `models_local.py`, `models_query.py` | Ejemplo ejecutable de `merge` sobre una tabla Delta (`tests/test_models_example.py` ya prueba los ejemplos) |
| `docs/api/datasets.md`, `docs/api/models.md` | Comprobar que las clases y métodos nuevos salen en la referencia |
| Docstrings | `query.py` (`save`: "There is no upsert"), `enums.py`, `meta.py`, `delta_table_dataset.py` (cabecera del módulo y opciones) |
| `docs/issues.md` | Referencias a "ver #10" (SC-035, SC-036) y estado de #10 al cerrarlo |

## 7. Criterios de aceptación

- [ ] `catalog.save_polars` con `mode: merge` inserta las filas nuevas y actualiza las existentes por `merge_keys`; no borra ninguna.
- [ ] `Customer.objects.save(df, mode="merge")` y con `merge_keys=[...]` funcionan; la llamada manda sobre `Meta`.
- [ ] Claves inexistentes en `Meta` → `ModelRegistrationError` al registrar; claves inexistentes en la llamada → error antes de escribir.
- [ ] Claves nulas o duplicadas en el origen → error claro, sin escritura parcial.
- [ ] `merge` en csv/excel/parquet → `ModelUnsupportedWriteModeError` antes de escribir.
- [ ] Sin inyección SQL en claves, columnas ni en el predicado de `update`.
- [ ] `update` actualiza por predicado y devuelve métricas.
- [ ] La cota de `deltalake` es coherente con la API usada.
- [ ] Suite sin fallos nuevos respecto a la línea base; `ruff`, `mypy` y `mkdocs build` en verde.
- [ ] Documentación de §6 actualizada.

## 8. Decisiones tomadas por defecto

Confirmadas implícitamente; el usuario puede cambiarlas:

- `update_columns` queda en `save_options`/`Meta`, no en la llamada a `save`.
- `merge` está permitido por defecto en todos los modelos Delta; un modelo sin claves falla al guardar, no al registrar.
- Sin deduplicación automática del origen: error claro.
- `update` queda como método del dataset, sin catálogo ni ORM.
