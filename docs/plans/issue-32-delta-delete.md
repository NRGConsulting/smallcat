# Plan de implementación — #32: Delta, borrado de filas por predicado (DELETE)

Documento de traspaso para el agente que implemente el issue. Es autosuficiente: recoge el diseño propuesto, el estado del código verificado a 2026-11-10 y los pasos a seguir. **Nada de esto está implementado todavía y el diseño aún no está confirmado por el usuario** (ver §8, decisiones abiertas).

- Issue: [#32](https://github.com/NRGConsulting/smallcat/issues/32) en `NRGConsulting/smallcat` (remoto `fork`, **no** `origin`). Registrado en `docs/issues.md` como SC-036.
- Relacionados: [#10](https://github.com/NRGConsulting/smallcat/issues/10) (merge/upsert y update; plan en `docs/plans/issue-10-delta-merge.md`), [#12](https://github.com/NRGConsulting/smallcat/issues/12) (`optimize`/`vacuum`).
- Reglas del repositorio: `AGENTS.md` (raíz de `smallcat/`).

## 1. Reglas que debes cumplir

1. **Antes de tocar nada, muestra el plan al usuario y espera su confirmación explícita** (`AGENTS.md` del espacio de trabajo). Este documento es el plan; la confirmación hay que pedirla igualmente, incluidas las decisiones abiertas de §8.
2. Commits **sin** `Co-Authored-By` ni atribución a ningún agente. Referencia el issue: `Closes #32` si queda resuelto por completo, `Refs #32` si es parcial.
3. Sin `uv` en workflows nuevos.
4. **No hagas `git push` a `fork` ni cierres el issue en GitHub salvo petición expresa del usuario.** Actualiza `docs/issues.md` (SC-036) al resolver.
5. Reproduce o comprueba antes de afirmar.

## 2. Orden respecto a #10

#10 y #32 tocan los mismos ficheros (`delta_table_dataset.py`, `enums.py`, `meta.py`, `registry.py`, `query.py`, `catalog.py` y la documentación). **#10 ya está mergeado en `main`** (`d154a12`): parte de ahí. Existen ya `WriteMode.MERGE`, `ModelWriteMode.MERGE`, `Meta.merge_keys`, `merge_keys` y `update_columns` en `DeltaTableSaveOptions` (con su validador) y `DeltaTableDataset.update`.

La tarea "cláusula `when_not_matched_by_source_delete`" se apoya en el merge de #10, que ya existe.

También conviene saber que #33 (SC-038, rama `feat/SC-038-where-lookups`) cambia `ModelQuery.where`: ahora acepta `sql=` y lookups estilo Django (`where(job_id__lte=2)`). Si esa rama aún no está en `main` cuando empieces, parte de ella o espera: el borrado se apoya en `where`.

## 3. Alcance

**Dentro**

| Capa | Resultado |
|---|---|
| Dataset | `DeltaTableDataset.delete(path, where)` que borra por predicado y devuelve el nº de filas borradas |
| Catálogo | `Catalog.delete(key, where=...)` y `EntryBase.delete(where)` |
| Modelos (ORM) | Borrado por predicado SQL, controlado por `allowed_write_modes` |
| Merge de #10 | Opción `when_not_matched_by_source_delete` (borra de la tabla lo que no viene en el origen) |
| Documentación | ver §6 |

**Fuera** (no lo implementes): `VACUUM` y `optimize` (#12; el borrado de Delta es **lógico**, los ficheros siguen en el almacenamiento hasta ejecutarlo); borrado en Parquet/CSV/Excel (no tienen log de transacciones); borrado con predicados Polars (no se pueden traducir a SQL de forma segura); `truncate` o borrado sin predicado; `update` (es de #10).

## 4. Diseño propuesto

### Comportamiento

- Solo `delta_table`. En cualquier otro formato, `NotImplementedError` con un mensaje claro. Databricks: `NotImplementedError` con `DATABRICKS_UNSUPPORTED_MSG`, como el resto del módulo.
- **Predicado obligatorio y no vacío.** `delete(path, where="")`, `None` o solo espacios → `ValueError`. Así un borrado nunca vacía la tabla por error. (Vaciar la tabla, si hiciera falta, es una decisión aparte: ver §8.)
- El predicado se valida con `BaseDataset._validated_where` (parseo con DuckDB; sin `;`, sin subconsultas, sin alterar la forma de la consulta). Es el mismo que protege las lecturas.
- Devuelve `int`: `num_deleted_rows` de las métricas de `DeltaTable.delete`.
- Si ninguna fila coincide, no es un error: devuelve `0` (verificado: no crea versión nueva).
- Si la tabla no existe, error claro (no crearla).
- El borrado crea una **nueva versión** de la tabla (lógico): los datos borrados siguen visibles por time travel y en el almacenamiento hasta `VACUUM` (#12). Hay que decirlo en la documentación.

### API

```python
# Dataset
n = ds.delete("silver/customers", where="id = 3 AND country = 'ES'")

# Catálogo
n = catalog.delete("customers", where="id = 3")

# ORM (propuesta; ver §8)
n = Customer.objects.where("id = 3").delete()
# o: Customer.objects.delete(where="id = 3")
```

### Modelos

- Añadir `ModelWriteMode.DELETE = "delete"`. Encaja con el diseño actual: `allowed_write_modes` ya decide qué puede hacer un modelo en cada entorno, y un modelo de solo lectura (`[]`) queda protegido del borrado sin más código.
- `unsupported_reason` rechaza `delete` fuera de Delta, antes de ejecutar nada.
- El borrado ORM acepta **solo consultas hechas con `where`**, sea con texto SQL (`where("id = 3")`, `where(sql="...")`) o con lookups (`where(id=3, country__in=[...])`, #33). Ambos se guardan ya como un predicado SQL (`ModelQuery.where_sql`), que se puede pasar tal cual a `DeltaTable.delete` tras `_validated_where`. Si la consulta lleva `filter(...)` (expresiones Polars) o `select(...)`, error claro: no se traducen a SQL. **Decidido con el usuario:** `filter(...).delete()` queda fuera de alcance.
- **Trampa:** `ModelRegistry.entry_for` inyecta `mode = write_mode.value` en `save_options` para Delta. `delete` **no es un modo de `DeltaTableSaveOptions`**: no se lo pases, o la validación (`extra="forbid"`, `WriteMode`) fallará. Construye la entrada sin `mode` para el borrado.

### Merge de #10: `when_not_matched_by_source_delete`

- Opción booleana en `DeltaTableSaveOptions`, por defecto `False`, solo válida con `mode == merge`. Convierte el merge en una sincronización: lo que está en la tabla y no en el origen se borra.
- Es **destructiva**: ponla desactivada por defecto, documéntala con un aviso y exige que `merge_keys` esté definido (ya lo exige `merge`).
- API: `TableMerger.when_not_matched_by_source_delete(predicate=None)`. Con `predicate=None` borra **todas** las filas no emparejadas. Valora acotarla con un predicado opcional (`delete_predicate`, validado con `_validated_where`) para no borrar toda la tabla fuera de un subconjunto, p. ej. al sincronizar solo una partición. Es una decisión abierta (§8).
- Un origen vacío con esta opción borraría toda la tabla: protégelo (error si el DataFrame está vacío y la opción está activa).

## 5. Estado verificado del código

Entorno: `deltalake` 1.6.6, Python 3.13.

| Pieza | Ubicación | Notas |
|---|---|---|
| `DeltaTableDataset`, `save_arrow_table` | `src/smallcat/datasets/delta_table_dataset.py` | Aquí va `delete`. `_delta_storage_options()` ya resuelve las credenciales; rechazo de Databricks ya hecho en cada método |
| `_validated_where` | `src/smallcat/datasets/base_dataset.py` (~L413) | Devuelve `None` si el predicado es vacío: **tú debes exigir que no lo sea** |
| `EntryBase.save_polars`/`load_polars`... | `src/smallcat/catalog.py` (L120-232) | Patrón: `self.build_dataset().<método>(self.location, ...)` |
| `DeltaTableEntry.build_dataset` | `catalog.py` (~L333) | Devuelve `DeltaTableDataset` |
| `Catalog.save_polars` | `catalog.py` (~L572) | Patrón: `_get_entry(key)` → `entry.<método>` |
| `ModelWriteMode` | `src/smallcat/models/enums.py` | `APPEND`, `OVERWRITE` y `MERGE` (de #10) |
| `unsupported_reason` | `src/smallcat/models/meta.py` (~L222) | Devuelve `None` para Delta |
| `entry_for` | `src/smallcat/models/registry.py` | Inyecta `mode` solo para Delta (trampa de §4) |
| `ModelManager`, `ModelQuery` | `src/smallcat/models/query.py` | `ModelQuery` es inmutable; tiene `filters` (Polars), `where` (SQL) y `columns` |
| `ModelManager.save` | `query.py` (~L303) | Patrón de comprobación: modo permitido → `unsupported_reason` → ejecutar |

API de `deltalake` 1.6.6 (verificada ejecutándola):

```python
DeltaTable.delete(predicate=None, writer_properties=None, ..., commit_properties=None,
                  post_commithook_properties=None) -> dict
# {'num_added_files': 1, 'num_removed_files': 1, 'num_deleted_rows': 1,
#  'num_copied_rows': 2, 'execution_time_ms': ..., ...}
TableMerger.when_not_matched_by_source_delete(predicate=None) -> TableMerger
```

Comprobado: `delete("id = 2 AND n = 'b'")` borra 1 fila y sube la versión; `delete("id = 99")` devuelve `num_deleted_rows: 0` sin error; una columna inexistente lanza `DeltaError` ("No field named ..."). **`delete(None)` borra todas las filas**: de ahí la exigencia de predicado no vacío.

Nota de dialecto: `_validated_where` valida con el parser de **DuckDB**, pero `DeltaTable.delete` ejecuta el predicado con **DataFusion**. Son casi idénticos para predicados simples; prueba comparaciones, `IN`, `LIKE`, `IS NULL`, fechas y cadenas con comillas, y documenta las diferencias que encuentres.

## 6. Plan de implementación

0. **Preparación**
   - Confirma que #10 está mergeado (§2). Rama `feat/SC-036-delta-delete` en `git worktree`.
   - El árbol principal tiene cambios del usuario (`pyproject.toml` staged, `requirements.txt` y `AGENTS.md` modificados, `docs/issues.md` y `pdm.lock` sin trackear): **no los commitees ni los toques**. `docs/issues.md` se edita en el árbol principal, no en el worktree. Para mergear en `main`, aparta esos ficheros con `git stash push -- <ficheros>` y restáuralos con `pop --index`, tras guardar un backup del diff.
   - Tests desde el worktree: usa `/e/Repositorios/NRG_GH/.dev/smallcat/.venv/Scripts/python.exe` con `PYTHONPATH=src`; el venv editable apunta al `src` del árbol principal y, si no, probarás el código equivocado.
   - Marca SC-036/#32 como "en curso" en `docs/issues.md`.
1. **Dataset** (`delta_table_dataset.py`): `delete(path, where) -> int`. Orden: rechazar Databricks → exigir predicado no vacío → `_validated_where` → abrir `DeltaTable` con `_delta_storage_options()` → `dt.delete(where)` → devolver `num_deleted_rows`. Errores de `deltalake` (columna inexistente, tabla inexistente) con mensaje comprensible.
2. **Catálogo** (`catalog.py`): `EntryBase.delete(where)` que lanza `NotImplementedError` ("solo `delta_table`"); `DeltaTableEntry.delete` delega en el dataset; `Catalog.delete(key, where)` con `_get_entry`.
3. **Merge** (sobre lo implementado en #10): `when_not_matched_by_source_delete` (y, si se decide, `delete_predicate`) con validación Pydantic, protección contra origen vacío y documentación del aviso.
4. **Modelos**
   - `enums.py`: `ModelWriteMode.DELETE`.
   - `meta.py`: `unsupported_reason` rechaza `delete` fuera de Delta.
   - `query.py`: el método de borrado (forma según §8), con comprobación de modo permitido (`ModelWriteNotAllowedError`), de formato, y rechazo de `filter`/`select` previos.
   - `registry.py`: `entry_for` sin `mode` para el borrado (trampa de §4).
5. **Tests**
   - `tests/test_delta_delete.py`, local con `tmp_path` y el fixture `local_conn`: borra las filas que cumplen; devuelve el recuento; 0 filas coincidentes → 0 sin error; predicado vacío/`None`/espacios → `ValueError`; **inyección** (`;`, subconsultas, `UNION`, comentarios) → `ValueError`; columna inexistente; tabla inexistente; Databricks; el borrado es lógico (la versión anterior sigue legible con `version=`); formato no Delta.
   - Opcionalmente contra S3 simulado con `moto`, como en `tests/test_aws_connection.py` (revisar cómo se monta allí).
   - Catálogo: `catalog.delete` y entradas no Delta.
   - Modelos: modo no permitido, modelo de solo lectura, formato no Delta, `filter`/`select` previos rechazados.
   - Merge: `when_not_matched_by_source_delete` borra lo no emparejado; origen vacío rechazado; no se admite sin `merge`.
6. **Documentación**: ver tabla siguiente.
7. **Verificación**
   - Suite completa. **Línea base en la máquina Windows del usuario: 14 fallos conocidos y ajenos** (10 por rutas `file://C:\...` — SC-022; 3 por no poder descargar la extensión `excel` de DuckDB; 1 por Airflow en Windows). No debe aparecer ninguno nuevo.
   - `ruff` y `mypy`. `python -m mkdocs build` aborta hoy por un aviso previo (`docs/issues.md` fuera del `nav`); comprueba que **no aparece ningún aviso nuevo** de tu cambio.
8. **Cierre**: commit con `Closes #32` (o `Refs #32`), sin `Co-Authored-By`; merge `--no-ff` a `main`; actualizar `docs/issues.md` (estado, tareas, cambios). Push a `fork` y cierre en GitHub solo si el usuario lo pide.

### Documentación a actualizar

| Fichero | Qué |
|---|---|
| `docs/catalog-reference.md` | `Catalog.delete(key, where)` con ejemplo; la nota de que **el borrado es lógico hasta `VACUUM`** (los ficheros siguen en el almacenamiento y el time travel los ve); el dialecto del predicado; `when_not_matched_by_source_delete` en la sección de merge, con aviso |
| `docs/models.md` | `ModelWriteMode.DELETE` en la tabla; cómo se borra con el ORM; qué errores da (modo no permitido, formato no Delta, `filter`/`select`); `allowed_write_modes=[]` protege del borrado |
| `README.md` | Un ejemplo corto de borrado |
| `docs/examples/models.md`, `models_local.py` | Ejemplo ejecutable de borrado (`tests/test_models_example.py` ya prueba los ejemplos) |
| `docs/api/datasets.md`, `docs/api/models.md`, `docs/api/catalog.md` | Comprobar que los métodos nuevos salen en la referencia |
| Docstrings | `enums.py`, `meta.py`, `query.py`, `delta_table_dataset.py` (cabecera del módulo), `catalog.py` |
| `docs/issues.md` | SC-036: estado, tareas y cambios; referencias cruzadas con #10 y #12 |

## 7. Criterios de aceptación

- [ ] `ds.delete(path, where)` y `catalog.delete(key, where)` borran solo las filas que cumplen y devuelven su número.
- [ ] Predicado vacío, `None` o solo espacios → `ValueError`; la tabla nunca se vacía por error.
- [ ] Predicados con `;`, subconsultas, `UNION` o comentarios se rechazan antes de ejecutar nada.
- [ ] Sobre un formato que no es Delta → `NotImplementedError`; Databricks → `NotImplementedError` con su mensaje.
- [ ] El ORM respeta `allowed_write_modes`: un modelo de solo lectura no puede borrar.
- [ ] `when_not_matched_by_source_delete` está desactivada por defecto, exige `mode: merge` y se niega a actuar con un origen vacío.
- [ ] La documentación avisa de que el borrado es lógico hasta `VACUUM` (#12).
- [ ] Suite sin fallos nuevos respecto a la línea base; `ruff` y `mypy` en verde; ningún aviso nuevo de `mkdocs`.
- [ ] Documentación de §6 actualizada.

## 8. Decisiones abiertas (pregúntalas al usuario antes de implementar)

1. **Forma del borrado en el ORM:** `Customer.objects.where("id = 3").delete()` (encadenado con la consulta, consistente con la lectura) frente a `Customer.objects.delete(where="id = 3")` (explícito y sin posibilidad de mezclar `filter`/`select`). Con #33 (`where` con lookups) la primera es la más natural y es la que recomiendo ahora; la segunda evita estados inválidos pero rompe la simetría con la lectura.
2. **Permiso:** `ModelWriteMode.DELETE` dentro de `allowed_write_modes` (recomendado) frente a un permiso propio. Por defecto, `allowed_write_modes` permite **todos** los modos: un modelo Delta existente podría borrar sin haberlo pedido. Valora excluir `DELETE` del valor por defecto y exigirlo explícitamente, por ser destructivo.
3. **Vaciar la tabla:** si hace falta, ¿se permite con un argumento explícito (p. ej. `delete_all=True`) o se deja fuera de alcance? Recomendación: fuera de alcance.
4. **`delete_predicate` en el merge:** ¿acotar el borrado por origen con un predicado opcional, o `when_not_matched_by_source_delete` borra todo lo no emparejado? Recomendación: añadir el predicado opcional.
5. **Pruebas contra S3 simulado (`moto`):** el issue las menciona; confirma que se quieren en este issue.
