# Models

::: smallcat.models.model.Model
    options:
      show_root_heading: true
      members: true

::: smallcat.models.registry.ModelRegistry
    options:
      show_root_heading: true
      members: true

::: smallcat.models.registry.use_registry
    options:
      show_root_heading: true
      members: true

::: smallcat.models.registry.current_registry
    options:
      show_root_heading: true
      members: true

## Reading and writing

::: smallcat.models.query.ModelManager
    options:
      heading_level: 3
      show_root_heading: true
      members: true

::: smallcat.models.query.ModelQuery
    options:
      heading_level: 3
      show_root_heading: true
      members: true

## Configuration

::: smallcat.models.meta.ModelMeta
    options:
      heading_level: 3
      show_root_heading: true
      members: true

::: smallcat.models.enums.ModelFileFormat
    options:
      heading_level: 3
      show_root_heading: true
      members: true

::: smallcat.models.enums.ModelWriteMode
    options:
      heading_level: 3
      show_root_heading: true
      members: true

::: smallcat.models.enums.ModelValidationMode
    options:
      heading_level: 3
      show_root_heading: true
      members: true

## Errors

::: smallcat.models.errors.ModelError
    options:
      heading_level: 3
      show_root_heading: true

::: smallcat.models.errors.ModelDefinitionError
    options:
      heading_level: 3
      show_root_heading: true

::: smallcat.models.errors.ModelRegistrationError
    options:
      heading_level: 3
      show_root_heading: true

::: smallcat.models.errors.ModelNotRegisteredError
    options:
      heading_level: 3
      show_root_heading: true

::: smallcat.models.errors.ModelQueryError
    options:
      heading_level: 3
      show_root_heading: true

::: smallcat.models.errors.ModelWriteNotAllowedError
    options:
      heading_level: 3
      show_root_heading: true

::: smallcat.models.errors.ModelUnsupportedWriteModeError
    options:
      heading_level: 3
      show_root_heading: true

::: smallcat.models.errors.ModelSchemaValidationError
    options:
      heading_level: 3
      show_root_heading: true

::: smallcat.models.errors.SchemaIssue
    options:
      heading_level: 3
      show_root_heading: true

## Lookups

::: smallcat.models.lookups
    options:
      heading_level: 3
      show_root_heading: true
      members:
        - lookups_to_sql
        - kind_of
        - OPERATORS
