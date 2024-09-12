"""Установите openapi-spec-validator во временном окружении и запустите этот скрипт."""
from openapi_spec_validator import validate
from openapi_spec_validator.readers import read_from_filename

spec, _ = read_from_filename("docs/openapi.yaml")
validate(spec)
print("OpenAPI: PASS")
