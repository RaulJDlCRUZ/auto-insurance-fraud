# Data Quality Rules — Silver Layer

Este documento define las reglas de calidad de datos para la capa **Silver** dentro de la arquitectura medallón del pipeline antifraude. Estas reglas están diseñadas para:

* garantizar consistencia semántica
* asegurar integridad referencial
* preparar datasets para modelado ML
* permitir migración directa futura a Delta Live Tables (DLT)

Ubicación sugerida en repo:

```
src/dlt_pipeline/rules/
```

---

# 1. Reglas para Claims

Archivo sugerido:

```
src/dlt_pipeline/rules/claims_rules.yml
```

## Campos obligatorios

```yaml
not_null:
  - claim_id
  - policy_id
  - timestamp
  - claimed_amount_eur
  - days_to_report
```

## Validaciones de dominio

```yaml
domain_constraints:
  claimed_amount_eur: "> 0"
  days_to_report: ">= 0"
  n_parties_involved: ">= 1"
```

## Integridad temporal

```yaml
temporal_rules:
  - "timestamp >= policy_start_date"
  - "label_available_date >= timestamp"
  - "vehicle_year <= year(timestamp)"
```

## Consistencia telematics

```yaml
conditional_rules:
  - condition: telematics_anomaly == 1
    requires: has_telematics == 1
```

## Consistencia injury vs parties

```yaml
conditional_rules:
  - condition: injury_level != 'none'
    requires: n_parties_involved >= 2
```

## Validación claim_channel

```yaml
allowed_values:
  claim_channel:
    - phone
    - web
    - agent
    - app_selfservice
```

## Outlier económico (flag analítico, no bloqueo)

```yaml
soft_rules:
  claimed_amount_vs_vehicle_value:
    expression: "claimed_amount_eur <= vehicle_value_eur * 1.5"
    action: flag_only
```

---

# 2. Reglas para Labels

Archivo sugerido:

```
src/dlt_pipeline/rules/labels_rules.yml
```

## Campos obligatorios

```yaml
not_null:
  - claim_id
  - is_fraud
  - label_available_date
```

## Dominio

```yaml
allowed_values:
  is_fraud:
    - 0
    - 1
```

## Consistencia temporal

```yaml
temporal_rules:
  - "label_available_date >= claim_timestamp"
```

---

# 3. Reglas para Policies

Archivo sugerido:

```
src/dlt_pipeline/rules/policies_rules.yml
```

## Campos obligatorios

```yaml
not_null:
  - policy_id
  - policy_start_date
  - policyholder_age
  - annual_premium_eur
  - vehicle_year
```

## Dominio

```yaml
domain_constraints:
  policyholder_age: "BETWEEN 18 AND 100"
  annual_premium_eur: "> 0"
  vehicle_year: "BETWEEN 1980 AND current_year"
  vehicle_value_eur: ">= 0"
  annual_mileage_km: ">= 0"
  bonus_malus_years: ">= 0"
```

## Valores categóricos válidos

```yaml
allowed_values:
  region_type:
    - urban
    - suburban
    - rural
```

---

# 4. Integridad Referencial

Archivo sugerido:

```
src/dlt_pipeline/rules/integrity_rules.yml
```

## Claim → Policy

```yaml
foreign_keys:
  claims.policy_id:
    references: policies.policy_id
    action: quarantine
```

## Label → Claim

```yaml
foreign_keys:
  labels.claim_id:
    references: claims.claim_id
    action: quarantine
```

---

# 5. Reglas temporales derivadas (features Silver)

Archivo sugerido:

```
src/dlt_pipeline/rules/derived_features.yml
```

## Label delay

```yaml
derived_columns:
  label_delay_days:
    expression: "datediff(label_available_date, timestamp)"
```

## Particionado lógico

```yaml
derived_columns:
  event_date:
    expression: "to_date(timestamp)"

  event_year:
    expression: "year(timestamp)"

  event_month:
    expression: "month(timestamp)"

  event_week:
    expression: "weekofyear(timestamp)"
```

---

# 6. Body shops de alto riesgo (concept drift 2025)

Archivo sugerido:

```
src/dlt_pipeline/rules/bodyshop_rules.yml
```

```yaml
reference_tables:
  high_risk_bodyshops:
    join_key: body_shop_id
    output_flag: is_high_risk_body_shop
```

---

# 7. Compatibilidad con Delta Live Tables

Estas reglas están diseñadas para mapear directamente a:

```
dlt.expect()
dlt.expect_or_drop()
dlt.expect_or_fail()
```

Ejemplo futuro en DLT:

```python
@dlt.expect_or_drop("valid_claim_amount", "claimed_amount_eur > 0")
@dlt.expect("valid_days_to_report", "days_to_report >= 0")
def silver_claims():
    return bronze_claims
```

---

# 8. Compatibilidad con ejecución local (PySpark)

Estas reglas YAML pueden cargarse con:

```python
import yaml

with open("src/dlt_pipeline/rules/claims_rules.yml") as f:
    rules = yaml.safe_load(f)
```

Y aplicarse dinámicamente en validaciones Spark antes de escribir tablas Silver.

Esto permite mantener un **único contrato declarativo portable** entre local y Databricks Cloud.

---

```python
from src.dlt_pipeline.rules import load_all_rules

rules = load_all_rules()

for rule in rules:
    print(rule["name"], rule["constraint"])
```
