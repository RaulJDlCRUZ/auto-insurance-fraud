"""
Synthetic Auto Insurance Fraud Dataset Generator
=================================================
Generates realistic claims data for insurance fraud detection with:

  context/policies.csv
      One record per policy (policyholder + vehicle).
      ~300,000 unique policies active across 2015-2025.
      Includes policy_updated_at for point-in-time joins.

  events/YYYY/MM/claims.json
      ~50,000 claims per month (one record per accident report).
      Label: is_fraud (1 = fraudulent/inflated claim, ~5% positive rate).
      Training : 2015-01 to 2024-12  (6,000,000 claims over 10 years)
      Production: 2025-01 to 2025-06 (300,000 claims)

BIAS DESIGN (domain-justified, documented in insurance literature):
-------------------------------------------------------------------
  age_group:
    - Group size disparity: ~55% adult (25-70), ~30% young (<25), ~15% senior (>70)
      Young over-represented in high-risk pool; seniors over-represented in organised fraud
    - Prevalence disparity:
        young  (<25)  : ~2.0x base fraud rate  (inflated repairs, staged accidents)
        senior (>70)  : ~1.8x base fraud rate  (used as fronts in organised fraud rings)
        adult  (25-70): ~0.75x base fraud rate (most stable segment)

  region:
    - Group size disparity: urban ~60%, suburban ~25%, rural ~15%
    - Prevalence disparity:
        urban    : ~1.6x base fraud rate (denser traffic, more staged accidents)
        suburban : ~1.0x base fraud rate
        rural    : ~0.6x base fraud rate (lower opportunity, stronger social deterrence)

  vehicle_age_group:
    - Prevalence disparity:
        old   (>10 years): ~1.7x (easier to inflate value, more total-loss fraud)
        mid   (4-10 yrs) : ~1.0x
        new   (<4 years) : ~0.8x (harder to inflate known market value)

  Gender: NO multiplier. Any observed correlation emerges naturally from
  the interaction between gender, age and vehicle type distributions.

2025 DRIFTS:
  Data drift (from 2025-01):
    - Digital claims channel surges: new "app_selfservice" channel appears
    - Electric vehicles (EV) appear as vehicle_type (new category)
    - Repair cost inflation: amounts shift upward ~12% vs. 2024 trend

  Concept drift (from 2025-03):
    - New organised fraud pattern: staged accidents involving EVs at charging stations
      (EV + charging_station_location + low_mileage → high fraud rate)
    - Fraud ring operates through a new intermediary body shop network:
      previously trusted body shops now associated with inflated claims
"""

import os
import json
import random
import math
import csv
import calendar
import hashlib
from datetime import datetime, timedelta, date

# =============================================================================
# DELAYED FEEDBACK HELPERS
# =============================================================================

def _gamma_delay(shape: float, scale: float, lo: int, hi: int) -> int:
    """Sample delay in days from a Gamma distribution, clamped to [lo, hi]."""
    return max(lo, min(hi, int(random.gammavariate(shape, scale))))

def _label_available_date(event_ts: str, delay_days: float = 0,
                           delay_hours: float = 0) -> str:
    """Return ISO timestamp when the label becomes available."""
    from datetime import datetime, timedelta
    dt = datetime.strptime(event_ts, "%Y-%m-%dT%H:%M:%SZ")
    dt += timedelta(days=delay_days, hours=delay_hours)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


# --- Reproducibility ----------------------------------------------------------
SEED = 42
random.seed(SEED)

# --- Volume -------------------------------------------------------------------
POLICIES_N       = 300_000
CLAIMS_PER_MONTH = 50_000

# --- Date range ---------------------------------------------------------------
START_YEAR, START_MONTH = 2015, 1
END_YEAR,   END_MONTH   = 2025, 6

# --- Base fraud rate ----------------------------------------------------------
BASE_FRAUD_RATE = 0.050


# =============================================================================
# FRAUD RATE SCHEDULE
# =============================================================================

def fraud_rate(year: int, month: int) -> float:
    """
    ~5% baseline with:
    - Seasonal peaks (summer: more driving, more accidents/fraud)
    - Slow upward trend (fraud rings grow more sophisticated)
    - 2025 spike: new organised fraud pattern
    """
    base     = BASE_FRAUD_RATE
    seasonal = 0.005 * math.sin(2 * math.pi * (month - 4) / 12)  # peaks July
    trend    = 0.0002 * ((year - 2015) * 12 + (month - 1)) / 12  # slow drift
    drift    = 0.012 if (year == 2025 and month >= 3) else 0.0
    return max(0.025, min(0.12, base + seasonal + trend + drift))


# =============================================================================
# BIAS CONFIG
# =============================================================================

# --- Age group ----------------------------------------------------------------
AGE_GROUP_WEIGHTS = {"young": 0.30, "adult": 0.55, "senior": 0.15}
AGE_FRAUD_MULT    = {"young": 2.00, "adult": 0.75, "senior": 1.80}

# --- Region -------------------------------------------------------------------
REGIONS_URBAN    = ["madrid", "barcelona", "valencia", "sevilla", "bilbao",
                    "malaga", "zaragoza", "murcia"]
REGIONS_SUBURBAN = ["alcala_henares", "getafe", "hospitalet", "badalona",
                    "alicante", "cordoba", "valladolid", "vigo"]
REGIONS_RURAL    = ["cuenca", "soria", "teruel", "zamora", "palencia",
                    "huesca", "avila", "segovia", "guadalajara", "lugo"]
REGION_TYPE_W    = {"urban": 0.60, "suburban": 0.25, "rural": 0.15}
REGION_FRAUD_MULT= {"urban": 1.60, "suburban": 1.00, "rural": 0.60}

# --- Vehicle age group --------------------------------------------------------
VEH_AGE_FRAUD_MULT = {"new": 0.80, "mid": 1.00, "old": 1.70}


def get_fraud_multiplier(age_group, region_type, veh_age_group):
    return (AGE_FRAUD_MULT[age_group] *
            REGION_FRAUD_MULT[region_type] *
            VEH_AGE_FRAUD_MULT[veh_age_group])


# =============================================================================
# HELPERS
# =============================================================================

def _clamp(v, lo, hi): return max(lo, min(hi, v))

def _gauss(mu, sigma, lo=None, hi=None):
    v = random.gauss(mu, sigma)
    if lo is not None: v = max(v, lo)
    if hi is not None: v = min(v, hi)
    return v

def _weighted_choice(options, weights):
    total = sum(weights)
    r, cum = random.random() * total, 0.0
    for o, w in zip(options, weights):
        cum += w
        if r <= cum: return o
    return options[-1]

def _choice(seq): return seq[int(random.random() * len(seq))]

def _fmt_date(d): return d.strftime("%Y-%m-%d")
def _fmt_ts(d):   return d.strftime("%Y-%m-%dT%H:%M:%SZ")

def _rand_date(start, end):
    delta = (end - start).days
    return start + timedelta(days=int(random.random() * delta))

def _hash_id(prefix, n):
    h = hashlib.md5(f"{prefix}{n}{SEED}".encode()).hexdigest()[:12].upper()
    return f"{prefix}{h}"

def _iter_months(sy, sm, ey, em):
    y, m = sy, sm
    while (y, m) <= (ey, em):
        yield y, m
        m += 1
        if m > 12: m, y = 1, y + 1


# =============================================================================
# POLICY GENERATION
# =============================================================================

VEHICLE_MAKES   = ["seat", "volkswagen", "renault", "ford", "opel",
                   "toyota", "bmw", "mercedes", "peugeot", "hyundai", "kia"]
VEHICLE_TYPES   = ["sedan", "suv", "hatchback", "van", "truck", "motorcycle"]
VEH_TYPE_W      = [28, 25, 22, 10, 8, 7]

COVERAGE_TYPES  = ["third_party", "third_party_plus", "comprehensive"]
COVERAGE_W      = [35, 30, 35]

PAYMENT_FREQ    = ["annual", "semi_annual", "quarterly", "monthly"]
PAYMENT_W       = [40, 20, 20, 20]

OCCUPATIONS     = ["employed", "self_employed", "retired", "student",
                   "unemployed", "public_servant"]
OCC_W           = [45, 15, 15, 10, 8, 7]

BODY_SHOPS      = [_hash_id("SHOP", i) for i in range(200)]
# A subset of body shops will become fraud-associated in 2025
FRAUD_SHOPS_2025 = BODY_SHOPS[150:175]   # 25 shops become compromised


def _sample_age_group():
    r = random.random()
    if r < AGE_GROUP_WEIGHTS["young"]:
        return int(_gauss(21, 3, 18, 24)), "young"
    if r < AGE_GROUP_WEIGHTS["young"] + AGE_GROUP_WEIGHTS["adult"]:
        return int(_gauss(42, 12, 25, 70)), "adult"
    return int(_gauss(74, 5, 71, 90)), "senior"

def _sample_region():
    r = random.random()
    if r < REGION_TYPE_W["urban"]:
        return _choice(REGIONS_URBAN), "urban"
    if r < REGION_TYPE_W["urban"] + REGION_TYPE_W["suburban"]:
        return _choice(REGIONS_SUBURBAN), "suburban"
    return _choice(REGIONS_RURAL), "rural"

def _sample_vehicle(age_group):
    make = _choice(VEHICLE_MAKES)
    vtype = _weighted_choice(VEHICLE_TYPES, VEH_TYPE_W)

    # Vehicle age: young drivers more likely to have older cars
    if age_group == "young":
        veh_year = int(_gauss(2010, 4, 2000, 2024))
    elif age_group == "senior":
        veh_year = int(_gauss(2013, 5, 2000, 2024))
    else:
        veh_year = int(_gauss(2016, 5, 2000, 2024))

    current_year = 2025
    veh_age = current_year - veh_year
    if veh_age < 4:
        veh_age_group = "new"
    elif veh_age <= 10:
        veh_age_group = "mid"
    else:
        veh_age_group = "old"

    # Market value: correlated with age and type
    base_value = {"sedan": 18000, "suv": 28000, "hatchback": 14000,
                  "van": 22000, "truck": 35000, "motorcycle": 8000}[vtype]
    depreciation = max(0.15, 1 - 0.08 * veh_age)
    market_value = round(_gauss(base_value * depreciation, base_value * 0.08, 500, 120000), 0)

    # Annual mileage: correlated with vehicle type and age
    annual_mileage = int(_gauss(
        18000 if vtype in ("van", "truck") else 12000,
        4000, 1000, 60000
    ))

    return {
        "vehicle_make":      make,
        "vehicle_type":      vtype,
        "vehicle_year":      veh_year,
        "vehicle_age_group": veh_age_group,
        "vehicle_value_eur": market_value,
        "annual_mileage_km": annual_mileage,
        "is_electric":       1 if (random.random() < 0.03 and veh_year >= 2020) else 0,
    }


def generate_policies(n: int) -> list[dict]:
    print(f"  Generating {n:,} policies...")
    policies   = []
    pol_start  = date(2010, 1, 1)
    pol_end    = date(2024, 12, 31)

    for i in range(n):
        pol_id  = _hash_id("POL", i)
        age, age_group = _sample_age_group()
        region, region_type = _sample_region()
        veh = _sample_vehicle(age_group)

        gender    = _weighted_choice(["M", "F"], [54, 46])
        occupation = _weighted_choice(OCCUPATIONS, OCC_W)
        coverage  = _weighted_choice(COVERAGE_TYPES, COVERAGE_W)
        payment   = _weighted_choice(PAYMENT_FREQ, PAYMENT_W)

        # Premium: correlated with age, vehicle value, coverage, region
        base_premium = veh["vehicle_value_eur"] * 0.04
        if age_group == "young":   base_premium *= 1.6
        if age_group == "senior":  base_premium *= 1.2
        if region_type == "urban": base_premium *= 1.15
        if coverage == "comprehensive": base_premium *= 1.4
        if coverage == "third_party":   base_premium *= 0.7
        annual_premium = round(_gauss(base_premium, base_premium * 0.1, 150, 8000), 2)

        # Bonus-malus: years without claim — correlates negatively with fraud
        bonus_malus = int(_gauss(5, 3, 0, 20))

        # Policy start date and update timestamp
        pol_date   = _rand_date(pol_start, pol_end)
        max_upd    = (date(2025, 6, 30) - pol_date).days
        updated_at = pol_date + timedelta(days=int(random.random() * max_upd))
        updated_ts = datetime(updated_at.year, updated_at.month, updated_at.day,
                              int(random.random() * 23), int(random.random() * 59))

        # Latent fraud propensity
        mult = get_fraud_multiplier(age_group, region_type, veh["vehicle_age_group"])
        fraud_propensity = _clamp(
            random.betavariate(2, max(2, int(8 / (mult * 0.5 + 0.5)))), 0.0, 1.0
        )

        policies.append({
            # Identifiers
            "policy_id":              pol_id,
            "policy_updated_at":      _fmt_ts(updated_ts),   # point-in-time key

            # Protected attributes (bias carriers)
            # NOTE: age_group → Gold: CASE WHEN policyholder_age < 25 THEN 'young' ...
            "policyholder_age":       age,
            "gender":                 gender,
            "region":                 region,
            "region_type":            region_type,   # kept: requires region→type reference table

            # Policyholder profile
            "occupation":             occupation,
            "policy_start_date":      _fmt_date(pol_date),
            "coverage_type":          coverage,
            "payment_frequency":      payment,
            "annual_premium_eur":     annual_premium,
            "bonus_malus_years":      bonus_malus,
            "has_telematics":         1 if random.random() < 0.25 else 0,
            "multi_policy":           1 if random.random() < 0.35 else 0,

            # Vehicle attributes
            "vehicle_make":           veh["vehicle_make"],
            "vehicle_type":           veh["vehicle_type"],
            "vehicle_year":           veh["vehicle_year"],
            # NOTE: vehicle_age_group → Gold: CASE WHEN (YEAR(current_date) - vehicle_year) < 4 ...
            "vehicle_value_eur":      veh["vehicle_value_eur"],
            "annual_mileage_km":      veh["annual_mileage_km"],
            "is_electric":            veh["is_electric"],

            # Internal
            "_fraud_propensity":      round(fraud_propensity, 6),
            "_age_group":             age_group,
            "_region_type":           region_type,
            "_veh_age_group":         veh["vehicle_age_group"],
            "_policy_start":          pol_date,
            "_is_electric":           veh["is_electric"],
        })

        if (i + 1) % 100_000 == 0:
            print(f"    {i+1:,} / {n:,}")

    return policies


# =============================================================================
# CLAIM GENERATION
# =============================================================================

ACCIDENT_TYPES  = ["rear_end", "side_collision", "single_vehicle", "theft",
                   "vandalism", "hail_damage", "parking", "animal_collision",
                   "hit_and_run", "rollover"]
ACC_W_LEGIT     = [25, 20, 15, 10, 8,  7, 8, 4, 2, 1]
ACC_W_FRAUD     = [30, 18,  8, 20, 6,  2, 5, 2, 8, 1]  # theft/hit-and-run more common in fraud

CLAIM_CHANNELS  = ["agent", "phone", "online", "app_selfservice"]
CHAN_W_PRE2025  = [35, 40, 20, 5]
CHAN_W_2025     = [20, 30, 20, 30]   # data drift: app channel surges

LOCATIONS       = ["highway", "city_center", "residential", "parking_lot",
                   "rural_road", "charging_station"]

WITNESSES       = ["none", "one", "multiple"]
WIT_W_LEGIT     = [20, 40, 40]
WIT_W_FRAUD     = [55, 30, 15]   # fraud often has no witnesses

INJURY_LEVELS   = ["none", "minor", "moderate", "severe"]
INJ_W_LEGIT     = [60, 25, 12, 3]
INJ_W_FRAUD     = [20, 45, 28, 7]   # whiplash/soft tissue claims inflated in fraud


def _claim_amount(accident_type, vehicle_value, coverage_type, is_fraud,
                  year, injury_level):
    """Claim amount: fraud inflates repair costs and injury compensation."""
    base_repair = {
        "rear_end":        (800,  3500),
        "side_collision":  (1200, 5000),
        "single_vehicle":  (600,  4000),
        "theft":           (vehicle_value * 0.7, vehicle_value * 1.05),
        "vandalism":       (300,  2000),
        "hail_damage":     (500,  3000),
        "parking":         (200,  1500),
        "animal_collision":(400,  2500),
        "hit_and_run":     (800,  4000),
        "rollover":        (3000, vehicle_value * 0.9),
    }
    lo, hi = base_repair.get(accident_type, (500, 3000))

    # Inflation trend year over year
    inflation = 1 + 0.03 * (year - 2015)
    # 2025 extra inflation
    if year == 2025:
        inflation *= 1.12

    if is_fraud:
        # Inflated: 30-90% above legitimate range
        inflation_mult = _gauss(1.55, 0.25, 1.20, 2.50)
        amount = _gauss((lo + hi) / 2 * inflation_mult, (hi - lo) * 0.3, lo * 0.8, hi * 3)
    else:
        amount = _gauss((lo + hi) / 2, (hi - lo) / 4, lo * 0.5, hi * 1.1)

    # Injury compensation added on top
    injury_comp = {"none": 0, "minor": _gauss(800, 300, 200, 2000),
                   "moderate": _gauss(4000, 1500, 1000, 12000),
                   "severe": _gauss(18000, 8000, 5000, 60000)}[injury_level]
    if is_fraud:
        injury_comp *= _gauss(1.4, 0.3, 1.0, 2.5)

    total = round((amount + injury_comp) * inflation, 2)
    return max(50.0, total)


def _days_to_report(is_fraud):
    """Fraud claims are sometimes reported very quickly (staged) or with delay."""
    if is_fraud:
        if random.random() < 0.3:
            return int(_gauss(1, 0.5, 0, 2))    # immediate — staged accident
        return int(_gauss(15, 10, 1, 60))         # delayed — organising the fraud
    return int(_gauss(4, 3, 0, 30))


def generate_claim(
    claim_index:  int,
    year:         int,
    month:        int,
    policy:       dict,
    is_fraud:     bool,
    is_2025:      bool,
) -> dict:

    days_in_month = calendar.monthrange(year, month)[1]
    day    = int(random.random() * days_in_month) + 1
    hour   = int(_gauss(11, 4, 0, 23))
    ts     = datetime(year, month, day, hour,
                      int(random.random() * 60), int(random.random() * 60))

    age_group    = policy["_age_group"]
    region_type  = policy["_region_type"]
    is_electric  = policy["_is_electric"]

    # Accident type
    acc_type = _weighted_choice(ACCIDENT_TYPES,
                                ACC_W_FRAUD if is_fraud else ACC_W_LEGIT)

    # Claim channel — data drift in 2025
    channel = _weighted_choice(CLAIM_CHANNELS,
                               CHAN_W_2025 if is_2025 else CHAN_W_PRE2025)

    # Location
    if is_2025 and is_electric and is_fraud and month >= 3:
        location = "charging_station"   # concept drift: EV fraud pattern
    else:
        location = _weighted_choice(LOCATIONS[:5],
                                    [25, 30, 20, 18, 7] if region_type == "urban"
                                    else [20, 10, 25, 15, 30])

    # Witnesses
    witnesses = _weighted_choice(WITNESSES,
                                 WIT_W_FRAUD if is_fraud else WIT_W_LEGIT)

    # Injury
    injury = _weighted_choice(INJURY_LEVELS,
                              INJ_W_FRAUD if is_fraud else INJ_W_LEGIT)

    # Body shop
    if is_fraud and is_2025 and month >= 3 and random.random() < 0.45:
        body_shop_id = _choice(FRAUD_SHOPS_2025)   # concept drift: compromised shops
    else:
        body_shop_id = _choice(BODY_SHOPS[:150])

    # Claim amount
    amount = _claim_amount(
        acc_type, policy["vehicle_value_eur"],
        policy["coverage_type"], is_fraud, year, injury
    )

    # Days to report
    days_report = _days_to_report(is_fraud)

    # Number of parties involved
    n_parties = 1 if acc_type in ("theft", "vandalism", "hail_damage", "single_vehicle") else (
        int(_gauss(2, 0.5, 2, 5)) if not is_fraud else int(_gauss(2.5, 1.0, 2, 6))
    )

    # Prior claims velocity: fraud policyholders tend to cluster claims
    prior_claims_12m = int(abs(_gauss(
        2.5 if is_fraud else 0.3, 1.5 if is_fraud else 0.5, 0, 8
    )))

    # Third-party involvement: fraud often has suspicious third parties
    has_third_party_injury = 1 if (injury != "none" and n_parties > 1) else 0
    third_party_same_insurer = (
        1 if (has_third_party_injury and random.random() < (0.35 if is_fraud else 0.08))
        else 0
    )

    # Police report: fraud often skips it or files it suspiciously fast
    police_report = 1 if (
        acc_type in ("theft", "hit_and_run", "rollover") or random.random() < 0.55
    ) else 0
    if is_fraud and random.random() < 0.25:
        police_report = 0   # fraud avoids police involvement

    # Claim filed outside business hours (more common in genuine accidents)
    outside_hours = 1 if hour < 8 or hour > 20 else 0

    # Telematics inconsistency: if insured has telematics, fraud may show GPS anomalies
    telematics_anomaly = 0
    if policy["has_telematics"] and is_fraud:
        telematics_anomaly = 1 if random.random() < 0.60 else 0

    return {
        "claim_id":                   _hash_id("CLM", claim_index),
        "policy_id":                  policy["policy_id"],
        "timestamp":                  _fmt_ts(ts),
        # NOTE: year_month removed → Gold: date_format(timestamp, 'yyyy-MM')
        # Accident details
        "accident_type":              acc_type,
        "accident_location_type":     location,
        "days_to_report":             days_report,   # kept: no separate accident_date field to derive from
        "n_parties_involved":         n_parties,
        "witnesses":                  witnesses,
        "injury_level":               injury,
        "police_report_filed":        police_report,
        "outside_business_hours":     outside_hours,
        # Claim financials
        "claimed_amount_eur":         amount,
        "body_shop_id":               body_shop_id,
        # Third-party signals
        "has_third_party_injury":     has_third_party_injury,
        "third_party_same_insurer":   third_party_same_insurer,
        "telematics_anomaly":         telematics_anomaly,   # kept: no raw telematics stream to derive from
        # Channel
        "claim_channel":              channel,
        # NOTE: prior_claims_12m removed → Gold: COUNT(claim_id) OVER
        #   (PARTITION BY policy_id ORDER BY timestamp RANGE 365 DAYS PRECEDING)
        # NOTE: coverage_type, vehicle_age_group, region_type, bonus_malus_years,
        #   annual_premium_eur, vehicle_value_eur, is_electric removed from claim event
        #   → Gold: point-in-time JOIN with policies.csv on policy_id + policy_updated_at
        # Internal field for concept-drift logic (not exported)
        "_is_electric":               is_electric,
        # Label
        "is_fraud":                   1 if is_fraud else 0,
    }


# =============================================================================
# 2025 CONCEPT DRIFT
# =============================================================================

def apply_2025_concept_drift(claim: dict, policy: dict, month: int) -> dict:
    """
    Concept drift from 2025-03:
    EV + charging_station + low mileage → new fraud ring pattern.
    Certain body shops now systematically inflate claims.
    """
    if month < 3:
        return claim

    # EV staged accident at charging station
    if (claim["_is_electric"] == 1 and
            claim["accident_location_type"] == "charging_station" and
            policy["annual_mileage_km"] < 8000):
        if random.random() < 0.40 and claim["is_fraud"] == 0:
            claim["is_fraud"] = 1
            claim["claimed_amount_eur"] = round(
                claim["claimed_amount_eur"] * _gauss(1.6, 0.2, 1.2, 2.5), 2
            )

    # Compromised body shop network
    if claim["body_shop_id"] in FRAUD_SHOPS_2025:
        if random.random() < 0.30 and claim["is_fraud"] == 0:
            claim["is_fraud"] = 1
            claim["claimed_amount_eur"] = round(
                claim["claimed_amount_eur"] * _gauss(1.45, 0.15, 1.1, 2.0), 2
            )

    return claim


# =============================================================================
# MAIN
# =============================================================================

def main():
    base_dir    = os.path.dirname(os.path.abspath(__file__))
    events_dir        = os.path.join(base_dir, "events")
    source_buffer_dir = os.path.join(base_dir, "source_buffer")
    context_dir = os.path.join(base_dir, "context")
    os.makedirs(context_dir, exist_ok=True)

    # -- 1. Policies -----------------------------------------------------------
    print("=" * 65)
    print("STEP 1: Generating policies")
    print("=" * 65)
    policies = generate_policies(POLICIES_N)

    pol_path      = os.path.join(context_dir, "policies.csv")
    export_fields = [k for k in policies[0].keys() if not k.startswith("_")]
    print(f"  Writing {pol_path} ...")
    with open(pol_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=export_fields)
        writer.writeheader()
        for p in policies:
            writer.writerow({k: p[k] for k in export_fields})

    n_young  = sum(1 for p in policies if p["_age_group"] == "young")
    n_senior = sum(1 for p in policies if p["_age_group"] == "senior")
    n_urban  = sum(1 for p in policies if p["_region_type"] == "urban")
    n_old_v  = sum(1 for p in policies if p["_veh_age_group"] == "old")
    print(f"\n  [BIAS] age_group  : young={n_young:,} ({n_young/POLICIES_N:.1%}) | "
          f"senior={n_senior:,} ({n_senior/POLICIES_N:.1%})")
    print(f"  [BIAS] region_type: urban={n_urban:,} ({n_urban/POLICIES_N:.1%})")
    print(f"  [BIAS] vehicle_age : old(>10y)={n_old_v:,} ({n_old_v/POLICIES_N:.1%})\n")

    # -- 2. Claims -------------------------------------------------------------
    print("=" * 65)
    print("STEP 2: Generating claims (2015-2025)")
    print("=" * 65)

    total_claims = total_fraud = 0
    claim_index  = 0

    for year, month in _iter_months(START_YEAR, START_MONTH, END_YEAR, END_MONTH):
        is_2025  = (year == 2025)
        base_out = source_buffer_dir if year == 2025 else events_dir
        fr       = fraud_rate(year, month)

        # Only policies active before this month can file claims
        cutoff    = date(year, month, 1)
        eligible = [p for p in policies if p["_policy_start"] < cutoff]
        if not eligible:
            raise ValueError(
                f"No eligible policies for {year}-{month:02d}. "
                f"Check that policy start dates precede the data window."
            )

        claims      = []
        month_fraud = 0

        for _ in range(CLAIMS_PER_MONTH):
            policy  = eligible[int(random.random() * len(eligible))]
            mult    = get_fraud_multiplier(
                policy["_age_group"], policy["_region_type"], policy["_veh_age_group"]
            )
            prop    = policy["_fraud_propensity"]
            eff_rate = _clamp(fr * (0.5 + mult * 0.5) * (0.6 + prop * 0.8), 0.005, 0.25)
            is_fraud = random.random() < eff_rate

            claim = generate_claim(claim_index, year, month, policy, is_fraud, is_2025)

            if is_2025:
                claim = apply_2025_concept_drift(claim, policy, month)

            if claim["is_fraud"]:
                month_fraud += 1

            claims.append(claim)
            claim_index += 1

        total_claims += len(claims)
        total_fraud  += month_fraud

        drift_label = ""
        if is_2025:
            parts = ["app_channel_drift"]
            if month >= 3: parts.append("EV_fraud_ring")
            drift_label = f"  [{', '.join(parts)}]"

        print(f"  [{year}-{month:02d}] fraud_rate={fr:.3%}  "
              f"eligible_policies={len(eligible):,}  "
              f"fraud={month_fraud:,} ({month_fraud/len(claims):.2%}){drift_label}")

        event_dir = os.path.join(base_out, "claims", str(year), f"{month:02d}")
        label_dir = os.path.join(base_out, "labels", str(year), f"{month:02d}")
        os.makedirs(event_dir, exist_ok=True)
        os.makedirs(label_dir, exist_ok=True)
        event_path = os.path.join(event_dir, "data.json")
        label_path = os.path.join(label_dir, "data.json")
        claims.sort(key=lambda x: x["timestamp"])
        with open(event_path, "w", encoding="utf-8") as f:
            for c in claims:
                row = {k: v for k, v in c.items() if k != "is_fraud" and not k.startswith("_")}
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
        label_records = [{
            "claim_id": c["claim_id"],
            "is_fraud": c["is_fraud"],
            "label_available_date": _label_available_date(
                c["timestamp"], delay_days=_gamma_delay(3, 12, 15, 90))
        } for c in claims]
        label_records.sort(key=lambda x: x["label_available_date"])
        with open(label_path, "w", encoding="utf-8") as f:
            for rec in label_records:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    # -- Summary ---------------------------------------------------------------
    n_months = sum(1 for _ in _iter_months(START_YEAR, START_MONTH, END_YEAR, END_MONTH))
    print("\n" + "=" * 65)
    print("SUMMARY")
    print("=" * 65)
    print(f"  Policies           : {POLICIES_N:,}")
    print(f"  Total months       : {n_months}")
    print(f"  Total claims       : {total_claims:,}")
    print(f"  Overall fraud rate : {total_fraud/total_claims:.3%}")
    print()
    print("  Bias (domain-justified):")
    print("    Prevalence disparity -> age_group  : young x2.0, senior x1.8, adult x0.75")
    print("    Prevalence disparity -> region_type: urban x1.6, rural x0.6")
    print("    Prevalence disparity -> vehicle_age: old x1.7, new x0.8")
    print("    Separability (organic): telematics_anomaly, witnesses, days_to_report,")
    print("      prior_claims_12m — stronger signals for urban/young segments")
    print()
    print("  2025 drifts:")
    print("    Data drift    -> claim_channel: app_selfservice surges")
    print("                  -> is_electric: EV claims appear")
    print("                  -> claimed_amount: +12% inflation")
    print("    Concept drift -> EV + charging_station + low mileage = new fraud pattern")
    print("                  -> 25 compromised body shops inflate claims systematically")
    print()
    print("  Output:")
    print("    context/policies.csv")
    print("    events/YYYY/MM/claims.json")
    print("\nDone!")


if __name__ == "__main__":
    main()
