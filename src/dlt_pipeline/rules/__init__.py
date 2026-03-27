from .claims import get_claims_rules
from .labels import get_labels_rules
from .policies import get_policies_rules
from .integrity import get_integrity_rules
from .derived import get_temporal_rules
from .bodyshops import get_bodyshop_rules


def load_all_rules():

    return (
        get_claims_rules()
        + get_labels_rules()
        + get_policies_rules()
        + get_integrity_rules()
        + get_temporal_rules()
        + get_bodyshop_rules()
    )