# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from db.connection import Base, db
from db.models._legacy import *
from db.models.company_group_registry import *
from db.models.company_registry import *
from db.models.company_registry_assertions import *
from db.models.company_registry_links import *
from db.models.custom_import import *
from db.models.custom_import_storage import *
from db.models.facility_address_contribution import *
from db.models.formulary_fhir import *
from db.models.formulary_fhir_admission import *
from db.models.hospital_price import *
from db.models.hospital_price_facts import *
from db.models.hospital_price_header import *
from db.models.manual_directory_registry import *
from db.models.network_membership_draft import *
from db.models.network_registry import *
from db.models.network_serving import *
from db.models.provider_directory_cms_npd_resource_witness import *
from db.models.provider_directory_entity_identity import *
from db.models.provider_directory_entity_redirect import *
from db.models.provider_directory_insurance_network_identity import *
from db.models.provider_directory_mrf_payer_binding import *
from db.models.provider_directory_resource_identity import *
from db.models.provider_directory_rooted_graph import *
from db.models.provider_directory_rooted_graph_publication import *
from db.models.provider_directory_rooted_graph_twin import *
from db.models.provider_directory_uhc_flex import *
from db.models.provider_directory_uhc_flex_practitioner import *
from db.models.provider_directory_uhc_flex_practitioner_publication import *
from db.models.provider_directory_uhc_flex_practitioner_twin import *
from db.models.provider_profile import *
from db.models.ptg_snapshot_local import (
    PTG2GroupTaxIdentitySource as PTG2GroupTaxIdentitySource,
)
from db.models.ptg_snapshot_local import (
    PTG2ProviderGroupTaxIdentity as PTG2ProviderGroupTaxIdentity,
)
from db.models.ptg_snapshot_local import (
    PTG2ProviderTaxIdentity as PTG2ProviderTaxIdentity,
)
from db.models.ptg_snapshot_local import (
    PTG2ProviderTaxIdentityManifest as PTG2ProviderTaxIdentityManifest,
)
from db.models.ptg_snapshot_local import (
    PTG2TaxIdentitySourceBinding as PTG2TaxIdentitySourceBinding,
)
from db.models.ptg_snapshot_local import (
    PTG2TaxIdentitySourceManifest as PTG2TaxIdentitySourceManifest,
)
from db.models.ptg_snapshot_local import (
    PTG2V4InferredTaxonomyCandidate as PTG2V4InferredTaxonomyCandidate,
)
from db.models.ptg_snapshot_local import (
    PTG2V4NPIPrefix as PTG2V4NPIPrefix,
)
from db.models.ptg_snapshot_local import (
    PTG2V4ProviderGraphDiagnostic as PTG2V4ProviderGraphDiagnostic,
)
from db.models.registry_approval import *
from db.models.registry_evidence import *
from db.models.registry_network_binding import *
from db.models.registry_publication_request import *
from db.models.registry_revision import *
from db.models.registry_site_binding import *
from db.models.system import *
