CREATE INDEX ON npis (npi);

CREATE INDEX ON npi_licenses (npi_id);
CREATE INDEX ON npis (business_location_id);
CREATE INDEX ON npis (practice_location_id);
CREATE INDEX ON npis (organization_official_id);
CREATE INDEX ON npi_other_identifiers (npi_id);
CREATE INDEX ON npis (parent_orgs_id);
CREATE INDEX ON npi_taxonomy_groups (npi_id);
