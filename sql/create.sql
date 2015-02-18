CREATE TABLE usgov_hhs_npis(
id uuid,
revision uuid,
npi bigint,
type character varying(12),
replacement_npi bigint,
employer_identification_number character varying(9),
business_name character varying(70),
last_name character varying(35),
first_name character varying(20),
middle_name character varying(20),
name_prefix character varying(5),
name_suffix character varying(5),
credential character varying(20),
other_name character varying(70),
other_name_type character varying(23),
other_last_name character varying(35),
other_first_name character varying(20),
other_middle_name character varying(20),
other_name_prefix character varying(5),
other_name_suffix character varying(5),
other_credential character varying(20),
other_last_name_type character varying(26),
enumeration_date timestamp,
last_update_date timestamp,
deactivation_reason character varying(12),
deactivation_date timestamp,
reactivation_date timestamp,
gender character varying(6),
sole_proprietor character varying(12),
organization_subpart character varying(12),
organization_official_id uuid,
parent_orgs_id uuid,
business_location_id uuid,
practice_location_id uuid,
bloom_created_at timestamp
);
CREATE TABLE usgov_hhs_npi_licenses(
id uuid,
revision uuid,
npi_id uuid,
healthcare_taxonomy_code character varying(10),
license_number character varying(20),
license_number_state character varying(9),
taxonomy_switch character varying(12),
bloom_created_at timestamp
);
CREATE TABLE usgov_hhs_npi_locations(
id uuid,
revision uuid,
address_line character varying(55),
address_details_line character varying(55),
city character varying(40),
state character varying(40),
zip character varying(20),
country_code character varying(2),
phone character varying(20),
fax character varying(20),
bloom_created_at timestamp
);
CREATE TABLE usgov_hhs_npi_organization_officials(
id uuid,
revision uuid,
last_name character varying(35),
first_name character varying(20),
middle_name character varying(20),
title character varying(35),
phone character varying(20),
name_prefix character varying(5),
name_suffix character varying(5),
credential character varying(20),
bloom_created_at timestamp
);
CREATE TABLE usgov_hhs_npi_other_identifiers(
id uuid,
revision uuid,
npi_id uuid,
identifier character varying(20),
type character varying(28),
state character varying(2),
issuer character varying(80),
bloom_created_at timestamp
);
CREATE TABLE usgov_hhs_npi_parent_orgs(
id uuid,
revision uuid,
business_name character varying(70),
tax_identification_number character varying(9),
bloom_created_at timestamp
);
CREATE TABLE usgov_hhs_npi_taxonomy_groups(
id uuid,
revision uuid,
npi_id uuid,
taxonomy character varying(70),
bloom_created_at timestamp
);
CREATE TABLE usgov_hhs_npis_revisions(
id uuid,
revision uuid,
npi bigint,
type character varying(12),
replacement_npi bigint,
employer_identification_number character varying(9),
business_name character varying(70),
last_name character varying(35),
first_name character varying(20),
middle_name character varying(20),
name_prefix character varying(5),
name_suffix character varying(5),
credential character varying(20),
other_name character varying(70),
other_name_type character varying(23),
other_last_name character varying(35),
other_first_name character varying(20),
other_middle_name character varying(20),
other_name_prefix character varying(5),
other_name_suffix character varying(5),
other_credential character varying(20),
other_last_name_type character varying(26),
enumeration_date timestamp,
last_update_date timestamp,
deactivation_reason character varying(12),
deactivation_date timestamp,
reactivation_date timestamp,
gender character varying(6),
sole_proprietor character varying(12),
organization_subpart character varying(12),
organization_official_id uuid,
parent_orgs_id uuid,
business_location_id uuid,
practice_location_id uuid,
bloom_created_at timestamp,
bloom_updated_at timestamp,
bloom_action character varying(255)
);
CREATE TABLE usgov_hhs_npi_licenses_revisions(
id uuid,
revision uuid,
npi_id uuid,
healthcare_taxonomy_code character varying(10),
license_number character varying(20),
license_number_state character varying(9),
taxonomy_switch character varying(12),
bloom_created_at timestamp,
bloom_updated_at timestamp,
bloom_action character varying(255)
);
CREATE TABLE usgov_hhs_npi_locations_revisions(
id uuid,
revision uuid,
address_line character varying(55),
address_details_line character varying(55),
city character varying(40),
state character varying(40),
zip character varying(20),
country_code character varying(2),
phone character varying(20),
fax character varying(20),
bloom_created_at timestamp,
bloom_updated_at timestamp,
bloom_action character varying(255)
);
CREATE TABLE usgov_hhs_npi_organization_officials_revisions(
id uuid,
revision uuid,
last_name character varying(35),
first_name character varying(20),
middle_name character varying(20),
title character varying(35),
phone character varying(20),
name_prefix character varying(5),
name_suffix character varying(5),
credential character varying(20),
bloom_created_at timestamp,
bloom_updated_at timestamp,
bloom_action character varying(255)
);
CREATE TABLE usgov_hhs_npi_other_identifiers_revisions(
id uuid,
revision uuid,
npi_id uuid,
identifier character varying(20),
type character varying(28),
state character varying(2),
issuer character varying(80),
bloom_created_at timestamp,
bloom_updated_at timestamp,
bloom_action character varying(255)
);
CREATE TABLE usgov_hhs_npi_parent_orgs_revisions(
id uuid,
revision uuid,
business_name character varying(70),
tax_identification_number character varying(9),
bloom_created_at timestamp,
bloom_updated_at timestamp,
bloom_action character varying(255)
);
CREATE TABLE usgov_hhs_npi_taxonomy_groups_revisions(
id uuid,
revision uuid,
npi_id uuid,
taxonomy character varying(70),
bloom_created_at timestamp,
bloom_updated_at timestamp,
bloom_action character varying(255)
);
INSERT INTO source_tables (id, source_id, name) VALUES ('4e2b6681-11be-3051-9755-75a7591acb75', '7e8cd826-94e5-3520-a632-2453a3d13b1d', 'usgov_hhs_npis');
INSERT INTO source_tables (id, source_id, name) VALUES ('476daf64-8ee1-30d1-ab3a-713cbd73b5cb', '7e8cd826-94e5-3520-a632-2453a3d13b1d', 'usgov_hhs_npi_licenses');
INSERT INTO source_tables (id, source_id, name) VALUES ('616cad33-a145-3d3d-ba61-b53ded5f509c', '7e8cd826-94e5-3520-a632-2453a3d13b1d', 'usgov_hhs_npi_locations');
INSERT INTO source_tables (id, source_id, name) VALUES ('2e3e8291-45dd-3a5c-8e30-be77e4685110', '7e8cd826-94e5-3520-a632-2453a3d13b1d', 'usgov_hhs_npi_organization_officials');
INSERT INTO source_tables (id, source_id, name) VALUES ('cf319a09-fd56-39e2-999b-53832a668824', '7e8cd826-94e5-3520-a632-2453a3d13b1d', 'usgov_hhs_npi_other_identifiers');
INSERT INTO source_tables (id, source_id, name) VALUES ('966e71e9-eea8-327a-8ddf-37e67ab70016', '7e8cd826-94e5-3520-a632-2453a3d13b1d', 'usgov_hhs_npi_parent_orgs');
INSERT INTO source_tables (id, source_id, name) VALUES ('0d7bc42a-15c1-36e8-a763-a4fb3f5921db', '7e8cd826-94e5-3520-a632-2453a3d13b1d', 'usgov_hhs_npi_taxonomy_groups');
INSERT INTO sources (id, name) VALUES ('7e8cd826-94e5-3520-a632-2453a3d13b1d', 'usgov.hhs.npi');
CREATE INDEX ON usgov_hhs_npis (id);
CREATE INDEX ON usgov_hhs_npis_revisions (id);
CREATE INDEX ON usgov_hhs_npis (revision);
CREATE INDEX ON usgov_hhs_npis_revisions (revision);
CREATE INDEX ON usgov_hhs_npis (organization_official_id);
CREATE INDEX ON usgov_hhs_npis_revisions (organization_official_id);
CREATE INDEX ON usgov_hhs_npis (parent_orgs_id);
CREATE INDEX ON usgov_hhs_npis_revisions (parent_orgs_id);
CREATE INDEX ON usgov_hhs_npis (business_location_id);
CREATE INDEX ON usgov_hhs_npis_revisions (business_location_id);
CREATE INDEX ON usgov_hhs_npis (practice_location_id);
CREATE INDEX ON usgov_hhs_npis_revisions (practice_location_id);
CREATE INDEX ON usgov_hhs_npis (bloom_created_at);
CREATE INDEX ON usgov_hhs_npis_revisions (bloom_created_at);
CREATE INDEX ON usgov_hhs_npis_revisions (bloom_action);
CREATE INDEX ON usgov_hhs_npis_revisions (bloom_updated_at);
CREATE INDEX ON usgov_hhs_npi_licenses (id);
CREATE INDEX ON usgov_hhs_npi_licenses_revisions (id);
CREATE INDEX ON usgov_hhs_npi_licenses (revision);
CREATE INDEX ON usgov_hhs_npi_licenses_revisions (revision);
CREATE INDEX ON usgov_hhs_npi_licenses (npi_id);
CREATE INDEX ON usgov_hhs_npi_licenses_revisions (npi_id);
CREATE INDEX ON usgov_hhs_npi_licenses (bloom_created_at);
CREATE INDEX ON usgov_hhs_npi_licenses_revisions (bloom_created_at);
CREATE INDEX ON usgov_hhs_npi_licenses_revisions (bloom_action);
CREATE INDEX ON usgov_hhs_npi_licenses_revisions (bloom_updated_at);
CREATE INDEX ON usgov_hhs_npi_locations (id);
CREATE INDEX ON usgov_hhs_npi_locations_revisions (id);
CREATE INDEX ON usgov_hhs_npi_locations (revision);
CREATE INDEX ON usgov_hhs_npi_locations_revisions (revision);
CREATE INDEX ON usgov_hhs_npi_locations (bloom_created_at);
CREATE INDEX ON usgov_hhs_npi_locations_revisions (bloom_created_at);
CREATE INDEX ON usgov_hhs_npi_locations_revisions (bloom_action);
CREATE INDEX ON usgov_hhs_npi_locations_revisions (bloom_updated_at);
CREATE INDEX ON usgov_hhs_npi_organization_officials (id);
CREATE INDEX ON usgov_hhs_npi_organization_officials_revisions (id);
CREATE INDEX ON usgov_hhs_npi_organization_officials (revision);
CREATE INDEX ON usgov_hhs_npi_organization_officials_revisions (revision);
CREATE INDEX ON usgov_hhs_npi_organization_officials (bloom_created_at);
CREATE INDEX ON usgov_hhs_npi_organization_officials_revisions (bloom_created_at);
CREATE INDEX ON usgov_hhs_npi_organization_officials_revisions (bloom_action);
CREATE INDEX ON usgov_hhs_npi_organization_officials_revisions (bloom_updated_at);
CREATE INDEX ON usgov_hhs_npi_other_identifiers (id);
CREATE INDEX ON usgov_hhs_npi_other_identifiers_revisions (id);
CREATE INDEX ON usgov_hhs_npi_other_identifiers (revision);
CREATE INDEX ON usgov_hhs_npi_other_identifiers_revisions (revision);
CREATE INDEX ON usgov_hhs_npi_other_identifiers (npi_id);
CREATE INDEX ON usgov_hhs_npi_other_identifiers_revisions (npi_id);
CREATE INDEX ON usgov_hhs_npi_other_identifiers (bloom_created_at);
CREATE INDEX ON usgov_hhs_npi_other_identifiers_revisions (bloom_created_at);
CREATE INDEX ON usgov_hhs_npi_other_identifiers_revisions (bloom_action);
CREATE INDEX ON usgov_hhs_npi_other_identifiers_revisions (bloom_updated_at);
CREATE INDEX ON usgov_hhs_npi_parent_orgs (id);
CREATE INDEX ON usgov_hhs_npi_parent_orgs_revisions (id);
CREATE INDEX ON usgov_hhs_npi_parent_orgs (revision);
CREATE INDEX ON usgov_hhs_npi_parent_orgs_revisions (revision);
CREATE INDEX ON usgov_hhs_npi_parent_orgs (bloom_created_at);
CREATE INDEX ON usgov_hhs_npi_parent_orgs_revisions (bloom_created_at);
CREATE INDEX ON usgov_hhs_npi_parent_orgs_revisions (bloom_action);
CREATE INDEX ON usgov_hhs_npi_parent_orgs_revisions (bloom_updated_at);
CREATE INDEX ON usgov_hhs_npi_taxonomy_groups (id);
CREATE INDEX ON usgov_hhs_npi_taxonomy_groups_revisions (id);
CREATE INDEX ON usgov_hhs_npi_taxonomy_groups (revision);
CREATE INDEX ON usgov_hhs_npi_taxonomy_groups_revisions (revision);
CREATE INDEX ON usgov_hhs_npi_taxonomy_groups (npi_id);
CREATE INDEX ON usgov_hhs_npi_taxonomy_groups_revisions (npi_id);
CREATE INDEX ON usgov_hhs_npi_taxonomy_groups (bloom_created_at);
CREATE INDEX ON usgov_hhs_npi_taxonomy_groups_revisions (bloom_created_at);
CREATE INDEX ON usgov_hhs_npi_taxonomy_groups_revisions (bloom_action);
CREATE INDEX ON usgov_hhs_npi_taxonomy_groups_revisions (bloom_updated_at);