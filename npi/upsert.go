package npi

import (
	"fmt"
	"github.com/gocodo/bloomdb"
	"github.com/gocodo/bloomnpi/csvHeaderReader"
	"io"
	"strconv"
	"sync"
)

type tableDesc struct {
	name    string
	channel chan []string
	columns []string
	parentId string
}

var typeCodes = map[string]string {
	"1": "individual",
	"2": "organization",
}

var otherOrgCodes = map[string]string {
	"1": "individual",
  "2": "individual",
  "3": "organization",
  "4": "organization",
  "5": "individual+organization",
}

var lastNameCodes = map[string]string {
	"1": "former name",
  "2": "professional name",
  "3": "doing business as",
  "4": "former legal business name",
  "5": "other name",
}

var deactReasons = map[string]string {
	"DT": "death",
  "DB": "disbandment",
  "FR": "fraud",
  "OT": "other",
}

var genders = map[string]string {
	"M": "male",
	"F": "female",
}

var taxonomySwitches = map[string]string {
  "X": "not answered",
  "Y": "yes",
  "N": "no",
}

var otherIdentifierCodes = map[string]string {
  "01": "other",
  "02": "medicare upin",
  "04": "medicare id-type unspecified",
  "05": "medicaid",
  "06": "medicare oscar/certification",
  "07": "medicare nsc",
  "08": "medicare pin",
}

var soleProprietorCodes = map[string]string {
  "X": "not answered",
  "Y": "yes",
  "N": "no",
}

var organizationSubpartCodes = map[string]string {
  "X": "not answered",
  "Y": "yes",
  "N": "no",
}

func Upsert(file io.ReadCloser, file_id string, withSync bool) {
	var wg sync.WaitGroup

	npis := make(chan []string, 100)
	npi_licenses := make(chan []string, 100)
	npi_locations := make(chan []string, 100)
	npi_organization_officials := make(chan []string, 100)
	npi_other_identifiers := make(chan []string, 100)
	npi_parent_orgs := make(chan []string, 100)
	npi_taxonomy_groups := make(chan []string, 100)

	wg.Add(1)
	go func() {
		defer wg.Done()
		reader := csvHeaderReader.NewReader(file)

		records_count := 0

		for {
			row, err := reader.Read()
			if err == io.EOF {
				break
			} else if err != nil {
				fmt.Println("Error: ", err)
				return
			}

			records_count += 1
			if records_count % 10000 == 0 {
				fmt.Println("Processed", records_count, "records")
			}

			npi_id := bloomdb.MakeKey(row.Value("NPI"))
			npi_revision := bloomdb.MakeKey(row.Value("NPI"), row.Value("Last Update Date"), row.Value("NPI Deactivation Date"))

			// Locations
			var business_location_id, practice_location_id string

			business_zip := row.Value("Provider Business Mailing Address Postal Code")
			if business_zip != "" {
				business_address := row.Value("Provider First Line Business Mailing Address")
				business_details := row.Value("Provider Second Line Business Mailing Address")
				business_city := row.Value("Provider Business Mailing Address City Name")
				business_state := row.Value("Provider Business Mailing Address State Name")
				business_country := row.Value("Provider Business Mailing Address Country Code (If outside U.S.)")
				business_phone := row.Value("Provider Business Mailing Address Telephone Number")
				business_fax := row.Value("Provider Business Mailing Address Fax Number")

				business_location_id = bloomdb.MakeKey(business_address, business_details, business_city, business_state, business_zip, business_country, business_phone, business_fax)

				business_location := make([]string, 10)
				business_location[0] = business_location_id
				business_location[1] = business_address
				business_location[2] = business_details
				business_location[3] = business_city
				business_location[4] = business_state
				business_location[5] = business_zip
				business_location[6] = business_country
				business_location[7] = business_phone
				business_location[8] = business_fax
				business_location[9] = business_location_id

				npi_locations <- business_location
			}

			practice_zip := row.Value("Provider Business Practice Location Address Postal Code")
			if practice_zip != "" {
				practice_address := row.Value("Provider First Line Business Practice Location Address")
				practice_details := row.Value("Provider Second Line Business Practice Location Address")
				practice_city := row.Value("Provider Business Practice Location Address City Name")
				practice_state := row.Value("Provider Business Practice Location Address State Name")
				practice_country := row.Value("Provider Business Practice Location Address Country Code (If outside U.S.)")
				practice_phone := row.Value("Provider Business Practice Location Address Telephone Number")
				practice_fax := row.Value("Provider Business Practice Location Address Fax Number")

				practice_location_id = bloomdb.MakeKey(practice_address, practice_details, practice_city, practice_state, practice_zip, practice_country, practice_phone, practice_fax)

				practice_location := make([]string, 10)
				practice_location[0] = practice_location_id
				practice_location[1] = practice_address
				practice_location[2] = practice_details
				practice_location[3] = practice_city
				practice_location[4] = practice_state
				practice_location[5] = practice_zip
				practice_location[6] = practice_country
				practice_location[7] = practice_phone
				practice_location[8] = practice_fax
				practice_location[9] = practice_location_id

				npi_locations <- practice_location
			}

			// Organization Officials
			var organization_official_id string

			official_last_name := row.Value("Authorized Official Last Name")
			if official_last_name != "" {
				first_name := row.Value("Authorized Official First Name")
				middle_name := row.Value("Authorized Official Middle Name")
				title := row.Value("Authorized Official Title or Position")
				telephone_number := row.Value("Authorized Official Telephone Number")
				name_prefix := row.Value("Authorized Official Name Prefix Text")
				name_suffix := row.Value("Authorized Official Name Suffix Text")
				credential := row.Value("Authorized Official Credential Text")

				organization_official_id = bloomdb.MakeKey(official_last_name, first_name, middle_name, title, telephone_number, name_prefix, name_suffix, credential)

				organization_official := make([]string, 10)
				organization_official[0] = organization_official_id
				organization_official[1] = official_last_name
				organization_official[2] = first_name
				organization_official[3] = middle_name
				organization_official[4] = title
				organization_official[5] = telephone_number
				organization_official[6] = name_prefix
				organization_official[7] = name_suffix
				organization_official[8] = credential
				organization_official[9] = organization_official_id

				npi_organization_officials <- organization_official
			}

			// Other Identifiers
			for i := 1; i <= 50; i++ {
				identifier := row.Value("Other Provider Identifier_" + strconv.Itoa(i))
				if identifier != "" {
					var idType string
					var ok bool
					if idType, ok = otherIdentifierCodes[row.Value("Other Provider Identifier Type Code_" + strconv.Itoa(i))]; ok != true {
						idType = ""
					}

					state := row.Value("Other Provider Identifier State_" + strconv.Itoa(i))
					issuer := row.Value("Other Provider Identifier Issuer_" + strconv.Itoa(i))

					id := bloomdb.MakeKey(npi_id, identifier, idType, state, issuer)

					other_identifier := make([]string, 7)
					other_identifier[0] = id
					other_identifier[1] = npi_id
					other_identifier[2] = identifier
					other_identifier[3] = idType
					other_identifier[4] = state
					other_identifier[5] = issuer
					other_identifier[6] = id

					npi_other_identifiers <- other_identifier
				}
			}

			// Licenses
			for i := 1; i <= 15; i++ {
				var (
					ok bool
					taxonomySwitch string
				)
				if taxonomySwitch, ok = taxonomySwitches[row.Value("Healthcare Provider Primary Taxonomy Switch_" + strconv.Itoa(i))]; ok != true {
					taxonomySwitch = ""
				}

				licenseState := row.Value("Provider License Number State Code_" + strconv.Itoa(i))
				taxonomy_code := row.Value("Healthcare Provider Taxonomy Code_" + strconv.Itoa(i))
				license_number := row.Value("Provider License Number_" + strconv.Itoa(i))

				if taxonomy_code != "" {
					npi_license_values := make([]string, 7)

					id := bloomdb.MakeKey(npi_id, taxonomy_code, license_number, licenseState, taxonomySwitch)

					npi_license_values[0] = id
					npi_license_values[1] = npi_id
					npi_license_values[2] = taxonomy_code
					npi_license_values[3] = license_number
					npi_license_values[4] = licenseState
					npi_license_values[5] = taxonomySwitch
					npi_license_values[6] = id

					npi_licenses <- npi_license_values
				}
			}

			// Parent Organizations
			var parent_orgs_id string

			parent_business_name := row.Value("Parent Organization LBN")
			if parent_business_name != "" {
				tax_identification_number := row.Value("Parent Organization TIN")

				parent_orgs_id = bloomdb.MakeKey(parent_business_name, tax_identification_number)

				parent_org := make([]string, 4)
				parent_org[0] = parent_orgs_id
				parent_org[1] = parent_business_name
				parent_org[2] = tax_identification_number
				parent_org[3] = parent_orgs_id

				npi_parent_orgs <- parent_org
			}

			// Taxonomy Groups
			for i := 1; i <= 15; i++ {
				taxonomy := row.Value("Healthcare Provider Taxonomy Group_" + strconv.Itoa(i))

				if taxonomy != "" {
					taxonomy_id := bloomdb.MakeKey(npi_id, taxonomy)

					taxonomy_group := make([]string, 4)
					taxonomy_group[0] = taxonomy_id
					taxonomy_group[1] = npi_id
					taxonomy_group[2] = taxonomy
					taxonomy_group[3] = taxonomy_id

					npi_taxonomy_groups <- taxonomy_group
				}
			}

			var entity_type string
			var ok bool
			if entity_type, ok = typeCodes[row.Value("Entity Type Code")]; ok != true {
				entity_type = ""
			}

			var otherOrgType string
			if otherOrgType, ok = otherOrgCodes[row.Value("Provider Other Organization Name Type Code")]; ok != true {
				otherOrgType = ""
			}

			var lastNameType string
			if lastNameType, ok = lastNameCodes[row.Value("Provider Other Last Name Type Code")]; ok != true {
				lastNameType = ""
			}

			var deactReason string
			if deactReason, ok = deactReasons[row.Value("NPI Deactivation Reason Code")]; ok != true {
				deactReason = ""
			}

			var gender string
			if gender, ok = genders[row.Value("Provider Gender Code")]; ok != true {
				gender = ""
			}

			var soleProprietor string
			if soleProprietor, ok = soleProprietorCodes[row.Value("Is Sole Proprietor")]; ok != true {
				soleProprietor = ""
			}

			var subpart string
			if subpart, ok = organizationSubpartCodes[row.Value("Is Organization Subpart")]; ok != true {
				subpart = ""
			}

			npi_values := make([]string, 34)
			npi_values[0] = npi_id
			npi_values[1] = row.Value("NPI")
			npi_values[2] = entity_type
			npi_values[3] = row.Value("Replacement NPI")
			npi_values[4] = row.Value("Employer Identification Number (EIN)")
			npi_values[5] = row.Value("Provider Organization Name (Legal Business Name)")
			npi_values[6] = row.Value("Provider Last Name (Legal Name)")
			npi_values[7] = row.Value("Provider First Name")
			npi_values[8] = row.Value("Provider Middle Name")
			npi_values[9] = row.Value("Provider Name Prefix Text")
			npi_values[10] = row.Value("Provider Name Suffix Text")
			npi_values[11] = row.Value("Provider Credential Text")
			npi_values[12] = row.Value("Provider Other Organization Name")
			npi_values[13] = otherOrgType
			npi_values[14] = row.Value("Provider Other Last Name")
			npi_values[15] = row.Value("Provider Other First Name")
			npi_values[16] = row.Value("Provider Other Middle Name")
			npi_values[17] = row.Value("Provider Other Name Prefix Text")
			npi_values[18] = row.Value("Provider Other Name Suffix Text")
			npi_values[19] = row.Value("Provider Other Credential Text")
			npi_values[20] = lastNameType
			npi_values[21] = row.Value("Provider Enumeration Date")
			npi_values[22] = row.Value("Last Update Date")
			npi_values[23] = deactReason
			npi_values[24] = row.Value("NPI Deactivation Date")
			npi_values[25] = row.Value("NPI Reactivation Date")
			npi_values[26] = gender
			npi_values[27] = soleProprietor
			npi_values[28] = subpart
			npi_values[29] = organization_official_id
			npi_values[30] = parent_orgs_id
			npi_values[31] = business_location_id
			npi_values[32] = practice_location_id
			npi_values[33] = npi_revision

			npis <- npi_values
		}

		fmt.Println("Processed", records_count, "records")

		close(npis)
		close(npi_licenses)
		close(npi_locations)
		close(npi_organization_officials)
		close(npi_other_identifiers)
		close(npi_parent_orgs)
		close(npi_taxonomy_groups)
	}()

	dests := []tableDesc{
		tableDesc{
			name:    "usgov_hhs_npis",
			channel: npis,
			columns: []string{
				"id",
				"npi",
				"type",
				"replacement_npi",
				"employer_identification_number",
				"business_name",
				"last_name",
				"first_name",
				"middle_name",
				"name_prefix",
				"name_suffix",
				"credential",
				"other_name",
				"other_name_type",
				"other_last_name",
				"other_first_name",
				"other_middle_name",
				"other_name_prefix",
				"other_name_suffix",
				"other_credential",
				"other_last_name_type",
				"enumeration_date",
				"last_update_date",
				"deactivation_reason",
				"deactivation_date",
				"reactivation_date",
				"gender",
				"sole_proprietor",
				"organization_subpart",
				"organization_official_id",
				"parent_orgs_id",
				"business_location_id",
				"practice_location_id",
				"revision",
			},
		},
		tableDesc{
			name:    "usgov_hhs_npi_licenses",
			channel: npi_licenses,
			parentId: "npi_id",
			columns: []string{
				"id",
				"npi_id",
				"healthcare_taxonomy_code",
				"license_number",
				"license_number_state",
				"taxonomy_switch",
				"revision",
			},
		},
		tableDesc{
			name:    "usgov_hhs_npi_locations",
			channel: npi_locations,
			columns: []string{
				"id",
				"address_line",
				"address_details_line",
				"city",
				"state",
				"zip",
				"country_code",
				"phone",
				"fax",
				"revision",
			},
		},
		tableDesc{
			name:    "usgov_hhs_npi_organization_officials",
			channel: npi_organization_officials,
			columns: []string{
				"id",
				"last_name",
				"first_name",
				"middle_name",
				"title",
				"phone",
				"name_prefix",
				"name_suffix",
				"credential",
				"revision",
			},
		},
		tableDesc{
			name:    "usgov_hhs_npi_other_identifiers",
			channel: npi_other_identifiers,
			parentId: "npi_id",
			columns: []string{
				"id",
				"npi_id",
				"identifier",
				"type",
				"state",
				"issuer",
				"revision",
			},
		},
		tableDesc{
			name:    "usgov_hhs_npi_parent_orgs",
			channel: npi_parent_orgs,
			columns: []string{
				"id",
				"business_name",
				"tax_identification_number",
				"revision",
			},
		},
		tableDesc{
			name:    "usgov_hhs_npi_taxonomy_groups",
			channel: npi_taxonomy_groups,
			parentId: "npi_id",
			columns: []string{
				"id",
				"npi_id",
				"taxonomy",
				"revision",
			},
		},
	}

	bdb := bloomdb.CreateDB()
	for _, dest := range dests {
		wg.Add(1)
		go func(dest tableDesc) {
			defer wg.Done()

			db, err := bdb.SqlConnection()
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			
			if withSync {
				err = bloomdb.Sync(db, dest.name, dest.columns, dest.channel)
				if err != nil {
					fmt.Println("Error:", err)
					return
				}
			} else {
				err = bloomdb.Upsert(db, dest.name, dest.columns, dest.channel, dest.parentId)
				if err != nil {
					fmt.Println("Error:", err)
					return
				}
			}
		}(dest)
	}

	wg.Wait()
}
