SELECT
        -- Transaction
        individual_contributions.file_year || '::trans::' || individual_contributions.sub_id as _id, -- PKey
        individual_contributions.transaction_dt as "date",
        individual_contributions.transaction_amt::float as amount,
        individual_contributions.transaction_tp as type,
        individual_contributions.memo_text as memo_text,
        individual_contributions.file_year as file_year,

        -- Contributor
        individual_contributions.entity_tp as contributor__entity_type,
        individual_contributions.name as contributor__name,
        individual_contributions.city as contributor__city,
        individual_contributions.state as contributor__state,
        individual_contributions.zip_code as contributor__zip_code,
        individual_contributions.employer as contributor__employer,
        individual_contributions.occupation as contributor__occupation,

        -- Committee
        committee_master.file_year || '::cmte::' || committee_master.cmte_id as committee__id,
        committee_master.cmte_nm as committee__name,
        committee_master.tres_nm as committee__treasurer_name,
        committee_master.cmte_city as committee__city,
        committee_master.cmte_st as committee__state,
        committee_master.cmte_zip as committee__zip_code,
        committee_master.cmte_pty_affiliation as committee__party_affiliation,
        committee_master.org_tp as committee__organization_type,
        committee_master.connected_org_nm as committee__organization_name,

        -- Candidate
        candidate_master.file_year || '::cand::' || candidate_master.cand_id as candidate__id,
        candidate_master.cand_name as candidate__name,
        candidate_master.cand_pty_affiliation as candidate__party_affiliation,
        candidate_master.cand_office as candidate__office,
        candidate_master.cand_ici as candidate__incumbent_challenger_status,
        candidate_master.cand_office_district as candidate__office_district,
        candidate_master.cand_city as candidate__city,
        candidate_master.cand_st as candidate__state,
        candidate_master.cand_zip as candidate__zip

FROM individual_contributions
LEFT JOIN committee_master
USING(cmte_id, file_year)
LEFT JOIN candidate_master
USING(cand_id, file_year)
WHERE individual_contributions.file_year = %(file_year)s
