create or replace view average_similarity_per_source (
SOURCE_MOLECULE,
AVERAGE_SIMILARITY_SCORE
) as
select sdm.chembl_id as source_molecule, avg(sf.tanimoto_similarity) as average_similarity_score
from silver_fact sf
join silver_dim_molecules sdm on sf.source_molregno = sdm.molregno
group by sdm.chembl_id;

create or replace view average_deviation_of_similar_from_source_alogp (
SOURCE_MOLECULE,
AVERAGE_DEVIATION_OF_SIMILAR_FROM_SOURCE
) as
select sdm2.chembl_id as source_molecule, avg(abs(sdm2.alogp - sdm.alogp))
from silver_fact sf
join silver_dim_molecules sdm on sf.target_molregno = sdm.molregno
join silver_dim_molecules sdm2 on sf.source_molregno = sdm2.molregno
group by sdm2.chembl_id;

create or replace view average_deviation_of_similar_from_source_alogp_no_Nulls (
SOURCE_MOLECULE,
AVERAGE_DEVIATION_OF_SIMILAR_FROM_SOURCE
) as
select sdm2.chembl_id as source_molecule, avg(abs(sdm2.alogp - sdm.alogp))
from silver_fact sf
join silver_dim_molecules sdm on sf.target_molregno = sdm.molregno
join silver_dim_molecules sdm2 on sf.source_molregno = sdm2.molregno
where (sdm.alogp is not null) and (sdm2.alogp is not null)
group by sdm2.chembl_id;