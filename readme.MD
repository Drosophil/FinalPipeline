At the moment the pipeline is not yet orchestrated.<br>
So, to populate the DB:<br>
1. Set the environment variables<br><br>
AWS_ACCESS_KEY_ID=key<br>
AWS_SECRET_ACCESS_KEY=key<br>
AWS_SESSION_TOKEN=token<br>
BUCKET_NAME=bucket_name<br>
DB_HOST=db_host<br>
DB_NAME=db_name<br>
DB_PASSWORD=db_password<br>
DB_PORT=port<br>
DB_USER=user<br>
S3_FOLDER_NAME=Output_folder1/.../folderN<br>
S3_INPUT_FOLDER=Input_folder/.../folder<br>
<br>
2. Execute the following sequence of functions from the modules:
<br><br>

import db_interact, S3_interact, compute_morgan, tanimoto, datamart
<br><br>
data_load=return_db_object()<br>
check_and_construct_bronze_tables(construct=True)<br>
S3_writer=return_S3_access_object()<br>
compute_source_morgan_fingerprints(data_load, S3_writer)<br>
get_similarities(data_load, S3_writer)<br>
injest_silver_tables(data_load, S3_writer)<br><br><br>

This should construct the following tables in db:<br><br>
bronze_molecule_dictionary<br>
bronze_compound_properties<br>
bronze_compound_structures<br>
bronze_chembl_id_lookup<br>
<br><br>
I used 'bronze_' prefix because I haven't found a way to create subschema in postgres.<br>
Setting up the DB can take a while, up to 3-5 hours depending on the system.<br><br>
Then it reads input files from the input folder, saves file names in <br><br>
used_input_files<br><br>
table in DB for future use, computes morgan fingerprints, saves them to parquet files, loads them back, takes top 10, checks for duplicates and constructs datamart tables:<br><br>
silver_fact<br>
silver_dim_molecules<br><br>

Further work will be done to create views and orchestrate all these functions in airflow.
<br><br>
Notes:
in the input files there was one with [Na+] in SMILES. As a chemist, I took it into consideration, 'cause it's the salt of something and shouldn't be thrown away.<br>
Also while counting has_duplicates_of_last_largest_score flag I used not direct comparison of floats, but <i>np.isclose</i> method with tolerance 10E-9, 'cause direct comparison of floats can be tricky. The flag is an integer that shows exactly how many compounds in the set have that Tanimoto score with that target.<br><br>

Still working on it.<br>
My plans are: <br>
1. write a dag that calls all the functions above (even now, if tables exist and there are no new input files, pipeline will not alter the data in DB)<br>
2. add to that dag operators to create views, <br>
3. move environment variables to docker_compose.yaml,<br>
4. Add some notification like TG bot logger.<br>
5. Think on tests. Maybe integration ones. I tested modules manually, but who knows...<br>
6. clean the code.<br>

Then deploying the pipeline will be more straightforward.<br>
Thank you.