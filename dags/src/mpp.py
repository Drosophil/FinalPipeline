from billiard.pool import Pool, cpu_count
import re
from time import time, ctime

import numpy as np
import pandas as pd
from rdkit import Chem
from rdkit.Chem import AllChem

from src.fp_log_config import logger


class MoleculeProcessingException(Exception):
    '''Exception class'''
    pass

class MolecularPropertiesProcessor:
    '''Molecular data processing utility
    input parameters: dataframe, hyperthreading
    if hyperthreading=True then computes on cpu_count()//2 cores, to compensate the hyperthreading on intel CPUs
    (otherwise half of processes will simply wait for the available physical core slowing down the computation).
    NB: Output file overwrites any existing file with the same name.
    '''
    def __init__(
            self,
            df: pd.DataFrame,
            target_col: str,
            mol_name_col: str,
            hyperthreading=True,
    ):
        self.start_time = time()
        current_time = ctime()
        logger.info(f"< NEW COMPUTATION RUN >  {current_time}")
        self.hyperthreading = hyperthreading
        self.mols_df = df
        self.target_col = self._column_finder("^" + target_col + "$")
        self.mol_name_col = self._column_finder("^"+mol_name_col+"$")

    def _column_finder(self, match_str):
        matcher = re.compile(match_str, re.IGNORECASE)
        column_to_find = next(filter(matcher.match, self.mols_df.columns))
        if not column_to_find:
            logger.error(f"DATA: No {match_str} column found in a dataframe")
            raise MoleculeProcessingException(f"DATA: No {match_str} column found in a dataframe")
        return column_to_find

    def _prepare_data(self):
        logger.info("Preparing data")
        logger.info("dropping duplicates")
        self.mols_df.drop_duplicates(subset=self.mol_name_col, inplace=True)

    def _compute_molecule_properties_chunk(
            self,
            chunk_df: pd.DataFrame,
            chunk_id: int,
    ) -> pd.DataFrame:
        """ Compute molecule properties for chunk dataframe """

        logger.info(f"PROCESS: process for chunk {chunk_id} started")
        #  logger.info("PROCESS: started migration")

        start_time = time()

        def get_mol_from_smiles_and_verify(input_s):
            '''generating mol object from smiles'''
            output_mol = AllChem.MolFromSmiles(input_s)
            if output_mol:
                return output_mol
            else:
                logger.warning(f"DATA: chunk {chunk_id} -> bad SMILES {input_s}")
                return None

        chunk_df["mol"] = chunk_df[self.target_col].apply(lambda s: get_mol_from_smiles_and_verify(s))

        logger.info("PROCESS: Migrated to molecules")
        #  drop None should only be done by "mol" column

        chunk_df.dropna(subset=["mol"], inplace=True)  # drop rows with None values in Mol column
        # chunk_df = chunk_df[chunk_df['mol'].notna()]

        morgan_fp_gen = Chem.rdFingerprintGenerator.GetMorganGenerator(includeChirality=True, radius=2, fpSize=2048)

        def compute_morgan_fingerprints_from_mols(morgan_generator, mol_obj):
            '''computing Morgan fingerprints from mol objects and converting to strings'''
            output_fingerprint = morgan_generator.GetFingerprint(mol_obj)
            if output_fingerprint:
                return output_fingerprint.ToBase64()  # convert to string
            else:
                logger.warning(f"Morgan fingers: cannot compute Morgan fgps.")
                return None

        chunk_df['morgan_fingerprint'] = chunk_df['mol'].apply(
            lambda mol: compute_morgan_fingerprints_from_mols(morgan_fp_gen, mol))
        chunk_df = chunk_df.drop([self.target_col, 'mol'], axis=1)

        logger.info("PROCESS: Morgan fingerprints computed.")

        stop_time = time() - start_time

        logger.info(f"PROCESS: Process finished chunk {chunk_id} in {stop_time} seconds")

        return chunk_df

    def _compute_molecule_properties(self) -> pd.DataFrame:
        """
        Compute Morgan fingerprints using RDKit
        in chunks
        """
        start_time = time()
        logger.info("Entering _compute_molecule_properties()")
        # const_size_of_chunks = 5
        max_amount_of_p = 1  #  cpu_count()  # with cpu_count() pool is failing on reaching the process response timeout
        if self.hyperthreading:
            max_amount_of_p //= 2  # to avoid hyperthreading
        logger.info(f"Max CPUs = {max_amount_of_p}")

        amount_of_chunk_df = max_amount_of_p  # one core left for the main process, it seems faster like that

        if amount_of_chunk_df > max_amount_of_p:
            amount_of_chunk_df = max_amount_of_p
        elif amount_of_chunk_df == 0:
            amount_of_chunk_df = 1

        logger.info("numpy array split")

        list_of_chunks = np.array_split(self.mols_df, amount_of_chunk_df)

        logger.info("setting the pool")

        with Pool(processes=amount_of_chunk_df) as pool:
            p_df = pool.starmap(self._compute_molecule_properties_chunk,
                            [(list_of_chunks[number-1], number) for number in range(1, amount_of_chunk_df + 1)])

        logger.info(f"Pool has finished")

        result = pd.concat(p_df)
        logger.info(f"Main compute function worked {time() - start_time} seconds")
        return result

    def process_data(self) -> pd.DataFrame:
        '''
        main processing func
        '''
        self._prepare_data()

        mol_properties_df = self._compute_molecule_properties()
        #  mol_properties_df.to_csv(self.output_file_name)
        stop_time = time()
        logger.info(f"PROCESS: wall execution time: {stop_time - self.start_time} sec.")

        return mol_properties_df
