# https://www.encodeproject.org/search/?type=Experiment&control_type!=*&status=released&perturbed=false&assay_title=TF+ChIP-seq&assay_title=Histone+ChIP-seq&assay_title=ATAC-seq
import dask.dataframe as dd
from snakemake.script import snakemake


def format_index(input_file, output_file):
    df = load_basic(input_file)
    df = df.dropna(axis=1, how="all").reset_index(drop=True)
    df.to_parquet(output_file, index=False)


def load_basic(file_path):
    ddf = (
        dd.read_csv(file_path, sep="\t", dtype=str)
        .query("`Biosample organism` == 'Homo sapiens'")
        .query("`File type` == 'bed'")
    )
    df = ddf.compute()
    return df


format_index(snakemake.input[0], snakemake.output[0])
